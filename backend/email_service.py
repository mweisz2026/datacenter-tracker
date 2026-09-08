"""
Email alert service for DC Sentinel.
Sends emails via Resend when HIGH or CRITICAL news is newly detected.

Required env vars:
  RESEND_API_KEY    — get from resend.com (free tier: 3k emails/month)
  ALERT_EMAIL_TO    — recipient address (default: mweisz@diametercap.com)
  ALERT_EMAIL_FROM  — sender address  (default: onboarding@resend.dev)
                      For production: verify your domain at resend.com and set
                      e.g. "DC Sentinel <alerts@diametercap.com>"
"""
import os
import httpx
from datetime import datetime, timezone

RESEND_API_KEY   = os.getenv("RESEND_API_KEY", "")
# Comma-separated list of recipients. Env var overrides the default entirely,
# so if ALERT_EMAIL_TO is set in Vercel it must include every recipient.
ALERT_EMAIL_TO   = os.getenv("ALERT_EMAIL_TO", "mweisz@diametercap.com,blogigian@diametercap.com")
ALERT_RECIPIENTS = [e.strip() for e in ALERT_EMAIL_TO.split(",") if e.strip()]
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "DC Sentinel <onboarding@resend.dev>")
DASHBOARD_URL    = "https://datacenter-tracker-kappa.vercel.app"


def _fmt_date(pub: str) -> str:
    try:
        dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%b %d, %Y %H:%M UTC")
    except Exception:
        return pub[:10] if pub else ""


def _build_digest(alerts: list) -> tuple[str, str]:
    """
    Build a single digest email covering alerts across all bonds.
    Each alert must have a bond_name field (added by all_alerts endpoint).
    Returns (subject, html_body).
    """
    cats     = [a.get("importance_category", "HIGH") for a in alerts]
    top_cat  = "CRITICAL" if "CRITICAL" in cats else "HIGH"
    count    = len(alerts)
    plural   = "s" if count > 1 else ""

    # Bond names for subject line (deduplicated, max 3)
    bond_names = list(dict.fromkeys(a.get("bond_name", "") for a in alerts if a.get("bond_name")))
    bond_str   = ", ".join(bond_names[:3]) + ("..." if len(bond_names) > 3 else "")
    subject    = f"[DC SENTINEL] {top_cat}: {count} new alert{plural} — {bond_str}"

    rows = ""
    for a in alerts:
        cat       = a.get("importance_category", "HIGH")
        c_main    = "#ef4444" if cat == "CRITICAL" else "#f59e0b"
        c_bg      = "#2d1010" if cat == "CRITICAL" else "#2d1f00"
        reason    = a.get("importance_reason", "")
        pub       = _fmt_date(a.get("published", ""))
        source    = a.get("source", "")
        title     = a.get("title", "")
        url       = a.get("url", "#")
        bond_name = a.get("bond_name", "")

        reason_row = (
            f'<div style="color:#8b949e;font-size:12px;font-style:italic;margin-top:4px;">'
            f'{reason}</div>'
        ) if reason else ""

        bond_badge = (
            f'<span style="background:#1f2937;color:#8b949e;font-size:10px;font-family:monospace;'
            f'padding:1px 6px;border-radius:3px;border:1px solid #30363d;">{bond_name}</span>'
        ) if bond_name else ""

        rows += f"""
        <tr>
          <td style="padding:16px 20px;border-bottom:1px solid #21262d;">
            <div style="margin-bottom:8px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
              <span style="background:{c_bg};color:{c_main};font-family:monospace;font-size:11px;
                           font-weight:700;padding:2px 8px;border-radius:4px;
                           border:1px solid {c_main}60;">{cat}</span>
              {bond_badge}
              <span style="color:#8b949e;font-size:11px;">{source}</span>
              <span style="color:#484f58;font-size:11px;">{pub}</span>
            </div>
            <a href="{url}" style="color:#58a6ff;font-size:14px;font-weight:600;
                                   text-decoration:none;line-height:1.5;display:block;">
              {title}
            </a>
            {reason_row}
          </td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0d1117;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;">
  <div style="max-width:600px;margin:0 auto;padding:24px 12px;">

    <!-- Header -->
    <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;
                padding:18px 24px;margin-bottom:16px;">
      <div style="margin-bottom:6px;">
        <span style="font-size:20px;font-weight:700;color:#f0b429;
                     letter-spacing:-0.5px;font-family:monospace;">DC SENTINEL</span>
        <span style="margin-left:10px;background:#1f2937;color:#8b949e;font-size:10px;
                     font-family:monospace;padding:2px 8px;border-radius:4px;
                     border:1px solid #30363d;vertical-align:middle;">
          BOND INTELLIGENCE
        </span>
      </div>
      <div style="color:#8b949e;font-size:13px;">
        {count} new alert{plural} across {len(bond_names)} bond{"s" if len(bond_names) != 1 else ""}
      </div>
    </div>

    <!-- Alert rows -->
    <div style="background:#161b22;border:1px solid #30363d;
                border-radius:8px;overflow:hidden;">
      <table style="width:100%;border-collapse:collapse;">
        <tbody>{rows}</tbody>
      </table>
    </div>

    <!-- CTA -->
    <div style="text-align:center;margin-top:20px;">
      <a href="{DASHBOARD_URL}"
         style="background:#1f6feb;color:#ffffff;font-size:13px;font-weight:600;
                text-decoration:none;padding:10px 24px;border-radius:6px;
                display:inline-block;">
        Open DC Sentinel Dashboard
      </a>
    </div>

    <!-- Footer -->
    <div style="text-align:center;margin-top:20px;color:#484f58;font-size:11px;
                font-family:monospace;">
      DC Sentinel · Alerts are AI-scored, verify before acting
    </div>

  </div>
</body>
</html>"""

    return subject, html


async def send_digest_email(alerts: list, recipients: list = None, subject_prefix: str = "") -> bool:
    """
    Send one digest email covering all new HIGH/CRITICAL alerts across all bonds.
    alerts must include bond_name on each item.
    recipients defaults to ALERT_RECIPIENTS; subject_prefix lets callers tag e.g. "[TEST] ".
    No-ops silently if RESEND_API_KEY is not set.
    """
    if not RESEND_API_KEY or not alerts:
        return False

    to = recipients if recipients else ALERT_RECIPIENTS
    subject, html = _build_digest(alerts)
    subject = f"{subject_prefix}{subject}"

    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type":  "application/json",
    }

    async def _post(client, to_list):
        return await client.post(
            "https://api.resend.com/emails",
            headers=headers,
            json={"from": ALERT_EMAIL_FROM, "to": to_list, "subject": subject, "html": html},
        )

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await _post(client, to)
            if r.status_code in (200, 201):
                print(f"[email] Digest sent: {len(alerts)} alert(s) → {', '.join(to)}")
                return True

            # Batch send failed. This commonly happens when one recipient is not
            # permitted by Resend (e.g. the default onboarding@resend.dev sender
            # can only email the account owner until a domain is verified) — and
            # Resend rejects the WHOLE send. Fall back to per-recipient so one bad
            # address can't blackout the others.
            print(f"[email] Batch send failed {r.status_code}: {r.text[:200]} — retrying per-recipient")
            ok_any = False
            for addr in to:
                rr = await _post(client, [addr])
                if rr.status_code in (200, 201):
                    ok_any = True
                    print(f"[email] sent → {addr}")
                else:
                    print(f"[email] FAILED → {addr}: {rr.status_code} {rr.text[:150]}")
            return ok_any
    except Exception as e:
        print(f"[email] Failed to send alert email: {e}")
        return False
