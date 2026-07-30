import os
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import time

from bonds_data import BONDS, BOND_MAP
from weather_service import get_weather
from news_service import (
    get_news, _news_cache, _emailed_urls,
    _is_email_eligible, _is_already_emailed, _mark_emailed,
    _EMAIL_MAX_AGE_DAYS,
)
from email_service import send_digest_email
from excel_service import get_live_prices
from relevance_service import clear_score_cache

_alerts_cache: dict = {"data": None, "ts": 0}
_ALERTS_TTL = 25 * 60  # 25 minutes

app = FastAPI(title="Datacenter Bond Tracker API")

_allowed_origins = ["http://localhost:5173", "http://127.0.0.1:5173"]
_extra = os.getenv("FRONTEND_URL", "")
if _extra:
    _allowed_origins.append(_extra)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_origin_regex=r"https://.*\.vercel\.app",   # any Vercel preview URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/bonds")
def list_bonds():
    """Return all bonds with static data."""
    return {"bonds": BONDS}


@app.get("/api/bonds/{bond_id}")
def get_bond(bond_id: str):
    bond = BOND_MAP.get(bond_id)
    if not bond:
        raise HTTPException(status_code=404, detail="Bond not found")
    return bond


@app.get("/api/weather/{bond_id}")
async def bond_weather(bond_id: str):
    bond = BOND_MAP.get(bond_id)
    if not bond:
        raise HTTPException(status_code=404, detail="Bond not found")
    weather = await get_weather(bond["lat"], bond["lon"])
    return weather


@app.get("/api/news/{bond_id}")
async def bond_news(bond_id: str):
    bond = BOND_MAP.get(bond_id)
    if not bond:
        raise HTTPException(status_code=404, detail="Bond not found")
    news = await get_news(
        bond_id=bond_id,
        news_queries=bond.get("news_queries", []),
        twitter_queries=bond.get("twitter_queries", []),
        bond_name=bond.get("name", ""),
        location=bond.get("location_display", ""),
        tenant=bond.get("lease_counterparty", ""),
        issuer=bond.get("guarantor", ""),
    )
    return news


@app.get("/api/alerts")
async def all_alerts():
    """Aggregate top-scored alerts across all bonds. Cached 25 min."""
    now = time.time()
    if _alerts_cache["data"] and (now - _alerts_cache["ts"]) < _ALERTS_TTL:
        return _alerts_cache["data"]

    async def fetch_bond(bond):
        try:
            news = await get_news(
                bond_id=bond["id"],
                news_queries=bond.get("news_queries", []),
                twitter_queries=bond.get("twitter_queries", []),
                bond_name=bond.get("name", ""),
                location=bond.get("location_display", ""),
                tenant=bond.get("lease_counterparty", ""),
                issuer=bond.get("guarantor", ""),
            )
            tagged = []
            for a in news.get("alerts", []):
                tagged.append({
                    **a,
                    "bond_id":       bond["id"],
                    "bond_name":     bond.get("name", ""),
                    "bond_location": bond.get("location_display", ""),
                })
            return tagged
        except Exception as e:
            print(f"[alerts] {bond['id']}: {e}")
            return []

    lists = await asyncio.gather(*[fetch_bond(b) for b in BONDS])
    flat  = [a for lst in lists for a in lst]
    flat.sort(key=lambda x: (-x.get("importance_score", 0), x.get("published", "")), reverse=False)
    result = {"alerts": flat[:30], "as_of": now}
    _alerts_cache["data"]  = result
    _alerts_cache["ts"]    = now
    return result


@app.get("/api/debug_email")
async def debug_email():
    """
    Shows exactly why emails are or aren't being sent.
    Busts the cache, re-fetches all bonds, and reports on each alert.
    """
    import os as _os
    from email_service import RESEND_API_KEY, ALERT_EMAIL_TO, ALERT_RECIPIENTS

    _alerts_cache["data"] = None
    _alerts_cache["ts"]   = 0
    _news_cache.clear()
    clear_score_cache()

    result = await all_alerts()
    all_alert_items = result.get("alerts", [])

    report = []
    for a in all_alert_items:
        eligible = _is_email_eligible(a)
        already_sent = a.get("url", "") in _emailed_urls
        report.append({
            "bond":       a.get("bond_name", ""),
            "title":      a.get("title", "")[:80],
            "category":   a.get("importance_category", ""),
            "score":      a.get("importance_score", 0),
            "published":  a.get("published", "")[:10],
            "email_eligible": eligible,
            "already_emailed": already_sent,
            "would_send": eligible and not already_sent,
        })

    return {
        "resend_configured": bool(RESEND_API_KEY),
        "email_to":          ALERT_EMAIL_TO,
        "email_recipients":  ALERT_RECIPIENTS,
        "email_window_days": _EMAIL_MAX_AGE_DAYS,
        "total_alerts":      len(all_alert_items),
        "would_send_count":  sum(1 for r in report if r["would_send"]),
        "alerts":            report,
    }


@app.get("/api/cron")
async def run_cron(request: Request, secret: str = ""):
    """
    Hourly cron endpoint — forces a fresh news fetch for every bond
    and sends emails for any new HIGH/CRITICAL alerts found.
    Vercel cron sends Authorization: Bearer <CRON_SECRET>.
    Manual trigger: pass ?secret=xxx or omit if CRON_SECRET is not set.
    """
    cron_secret = os.getenv("CRON_SECRET", "")
    if cron_secret:
        auth_header = request.headers.get("authorization", "")
        bearer_ok = auth_header == f"Bearer {cron_secret}"
        query_ok  = secret == cron_secret
        if not bearer_ok and not query_ok:
            raise HTTPException(status_code=401, detail="Unauthorized")

    # Clear all caches so every bond gets a fresh fetch
    _alerts_cache["data"] = None
    _alerts_cache["ts"]   = 0
    _news_cache.clear()
    clear_score_cache()

    # Fetch all bonds fresh
    result = await all_alerts()
    all_alert_items = result.get("alerts", [])
    n = len(all_alert_items)

    # Find alerts that are recent enough and haven't been emailed yet
    eligible   = [a for a in all_alert_items if a.get("url") and _is_email_eligible(a)]
    new_alerts = []
    if eligible:
        already_sent = await asyncio.gather(*[_is_already_emailed(a["url"]) for a in eligible])
        new_alerts   = [a for a, sent in zip(eligible, already_sent) if not sent]
        if new_alerts:
            sent = await send_digest_email(new_alerts)
            if sent:
                await asyncio.gather(*[_mark_emailed(a["url"]) for a in new_alerts])
                print(f"[cron] Digest email sent — {len(new_alerts)} new alert(s)")
            else:
                print(f"[cron] Digest email failed to send")
        else:
            print(f"[cron] No new alerts to email")
    else:
        print(f"[cron] No eligible alerts to email")

    print(f"[cron] Ran successfully — {n} total alerts across all bonds")
    return {"ok": True, "alerts_found": n, "emailed": len(new_alerts)}


@app.get("/api/test_email")
async def test_email(to: str = ""):
    """
    Send a single clearly-labeled [TEST] digest to verify delivery/formatting.
    Does NOT mark anything as emailed (real dedup untouched).
    Safety: can only send to addresses already on the approved recipient list,
    so it can't be used as an open mailer. `?to=` narrows to a subset (e.g.
    just yourself); omit it to send to the full list.
    """
    from datetime import datetime, timezone
    from email_service import send_digest_email, ALERT_RECIPIENTS, RESEND_API_KEY

    if not RESEND_API_KEY:
        return {"sent": False, "error": "RESEND_API_KEY not configured"}

    requested = [e.strip() for e in to.split(",") if e.strip()]
    # Whitelist: only allow addresses already approved as recipients
    recips = [e for e in requested if e in ALERT_RECIPIENTS] or ALERT_RECIPIENTS

    sample = [{
        "bond_name":            "Tract",
        "importance_category":  "HIGH",
        "importance_score":     7,
        "importance_reason":    "TEST alert — keyword match: sues",
        "published":            datetime.now(timezone.utc).isoformat(),
        "source":               "KOLO 8 (TEST)",
        "title":                "[TEST] NV Energy sues Tract over data center regulations",
        "url":                  "https://www.kolotv.com/2026/07/29/nv-energy-sues-tract-over-data-center-regulations/",
    }]
    sent = await send_digest_email(sample, recipients=recips, subject_prefix="[TEST] ")
    return {"sent": sent, "recipients": recips}


@app.get("/api/weather_all")
async def all_weather():
    """Weather for all bond locations, fetched concurrently."""
    results = await asyncio.gather(
        *[get_weather(b["lat"], b["lon"]) for b in BONDS],
        return_exceptions=True,
    )
    return {
        BONDS[i]["id"]: (r if not isinstance(r, Exception) else None)
        for i, r in enumerate(results)
    }


@app.get("/api/prices")
def live_prices():
    """
    Returns Bloomberg-pushed live pricing for all bonds.
    Reads directly from the Excel file, cached 30 seconds.
    Frontend polls this every 2 minutes.
    """
    return get_live_prices()


@app.get("/health")
def health():
    return {"status": "ok", "bonds": len(BONDS)}
