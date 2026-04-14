"""
News relevance scorer for datacenter bond intelligence.

Uses Claude Haiku to evaluate each headline against bond-specific context.
Results are cached by URL hash so each headline is only scored once.
Falls back to keyword heuristics if ANTHROPIC_API_KEY is not set.

Scoring scale:
  9-10  CRITICAL  — natural disaster, confirmed major delay, strike, issuer distress, force majeure
  7-8   HIGH      — community/regulatory opposition, cost overruns, permitting issues,
                    power supply problems, construction setbacks, environmental concerns
  4-6   MEDIUM    — general project updates, hiring news, local govt activity re: project
  1-3   LOW       — tangentially relevant regional news
  0     IRRELEVANT — local crime, sports, entertainment, unrelated community events

Items scoring >= 7 are surfaced as "alerts" at the top of the news feed.
Items scoring 0 (IRRELEVANT) are silently dropped.
"""

import anthropic
import asyncio
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta

ANTHROPIC_API_KEY    = os.getenv("ANTHROPIC_API_KEY", "")
HIGHLIGHT_THRESHOLD  = 7          # score >= 7 → surfaced as alert
CACHE_TTL_SECONDS    = 4 * 3600   # re-score headlines after 4 hours
ALERT_MAX_AGE_DAYS   = 30         # don't surface alerts older than this

# url_hash -> {score, reason, category, ts}
_cache: dict = {}

# ── Keyword fallback (used when no API key) ────────────────────────────────
_CRITICAL_KW = [
    # Natural disasters / weather
    "hurricane", "tornado", "flood", "flooding", "wildfire", "earthquake",
    "blizzard", "ice storm", "hail", "drought", "heat emergency",
    # Labor / construction stoppage
    "strike", "walkout", "work stoppage", "labor stoppage", "union vote",
    "picket", "lockout",
    # Financial distress
    "bankrupt", "bankruptcy", "insolvency", "default", "missed payment",
    "debt restructuring", "chapter 11", "chapter 7", "receivership",
    "credit downgrade", "rating cut", "covenant breach", "waiver",
    # Physical / operational failure
    "force majeure", "collapse", "explosion", "fire", "major fire",
    "power outage", "blackout", "infrastructure failure", "cooling failure",
    "evacuate", "evacuation", "emergency shutdown", "stop work order",
    "halt construction", "construction halted", "project canceled",
    "project cancelled", "project terminated", "project abandoned",
    # Regulatory / legal emergency
    "injunction granted", "court order halts", "cease and desist",
    "permit revoked", "license revoked",
]
_HIGH_KW = [
    # Community / political opposition
    "opposition", "pushback", "protest", "protesters", "objection",
    "residents oppose", "residents against", "community concern",
    "community opposition", "neighborhood opposition", "public outcry",
    "petition against", "signed petition", "town hall opposition",
    "county commissioner opposes", "mayor opposes", "city council opposes",
    "local opposition", "moratorium", "ban proposed", "pause construction",
    # Legal / regulatory challenges
    "lawsuit", "sued", "legal challenge", "legal action", "court challenge",
    "injunction", "restraining order", "permit denied", "permit rejected",
    "permit delayed", "zoning denied", "zoning rejected", "variance denied",
    "environmental review", "environmental impact", "impact assessment",
    "eia", "regulatory hold", "faa objection", "utility objection",
    # Construction / schedule risk
    "construction delay", "delayed", "behind schedule", "setback",
    "cost overrun", "over budget", "cost increase", "budget increase",
    "contractor dispute", "subcontractor", "supply chain delay",
    "transformer delay", "equipment delay", "materials shortage",
    # Power / utilities risk
    "power shortage", "power constraint", "grid concern", "grid capacity",
    "interconnection delay", "interconnection queue", "utility dispute",
    "ppa dispute", "power purchase", "grid upgrade required",
    "transmission constraint", "curtailment", "load shedding",
    # Water risk
    "water dispute", "water rights", "water shortage", "drought impact",
    "water permit denied", "water moratorium",
    # Safety
    "worker safety", "osha", "osha citation", "safety violation",
    "accident at site", "injury at site", "fatality",
    # Financial / lease risk
    "lease concern", "tenant risk", "anchor tenant", "credit concern",
    "occupancy risk", "off-take risk",
    # Noise / environment
    "noise complaint", "noise ordinance", "light pollution", "traffic concern",
    "traffic impact", "air quality",
]
_IRRELEVANT_KW = [
    # Crime
    "arrested", "murder", "robbery", "theft", "burglary", "assault",
    "dui", "drug bust", "shooting", "homicide", "prison", "jail",
    "convicted", "sentenced", "indicted", "charged with",
    # Sports
    "football", "baseball", "basketball", "soccer", "tennis", "golf",
    "nfl", "nba", "mlb", "nhl", "ncaa", "high school sports",
    "game preview", "box score", "standings", "playoffs",
    # Lifestyle / fluff
    "restaurant review", "recipe", "food festival", "obituary", "wedding",
    "lottery", "bingo", "county fair", "festival", "parade", "carnival",
    "prom", "graduation ceremony", "spelling bee",
    # Unrelated local politics
    "school board", "superintendent", "teacher contract",
    "library budget", "parks department",
]


def _keyword_score(title: str, summary: str) -> dict:
    text = (title + " " + summary).lower()
    for kw in _CRITICAL_KW:
        if kw in text:
            return {"score": 9, "reason": f"Keyword match: {kw}", "category": "CRITICAL"}
    for kw in _HIGH_KW:
        if kw in text:
            return {"score": 7, "reason": f"Keyword match: {kw}", "category": "HIGH"}
    for kw in _IRRELEVANT_KW:
        if kw in text:
            return {"score": 0, "reason": f"Irrelevant: {kw}", "category": "IRRELEVANT"}
    return {"score": 4, "reason": "General relevance", "category": "MEDIUM"}


_SCORE_PROMPT = """\
You are a senior credit analyst at a distressed/high-yield debt fund. You monitor datacenter \
construction bonds for material risks that could impair bond repayment or increase default risk. \
You are hyper-alert to any signal — even early-stage — that could become a problem.

Bond context:
  Project: {bond_name}
  Location: {location}
  Primary Tenant / Anchor Customer: {tenant}
  Operator / Bond Issuer: {issuer}

Score each headline 0-10 for credit relevance to THIS specific bond.

━━ SCORING GUIDE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

9-10  CRITICAL — Immediate material risk to bond repayment:
  • Natural disaster (hurricane, tornado, wildfire, flood, earthquake) at or near site
  • Confirmed major construction stoppage or project cancellation
  • Worker strike or work stoppage at the site
  • Issuer, operator, or anchor tenant financial distress, bankruptcy, default, or rating cut
  • Force majeure event, facility fire, structural collapse, or major safety incident
  • Permit revocation, court order halting construction, or regulatory shutdown
  • Confirmed cost overrun >20% or schedule delay >6 months

7-8   HIGH — Significant risk signal requiring close monitoring:
  • Community opposition: residents/homeowners protesting, petitions, town halls against project
  • Local government opposition: city council, county commissioners, mayor opposing the project
  • Permit denial or delay, zoning rejection, variance denied, environmental review triggered
  • Legal challenge: lawsuit filed, injunction sought, restraining order against project
  • Power/grid risk: interconnection delay, utility objection, grid capacity constraint,
    transmission issues, power purchase agreement dispute
  • Water rights dispute, water permit denial, water moratorium affecting site
  • Construction setback: contractor dispute, key equipment delay (transformers, switchgear),
    supply chain disruption affecting timeline
  • OSHA citation, safety violation, or serious injury/fatality at site
  • Anchor tenant (e.g. {tenant}) reducing AI/cloud capex, canceling leases, or credit concerns
  • Noise/light/traffic ordinance challenge that could restrict operations
  • Any regulatory agency (FAA, EPA, state PUC, county) raising concerns about the project
  • Political opposition: local politicians, state legislators opposing datacenter development

4-6   MEDIUM — Noteworthy but not immediately alarming:
  • General construction progress updates (on schedule, milestone reached)
  • Hiring announcements, job fair, workforce development related to the project
  • Local government approvals, tax incentive agreements, economic development deals
  • Industry news about the operator or tenant that is not distress-related
  • Nearby infrastructure upgrades (road, grid, fiber) related to datacenter campus
  • General datacenter industry trends affecting the region
  • Community engagement events (informational meetings, open houses) without opposition

1-3   LOW — Tangential or background information:
  • Regional economic news not directly tied to the project
  • Unrelated projects in the same county or city
  • General AI/cloud industry news with no specific connection to this bond
  • Opinion pieces or editorials about datacenter industry broadly

0     IRRELEVANT — Drop entirely:
  • Local crime, arrests, accidents unrelated to the project
  • Sports, high school events, entertainment
  • Obituaries, weddings, lifestyle content
  • Unrelated school board, library, or municipal budget news
  • Weather forecasts (non-disaster, non-emergency)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

IMPORTANT RULES:
- Any community pushback, protest, or opposition to the datacenter = score 7 minimum
- Any government body (city, county, state) opposing or challenging the project = score 7 minimum
- Any legal filing against the project = score 7 minimum
- Any power/grid/water constraint = score 7 minimum
- Financial distress of issuer OR anchor tenant = score 9 minimum
- When in doubt between two scores, pick the higher one — missing a risk is worse than a false positive
- Local crime/sports/fluff with no datacenter connection = score 0 always

Headlines to evaluate:
{headlines}

Return ONLY a valid JSON array. No markdown, no explanation outside the array:
[{{"index":1,"score":N,"reason":"concise reason under 10 words","category":"CRITICAL|HIGH|MEDIUM|LOW|IRRELEVANT"}}]"""


async def _claude_score(items: list, bond_name: str, location: str, tenant: str, issuer: str) -> dict:
    """Call Claude Haiku to score a batch of news items. Returns dict index->score_dict."""
    if not ANTHROPIC_API_KEY or not items:
        return {}

    headlines = "\n".join(
        f'{i+1}. Title: {it.get("title","")}\n'
        f'   Summary: {(it.get("summary") or "")[:120]}'
        for i, it in enumerate(items)
    )

    prompt = _SCORE_PROMPT.format(
        bond_name=bond_name,
        location=location,
        tenant=tenant,
        issuer=issuer,
        headlines=headlines,
    )

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        def _call():
            return client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )

        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(None, _call)
        raw = response.content[0].text.strip()

        # Strip any markdown code fences
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        scores = json.loads(raw)
        # Map by index (1-based) back to item position
        return {s["index"]: s for s in scores if isinstance(s, dict) and "index" in s}

    except Exception as e:
        print(f"[relevance] Claude scoring failed: {e}")
        return {}


async def score_and_filter(
    items: list,
    bond_name: str,
    location: str,
    tenant: str,
    issuer: str = "",
) -> tuple[list, list]:
    """
    Score all news items and return (alerts, regular_feed).
    - alerts: items with score >= HIGHLIGHT_THRESHOLD, sorted by score desc
    - regular_feed: remaining items, IRRELEVANT filtered out, sorted by date
    """
    if not items:
        return [], []

    now = time.time()

    # Split into cached and uncached
    uncached = []
    for item in items:
        key = hashlib.md5((item.get("url") or item.get("title", "")).encode()).hexdigest()
        cached = _cache.get(key)
        if not cached or (now - cached.get("ts", 0)) > CACHE_TTL_SECONDS:
            uncached.append(item)

    # Score uncached items
    if uncached:
        if ANTHROPIC_API_KEY:
            claude_scores = await _claude_score(uncached, bond_name, location, tenant, issuer)
        else:
            claude_scores = {}

        for i, item in enumerate(uncached):
            url = item.get("url") or item.get("title", "")
            key = hashlib.md5(url.encode()).hexdigest()
            # Claude returns 1-based index
            if (i + 1) in claude_scores:
                result = claude_scores[i + 1]
            else:
                # Fallback to keyword heuristic
                result = _keyword_score(item.get("title", ""), item.get("summary", ""))
            _cache[key] = {**result, "ts": now}

    # Enrich all items
    alerts = []
    regular = []
    cutoff  = datetime.now(timezone.utc) - timedelta(days=ALERT_MAX_AGE_DAYS)

    for item in items:
        url = item.get("url") or item.get("title", "")
        key  = hashlib.md5(url.encode()).hexdigest()
        meta = _cache.get(key, {"score": 3, "reason": "", "category": "MEDIUM"})

        score    = meta.get("score", 3)
        category = meta.get("category", "MEDIUM")
        reason   = meta.get("reason", "")

        if category == "IRRELEVANT" or score == 0:
            continue   # silently drop

        enriched = {
            **item,
            "importance_score":    score,
            "importance_reason":   reason,
            "importance_category": category,
            "is_highlighted":      score >= HIGHLIGHT_THRESHOLD,
        }

        # Only surface as alert if recent enough
        if score >= HIGHLIGHT_THRESHOLD:
            pub = item.get("published", "")
            too_old = False
            if pub:
                try:
                    dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    too_old = dt < cutoff
                except Exception:
                    pass  # unparseable date → allow through
            if too_old:
                enriched["is_highlighted"] = False
                regular.append(enriched)
            else:
                alerts.append(enriched)
        else:
            regular.append(enriched)

    # Sort alerts by score desc, regular by published date desc
    alerts.sort(key=lambda x: x["importance_score"], reverse=True)

    return alerts, regular
