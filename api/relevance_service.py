"""
News relevance scorer for datacenter bond intelligence.

Uses Claude Haiku to evaluate each headline against bond-specific context.
Results are cached by URL hash so each headline is only scored once.
Falls back to keyword heuristics if ANTHROPIC_API_KEY is not set.

Scoring scale:
  9-10  CRITICAL  -- natural disaster, confirmed major delay, strike, issuer distress, force majeure
  7-8   HIGH      -- community/regulatory opposition, cost overruns, permitting issues,
                     power supply problems, construction setbacks, environmental concerns
  4-6   MEDIUM    -- general project updates, hiring news, local govt activity re: project
  1-3   LOW       -- tangentially relevant regional news
  0     IRRELEVANT -- local crime, sports, entertainment, unrelated community events

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
HIGHLIGHT_THRESHOLD  = 7          # score >= 7 -> surfaced as alert
CACHE_TTL_SECONDS    = 2 * 3600   # re-score headlines after 2 hours
ALERT_MAX_AGE_DAYS   = 30         # don't surface alerts older than this

# url_hash -> {score, reason, category, ts}
_cache: dict = {}

# ── Keyword fallback (used when Claude API is unavailable) ─────────────────
# IMPORTANT: IRRELEVANT is checked FIRST so crime/accidents can never
# escalate to CRITICAL due to broad substring matches like "fire" in "gunfire".
#
# Single-word keywords use word-boundary matching (\b) to prevent substring
# false positives. Multi-word phrases are matched as exact substrings.

_IRRELEVANT_KW = [
    # Crime -- always score 0 regardless of location
    "arrested", "murder", "robbery", "theft", "burglary", "assault",
    "dui", "drug bust", "shooting", "gunfire", "homicide", "prison", "jail",
    "convicted", "sentenced", "indicted", "charged with",
    "shot and killed", "found dead", "body found",
    # Local accidents / medical (not construction-site related)
    "house fire", "car accident", "car crash", "traffic accident",
    "boil water notice", "boil water advisory", "water main break",
    "medical emergency", "overdose",
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

_CRITICAL_KW = [
    # Natural disasters
    "hurricane", "tornado", "wildfire", "earthquake",
    "blizzard", "ice storm", "heat emergency",
    "flooding", "flash flood", "major flood",
    # Labor / construction stoppage
    "work stoppage", "labor stoppage", "union vote", "picket", "lockout",
    # Financial distress
    "bankrupt", "bankruptcy", "insolvency", "missed payment",
    "debt restructuring", "chapter 11", "chapter 7", "receivership",
    "credit downgrade", "rating cut", "covenant breach",
    # Physical failure -- specific phrases only, NOT bare "fire" or "collapse"
    "structural collapse", "building collapse", "structure collapsed",
    "caught fire", "building fire", "on-site fire", "datacenter fire",
    "major fire", "explosion at",
    "cooling failure", "infrastructure failure",
    "emergency shutdown", "stop work order",
    "halt construction", "construction halted",
    "project canceled", "project cancelled", "project terminated", "project abandoned",
    # Regulatory / legal emergency
    "injunction granted", "court order halts", "cease and desist",
    "permit revoked", "license revoked",
    # Operational
    "force majeure", "power outage", "blackout",
    "evacuate", "evacuation",
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
    "regulatory hold", "faa objection", "utility objection",
    # Construction / schedule risk
    "construction delay", "behind schedule", "setback",
    "cost overrun", "over budget", "cost increase", "budget increase",
    "contractor dispute", "supply chain delay",
    "transformer delay", "equipment delay", "materials shortage",
    # Power / utilities risk
    "power shortage", "power constraint", "grid concern", "grid capacity",
    "interconnection delay", "interconnection queue", "utility dispute",
    "ppa dispute", "grid upgrade required",
    "transmission constraint", "curtailment", "load shedding",
    # Water risk
    "water dispute", "water rights", "water moratorium", "water permit denied",
    # Safety
    "osha citation", "safety violation", "accident at site", "injury at site", "fatality",
    # Financial / lease risk
    "lease concern", "tenant risk", "credit concern",
]


def _kw_match(kw: str, text: str) -> bool:
    """Match keyword. Single words use word boundaries; phrases use substring."""
    if " " in kw:
        return kw in text
    return bool(re.search(r"\b" + re.escape(kw) + r"\b", text))


def _keyword_score(title: str, summary: str) -> dict:
    text = (title + " " + summary).lower()
    # IRRELEVANT first -- crime/accidents must never escalate to CRITICAL
    for kw in _IRRELEVANT_KW:
        if _kw_match(kw, text):
            return {"score": 0, "reason": f"Irrelevant: {kw}", "category": "IRRELEVANT"}
    for kw in _CRITICAL_KW:
        if _kw_match(kw, text):
            return {"score": 9, "reason": f"Keyword match: {kw}", "category": "CRITICAL"}
    for kw in _HIGH_KW:
        if _kw_match(kw, text):
            return {"score": 7, "reason": f"Keyword match: {kw}", "category": "HIGH"}
    return {"score": 4, "reason": "General relevance", "category": "MEDIUM"}


def _parse_pub_date(pub: str):
    """Parse ISO 8601 or RFC 2822 date string. Returns datetime or None."""
    if not pub:
        return None
    try:
        dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(pub)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    return None


_SCORE_PROMPT = """\
You are a senior credit analyst at a distressed/high-yield debt fund. You monitor datacenter \
construction bonds for material risks that could impair bond repayment or increase default risk.

Bond context:
  Project: {bond_name}
  Location: {location}
  Primary Tenant / Anchor Customer: {tenant}
  Operator / Bond Issuer: {issuer}

Score each headline 0-10 for credit relevance to THIS specific bond.

== MANDATORY PRE-FILTER — RUN BEFORE SCORING ==
Step 1: Does this article explicitly name one of:
  (a) this project: {bond_name}
  (b) this issuer: {issuer}
  (c) this location: {location}
  (d) this tenant: {tenant} -- AND is the news about a RISK (distress, cancellation, default)?
If NONE of (a)-(d) apply -> score 1-2 MAX. Do not score higher.

Step 2: Is this a crime, accident, or local human-interest story (fire at a house, shooting,
car crash, injury, boil water notice, etc.)? -> score 0 always, no exceptions.

== SCORING GUIDE ==

9-10  CRITICAL -- Immediate material risk to bond repayment:
  - Natural disaster physically AT {location}: hurricane, tornado, wildfire, earthquake, flood
  - Confirmed construction stoppage or project cancellation for {bond_name}
  - Worker strike at {bond_name} site
  - {issuer} or {tenant} bankruptcy, default, missed payment, or major credit downgrade
  - Fire or structural collapse at the {bond_name} datacenter site itself
  - Court order or permit revocation halting {bond_name} construction
  - Confirmed cost overrun >20% or delay >6 months on {bond_name}

7-8   HIGH -- Significant risk signal for {bond_name} specifically:
  - Community opposition NAMING {bond_name}, {issuer}, or {location}: protests, petitions, town halls
  - Local government opposing {bond_name} by name: city/county/mayor
  - Permit denial/delay or zoning rejection for {bond_name}
  - Lawsuit against {bond_name} or {issuer}
  - Power/grid or water risk specific to {location} that would block {bond_name}
  - Construction setback on {bond_name}: contractor dispute, equipment delay
  - {tenant} reducing AI/cloud capex, canceling leases, or showing credit deterioration

4-6   MEDIUM -- Noteworthy context for this bond:
  - Construction progress or milestones for {bond_name}
  - Broader datacenter opposition/legislation in the same STATE that could affect {bond_name}
  - News about {issuer} or {tenant} that is not a risk signal
  - A different datacenter project facing opposition in the same region (precedent only)

1-2   LOW -- Indirect or geographic-only connection:
  - Regional economic/political news with no direct project tie
  - {tenant} announcing expansion ELSEWHERE (e.g. Google building in a different city/state)
  - General AI/cloud industry news with no specific connection to {bond_name}
  - Another company's datacenter news in the same state

0     IRRELEVANT -- Drop entirely:
  - Local crime: shootings, assaults, arrests -- even if in {location}
  - House fires, car accidents, medical emergencies -- even if near the site
  - Boil water notices, water main breaks, local infrastructure for general public
  - Sports, school events, entertainment, obituaries, lifestyle
  - Any datacenter news about a company with NO tie to {issuer} or {tenant}

== HARD RULES (non-negotiable) ==
- Crime/accident/local incident = score 0. Always. No exceptions.
- {tenant} announcing a datacenter in a DIFFERENT city or state = score 1-2, NOT high or critical.
  Example: Google announcing LaGrange GA site is score 1-2 for any bond NOT in LaGrange GA.
- Natural disaster in same city but NOT at the project site = score 3-4, never CRITICAL
- A moratorium or opposition story about a DIFFERENT project = score 3-4
- Opposition MUST name {bond_name}, {issuer}, or {location} to score >= 7
- Financial distress of {issuer} OR {tenant} = score 9 minimum
- When in doubt, score lower

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
) -> tuple:
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
            if (i + 1) in claude_scores:
                result = claude_scores[i + 1]
            else:
                result = _keyword_score(item.get("title", ""), item.get("summary", ""))
            # Hard override: if IRRELEVANT keywords match, always score 0 regardless of Claude
            kw_check = _keyword_score(item.get("title", ""), item.get("summary", ""))
            if kw_check["category"] == "IRRELEVANT":
                result = kw_check
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

        # Hard override on read: keyword IRRELEVANT always wins over any cached score
        kw = _keyword_score(item.get("title", ""), item.get("summary", ""))
        if kw["category"] == "IRRELEVANT":
            continue

        if category == "IRRELEVANT" or score == 0:
            continue

        enriched = {
            **item,
            "importance_score":    score,
            "importance_reason":   reason,
            "importance_category": category,
            "is_highlighted":      score >= HIGHLIGHT_THRESHOLD,
        }

        if score >= HIGHLIGHT_THRESHOLD:
            pub = item.get("published", "")
            too_old = False
            if pub:
                dt = _parse_pub_date(pub)
                if dt is not None:
                    too_old = dt < cutoff
            if too_old:
                enriched["is_highlighted"] = False
                regular.append(enriched)
            else:
                alerts.append(enriched)
        else:
            regular.append(enriched)

    alerts.sort(key=lambda x: x["importance_score"], reverse=True)

    return alerts, regular


def clear_score_cache():
    """Wipe the in-memory scoring cache so stale Claude scores don't persist."""
    _cache.clear()
