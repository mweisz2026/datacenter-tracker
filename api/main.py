import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import time

from bonds_data import BONDS, BOND_MAP
from weather_service import get_weather
from news_service import get_news
from excel_service import get_live_prices

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
