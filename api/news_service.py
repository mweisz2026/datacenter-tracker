"""
News aggregation for each datacenter bond.
Sources per bond:
  1. Pinned DC-specific articles (always shown, project/financing direct links)
  2. Local outlet RSS — targeted Google News site: queries for each local paper/TV
  3. NewsAPI — 3 targeted queries per bond
  4. DataCenter Dynamics / DataCenter Knowledge — industry RSS, keyword filtered
  5. Twitter/X v2 — if Bearer Token valid and on Basic+ plan
"""
import httpx
import feedparser
import asyncio
import os
import re
import urllib.parse
from datetime import datetime, timezone

from relevance_service import score_and_filter
import hashlib
import time
from datetime import datetime, timezone, timedelta

# Only email alerts published within this window — prevents old articles firing on cold start
_EMAIL_MAX_AGE_DAYS = 7

# Upstash Redis REST — persistent dedup across cold starts (falls back to in-memory if not set)
UPSTASH_URL   = os.getenv("UPSTASH_REDIS_REST_URL", "")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")


def _parse_date(pub: str) -> datetime | None:
    """Parse a date string in either ISO 8601 or RFC 2822 format. Returns None if unparseable."""
    if not pub:
        return None
    # Try ISO 8601 first (NewsAPI: "2026-04-17T10:00:00Z")
    try:
        dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    # Try RFC 2822 (RSS/feedparser: "Fri, 17 Apr 2026 10:00:00 +0000")
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(pub)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    return None


def _is_email_eligible(item: dict) -> bool:
    """True only if the item has a parseable published date within the last 7 days."""
    dt = _parse_date(item.get("published", ""))
    if dt is None:
        return False  # no date or unparseable → never email
    cutoff = datetime.now(timezone.utc) - timedelta(days=_EMAIL_MAX_AGE_DAYS)
    return dt >= cutoff

NEWSAPI_KEY      = os.getenv("NEWSAPI_KEY", "")
TWITTER_BEARER   = urllib.parse.unquote(os.getenv("TWITTER_BEARER_TOKEN", ""))
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")

# Cached Reddit OAuth token (client_credentials — free, no user login needed)
_reddit_token: dict = {"token": "", "expires_at": 0.0}

# Per-bond news cache so the landing page /api/alerts reuses already-fetched data
_news_cache: dict = {}   # bond_id -> {"data": {...}, "ts": float}
_NEWS_CACHE_TTL  = 20 * 60  # 20 minutes

# In-memory dedup fallback (used when Upstash is not configured)
_emailed_urls: set = set()


def _email_key(url: str) -> str:
    return f"dcs:emailed:{hashlib.md5(url.encode()).hexdigest()}"


async def _is_already_emailed(url: str) -> bool:
    """Check if this URL has been emailed. Uses Upstash Redis if configured, else in-memory."""
    if url in _emailed_urls:
        return True
    if not UPSTASH_URL:
        return False
    try:
        async with httpx.AsyncClient(timeout=4) as client:
            r = await client.get(
                f"{UPSTASH_URL}/get/{_email_key(url)}",
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            )
        return r.json().get("result") is not None
    except Exception:
        return False


async def _mark_emailed(url: str) -> None:
    """Record URL as emailed with a 7-day TTL. Writes to both in-memory and Upstash."""
    _emailed_urls.add(url)
    if not UPSTASH_URL:
        return
    try:
        ttl = _EMAIL_MAX_AGE_DAYS * 24 * 3600
        async with httpx.AsyncClient(timeout=4) as client:
            await client.get(
                f"{UPSTASH_URL}/set/{_email_key(url)}/1/ex/{ttl}",
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            )
    except Exception:
        pass

DC_DYNAMICS_RSS  = "https://www.datacenterdynamics.com/en/rss/"
DC_KNOWLEDGE_RSS = "https://www.datacenterknowledge.com/rss.xml"

# ── Pinned DC project articles ───────────────────────────────────────────────
# Always shown at top of feed — direct links to project/financing news.
PINNED_ARTICLES = {
    "beignet": [
        {"title": "Meta Richland Parish Data Center — Official Project Page", "url": "https://datacenters.atmeta.com/richland-parish-data-center/", "source": "Meta", "type": "pinned"},
        {"title": "Opportunity Louisiana — Meta Data Center Economic Development Page", "url": "https://www.opportunitylouisiana.gov/metadatacenter", "source": "Louisiana Econ Dev", "type": "pinned"},
        {"title": "Louisiana Illuminator — Meta DC Coverage (Richland Parish)", "url": "https://lailluminator.com/place/richland-parish/", "source": "Louisiana Illuminator", "type": "pinned"},
        {"title": "Louisiana.gov — Richland Parish Official Page", "url": "https://www.louisiana.gov/local-louisiana/richland-parish", "source": "Louisiana.gov", "type": "pinned"},
    ],
    "hut_google": [
        {"title": "HUT 8 — Corporate IR Page", "url": "https://hut8.com/investors/", "source": "HUT 8 IR", "type": "pinned"},
        {"title": "DCD — HUT 8 and Google Partner on Louisiana Datacenter", "url": "https://www.datacenterdynamics.com/en/news/", "source": "DataCenter Dynamics", "type": "pinned"},
        {"title": "The Advocate — West Feliciana Community Section", "url": "https://www.theadvocate.com/baton_rouge/news/communities/west_feliciana/", "source": "The Advocate", "type": "pinned"},
        {"title": "St. Francisville Democrat — Weekly News (via The Advocate)", "url": "https://www.theadvocate.com/newsletters/st_francisville_democrat_weekly_news/", "source": "St. Francisville Democrat", "type": "pinned"},
        {"title": "West Feliciana Parish — Official Government Site", "url": "https://westfelicianaparish.com/", "source": "West Feliciana Parish", "type": "pinned"},
    ],
    "related_bx": [
        {"title": "Washtenaw County Government — Official Portal", "url": "https://www.washtenaw.org/", "source": "Washtenaw County", "type": "pinned"},
        {"title": "Ann Arbor SPARK — Economic Development Portal", "url": "https://jobs.annarborusa.org/jobs", "source": "Ann Arbor SPARK", "type": "pinned"},
        {"title": "Ann Arbor SPARK — News", "url": "https://www.annarborusa.org/news", "source": "Ann Arbor SPARK", "type": "pinned"},
        {"title": "Saline Township — Official Government Site", "url": "https://www.salinetownship.org/", "source": "Saline Township", "type": "pinned"},
        {"title": "DTE Energy — Official Site", "url": "https://www.dteenergy.com/", "source": "DTE Energy", "type": "pinned"},
    ],
    "vantage": [
        {"title": "Vantage DC — Shackelford County Campus (Official)", "url": "https://vantage-dc.com/data-center-locations/north-america/shackelford-county-tx", "source": "Vantage DC", "type": "pinned"},
        {"title": "Albany News — Shackelford Data Center Named Official Stargate AI Campus", "url": "https://www.thealbanynews.net/news/shackelford-data-center-named-official-stargate-ai-campus", "source": "The Albany News", "type": "pinned"},
        {"title": "Albany News — Data Center Progress: Full Steam Ahead", "url": "https://www.thealbanynews.net/news/data-center-progress-full-steam-ahead", "source": "The Albany News", "type": "pinned"},
    ],
    "stack_nm": [
        {"title": "MVEDA — Mesilla Valley Economic Development Alliance", "url": "https://www.mveda.com/", "source": "MVEDA", "type": "pinned"},
        {"title": "Dona Ana County Government — Official Portal", "url": "https://www.donaanacounty.org/", "source": "Dona Ana County", "type": "pinned"},
        {"title": "New Mexico Public Regulation Commission", "url": "https://www.nmprc.state.nm.us/", "source": "NM PRC", "type": "pinned"},
    ],
    "tract": [
        {"title": "TRIC — Tahoe Reno Industrial Center Data Center Info", "url": "https://tahoereno.com/data-center/", "source": "TRIC", "type": "pinned"},
        {"title": "PowerHouse Data Centers — Storey County News", "url": "https://www.powerhousedata.com/news/", "source": "PowerHouse DC", "type": "pinned"},
        {"title": "Novva Data Centers — Tahoe Reno Facility", "url": "https://www.novva.com/data-center-facilities/tahoe-reno-nevada/", "source": "Novva DC", "type": "pinned"},
        {"title": "Storey County Business Development", "url": "https://storeycounty.org/277/Business-Development", "source": "Storey County", "type": "pinned"},
        {"title": "Comstock Chronicle — Virginia City Weekly (hyperlocal)", "url": "https://www.thecomstockchronicle.com/", "source": "Comstock Chronicle", "type": "pinned"},
        {"title": "Northern Nevada Business Weekly — Broke Tract CEO Interview", "url": "https://www.nnbw.com/", "source": "NNBW", "type": "pinned"},
        {"title": "This Is Reno — Nonprofit Digital News", "url": "https://thisisreno.com/", "source": "This Is Reno", "type": "pinned"},
        {"title": "The Nevada Independent — Statewide Policy Coverage", "url": "https://thenevadaindependent.com/", "source": "Nevada Independent", "type": "pinned"},
        {"title": "Nevada Newsmakers — CEO van Rooyen Interview", "url": "https://nevadanewsmakers.com/", "source": "Nevada Newsmakers", "type": "pinned"},
        {"title": "Storey County Government — Meetings & News Flash", "url": "https://www.storeycounty.org/", "source": "Storey County Gov", "type": "pinned"},
        {"title": "Ground News — Storey County Aggregator", "url": "https://ground.news/interest/storey-county", "source": "Ground News", "type": "pinned"},
        {"title": "Storey County — Notify Me (Road/Permit/Construction Alerts)", "url": "https://storeycounty.org/list.aspx", "source": "Storey County Gov", "type": "pinned"},
    ],
    "cifr_black_pearl": [
        {"title": "DCD — Cipher Mining to Develop 300MW Black Pearl Site in West Texas", "url": "https://www.datacenterdynamics.com/en/news/cipher-mining-to-develop-300mw-cryptomining-data-center-site-in-west-texas/", "source": "DataCenter Dynamics", "type": "pinned"},
        {"title": "GlobeNewswire — Cipher Mining Prices $2.0B Senior Secured Notes", "url": "https://www.globenewswire.com/news-release/2026/02/04/3232548/0/en/Cipher-Mining-Inc-Announces-Pricing-of-2-0-Billion-of-Senior-Secured-Notes.html", "source": "GlobeNewswire", "type": "pinned"},
        {"title": "City of Wink — Local Government Portal", "url": "https://cityofwink.com/", "source": "City of Wink", "type": "pinned"},
    ],
    "wulf": [
        {"title": "TeraWulf Investor Relations — Official Press Releases", "url": "https://investors.terawulf.com/", "source": "TeraWulf IR", "type": "pinned"},
        {"title": "Niagara Gazette — Somerset AI Data Center Gets County Planner Support", "url": "https://www.niagara-gazette.com/news/local_news/somerset-ai-data-center-proposal-gets-county-planners-support/article_04e3f6f2-9228-11ef-896b-a7548a88c91b.html", "source": "Niagara Gazette", "type": "pinned"},
        {"title": "DCD — TeraWulf Gets Approval for More Data Centers at Lake Mariner Campus", "url": "https://www.datacenterdynamics.com/en/news/terawulf-gets-approval-for-more-data-centers-at-lake-mariner-campus-in-new-york/", "source": "DataCenter Dynamics", "type": "pinned"},
        {"title": "TeraWulf WULF Compute — Services Info", "url": "https://www.terawulf.com/wulf-compute", "source": "TeraWulf", "type": "pinned"},
        {"title": "Town of Somerset, NY — Official Government Site", "url": "https://www.somersetny.org/", "source": "Town of Somerset", "type": "pinned"},
        {"title": "Niagara County Center for Economic Development (NCIDA)", "url": "https://www.niagaracountybusiness.com/", "source": "NCCED/NCIDA", "type": "pinned"},
        {"title": "NYPA — New York Power Authority", "url": "https://www.nypa.gov/", "source": "NYPA", "type": "pinned"},
    ],
    "flashc": [
        {"title": "EverythingLubbock — New AI Data Center Breaks Ground Near Abernathy (April 2026)", "url": "https://www.everythinglubbock.com/news/latest/new-ai-data-center-breaks-ground-near-abernathy/", "source": "EverythingLubbock", "type": "pinned"},
        {"title": "Aligned DC — Project Caprock Groundbreaking (540MW, $5B)", "url": "https://aligneddc.com/press-release/aligned-breaks-ground-on-project-caprock/", "source": "Aligned DC", "type": "pinned"},
    ],
    "cifr_barber_lake": [
        {"title": "DatacenterMap — Cipher Mining Barber Lake (500MW+, AWS tenant)", "url": "https://www.datacentermap.com/usa/texas/colorado-city/cipher-barber-lake/", "source": "DatacenterMap", "type": "pinned"},
        {"title": "Cipher Mining IR — Signs Additional 56MW 10-Year AI Hosting Agreement", "url": "https://investors.ciphermining.com/news-releases/news-release-details/cipher-mining-signs-additional-56-mw-10-year-ai-hosting", "source": "Cipher Mining IR", "type": "pinned"},
    ],
    "apld_pf2": [
        {"title": "ND Monitor — Data Center Proposed for Harwood Prompts Community Questions ($3B)", "url": "https://northdakotamonitor.com/2025/08/25/data-center-proposed-for-harwood-prompts-anger-questions-from-community-members/", "source": "ND Monitor", "type": "pinned"},
        {"title": "InForum — Controversial Giant AI Data Center Community Meeting in Harwood", "url": "https://www.inforum.com/news/fargo/controversial-giant-ai-data-center-holds-headed-meeting-in-small-town-harwood", "source": "InForum", "type": "pinned"},
        {"title": "Cass County, ND — Official Government Site", "url": "https://www.casscountynd.gov/", "source": "Cass County", "type": "pinned"},
    ],
    "apld": [
        {"title": "DCD — Applied Blockchain Breaks Ground on 180MW Facility in Ellendale, ND", "url": "https://www.datacenterdynamics.com/en/news/applied-blockchain-breaks-ground-on-180mw-cryptomine-in-ellendale-north-dakota/", "source": "DataCenter Dynamics", "type": "pinned"},
        {"title": "Jamestown Sun — Applied Digital Plans to Expand in Ellendale, ND", "url": "https://www.jamestownsun.com/news/local/applied-digital-plans-to-expand-in-ellendale-nd", "source": "Jamestown Sun", "type": "pinned"},
        {"title": "KFYR TV — Massive AI Footprint in Ellendale: Is It There to Stay?", "url": "https://www.kfyrtv.com/2025/05/14/massive-ai-footprint-is-ellendale-its-there-stay/", "source": "KFYR TV", "type": "pinned"},
        {"title": "Baxtel — Applied Digital Ellendale ND Facility Profile", "url": "https://baxtel.com/data-center/applied-digital-ellendale-nd", "source": "Baxtel", "type": "pinned"},
    ],
    "qts": [
        {"title": "AJC — Microsoft's Newest AI Superfactory Opens at Fayetteville Campus", "url": "https://www.ajc.com/business/2025/11/microsofts-newest-ai-superfactory-opens-at-sprawling-fayetteville-campus/", "source": "Atlanta Journal-Constitution", "type": "pinned"},
        {"title": "AJC — Gigantic Data Center Campus Planned for 615-Acre Site South of Atlanta", "url": "https://www.ajc.com/news/gigantic-data-center-campus-planned-for-615-acre-site-south-of-atlanta/XKF77UM4FFBOZDPHFWMTOQ4EGE/", "source": "Atlanta Journal-Constitution", "type": "pinned"},
        {"title": "QTS Data Centers — Fayetteville (Project Excalibur)", "url": "https://qtsdatacenters.com/fayetteville-community/", "source": "QTS", "type": "pinned"},
        {"title": "City of Fayetteville — Official Data Center Discussion Page", "url": "https://www.fayetteville-ga.gov/746/Data-Center-Discussion", "source": "City of Fayetteville", "type": "pinned"},
        {"title": "Fayette News — Construction Safety at Georgia Data Center (Suit)", "url": "https://www.fayette-news.net/news/shoddy-construction-at-georgia-data-center-killed-worker-suit-says/article_0f93529a-a41e-4458-b973-18817c8164ce.html", "source": "Fayette County News", "type": "pinned"},
        {"title": "The Citizen — Fayetteville Hyperlocal Coverage", "url": "https://thecitizen.com/", "source": "The Citizen", "type": "pinned"},
        {"title": "Atlanta News First — Broke the QTS Fayette Water-Use Investigation", "url": "https://www.atlantanewsfirst.com/", "source": "Atlanta News First", "type": "pinned"},
        {"title": "Fayette County Government — Official Portal", "url": "https://fayettecountyga.gov/", "source": "Fayette County", "type": "pinned"},
        {"title": "Fayette County Development Authority", "url": "https://www.fayettega.org/", "source": "Fayette County Dev. Authority", "type": "pinned"},
    ],
    "meridian": [
        {"title": "Indiana Economic Development Corporation — Data Center Projects", "url": "https://iedc.in.gov/", "source": "IEDC", "type": "pinned"},
        {"title": "Sullivan County Government — Official Portal", "url": "https://www.sullivancounty.in.gov/", "source": "Sullivan County", "type": "pinned"},
        {"title": "Potentia — Sullivan County Industrial Park (Developer Project Page)", "url": "https://potentia.inc/sullivan-county-industrial-park", "source": "Potentia", "type": "pinned"},
        {"title": "Indiana Utility Regulatory Commission (large-load affordability review)", "url": "https://www.in.gov/iurc/", "source": "IURC", "type": "pinned"},
    ],
    "edged_compute": [
        {"title": "Edged Energy — Official Company Site", "url": "https://www.edgedenergy.com/", "source": "Edged Energy", "type": "pinned"},
        {"title": "Koch Industries — Infrastructure Investment Portfolio", "url": "https://www.kochind.com/businesses/infrastructure", "source": "Koch Industries", "type": "pinned"},
        {"title": "City of Aurora, IL — News Articles", "url": "https://www.aurora.il.us/News-articles", "source": "City of Aurora", "type": "pinned"},
        {"title": "Kane County, IL — Official Government Site", "url": "https://www.kanecountyil.gov/", "source": "Kane County", "type": "pinned"},
        {"title": "Aurora Regional Chamber of Commerce", "url": "https://aurorachamber.com/", "source": "Aurora Chamber", "type": "pinned"},
        {"title": "Illinois Clean Jobs Coalition — IL POWER Act Tracker", "url": "https://ilcleanjobs.org/", "source": "IL Clean Jobs Coalition", "type": "pinned"},
        {"title": "Develop Fulton — Fulton County, GA Economic Development", "url": "https://developfulton.com/", "source": "Develop Fulton", "type": "pinned"},
        {"title": "City of Atlanta — Department of City Planning", "url": "https://www.atlantaga.gov/government/departments/city-planning", "source": "City of Atlanta", "type": "pinned"},
        {"title": "Technology Association of Georgia (TAG)", "url": "https://www.tagonline.org/", "source": "TAG", "type": "pinned"},
    ],
    "core_scientific": [
        {"title": "Core Scientific Investor Relations — Press Releases", "url": "https://investors.corescientific.com/news-releases", "source": "Core Scientific IR", "type": "pinned"},
        {"title": "Core Scientific — HPC Hosting Services", "url": "https://www.corescientific.com/hpc-hosting", "source": "Core Scientific", "type": "pinned"},
        {"title": "CoStar — Denton $6.1B Conversion Coverage", "url": "https://www.costar.com/", "source": "CoStar", "type": "pinned"},
        {"title": "Denton Municipal Electric (City of Denton, TX)", "url": "https://www.cityofdenton.com/331/Denton-Municipal-Electric-DME", "source": "City of Denton", "type": "pinned"},
        {"title": "City of Dalton, GA — Official Government Site", "url": "https://www.daltonga.gov/", "source": "City of Dalton", "type": "pinned"},
        {"title": "Dalton Utilities", "url": "https://www.daltonutilities.com/", "source": "Dalton Utilities", "type": "pinned"},
        {"title": "Whitfield County, GA — Official Government Site", "url": "https://www.whitfieldcountyga.com/", "source": "Whitfield County", "type": "pinned"},
        {"title": "Cherokee County, NC — Official Government Site", "url": "https://www.cherokeecounty-nc.gov/", "source": "Cherokee County NC", "type": "pinned"},
        {"title": "Town of Andrews, NC — Official Government Site", "url": "https://www.townofandrews.com/", "source": "Town of Andrews", "type": "pinned"},
        {"title": "Port of Muskogee", "url": "https://www.portmuskogee.com/", "source": "Port of Muskogee", "type": "pinned"},
        {"title": "City of Muskogee, OK — Official Government Site", "url": "https://www.cityofmuskogee.com/", "source": "City of Muskogee", "type": "pinned"},
        {"title": "Northwest Georgia Regional Commission", "url": "https://www.nwgrc.org/", "source": "NWGRC", "type": "pinned"},
        {"title": "SEC EDGAR — Core Scientific (CORZ) Filings", "url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=Core+Scientific&type=10-K&dateb=&owner=include&count=40", "source": "SEC EDGAR", "type": "pinned"},
    ],
    "sbe_softbank": [
        {"title": "Softbank Group — Investor Relations", "url": "https://group.softbank/en/ir", "source": "SoftBank IR", "type": "pinned"},
        {"title": "Austin Business Journal — Softbank Datacenter in Austin", "url": "https://www.bizjournals.com/austin/", "source": "Austin Business Journal", "type": "pinned"},
    ],
    "hut_beacon_point": [
        {"title": "HUT 8 — Corporate IR Page", "url": "https://hut8.com/investors/", "source": "HUT 8 IR", "type": "pinned"},
        {"title": "Corpus Christi Caller-Times — Local Coverage", "url": "https://www.caller.com/", "source": "Caller-Times", "type": "pinned"},
        {"title": "Corpus Christi Business News", "url": "https://ccbiznews.com/", "source": "CC Biz News", "type": "pinned"},
    ],
    "tract_d": [
        {"title": "TRIC — Tahoe Reno Industrial Center Data Center Info", "url": "https://tahoereno.com/data-center/", "source": "TRIC", "type": "pinned"},
        {"title": "Storey County Business Development", "url": "https://storeycounty.org/277/Business-Development", "source": "Storey County", "type": "pinned"},
        {"title": "Comstock Chronicle — Virginia City Weekly (hyperlocal)", "url": "https://www.thecomstockchronicle.com/", "source": "Comstock Chronicle", "type": "pinned"},
        {"title": "Northern Nevada Business Weekly — Broke Tract CEO Interview", "url": "https://www.nnbw.com/", "source": "NNBW", "type": "pinned"},
        {"title": "This Is Reno — Nonprofit Digital News", "url": "https://thisisreno.com/", "source": "This Is Reno", "type": "pinned"},
        {"title": "The Nevada Independent — Statewide Policy Coverage", "url": "https://thenevadaindependent.com/", "source": "Nevada Independent", "type": "pinned"},
        {"title": "Nevada Newsmakers — CEO van Rooyen Interview", "url": "https://nevadanewsmakers.com/", "source": "Nevada Newsmakers", "type": "pinned"},
        {"title": "Storey County Government — Meetings & News Flash", "url": "https://www.storeycounty.org/", "source": "Storey County Gov", "type": "pinned"},
        {"title": "Ground News — Storey County Aggregator", "url": "https://ground.news/interest/storey-county", "source": "Ground News", "type": "pinned"},
        {"title": "Storey County — Notify Me (Road/Permit/Construction Alerts)", "url": "https://storeycounty.org/list.aspx", "source": "Storey County Gov", "type": "pinned"},
    ],
    "polar_dc": [
        {"title": "Herøya Industripark — Industrial Park Site", "url": "https://www.heroya-industripark.no/", "source": "Herøya Industripark", "type": "pinned"},
        {"title": "Drangedal Kommune — Municipality Portal", "url": "https://www.drangedal.kommune.no/", "source": "Drangedal Kommune", "type": "pinned"},
        {"title": "Porsgrunn Kommune — Municipality Portal", "url": "https://www.porsgrunn.kommune.no/", "source": "Porsgrunn Kommune", "type": "pinned"},
        {"title": "E24 — Norwegian Business News", "url": "https://e24.no/", "source": "E24", "type": "pinned"},
        {"title": "NVE — Norwegian Water Resources and Energy Directorate", "url": "https://www.nve.no/", "source": "NVE", "type": "pinned"},
        {"title": "Statnett — Norwegian Grid Operator", "url": "https://www.statnett.no/", "source": "Statnett", "type": "pinned"},
    ],
    "prime_dc": [
        {"title": "Site Selection Magazine — Prime ORD Coverage", "url": "https://siteselection.com/", "source": "Site Selection", "type": "pinned"},
        {"title": "Village of Elk Grove Village — Official Government Portal", "url": "https://www.elkgrove.org/", "source": "Village of Elk Grove", "type": "pinned"},
        {"title": "Crain's Chicago Business — Data Center Coverage", "url": "https://www.chicagobusiness.com/", "source": "Crain's Chicago", "type": "pinned"},
        {"title": "Elk Grove Village — Agendas & Public Meetings", "url": "https://www.elkgrove.org/government/agendas-public-meetings", "source": "Village of Elk Grove", "type": "pinned"},
        {"title": "Cook County Government", "url": "https://www.cookcountyil.gov/", "source": "Cook County", "type": "pinned"},
        {"title": "CoreWeave — Newsroom", "url": "https://www.coreweave.com/news", "source": "CoreWeave", "type": "pinned"},
    ],
    "sopaipilla": [
        {"title": "El Paso Matters — Meta / Data Center Investigative Coverage", "url": "https://elpasomatters.org/", "source": "El Paso Matters", "type": "pinned"},
        {"title": "El Paso Inc. — Business Coverage (Meta El Paso)", "url": "https://www.elpasoinc.com/", "source": "El Paso Inc.", "type": "pinned"},
        {"title": "El Paso Times — Local Daily Coverage", "url": "https://www.elpasotimes.com/", "source": "El Paso Times", "type": "pinned"},
        {"title": "Diario.mx — El Paso (Spanish-Language Coverage)", "url": "https://diario.mx/elpaso/", "source": "Diario.mx", "type": "pinned"},
        {"title": "El Paso Electric — Official Site", "url": "https://www.epelectric.com/", "source": "El Paso Electric", "type": "pinned"},
        {"title": "City of El Paso — Official Government Site", "url": "https://www.elpasotexas.gov/", "source": "City of El Paso", "type": "pinned"},
    ],
    "glxy_helios": [
        {"title": "The Texas Spur — Dickens County Local Paper (broke Galaxy/CoreWeave lease)", "url": "https://www.thetexasspur.com/", "source": "The Texas Spur", "type": "pinned"},
        {"title": "EverythingLubbock — Broke Helios Phase 2 ($3.5B) Expansion Story", "url": "https://www.everythinglubbock.com/", "source": "EverythingLubbock", "type": "pinned"},
        {"title": "Galaxy Digital — Investor Relations", "url": "https://investor.galaxydigital.io/", "source": "Galaxy Digital IR", "type": "pinned"},
        {"title": "Dickens County, TX — Official Government Site", "url": "https://www.co.dickens.tx.us/", "source": "Dickens County", "type": "pinned"},
    ],
    "sbe_milam": [
        {"title": "Austin Business Journal — Broke the SBE / Stargate Milam Story", "url": "https://www.bizjournals.com/austin/", "source": "Austin Business Journal", "type": "pinned"},
        {"title": "The Cameron Herald — Milam County Seat Weekly", "url": "https://www.cameronherald.com/", "source": "The Cameron Herald", "type": "pinned"},
        {"title": "Rockdale Reporter — Milam County Weekly", "url": "https://www.rockdalereporter.com/", "source": "Rockdale Reporter", "type": "pinned"},
    ],
    "qts_magnolia": [
        {"title": "Columbus Business First — Broke QTS New Albany CRA Approval", "url": "https://www.bizjournals.com/columbus/", "source": "Columbus Business First", "type": "pinned"},
        {"title": "Dallas Business Journal — Broke the QTS Wilmer TDLR Filings", "url": "https://www.bizjournals.com/dallas/", "source": "Dallas Business Journal", "type": "pinned"},
        {"title": "News 5 Cleveland — New Albany Data Center Feature", "url": "https://www.news5cleveland.com/", "source": "News 5 Cleveland", "type": "pinned"},
        {"title": "Arizona Republic (azcentral) — Phoenix / West Valley Data Center Coverage", "url": "https://www.azcentral.com/", "source": "Arizona Republic", "type": "pinned"},
        {"title": "The Glendale Star — West Valley Hyperlocal (PHX3 campus)", "url": "https://www.glendalestar.com/", "source": "The Glendale Star", "type": "pinned"},
        {"title": "Columbus Dispatch — New Albany Coverage", "url": "https://www.dispatch.com/", "source": "Columbus Dispatch", "type": "pinned"},
        {"title": "Richmond BizSense — Henrico QTS / Data Center Coverage", "url": "https://richmondbizsense.com/", "source": "Richmond BizSense", "type": "pinned"},
        {"title": "Richmond Times-Dispatch — Henrico Data Center Coverage", "url": "https://richmond.com/", "source": "Richmond Times-Dispatch", "type": "pinned"},
        {"title": "Henrico Citizen — Hyperlocal (RIC1 / RIC3 campuses)", "url": "https://www.henricocitizen.com/", "source": "Henrico Citizen", "type": "pinned"},
        {"title": "New Albany, OH — Official Government Site", "url": "https://www.newalbanyohio.org/", "source": "New Albany, OH", "type": "pinned"},
        {"title": "City of Columbus, OH — Official Government Site", "url": "https://www.columbus.gov/", "source": "City of Columbus", "type": "pinned"},
        {"title": "City of Glendale, AZ — Official Government Site", "url": "https://www.glendaleaz.com/", "source": "City of Glendale", "type": "pinned"},
        {"title": "Town of Litchfield Park, AZ — Official Government Site", "url": "https://www.litchfieldpark.gov/", "source": "Litchfield Park", "type": "pinned"},
        {"title": "City of Denton, TX — Official Government Site", "url": "https://www.cityofdenton.com/", "source": "City of Denton", "type": "pinned"},
        {"title": "City of Irving, TX — Official Government Site", "url": "https://www.irvingtx.gov/", "source": "City of Irving", "type": "pinned"},
        {"title": "City of Fort Worth, TX — Official Government Site", "url": "https://www.fortworthtexas.gov/", "source": "City of Fort Worth", "type": "pinned"},
        {"title": "Henrico County, VA — Official Government Site", "url": "https://henrico.gov/", "source": "Henrico County", "type": "pinned"},
        {"title": "Chesterfield County, VA — Official Government Site", "url": "https://www.chesterfield.gov/", "source": "Chesterfield County", "type": "pinned"},
        {"title": "CBS News Texas", "url": "https://www.cbsnews.com/texas/", "source": "CBS News Texas", "type": "pinned"},
        {"title": "CultureMap Dallas", "url": "https://culturemap.com/dallas/", "source": "CultureMap Dallas", "type": "pinned"},
        {"title": "Columbus Alive", "url": "https://www.columbusalive.com/", "source": "Columbus Alive", "type": "pinned"},
        {"title": "Phoenix New Times", "url": "https://www.phoenixnewtimes.com/", "source": "Phoenix New Times", "type": "pinned"},
        {"title": "Richmond Free Press", "url": "https://richmondfreepress.com/", "source": "Richmond Free Press", "type": "pinned"},
        {"title": "Cardinal News", "url": "https://cardinalnews.org/", "source": "Cardinal News", "type": "pinned"},
    ],
    "voltag": [
        {"title": "Vantage DC — Shackelford County Campus (Official)", "url": "https://vantage-dc.com/data-center-locations/north-america/shackelford-county-tx", "source": "Vantage DC", "type": "pinned"},
        {"title": "Albany News — Shackelford Data Center Named Official Stargate AI Campus", "url": "https://www.thealbanynews.net/news/shackelford-data-center-named-official-stargate-ai-campus", "source": "The Albany News", "type": "pinned"},
        {"title": "Albany News — Data Center Progress: Full Steam Ahead", "url": "https://www.thealbanynews.net/news/data-center-progress-full-steam-ahead", "source": "The Albany News", "type": "pinned"},
        {"title": "VoltaGrid — Official Company Site", "url": "https://voltagrid.com/", "source": "VoltaGrid", "type": "pinned"},
        {"title": "VoltaGrid — Collaborates with Oracle to Power Next-Gen AI Data Centers", "url": "https://voltagrid.com/voltagrid-collaborates-with-oracle-to-power-next-gen-ai-data-centers", "source": "VoltaGrid", "type": "pinned"},
        {"title": "GlobeNewswire — VoltaGrid Closes $5.0B Financing ($2.0B Second Lien Notes + $3.0B ABL)", "url": "https://www.globenewswire.com/news-release/2025/11/10/3184496/0/en/VoltaGrid-Closes-5-0-Billion-Comprehensive-Financing-Package-Consisting-of-2-0-Billion-of-Senior-Secured-Second-Lien-Notes-and-3-0-Billion-Asset-Based-Loan-Facility.html", "source": "GlobeNewswire", "type": "pinned"},
        {"title": "DCD — VoltaGrid to Supply Oracle with 2.3GW of Natural Gas Power for AI Data Centers", "url": "https://www.datacenterdynamics.com/en/news/voltagrid-to-supply-oracle-with-23gw-of-natural-gas-power-for-ai-data-centers/", "source": "DataCenter Dynamics", "type": "pinned"},
        {"title": "POWER Magazine — Oracle Taps VoltaGrid for 2.3-GW Modular Gas Fleet in Texas", "url": "https://www.powermag.com/oracle-taps-voltagrid-for-2-3-gw-modular-gas-fleet-to-power-ai-data-centers-across-texas/", "source": "POWER Magazine", "type": "pinned"},
        {"title": "GlobeNewswire — VoltaGrid $1B Equity Investment from Blackstone & Halliburton (Propell Acquisition)", "url": "https://www.globenewswire.com/news-release/2026/05/11/3291825/0/en/voltagrid-announces-1-billion-strategic-equity-investment-from-blackstone-and-halliburton-to-fund-growth-and-acquisition-of-propell.html", "source": "GlobeNewswire", "type": "pinned"},
        {"title": "Albany News — Local Weekly (Shackelford County Seat)", "url": "https://www.thealbanynews.net/", "source": "The Albany News", "type": "pinned"},
        {"title": "BigCountryHomepage — KTAB/KRBC Abilene (broke the Vantage jobs story)", "url": "https://www.bigcountryhomepage.com/", "source": "BigCountryHomepage", "type": "pinned"},
        {"title": "KTXS ABC Abilene — Shackelford County Topic Page", "url": "https://ktxs.com/topic/Shackelford%20County,Texas,United%20States", "source": "KTXS", "type": "pinned"},
        {"title": "KTXS — Big Country News Section", "url": "https://ktxs.com/news/big-country", "source": "KTXS", "type": "pinned"},
        {"title": "Abilene Reporter-News — Gannett Daily", "url": "https://www.reporternews.com/", "source": "Abilene Reporter-News", "type": "pinned"},
        {"title": "Texas Tribune", "url": "https://www.texastribune.org/", "source": "Texas Tribune", "type": "pinned"},
        {"title": "Telborg DC Intel — Vantage \"Frontier\" Shackelford Briefing (TDLR filings)", "url": "https://telborg.com/dc-intel/briefings/vantage-frontier-shackelford", "source": "Telborg DC Intel", "type": "pinned"},
        {"title": "Avanza Energy Substack — Shackelford County Carbon/Permitting Analysis", "url": "https://avanzaenergy.substack.com/", "source": "Avanza Energy", "type": "pinned"},
        {"title": "TCEQ — Air Permits (VoltaGrid Generators)", "url": "https://www.tceq.texas.gov/", "source": "TCEQ", "type": "pinned"},
        {"title": "TX TDLR — Building Filings (TABS)", "url": "https://www.tdlr.texas.gov/TABS/", "source": "TX TDLR", "type": "pinned"},
    ],
    "cifr_stingray": [
        {"title": "Cipher Mining IR — Signs Additional 56MW 10-Year AI Hosting Agreement", "url": "https://investors.ciphermining.com/news-releases/news-release-details/cipher-mining-signs-additional-56-mw-10-year-ai-hosting", "source": "Cipher Mining IR", "type": "pinned"},
        {"title": "Permian Basin 360 — Regional Coverage", "url": "https://www.permianbasin360.com/", "source": "Permian Basin 360", "type": "pinned"},
        {"title": "Andrews County News — Local Twice-Weekly Paper", "url": "https://www.andrewscountynews.com/", "source": "Andrews County News", "type": "pinned"},
    ],
    "apld_eln4": [
        {"title": "DCD — Applied Blockchain Breaks Ground on 180MW Facility in Ellendale, ND", "url": "https://www.datacenterdynamics.com/en/news/applied-blockchain-breaks-ground-on-180mw-cryptomine-in-ellendale-north-dakota/", "source": "DataCenter Dynamics", "type": "pinned"},
        {"title": "KFYR TV — Massive AI Footprint in Ellendale: Is It There to Stay?", "url": "https://www.kfyrtv.com/2025/05/14/massive-ai-footprint-is-ellendale-its-there-stay/", "source": "KFYR TV", "type": "pinned"},
        {"title": "South Dakota Searchlight — Ellendale Data Center Case Study", "url": "https://southdakotasearchlight.com/", "source": "SD Searchlight", "type": "pinned"},
        {"title": "Baxtel — Applied Digital Ellendale ND Facility Profile", "url": "https://baxtel.com/data-center/applied-digital-ellendale-nd", "source": "Baxtel", "type": "pinned"},
    ],
    "yondr_nova": [
        {"title": "Piedmont Environmental Council — Data Center News Tracker", "url": "https://www.pecva.org/our-work/energy-matters/data-centers-energy-demand/data-center-news/", "source": "PEC", "type": "pinned"},
        {"title": "Loudoun Now — Nonprofit Community News", "url": "https://www.loudounnow.com/", "source": "Loudoun Now", "type": "pinned"},
        {"title": "Loudoun Times-Mirror — Weekly Since 1924", "url": "https://www.loudountimes.com/", "source": "Loudoun Times-Mirror", "type": "pinned"},
    ],
    "zenith": [
        {"title": "Okmulgee Times — Local Weekly", "url": "https://www.yourokmulgee.com/", "source": "Okmulgee Times", "type": "pinned"},
        {"title": "The Frontier — Tulsa Investigative Nonprofit", "url": "https://www.readfrontier.org/", "source": "The Frontier", "type": "pinned"},
    ],
    "digital_drive": [
        {"title": "Chesterfield County — Official Data Center Page", "url": "https://www.chesterfield.gov/datacenters", "source": "Chesterfield County", "type": "pinned"},
        {"title": "Richmond BizSense — Business Coverage", "url": "https://richmondbizsense.com/", "source": "Richmond BizSense", "type": "pinned"},
        {"title": "Virginia Business Magazine", "url": "https://www.virginiabusiness.com/", "source": "Virginia Business", "type": "pinned"},
        {"title": "Chesterfield County — Happening In Chesterfield Newsletter", "url": "https://www.chesterfield.gov/1653/Happening-In-Chesterfield-and-County-New", "source": "Chesterfield County", "type": "pinned"},
        {"title": "PowerHouse Data Centers — News", "url": "https://www.powerhousedata.com/news/", "source": "PowerHouse DC", "type": "pinned"},
    ],
    "novva_lv_reno": [
        {"title": "Novva Data Centers — Official Site", "url": "https://www.novva.com/", "source": "Novva DC", "type": "pinned"},
        {"title": "The Nevada Independent — Statewide Policy Coverage", "url": "https://thenevadaindependent.com/", "source": "Nevada Independent", "type": "pinned"},
        {"title": "Las Vegas Review-Journal", "url": "https://www.reviewjournal.com/", "source": "LV Review-Journal", "type": "pinned"},
    ],
}

# ── Local outlet Google News RSS (site-targeted, reliable) ───────────────────
# Format: Google News RSS filtered to specific local outlets + relevant keywords
LOCAL_OUTLET_RSS = {
    "beignet": [
        # Louisiana Illuminator covers Meta DC extensively
        "https://news.google.com/rss/search?q=site:lailluminator.com+%22Richland+Parish%22+OR+%22Meta%22&hl=en-US&gl=US&ceid=US:en",
        # KNOE TV + Monroe News-Star
        "https://news.google.com/rss/search?q=(site:knoe.com+OR+site:thenewsstar.com)+%22Richland+Parish%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Richland Beacon-News (both domains)
        "https://news.google.com/rss/search?q=(site:richlandtoday.com+OR+site:therichlandbeaconnews.com)&hl=en-US&gl=US&ceid=US:en",
        # KTVE/KARD Monroe (closest TV market) + KTAL/KMSS Shreveport (ran the Holly Ridge feature)
        "https://news.google.com/rss/search?q=(site:myarklamiss.com+OR+site:ktalnews.com)+%22Richland+Parish%22+OR+%22Meta%22+OR+%22Holly+Ridge%22&hl=en-US&gl=US&ceid=US:en",
        # American Press + The Advocate / NOLA.com — statewide
        "https://news.google.com/rss/search?q=(site:americanpress.com+OR+site:theadvocate.com+OR+site:nola.com)+%22Richland+Parish%22+OR+%22Meta%22+datacenter&hl=en-US&gl=US&ceid=US:en",
        # Baton Rouge Business Report + GovTech — business/industry trade press
        "https://news.google.com/rss/search?q=(site:businessreport.com+OR+site:govtech.com)+%22Richland+Parish%22+OR+%22Meta%22+datacenter&hl=en-US&gl=US&ceid=US:en",
    ],
    "hut_google": [
        # The Advocate / Times-Picayune
        "https://news.google.com/rss/search?q=(site:theadvocate.com+OR+site:nola.com)+%22HUT%22+OR+%22Google%22+OR+%22River+Bend%22+OR+%22data+center%22+Louisiana&hl=en-US&gl=US&ceid=US:en",
        # Louisiana Illuminator
        "https://news.google.com/rss/search?q=site:lailluminator.com+%22HUT%22+OR+%22Google%22+OR+%22River+Bend%22+OR+%22datacenter%22&hl=en-US&gl=US&ceid=US:en",
        # WBRZ + WAFB + BRProud — Baton Rouge broadcast
        "https://news.google.com/rss/search?q=(site:wbrz.com+OR+site:wafb.com+OR+site:brproud.com)+%22West+Feliciana%22+OR+%22HUT%22+OR+%22Google%22+data+center&hl=en-US&gl=US&ceid=US:en",
        # Baton Rouge Business Report
        "https://news.google.com/rss/search?q=site:businessreport.com+%22West+Feliciana%22+OR+%22HUT%22+OR+%22Google%22+data+center&hl=en-US&gl=US&ceid=US:en",
        # Technical.ly + Area Development — industry trade press
        "https://news.google.com/rss/search?q=(site:technical.ly+OR+site:areadevelopment.com)+%22West+Feliciana%22+OR+%22River+Bend%22+data+center&hl=en-US&gl=US&ceid=US:en",
        # West Feliciana Parish Government
        "https://news.google.com/rss/search?q=site:westfelicianaparish.com&hl=en-US&gl=US&ceid=US:en",
    ],
    "related_bx": [
        # Saline Post + Saline Sun Times — hyperlocal to Saline Township
        "https://news.google.com/rss/search?q=(site:thesalinepost.com+OR+site:thesuntimesnews.com)+%22Saline%22+OR+%22data+center%22+OR+%22Oracle%22&hl=en-US&gl=US&ceid=US:en",
        # MLive
        "https://news.google.com/rss/search?q=site:mlive.com+%22Saline%22+OR+%22data+center%22+OR+%22Oracle%22+OR+%22Washtenaw%22&hl=en-US&gl=US&ceid=US:en",
        # Patch Ann Arbor + Ypsilanti
        "https://news.google.com/rss/search?q=(site:patch.com/michigan/ann-arbor-mi+OR+site:patch.com/michigan/ypsilanti-mi)+%22Saline%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Crain's Detroit Business
        "https://news.google.com/rss/search?q=site:crainsdetroit.com+%22Saline%22+OR+%22Oracle%22+data+center+Michigan&hl=en-US&gl=US&ceid=US:en",
        # WEMU (Ypsilanti NPR)
        "https://news.google.com/rss/search?q=site:wemu.org+%22Saline%22+OR+%22data+center%22+OR+%22Oracle%22&hl=en-US&gl=US&ceid=US:en",
        # Concentrate (Second Wave Media) + Ann Arbor Observer
        "https://news.google.com/rss/search?q=(site:secondwavemedia.com+OR+site:annarborobserver.com)+%22Saline%22+OR+%22data+center%22+OR+%22Oracle%22&hl=en-US&gl=US&ceid=US:en",
        # ENR — construction trade press
        "https://news.google.com/rss/search?q=site:enr.com+%22Saline%22+OR+%22Ann+Arbor%22+Oracle+data+center&hl=en-US&gl=US&ceid=US:en",
    ],
    "vantage": [
        # The Albany News — primary local source, covers Stargate/Vantage extensively
        "https://news.google.com/rss/search?q=site:thealbanynews.net&hl=en-US&gl=US&ceid=US:en",
        # KTAB/KRBC Big Country
        "https://news.google.com/rss/search?q=site:bigcountryhomepage.com+%22Shackelford%22+OR+%22data+center%22+OR+%22Vantage%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "stack_nm": [
        # Las Cruces Sun-News
        "https://news.google.com/rss/search?q=site:lcsun-news.com+%22data+center%22+OR+%22Stack%22+OR+%22Santa+Teresa%22+OR+%22Oracle%22&hl=en-US&gl=US&ceid=US:en",
        # Las Cruces Bulletin + Organ Mountain News + KRWG
        "https://news.google.com/rss/search?q=(site:lascrucesbulletin.com+OR+site:organmountainnews.com+OR+site:krwg.org)+%22data+center%22+OR+%22Dona+Ana%22&hl=en-US&gl=US&ceid=US:en",
        # El Paso TV market — bleeds heavily into Las Cruces
        "https://news.google.com/rss/search?q=(site:kvia.com+OR+site:ktsm.com+OR+site:kfoxtv.com)+%22Santa+Teresa%22+OR+%22Dona+Ana%22+OR+%22Stack%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # El Paso Matters
        "https://news.google.com/rss/search?q=site:elpasomatters.org+%22Santa+Teresa%22+OR+%22Dona+Ana%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Source NM + NM Political Report + Albuquerque Journal — statewide NM
        "https://news.google.com/rss/search?q=(site:sourcenm.com+OR+site:nmpoliticalreport.com+OR+site:abqjournal.com)+%22Santa+Teresa%22+OR+%22Dona+Ana%22+OR+%22Stack%22+data+center&hl=en-US&gl=US&ceid=US:en",
    ],
    "tract": [
        # Comstock Chronicle — most local to Storey County
        "https://news.google.com/rss/search?q=site:thecomstockchronicle.com&hl=en-US&gl=US&ceid=US:en",
        # Reno Gazette-Journal + Nevada Appeal
        "https://news.google.com/rss/search?q=(site:rgj.com+OR+site:nevadaappeal.com)+%22Storey+County%22+OR+%22TRIC%22+OR+%22Tahoe+Reno%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # KOLO TV + KRNV (MyNews4) — 2news.com deduped (same station as KTVN, listed below)
        "https://news.google.com/rss/search?q=(site:kolotv.com+OR+site:mynews4.com)+%22Storey+County%22+OR+%22TRIC%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # NNBW (broke CEO interview) + This Is Reno + Nevada Independent + Nevada Current + KTVN CBS
        "https://news.google.com/rss/search?q=(site:nnbw.com+OR+site:thisisreno.com+OR+site:thenevadaindependent.com+OR+site:nevadacurrent.com+OR+site:ktvn.com)+%22Storey%22+OR+%22Tract%22+OR+%22NVIDIA%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Storey County Gov (permits/meetings) + Nevada Newsmakers
        "https://news.google.com/rss/search?q=(site:storeycounty.org+OR+site:nevadanewsmakers.com)+%22Tract%22+OR+%22data+center%22+OR+%22permit%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "cifr_black_pearl": [
        # Odessa American + Midland Reporter-Telegram
        "https://news.google.com/rss/search?q=(site:oaoa.com+OR+site:mrt.com)+%22Wink%22+OR+%22Cipher%22+OR+%22data+center%22+OR+%22Winkler%22&hl=en-US&gl=US&ceid=US:en",
        # CBS7 + YourBasin + Permian Basin 360
        "https://news.google.com/rss/search?q=(site:cbs7.com+OR+site:yourbasin.com+OR+site:permianbasin360.com)+%22Wink%22+OR+%22Cipher+Mining%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Texas Tribune + Winkler County Government
        "https://news.google.com/rss/search?q=(site:texastribune.org+OR+site:co.winkler.tx.us)+%22Winkler+County%22+OR+%22Wink%22+data+center+Texas&hl=en-US&gl=US&ceid=US:en",
    ],
    "wulf": [
        # Niagara Gazette — primary local source for Lake Mariner
        "https://news.google.com/rss/search?q=site:niagara-gazette.com+%22data+center%22+OR+%22TeraWulf%22+OR+%22Somerset%22+OR+%22Lake+Mariner%22&hl=en-US&gl=US&ceid=US:en",
        # Buffalo News + Lockport Journal
        "https://news.google.com/rss/search?q=(site:buffalonews.com+OR+site:lockportjournal.com)+%22TeraWulf%22+OR+%22Lake+Mariner%22+OR+%22Somerset%22+data+center&hl=en-US&gl=US&ceid=US:en",
        # WGRZ + WIVB + WKBW
        "https://news.google.com/rss/search?q=(site:wgrz.com+OR+site:wivb.com+OR+site:wkbw.com)+%22TeraWulf%22+OR+%22Lake+Mariner%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Investigative Post + Orleans Hub + Spectrum Local News Buffalo
        "https://news.google.com/rss/search?q=(site:investigativepost.org+OR+site:orleanshub.com+OR+site:spectrumlocalnews.com%2Fnys%2Fbuffalo)+%22TeraWulf%22+OR+%22Lake+Mariner%22+OR+%22Somerset%22+OR+%22fire%22&hl=en-US&gl=US&ceid=US:en",
        # WBEN + Western NY Papers
        "https://news.google.com/rss/search?q=(site:wben.com+OR+site:wnypapers.com)+%22TeraWulf%22+OR+%22Lake+Mariner%22+OR+%22Somerset%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "flashc": [
        # Lubbock Avalanche-Journal
        "https://news.google.com/rss/search?q=site:lubbockonline.com+%22Abernathy%22+OR+%22data+center%22+OR+%22Fluidstack%22+OR+%22Hale+County%22&hl=en-US&gl=US&ceid=US:en",
        # KCBD + EverythingLubbock + Fox34
        "https://news.google.com/rss/search?q=(site:kcbd.com+OR+site:everythinglubbock.com+OR+site:fox34lubbock.com)+%22Abernathy%22+OR+%22data+center%22+OR+%22Fluidstack%22&hl=en-US&gl=US&ceid=US:en",
        # Plainview Daily Herald (Hale County seat) + MyFOXLubbock + KAMC
        "https://news.google.com/rss/search?q=(site:plainviewdailyherald.com+OR+site:myfoxlubbock.com+OR+site:kamc.com)+%22Abernathy%22+OR+%22Hale+County%22+OR+%22data+center%22+OR+%22Fluidstack%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "cifr_barber_lake": [
        # Colorado City Record — primary local paper
        "https://news.google.com/rss/search?q=site:coloradocityrecord.com&hl=en-US&gl=US&ceid=US:en",
        # KTAB/KRBC + KTXS
        "https://news.google.com/rss/search?q=(site:bigcountryhomepage.com+OR+site:ktxs.com)+%22Colorado+City%22+OR+%22Mitchell+County%22+OR+%22Cipher%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Abilene Reporter-News + Odessa American + Midland Reporter-Telegram — regional bleed
        "https://news.google.com/rss/search?q=(site:reporternews.com+OR+site:oaoa.com+OR+site:mrt.com)+%22Colorado+City%22+OR+%22Mitchell+County%22+OR+%22Cipher%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "apld_pf2": [
        # InForum (Fargo Forum) — primary regional source
        "https://news.google.com/rss/search?q=site:inforum.com+%22Harwood%22+OR+%22Applied+Digital%22+OR+%22APLD%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Valley News Live + KVRR
        "https://news.google.com/rss/search?q=(site:valleynewslive.com+OR+site:kvrr.com)+%22Harwood%22+OR+%22Applied+Digital%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # ND Monitor
        "https://news.google.com/rss/search?q=site:northdakotamonitor.com+%22Harwood%22+OR+%22Applied+Digital%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # KFYR-TV + WDAY + KX News
        "https://news.google.com/rss/search?q=(site:kfyrtv.com+OR+site:wday.com+OR+site:kxnet.com)+%22Harwood%22+OR+%22Applied+Digital%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # AgWeek + Prairie Business — regional trade press
        "https://news.google.com/rss/search?q=(site:agweek.com+OR+site:prairiebizmag.com)+%22Harwood%22+OR+%22Applied+Digital%22+OR+%22Cass+County%22+data+center&hl=en-US&gl=US&ceid=US:en",
    ],
    "apld": [
        # Dickey County Leader — primary local source
        "https://news.google.com/rss/search?q=site:dickeycountyleader.com&hl=en-US&gl=US&ceid=US:en",
        # Jamestown Sun + InForum
        "https://news.google.com/rss/search?q=(site:jamestownsun.com+OR+site:inforum.com)+%22Ellendale%22+OR+%22Applied+Digital%22+OR+%22CoreWeave%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "qts": [
        # The Citizen + Fayette County News — most local
        "https://news.google.com/rss/search?q=(site:thecitizen.com+OR+site:fayette-news.net)+%22data+center%22+OR+%22QTS%22+OR+%22Microsoft%22+OR+%22Fayetteville%22&hl=en-US&gl=US&ceid=US:en",
        # AJC — covers this extensively
        "https://news.google.com/rss/search?q=site:ajc.com+%22QTS%22+OR+%22Fayetteville%22+data+center+OR+Microsoft&hl=en-US&gl=US&ceid=US:en",
        # FOX 5 Atlanta
        "https://news.google.com/rss/search?q=site:fox5atlanta.com+%22Fayette%22+OR+%22QTS%22+OR+%22data+center%22+OR+%22Microsoft%22&hl=en-US&gl=US&ceid=US:en",
        # WSB-TV + Atlanta News First (broke the QTS water-use investigation)
        "https://news.google.com/rss/search?q=(site:wsbtv.com+OR+site:atlantanewsfirst.com)+%22QTS%22+OR+%22Fayette%22+OR+%22data+center%22+OR+%22Microsoft%22&hl=en-US&gl=US&ceid=US:en",
        # Patch Fayetteville + NewsBreak Fayetteville
        "https://news.google.com/rss/search?q=(site:patch.com%2Fgeorgia%2Ffayetteville+OR+site:newsbreak.com%2Ffayetteville-ga)+%22QTS%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
    ],
    # VOLTAG — identical campus to Vantage (Shackelford Co., TX)
    "voltag": [
        # Albany News — primary local paper, covers the campus extensively
        "https://news.google.com/rss/search?q=site:thealbanynews.net&hl=en-US&gl=US&ceid=US:en",
        # KTAB/KRBC Big Country
        "https://news.google.com/rss/search?q=site:bigcountryhomepage.com+%22Shackelford%22+OR+%22Albany%22+OR+%22Vantage%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # KTXS ABC Abilene
        "https://news.google.com/rss/search?q=site:ktxs.com+%22Shackelford%22+OR+%22Albany%22+OR+%22Vantage%22+OR+%22VoltaGrid%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Abilene Reporter-News (Gannett)
        "https://news.google.com/rss/search?q=site:reporternews.com+%22Shackelford%22+OR+%22Albany%22+OR+%22Vantage%22+OR+%22VoltaGrid%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Broader regional search — Shackelford + Vantage/Oracle/OpenAI/Stargate/VoltaGrid
        "https://news.google.com/rss/search?q=%22Shackelford+County%22+Texas+%22Vantage%22+OR+%22Oracle%22+OR+%22OpenAI%22+OR+%22Stargate%22+OR+%22VoltaGrid%22+data+center&hl=en-US&gl=US&ceid=US:en",
    ],
    # MERIDIAN — New Lebanon, Sullivan County, IN
    "meridian": [
        # Sullivan Times + Sun-Commercial (Vincennes) + WBIW — closest local outlets
        "https://news.google.com/rss/search?q=(site:sullivan-times.com+OR+site:suncommercial.com+OR+site:wbiw.com)+%22Sullivan+County%22+OR+%22data+center%22+OR+%22Fluidstack%22&hl=en-US&gl=US&ceid=US:en",
        # Terre Haute Tribune-Star + WTHI-TV + MyWabashValley — Terre Haute regional
        "https://news.google.com/rss/search?q=(site:tribstar.com+OR+site:wthitv.com+OR+site:mywabashvalley.com)+%22Sullivan+County%22+OR+%22data+center%22+OR+%22Fluidstack%22+OR+%22New+Lebanon%22&hl=en-US&gl=US&ceid=US:en",
        # Inside Indiana Business + Indiana Public Media + InkFreeNews — statewide business/public
        "https://news.google.com/rss/search?q=(site:insideindianabusiness.com+OR+site:ipm.org+OR+site:inkfreenews.com)+%22Sullivan%22+OR+%22data+center%22+Indiana+%22Google%22+OR+%22Fluidstack%22&hl=en-US&gl=US&ceid=US:en",
        # WIBC Indianapolis
        "https://news.google.com/rss/search?q=site:wibc.com+%22Sullivan+County%22+OR+%22data+center%22+Indiana+%22Google%22+OR+%22Fluidstack%22&hl=en-US&gl=US&ceid=US:en",
    ],
    # EDGED COMPUTE — Aurora, IL (Chicago) + Atlanta, GA
    "edged_compute": [
        # Aurora Beacon-News + Daily Herald + Aurora Patch — local Aurora/suburban IL
        "https://news.google.com/rss/search?q=(site:suburbantribune.com+OR+site:dailyherald.com+OR+site:patch.com%2Fillinois%2Faurora)+%22Aurora%22+OR+%22data+center%22+OR+%22CoreWeave%22+OR+%22Edged%22&hl=en-US&gl=US&ceid=US:en",
        # Chicago Construction News + Crain's Chicago Business + Illinois Times — business/trade press
        "https://news.google.com/rss/search?q=(site:chicagoconstructionnews.com+OR+site:chicagobusiness.com+OR+site:illinoistimes.com)+%22Aurora%22+OR+%22data+center%22+OR+%22CoreWeave%22+OR+%22Edged%22&hl=en-US&gl=US&ceid=US:en",
        # WGN + CBS Chicago + Fox 32 + NPR Illinois — Chicago broadcast
        "https://news.google.com/rss/search?q=(site:wgntv.com+OR+site:cbsnews.com+OR+site:fox32chicago.com+OR+site:nprillinois.org)+%22Aurora%22+%22data+center%22+OR+%22CoreWeave%22+OR+%22Edged%22&hl=en-US&gl=US&ceid=US:en",
        # AJC + Saporta Report + Patch Atlanta — Atlanta local
        "https://news.google.com/rss/search?q=(site:ajc.com+OR+site:saportareport.com+OR+site:patch.com%2Fgeorgia%2Fatlanta)+%22data+center%22+OR+%22CoreWeave%22+OR+%22Alibaba%22+OR+%22Edged%22&hl=en-US&gl=US&ceid=US:en",
        # Atlanta Business Chronicle + Atlanta Civic Circle + Rough Draft Atlanta — Atlanta business/civic
        "https://news.google.com/rss/search?q=(site:bizjournals.com+OR+site:atlantaciviccircle.org+OR+site:roughdraftatlanta.com)+%22Atlanta%22+%22data+center%22+OR+%22CoreWeave%22+OR+%22Alibaba%22+OR+%22Edged%22&hl=en-US&gl=US&ceid=US:en",
    ],
    # CORE SCIENTIFIC — Multiple sites: Marble NC / Dalton GA / Denton TX / Muskogee OK / Austin TX
    "core_scientific": [
        # Cherokee Scout + Smoky Mountain News + Mountain Xpress + WLOS — Marble/Cherokee County NC
        "https://news.google.com/rss/search?q=(site:cherokeescout.com+OR+site:smokymountainnews.com+OR+site:mountainx.com+OR+site:wlos.com)+%22data+center%22+OR+%22Core+Scientific%22+OR+%22Cherokee%22&hl=en-US&gl=US&ceid=US:en",
        # Asheville Citizen-Times + Blue Ridge Public Radio + WFAE + WUNC + NC Newsroom — regional NC
        "https://news.google.com/rss/search?q=(site:citizen-times.com+OR+site:bpr.org+OR+site:wfae.org+OR+site:wunc.org+OR+site:ncnewsroom.org)+%22data+center%22+OR+%22Core+Scientific%22+OR+%22CoreWeave%22+OR+%22Cherokee%22&hl=en-US&gl=US&ceid=US:en",
        # Dalton Daily Citizen + NW Georgia News + NewsChannel 9 + Chattanooga TFP — Dalton GA
        "https://news.google.com/rss/search?q=(site:daltoncitizen.com+OR+site:northwestgeorgianews.com+OR+site:newschannel9.com+OR+site:timesfreepress.com)+%22Dalton%22+%22data+center%22+OR+%22Core+Scientific%22+OR+%22CoreWeave%22&hl=en-US&gl=US&ceid=US:en",
        # Denton Record-Chronicle + Cross Timbers Gazette + Denton Patch — Denton TX local
        "https://news.google.com/rss/search?q=(site:dentonrc.com+OR+site:crosstimbersgazette.com+OR+site:patch.com%2Ftexas%2Fdenton)+%22data+center%22+OR+%22Core+Scientific%22+OR+%22CoreWeave%22+OR+%22CORZ%22&hl=en-US&gl=US&ceid=US:en",
        # Dallas Morning News + WFAA + NBC DFW + Fort Worth Star-Telegram — DFW regional
        "https://news.google.com/rss/search?q=(site:dallasnews.com+OR+site:wfaa.com+OR+site:nbcdfw.com+OR+site:star-telegram.com)+%22Denton%22+%22data+center%22+OR+%22Core+Scientific%22+OR+%22CoreWeave%22&hl=en-US&gl=US&ceid=US:en",
        # Muskogee Phoenix + Tulsa World + News on 6 + KJRH — Muskogee OK
        "https://news.google.com/rss/search?q=(site:muskogeephoenix.com+OR+site:tulsaworld.com+OR+site:newson6.com+OR+site:kjrh.com)+%22data+center%22+OR+%22Core+Scientific%22+OR+%22Muskogee%22+OR+%22CoreWeave%22&hl=en-US&gl=US&ceid=US:en",
        # Fox23 + Journal Record + OKC Fox — broader Oklahoma coverage
        "https://news.google.com/rss/search?q=(site:fox23.com+OR+site:journalrecord.com+OR+site:okcfox.com)+%22data+center%22+OR+%22Core+Scientific%22+OR+%22CoreWeave%22+OR+%22Oklahoma%22&hl=en-US&gl=US&ceid=US:en",
        # Austin Statesman + Austin Monitor + Community Impact — Austin TX local
        "https://news.google.com/rss/search?q=(site:statesman.com+OR+site:austinmonitor.com+OR+site:communityimpact.com)+%22data+center%22+OR+%22Core+Scientific%22+OR+%22CoreWeave%22+OR+%22CORZ%22&hl=en-US&gl=US&ceid=US:en",
        # KUT + KVUE + KXAN + Austin Business Journal — Austin broadcast + business
        "https://news.google.com/rss/search?q=(site:kut.org+OR+site:kvue.com+OR+site:kxan.com+OR+site:bizjournals.com)+%22data+center%22+OR+%22Core+Scientific%22+OR+%22CoreWeave%22+%22Austin%22&hl=en-US&gl=US&ceid=US:en",
        # CoStar — broke the $6.1B Denton conversion story
        "https://news.google.com/rss/search?q=site:costar.com+%22Core+Scientific%22+OR+%22Denton%22+data+center&hl=en-US&gl=US&ceid=US:en",
    ],
    "sbe_softbank": [
        # Austin American-Statesman + Austin Business Journal
        "https://news.google.com/rss/search?q=(site:statesman.com+OR+site:bizjournals.com/austin)+%22Softbank%22+OR+%22SBE%22+OR+%22data+center%22+Austin&hl=en-US&gl=US&ceid=US:en",
        # KVUE + KXAN + Austin Monitor
        "https://news.google.com/rss/search?q=(site:kvue.com+OR+site:kxan.com+OR+site:austinmonitor.com)+%22Softbank%22+OR+%22data+center%22+Austin&hl=en-US&gl=US&ceid=US:en",
    ],
    "hut_beacon_point": [
        # Caller-Times — primary Corpus Christi daily
        "https://news.google.com/rss/search?q=site:caller.com+%22Nueces+County%22+OR+%22NVIDIA%22+OR+%22data+center%22+OR+%22Robstown%22+OR+%22Beacon+Point%22&hl=en-US&gl=US&ceid=US:en",
        # KRIS 6 + KIII-TV + Fox 38 — Corpus Christi broadcast
        "https://news.google.com/rss/search?q=(site:kristv.com+OR+site:kiiitv.com+OR+site:fox38corpuschristi.com)+%22data+center%22+OR+%22NVIDIA%22+OR+%22Nueces%22+OR+%22HUT%22&hl=en-US&gl=US&ceid=US:en",
        # CC Biz News + Texas Tribune + San Antonio Express-News
        "https://news.google.com/rss/search?q=(site:ccbiznews.com+OR+site:texastribune.org+OR+site:expressnews.com)+%22Nueces+County%22+OR+%22Corpus+Christi%22+%22data+center%22+OR+%22NVIDIA%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "tract_d": [
        # Comstock Chronicle — most local to Storey County
        "https://news.google.com/rss/search?q=site:thecomstockchronicle.com&hl=en-US&gl=US&ceid=US:en",
        # Reno Gazette-Journal + Nevada Appeal
        "https://news.google.com/rss/search?q=(site:rgj.com+OR+site:nevadaappeal.com)+%22Storey+County%22+OR+%22TRIC%22+OR+%22Tahoe+Reno%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # KOLO TV + KRNV (MyNews4) — 2news.com deduped (same station as KTVN, listed below)
        "https://news.google.com/rss/search?q=(site:kolotv.com+OR+site:mynews4.com)+%22Storey+County%22+OR+%22TRIC%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # NNBW (broke CEO interview) + This Is Reno + Nevada Independent + Nevada Current + KTVN CBS
        "https://news.google.com/rss/search?q=(site:nnbw.com+OR+site:thisisreno.com+OR+site:thenevadaindependent.com+OR+site:nevadacurrent.com+OR+site:ktvn.com)+%22Storey%22+OR+%22Tract%22+OR+%22NVIDIA%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Storey County Gov (permits/meetings) + Nevada Newsmakers
        "https://news.google.com/rss/search?q=(site:storeycounty.org+OR+site:nevadanewsmakers.com)+%22Tract%22+OR+%22data+center%22+OR+%22permit%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "polar_dc": [
        # Drangedalsposten + Porsgrunns Dagblad + Telemarksavisa + Kragerø Blad Vestmar
        "https://news.google.com/rss/search?q=(site:drangedalsposten.no+OR+site:pd.no+OR+site:ta.no+OR+site:kv.no)+datasenter+OR+PolarDC+OR+Herøya+OR+Drangedal&hl=no&gl=NO&ceid=NO:no",
        # NRK Vestfold og Telemark (regional bureau) — state broadcaster
        "https://news.google.com/rss/search?q=site:nrk.no/vestfoldogtelemark+datasenter+OR+%22PolarDC%22+OR+%22Drangedal%22+OR+%22Herøya%22&hl=no&gl=NO&ceid=NO:no",
        # E24 + Dagens Næringsliv — Norwegian business press
        "https://news.google.com/rss/search?q=(site:e24.no+OR+site:dn.no)+datasenter+OR+PolarDC+OR+Crusoe+OR+CoreWeave+OR+Drangedal&hl=no&gl=NO&ceid=NO:no",
        # Varden + enerWE + Teknisk Ukeblad — regional + energy trade press
        "https://news.google.com/rss/search?q=(site:varden.no+OR+site:enerwe.no+OR+site:tu.no)+datasenter+OR+PolarDC+OR+Crusoe+OR+Drangedal+OR+Herøya&hl=no&gl=NO&ceid=NO:no",
    ],
    "prime_dc": [
        # Journal & Topics + Daily Herald + Elk Grove Patch — suburban local
        "https://news.google.com/rss/search?q=(site:journal-topics.com+OR+site:dailyherald.com+OR+site:patch.com/illinois/elkgrove)+%22data+center%22+OR+%22CoreWeave%22+OR+%22Prime+DC%22+OR+%22Elk+Grove%22&hl=en-US&gl=US&ceid=US:en",
        # Chicago Construction News + Crain's Chicago Business
        "https://news.google.com/rss/search?q=(site:chicagoconstructionnews.com+OR+site:chicagobusiness.com)+%22Elk+Grove%22+OR+%22data+center%22+OR+%22CoreWeave%22+OR+%22Prime+DC%22&hl=en-US&gl=US&ceid=US:en",
        # WGN + CBS Chicago — broadcast
        "https://news.google.com/rss/search?q=(site:wgntv.com+OR+site:cbsnews.com)+%22Elk+Grove%22+%22data+center%22+OR+%22CoreWeave%22+OR+%22Prime+DC%22&hl=en-US&gl=US&ceid=US:en",
    ],
    # SOPAIPILLA — El Paso, TX (Meta)
    "sopaipilla": [
        # El Paso Times + El Paso Matters — primary daily + investigative nonprofit
        "https://news.google.com/rss/search?q=(site:elpasotimes.com+OR+site:elpasomatters.org)+%22data+center%22+OR+%22Meta%22+OR+%22Sopaipilla%22&hl=en-US&gl=US&ceid=US:en",
        # El Paso TV market — KVIA / KTSM / CBS4 / KFOX
        "https://news.google.com/rss/search?q=(site:kvia.com+OR+site:ktsm.com+OR+site:cbs4local.com+OR+site:kfoxtv.com)+%22data+center%22+OR+%22Meta%22&hl=en-US&gl=US&ceid=US:en",
        # El Paso Inc. + Spotlight EP — business / local
        "https://news.google.com/rss/search?q=(site:elpasoinc.com+OR+site:spotlightepnews.com)+%22data+center%22+OR+%22Meta%22&hl=en-US&gl=US&ceid=US:en",
        # Diario.mx (Spanish-language) + NewsBreak El Paso
        "https://news.google.com/rss/search?q=(site:diario.mx%2Felpaso+OR+site:newsbreak.com%2Fel-paso-tx)+%22data+center%22+OR+%22Meta%22&hl=en-US&gl=US&ceid=US:en",
    ],
    # GALAXY HELIOS — Afton, Dickens County, TX (Galaxy / CoreWeave)
    "glxy_helios": [
        # The Texas Spur — the dedicated Dickens County paper (broke the lease story)
        "https://news.google.com/rss/search?q=site:thetexasspur.com&hl=en-US&gl=US&ceid=US:en",
        # Lubbock market — EverythingLubbock (broke Helios Phase 2) / Avalanche-Journal / KCBD / Fox34
        "https://news.google.com/rss/search?q=(site:everythinglubbock.com+OR+site:lubbockonline.com+OR+site:kcbd.com+OR+site:fox34lubbock.com)+%22data+center%22+OR+%22Helios%22+OR+%22Galaxy%22+OR+%22Afton%22&hl=en-US&gl=US&ceid=US:en",
        # NewsBreak — Spur, TX local aggregator
        "https://news.google.com/rss/search?q=site:newsbreak.com%2Fspur-tx&hl=en-US&gl=US&ceid=US:en",
    ],
    # SBE MILAM — Milam County, TX (OpenAI / SBE)
    "sbe_milam": [
        # Milam County weeklies — Cameron Herald + Rockdale Reporter
        "https://news.google.com/rss/search?q=(site:cameronherald.com+OR+site:rockdalereporter.com)+%22data+center%22+OR+%22OpenAI%22+OR+%22Stargate%22&hl=en-US&gl=US&ceid=US:en",
        # Waco / Temple / Bryan-College Station market — KWTX / KXXV / FOX 44 (Milam-scoped) / Temple Telegram / KBTX
        "https://news.google.com/rss/search?q=(site:kwtx.com+OR+site:kxxv.com+OR+site:fox44news.com%2Fnews%2Flocal-news%2Fmilam-county+OR+site:tdtnews.com+OR+site:kbtx.com)+%22Milam%22+OR+%22data+center%22+OR+%22Stargate%22&hl=en-US&gl=US&ceid=US:en",
        # Austin Business Journal (broke SBE/Stargate) + Bryan-College Station Eagle
        "https://news.google.com/rss/search?q=(site:bizjournals.com+OR+site:theeagle.com)+%22Milam%22+%22data+center%22+OR+%22Stargate%22+OR+%22SBE%22&hl=en-US&gl=US&ceid=US:en",
        # NewsBreak — Cameron, TX local aggregator
        "https://news.google.com/rss/search?q=site:newsbreak.com%2Fcameron-tx&hl=en-US&gl=US&ceid=US:en",
    ],
    # QTS MAGNOLIA — 12-asset portfolio: Richmond VA / Phoenix + Glendale AZ / New Albany (Columbus) OH / DFW TX
    "qts_magnolia": [
        # Columbus / New Albany, OH — Dispatch, NBC4, 10TV, ThisWeek
        "https://news.google.com/rss/search?q=(site:dispatch.com+OR+site:nbc4i.com+OR+site:10tv.com+OR+site:thisweeknews.com)+%22QTS%22+OR+%22New+Albany%22+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Columbus business/civic — Business First (broke CRA), Columbus Underground, News 5 Cleveland
        "https://news.google.com/rss/search?q=(site:bizjournals.com+OR+site:columbusunderground.com+OR+site:news5cleveland.com)+%22QTS%22+OR+%22New+Albany%22+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Phoenix, AZ — azcentral, 12 News, azfamily, ABC15, FOX10
        "https://news.google.com/rss/search?q=(site:azcentral.com+OR+site:12news.com+OR+site:azfamily.com+OR+site:abc15.com+OR+site:fox10phoenix.com)+%22QTS%22+OR+%22data+center%22+Phoenix+OR+Glendale&hl=en-US&gl=US&ceid=US:en",
        # West Valley hyperlocal (PHX3 / Glendale-Litchfield Park) — Glendale Star, West Valley View, AZ Big Media
        "https://news.google.com/rss/search?q=(site:glendalestar.com+OR+site:westvalleyview.com+OR+site:azbigmedia.com)+%22data+center%22+OR+%22QTS%22&hl=en-US&gl=US&ceid=US:en",
        # Dallas-Fort Worth, TX (Irving / Las Colinas / Wilmer) — DMN, Star-Telegram, WFAA, NBC DFW, FOX4
        "https://news.google.com/rss/search?q=(site:dallasnews.com+OR+site:star-telegram.com+OR+site:wfaa.com+OR+site:nbcdfw.com+OR+site:fox4news.com)+%22QTS%22+OR+%22data+center%22+Irving+OR+Wilmer+OR+%22Las+Colinas%22&hl=en-US&gl=US&ceid=US:en",
        # DFW business/local — Dallas Business Journal (broke Wilmer TDLR), Fort Worth Report, Irving Rambler
        "https://news.google.com/rss/search?q=(site:bizjournals.com+OR+site:fortworthreport.org+OR+site:irvingrambler.com)+%22QTS%22+OR+%22data+center%22+Irving+OR+Wilmer&hl=en-US&gl=US&ceid=US:en",
        # Richmond / Henrico, VA (RIC1 + RIC3 campuses, 255 MW) — Times-Dispatch, BizSense, Henrico Citizen, Virginia Business
        "https://news.google.com/rss/search?q=(site:richmond.com+OR+site:richmondbizsense.com+OR+site:henricocitizen.com+OR+site:virginiabusiness.com)+%22QTS%22+OR+%22data+center%22+OR+%22Henrico%22&hl=en-US&gl=US&ceid=US:en",
        # Richmond broadcast + public media — NBC12, WRIC 8News, WTVR CBS6, VPM
        "https://news.google.com/rss/search?q=(site:nbc12.com+OR+site:wric.com+OR+site:wtvr.com+OR+site:vpm.org)+%22QTS%22+OR+%22data+center%22+OR+%22Henrico%22&hl=en-US&gl=US&ceid=US:en",
        # CBS News Texas + CultureMap Dallas + Columbus Alive + Phoenix New Times + Richmond Free Press + Cardinal News
        "https://news.google.com/rss/search?q=(site:cbsnews.com%2Ftexas+OR+site:culturemap.com%2Fdallas+OR+site:columbusalive.com+OR+site:phoenixnewtimes.com+OR+site:richmondfreepress.com+OR+site:cardinalnews.org)+%22QTS%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "cifr_stingray": [
        # Andrews County News — primary local twice-weekly paper
        "https://news.google.com/rss/search?q=site:andrewscountynews.com&hl=en-US&gl=US&ceid=US:en",
        # Odessa American + Midland Reporter-Telegram
        "https://news.google.com/rss/search?q=(site:oaoa.com+OR+site:mrt.com)+%22Andrews%22+OR+%22Cipher%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # KMID/KPEJ (YourBasin) + KOSA/First Alert 7 + Permian Basin 360
        "https://news.google.com/rss/search?q=(site:yourbasin.com+OR+site:cbs7.com+OR+site:permianbasin360.com)+%22Andrews%22+OR+%22Cipher+Mining%22+OR+%22Stingray%22&hl=en-US&gl=US&ceid=US:en",
        # Texas Tribune
        "https://news.google.com/rss/search?q=site:texastribune.org+%22Andrews+County%22+OR+%22data+center%22+Texas&hl=en-US&gl=US&ceid=US:en",
    ],
    "apld_eln4": [
        # InForum + Jamestown Sun — same pattern as APLD (shared Ellendale campus)
        "https://news.google.com/rss/search?q=(site:jamestownsun.com+OR+site:inforum.com)+%22Ellendale%22+OR+%22Applied+Digital%22+OR+%22ELN4%22+OR+%22CoreWeave%22&hl=en-US&gl=US&ceid=US:en",
        # KFYR-TV + Valley News Live + WDAY + Bismarck Tribune
        "https://news.google.com/rss/search?q=(site:kfyrtv.com+OR+site:valleynewslive.com+OR+site:wday.com+OR+site:bismarcktribune.com)+%22Ellendale%22+OR+%22Applied+Digital%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # ND Monitor + South Dakota Searchlight + Aberdeen American News + Prairie Business
        "https://news.google.com/rss/search?q=(site:northdakotamonitor.com+OR+site:southdakotasearchlight.com+OR+site:aberdeennews.com+OR+site:prairiebizmag.com)+%22Ellendale%22+OR+%22Applied+Digital%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "yondr_nova": [
        # Loudoun Times-Mirror + Loudoun Now — primary local weeklies
        "https://news.google.com/rss/search?q=(site:loudountimes.com+OR+site:loudounnow.com)+%22Yondr%22+OR+%22Aldie%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # The Burn + Blue Ridge Leader + InsideNoVa + Patch (Ashburn/Leesburg)
        "https://news.google.com/rss/search?q=(site:theburn.com+OR+site:blueridgeleader.com+OR+site:insidenova.com)+%22Loudoun%22+OR+%22Aldie%22+OR+%22Yondr%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # DC broadcast — WTOP, NBC4, FOX5 DC, WJLA, DC News Now
        "https://news.google.com/rss/search?q=(site:wtop.com+OR+site:nbcwashington.com+OR+site:fox5dc.com+OR+site:wjla.com+OR+site:dcnewsnow.com)+%22Loudoun+County%22+%22data+center%22+OR+%22Yondr%22&hl=en-US&gl=US&ceid=US:en",
        # Washington Business Journal
        "https://news.google.com/rss/search?q=site:bizjournals.com+%22Yondr%22+OR+%22Loudoun%22+data+center+Virginia&hl=en-US&gl=US&ceid=US:en",
    ],
    "zenith": [
        # Okmulgee Times — primary local weekly
        "https://news.google.com/rss/search?q=site:yourokmulgee.com&hl=en-US&gl=US&ceid=US:en",
        # Tulsa broadcast — KTUL, KJRH, News on 6, Fox23
        "https://news.google.com/rss/search?q=(site:ktul.com+OR+site:kjrh.com+OR+site:newson6.com+OR+site:fox23.com)+%22Okmulgee%22+%22data+center%22+OR+%22Zenith%22+OR+%22Fluidstack%22&hl=en-US&gl=US&ceid=US:en",
        # Tulsa World + The Frontier + Journal Record
        "https://news.google.com/rss/search?q=(site:tulsaworld.com+OR+site:readfrontier.org+OR+site:journalrecord.com)+%22Okmulgee%22+OR+%22Zenith%22+OR+%22Jane+Street%22+OR+%22Fluidstack%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "digital_drive": [
        # Richmond Times-Dispatch + BizSense
        "https://news.google.com/rss/search?q=(site:richmond.com+OR+site:richmondbizsense.com)+%22Chesterfield%22+%22data+center%22+OR+%22CoreWeave%22+OR+%22Digital+Drive%22&hl=en-US&gl=US&ceid=US:en",
        # WTVR CBS6 + WRIC 8News + NBC12
        "https://news.google.com/rss/search?q=(site:wtvr.com+OR+site:wric.com+OR+site:nbc12.com)+%22Chesterfield%22+%22data+center%22+OR+%22CoreWeave%22&hl=en-US&gl=US&ceid=US:en",
        # Virginia Business + VPM + Cardinal News
        "https://news.google.com/rss/search?q=(site:virginiabusiness.com+OR+site:vpm.org+OR+site:cardinalnews.org)+%22Chesterfield%22+%22data+center%22+OR+%22CoreWeave%22&hl=en-US&gl=US&ceid=US:en",
        # Greater Richmond Partnership
        "https://news.google.com/rss/search?q=site:grpva.com+%22data+center%22+OR+%22CoreWeave%22+OR+%22Chesterfield%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "novva_lv_reno": [
        # Las Vegas Review-Journal + LV Sun
        "https://news.google.com/rss/search?q=(site:reviewjournal.com+OR+site:lasvegassun.com)+%22Novva%22+OR+%22North+Las+Vegas%22+%22data+center%22+OR+%22CoreWeave%22+OR+%22eBay%22&hl=en-US&gl=US&ceid=US:en",
        # LV broadcast — 8NewsNow, KTNV, Fox5 Vegas, News3LV
        "https://news.google.com/rss/search?q=(site:8newsnow.com+OR+site:ktnv.com+OR+site:fox5vegas.com+OR+site:news3lv.com)+%22Novva%22+OR+%22data+center%22+%22North+Las+Vegas%22&hl=en-US&gl=US&ceid=US:en",
        # Reno Gazette-Journal + This Is Reno + NNBW
        "https://news.google.com/rss/search?q=(site:rgj.com+OR+site:thisisreno.com+OR+site:nnbw.com)+%22Novva%22+OR+%22Reno%22+%22data+center%22+OR+%22CoreWeave%22&hl=en-US&gl=US&ceid=US:en",
        # Reno broadcast — KOLO, KRNV/MyNews4, KTVN + Nevada Appeal
        "https://news.google.com/rss/search?q=(site:kolotv.com+OR+site:mynews4.com+OR+site:ktvn.com+OR+site:nevadaappeal.com)+%22Novva%22+OR+%22Reno%22+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Nevada Independent
        "https://news.google.com/rss/search?q=site:thenevadaindependent.com+%22Novva%22+OR+%22data+center%22+Nevada+CoreWeave&hl=en-US&gl=US&ceid=US:en",
    ],
}

# ── NewsAPI targeted queries ─────────────────────────────────────────────────
NEWSAPI_QUERIES = {
    "beignet": [
        '"Project Beignet" Meta datacenter Louisiana',
        'Meta "Richland Parish" datacenter hyperscale construction',
        'Meta datacenter Louisiana 2025 OR 2026 construction',
    ],
    "hut_google": [
        '"HUT 8" Google datacenter Louisiana "River Bend"',
        '"HUT 8" OR "HUT8" Google datacenter Louisiana construction 2025 OR 2026',
        'HUT Google "River Bend" OR "St Francisville" Louisiana datacenter hyperscale',
    ],
    "related_bx": [
        '"Related Companies" Oracle datacenter Michigan "Washtenaw"',
        'Related Blackstone Oracle "Ann Arbor" datacenter construction',
        'Oracle hyperscale Michigan datacenter 2025 OR 2026',
    ],
    "vantage": [
        '"Vantage Data Centers" Oracle "Shackelford" Texas Stargate',
        '"Vantage Data Centers" Oracle hyperscale Texas construction',
        'Vantage Oracle Wisconsin datacenter construction 2025 OR 2026',
    ],
    "stack_nm": [
        '"Stack Infrastructure" Oracle "Santa Teresa" OR "Dona Ana" "New Mexico"',
        '"Stack Infrastructure" Oracle hyperscale "New Mexico" construction',
        'Stack Oracle "New Mexico" datacenter 2025 OR 2026',
    ],
    "tract": [
        'Tract Fleet NVIDIA datacenter "Storey County" Nevada',
        'NVIDIA datacenter Nevada "Storey County" OR "Tahoe Reno" construction',
        'TRIC datacenter Nevada NVIDIA 2025 OR 2026',
    ],
    "cifr_black_pearl": [
        '"Cipher Mining" "Black Pearl" Amazon datacenter "Wink" Texas',
        '"Cipher Mining" Amazon datacenter Texas construction 2025 OR 2026',
        'CIFR "Black Pearl" Amazon AWS "West Texas" datacenter',
    ],
    "wulf": [
        '"TeraWulf" "Lake Mariner" Google Fluidstack datacenter',
        '"TeraWulf" datacenter "New York" Google construction 2025 OR 2026',
        'TeraWulf WULF "Lake Mariner" Somerset "New York" expansion',
    ],
    "flashc": [
        'Fluidstack Google "Abernathy" Texas datacenter FLASHC Hypertec',
        '"Fluidstack" Google Texas datacenter construction 2025 OR 2026',
        'FLASHC Hypertec "Abernathy" OR "Hale County" Texas datacenter',
    ],
    "cifr_barber_lake": [
        '"Cipher Mining" "Barber Lake" Google "Colorado City" Texas',
        '"Cipher Mining" Google "Colorado City" OR "Mitchell County" Texas datacenter',
        'CIFR "Barber Lake" Google AWS datacenter Texas 2025 OR 2026',
    ],
    "apld_pf2": [
        '"Applied Digital" Oracle "Harwood" "North Dakota" datacenter',
        '"Applied Digital" OR "APLD" Oracle "Cass County" "North Dakota" construction',
        'APLD PF-2 Oracle "North Dakota" datacenter 2025 OR 2026',
    ],
    "apld": [
        '"Applied Digital" CoreWeave "Ellendale" "North Dakota" datacenter',
        '"Applied Digital" OR "APLD" CoreWeave Meta "North Dakota" construction',
        'APLD CoreWeave "Ellendale" OR "Dickey County" "North Dakota" datacenter',
    ],
    "voltag": [
        'Vantage Oracle "Shackelford County" Texas datacenter OpenAI Stargate',
        '"Vantage Data Centers" Oracle "Albany" OR "Shackelford" Texas construction 2025 OR 2026',
        '"VoltaGrid" Oracle natural gas power datacenter Texas',
    ],
    "qts": [
        '"QTS" Microsoft "Fayetteville" Georgia datacenter "Project Excalibur"',
        '"QTS Realty" Microsoft "Fayette County" Georgia datacenter construction',
        '"QTS" MSFT Microsoft Georgia AI superfactory datacenter 2025 OR 2026',
    ],
    "meridian": [
        '"Meridian" "Sullivan County" Indiana datacenter Google Fluidstack',
        '"New Lebanon" Indiana datacenter construction Fluidstack "Next Frontier"',
        'Meridian datacenter Indiana Google "Next Frontier" construction 2025 OR 2026',
    ],
    "edged_compute": [
        '"Edged Compute" OR "Edged Energy" CoreWeave Alibaba Aurora Illinois datacenter',
        '"Edged Compute" CoreWeave OR Alibaba Atlanta Georgia datacenter construction',
        'Edged Koch datacenter Illinois OR Georgia CoreWeave OR Alibaba 2025 OR 2026',
    ],
    "core_scientific": [
        '"Core Scientific" CoreWeave HPC hosting datacenter 2025 OR 2026',
        '"CORZ" OR "Core Scientific" datacenter CoreWeave construction lease',
        '"Core Scientific" datacenter Texas OR "North Carolina" OR Georgia OR Oklahoma CoreWeave',
    ],
    "sbe_softbank": [
        '"SBE" OR "Softbank" datacenter Austin Texas construction 2025 OR 2026',
        'Softbank "Austin" Texas AI datacenter hyperscale',
        'SBE Softbank "Travis County" OR Austin Texas datacenter',
    ],
    "hut_beacon_point": [
        '"HUT 8" NVIDIA datacenter "Nueces County" OR "Corpus Christi" Texas',
        '"HUT 8" OR "Beacon Point" NVIDIA Texas datacenter construction 2025 OR 2026',
        'HUT NVIDIA "Nueces County" OR Robstown Texas hyperscale datacenter',
    ],
    "tract_d": [
        'Tract Fleet NVIDIA datacenter "Storey County" Nevada',
        'NVIDIA datacenter Nevada "Storey County" OR "Tahoe Reno" construction',
        'TRIC datacenter Nevada NVIDIA 2025 OR 2026',
    ],
    "polar_dc": [
        '"PolarDC" OR "Polar DC" Norway Crusoe CoreWeave datacenter',
        '"PolarDC" Norway datasenter Drangedal OR Herøya OR Porsgrunn',
        'Crusoe CoreWeave Norway datacenter construction 2025 OR 2026',
    ],
    "prime_dc": [
        '"Prime DC" OR "Prime ORD" CoreWeave "Elk Grove" Illinois datacenter',
        '"Prime DC" CoreWeave datacenter Chicago OR "Elk Grove Village" construction',
        'CoreWeave "Elk Grove" OR "Cook County" Illinois datacenter 2025 OR 2026',
    ],
    "sopaipilla": [
        '"Meta" "El Paso" data center OR datacenter',
        'Meta datacenter "El Paso" Texas construction 2026',
        '"Project Sopaipilla" OR "Meta El Paso" hyperscale',
    ],
    "glxy_helios": [
        '"Helios" Galaxy OR CoreWeave data center Texas',
        'Galaxy Digital "Dickens County" OR "Afton" data center',
        'CoreWeave Helios Texas hyperscale 2026 construction',
    ],
    "sbe_milam": [
        '"Milam County" data center OpenAI OR Stargate',
        'SBE OR "Energy Global" Milam Texas data center',
        'OpenAI Stargate "Milam" OR "Rosebud" Texas 2026',
    ],
    "qts_magnolia": [
        '"QTS" data center "New Albany" OR Columbus Ohio',
        '"QTS" OR "Magnolia" data center Phoenix OR Glendale Arizona',
        '"QTS" data center Irving OR Wilmer OR "Fort Worth" Texas',
    ],
    "cifr_stingray": [
        '"Cipher Mining" "Stingray" Amazon datacenter "Andrews County" Texas',
        '"Cipher Mining" Amazon AWS datacenter "Andrews County" Texas construction 2025 OR 2026',
        'CIFR Stingray Amazon "Permian Basin" Texas datacenter',
    ],
    "apld_eln4": [
        '"Applied Digital" CoreWeave "Ellendale" "ELN4" "North Dakota" datacenter',
        '"Applied Digital" OR "APLD" CoreWeave Meta "Dickey County" "North Dakota" construction',
        'APLD ELN4 CoreWeave "North Dakota" datacenter 2025 OR 2026',
    ],
    "yondr_nova": [
        '"Yondr" "NoVa North" OR "Aldie" Oracle Virginia datacenter',
        'Yondr data center "Loudoun County" Virginia Oracle construction 2025 OR 2026',
        '"Yondr" Virginia hyperscale datacenter Aldie Loudoun',
    ],
    "zenith": [
        '"Zenith" Fluidstack "Jane Street" Oklahoma datacenter',
        'Fluidstack Oklahoma "Okmulgee" data center construction 2025 OR 2026',
        '"Jane Street" Oklahoma datacenter Zenith Fluidstack',
    ],
    "digital_drive": [
        '"Digital Drive" CoreWeave "Chesterfield County" Virginia datacenter',
        'CoreWeave Virginia "Chesterfield" data center construction 2025 OR 2026',
        '"Chesterfield County" Virginia data center CoreWeave "Blue Owl"',
    ],
    "novva_lv_reno": [
        '"Novva" CoreWeave eBay "North Las Vegas" OR Reno Nevada datacenter',
        'Novva Data Centers Nevada CoreWeave construction 2025 OR 2026',
        '"Novva" CIM Reno OR "North Las Vegas" data center eBay',
    ],
}

# ── Industry RSS keywords per bond ───────────────────────────────────────────
DC_INDUSTRY_KEYWORDS = {
    "beignet":          ["meta", "louisiana", "richland", "beignet", "holly ridge"],
    "hut_google":       ["hut", "hut 8", "google", "louisiana", "river bend", "fluidstack"],
    "related_bx":       ["related", "oracle", "michigan", "washtenaw", "ann arbor"],
    "vantage":          ["vantage", "oracle", "shackelford", "texas", "stargate"],
    "stack_nm":         ["stack", "oracle", "new mexico", "santa teresa"],
    "tract":            ["tract", "nvidia", "nevada", "storey", "tric", "fleet"],
    "cifr_black_pearl": ["cipher", "cifr", "amazon", "wink", "texas", "black pearl"],
    "wulf":             ["terawulf", "wulf", "lake mariner", "fluidstack", "google", "new york", "somerset", "barker", "fire"],
    "flashc":           ["fluidstack", "abernathy", "hypertec", "google", "hale county"],
    "cifr_barber_lake": ["cipher", "cifr", "barber lake", "colorado city", "google", "mitchell"],
    "apld_pf2":         ["applied digital", "apld", "oracle", "harwood", "north dakota", "cass"],
    "apld":             ["applied digital", "apld", "coreweave", "ellendale", "north dakota"],
    "voltag":           ["vantage", "oracle", "shackelford", "albany", "openai", "texas", "voltagrid"],
    "qts":              ["qts", "microsoft", "fayetteville", "georgia", "fayette", "excalibur"],
    "meridian":        ["meridian", "sullivan county", "new lebanon", "indiana", "fluidstack", "next frontier"],
    "edged_compute":   ["edged compute", "edged energy", "aurora", "illinois", "coreweave", "alibaba", "koch"],
    "core_scientific": ["core scientific", "corz", "coreweave", "denton", "marble", "muskogee", "dalton", "austin"],
    "sbe_softbank":    ["sbe", "softbank", "austin", "texas", "travis"],
    "hut_beacon_point": ["hut 8", "hut8", "beacon point", "nvidia", "nueces", "corpus christi", "robstown"],
    "tract_d":          ["tract", "nvidia", "nevada", "storey", "tric", "fleet"],
    "polar_dc":         ["polar dc", "polardc", "norway", "crusoe", "coreweave", "drangedal", "heroya", "porsgrunn"],
    "prime_dc":         ["prime dc", "prime ord", "elk grove", "coreweave", "illinois", "chicago"],
    "sopaipilla":       ["meta", "el paso", "sopaipilla", "data center", "texas", "hyperscale"],
    "glxy_helios":      ["galaxy", "glxy", "helios", "coreweave", "afton", "dickens county", "spur", "texas"],
    "sbe_milam":        ["openai", "stargate", "sbe", "energy global", "milam", "rosebud", "cameron", "texas"],
    "qts_magnolia":     ["qts", "magnolia", "microsoft", "meta", "oracle", "new albany", "columbus", "phoenix", "glendale", "dallas", "fort worth", "irving", "wilmer", "richmond", "henrico", "data center"],
    "cifr_stingray":    ["cipher", "cifr", "stingray", "andrews", "amazon", "texas", "permian basin"],
    "apld_eln4":        ["applied digital", "apld", "eln4", "coreweave", "ellendale", "north dakota", "dickey county"],
    "yondr_nova":       ["yondr", "nova north", "aldie", "loudoun", "oracle", "virginia", "data center"],
    "zenith":           ["zenith", "fluidstack", "jane street", "okmulgee", "oklahoma", "data center"],
    "digital_drive":    ["digital drive", "coreweave", "chesterfield", "virginia", "blue owl", "data center"],
    "novva_lv_reno":    ["novva", "coreweave", "ebay", "cim", "north las vegas", "reno", "nevada", "data center"],
}

# ── Reddit subreddit-targeted queries ─────────────────────────────────────────
# Each entry is a list of (subreddit, query) tuples.
# Searches are restricted to the named subreddit (restrict_sr=on), which gives
# far better signal than global Reddit search.
# Pattern per bond: local/regional sub + industry sub + finance sub (for public cos)
REDDIT_SUBREDDIT_QUERIES: dict[str, list[tuple[str, str]]] = {
    "beignet": [
        ("Louisiana",   "Meta datacenter Richland Parish"),
        ("datacenter",  "Meta Louisiana Richland"),
    ],
    "hut_google": [
        ("Louisiana",   "HUT Google datacenter River Bend"),
        ("datacenter",  "HUT 8 Google Louisiana"),
    ],
    "related_bx": [
        ("AnnArbor",    "Oracle data center"),
        ("Michigan",    "Oracle datacenter Washtenaw"),
        ("datacenter",  "Oracle Michigan Ann Arbor"),
    ],
    "vantage": [
        ("Texas",       "Vantage datacenter Shackelford Albany"),
        ("datacenter",  "Vantage Stargate Texas Oracle Shackelford"),
    ],
    "stack_nm": [
        ("newmexico",   "data center Stack Oracle Santa Teresa"),
        ("LasCruces",   "data center Oracle Stack"),
        ("datacenter",  "Stack Infrastructure New Mexico Oracle"),
    ],
    "tract": [
        ("Reno",        "NVIDIA datacenter Storey County"),
        ("Nevada",      "NVIDIA data center Storey Tahoe Reno"),
        ("datacenter",  "NVIDIA Nevada Tahoe Reno TRIC Fleet"),
    ],
    "cifr_black_pearl": [
        ("Bitcoin",         "Cipher Mining Black Pearl Texas"),
        ("CryptoCurrency",  "Cipher Mining datacenter Texas"),
        ("Texas",           "Cipher Mining Wink datacenter"),
        ("datacenter",      "Cipher Mining Black Pearl Wink Texas"),
    ],
    "wulf": [
        ("upstatenewyork",  "TeraWulf Lake Mariner Somerset"),
        ("Bitcoin",         "TeraWulf Lake Mariner datacenter"),
        ("datacenter",      "TeraWulf New York Lake Mariner"),
    ],
    "flashc": [
        ("Lubbock",     "datacenter Abernathy Fluidstack Aligned"),
        ("Texas",       "Fluidstack datacenter Abernathy Hale County"),
        ("datacenter",  "Fluidstack Google Texas Abernathy"),
    ],
    "cifr_barber_lake": [
        ("Texas",           "Cipher Mining Colorado City datacenter"),
        ("CryptoCurrency",  "Cipher Mining Barber Lake AWS"),
        ("Bitcoin",         "Cipher Mining Texas datacenter"),
        ("datacenter",      "Cipher Mining Barber Lake Colorado City Texas"),
    ],
    "apld_pf2": [
        ("northdakota", "Applied Digital datacenter Harwood"),
        ("fargo",       "data center Applied Digital APLD"),
        ("datacenter",  "Applied Digital North Dakota Oracle Harwood"),
    ],
    "apld": [
        ("northdakota", "Applied Digital Ellendale datacenter CoreWeave"),
        ("datacenter",  "Applied Digital Ellendale CoreWeave"),
    ],
    "voltag": [
        ("Texas",       "Vantage datacenter Shackelford Oracle"),
        ("datacenter",  "VoltaGrid Vantage Texas Oracle Stargate"),
    ],
    "qts": [
        ("Georgia",     "QTS datacenter Fayetteville Microsoft"),
        ("Atlanta",     "QTS data center Microsoft Fayette"),
        ("datacenter",  "QTS Microsoft Fayetteville Georgia Excalibur"),
    ],
    "meridian": [
        ("Indiana",     "data center Sullivan County Fluidstack Google"),
        ("datacenter",  "Meridian Indiana Sullivan County Google Fluidstack"),
    ],
    "edged_compute": [
        ("chicago",     "data center Aurora CoreWeave Edged"),
        ("Atlanta",     "datacenter Edged Alibaba CoreWeave"),
        ("datacenter",  "Edged Compute CoreWeave Alibaba Illinois Georgia"),
    ],
    "core_scientific": [
        ("datacenter",  "Core Scientific CoreWeave HPC hosting"),
        ("Bitcoin",     "Core Scientific CoreWeave datacenter Texas"),
        ("texas",       "Core Scientific datacenter Denton CoreWeave CORZ"),
    ],
    "sbe_softbank": [
        ("Austin",      "Softbank datacenter SBE"),
        ("texas",       "Softbank SBE datacenter Austin"),
    ],
    "hut_beacon_point": [
        ("texas",       "HUT 8 NVIDIA datacenter Nueces County Corpus Christi"),
        ("datacenter",  "HUT 8 NVIDIA Beacon Point Texas hyperscale"),
    ],
    "tract_d": [
        ("Reno",        "NVIDIA datacenter Storey County"),
        ("Nevada",      "NVIDIA data center Storey Tahoe Reno"),
        ("datacenter",  "NVIDIA Nevada Tahoe Reno TRIC Fleet"),
    ],
    "polar_dc": [
        ("europe",      "PolarDC Norway Crusoe CoreWeave datacenter"),
        ("datacenter",  "PolarDC Norway Drangedal Crusoe CRWV"),
    ],
    "prime_dc": [
        ("chicago",     "Prime DC CoreWeave Elk Grove datacenter"),
        ("illinois",    "Prime DC CoreWeave Elk Grove Village data center"),
        ("datacenter",  "Prime DC CoreWeave Elk Grove Illinois"),
    ],
    "sopaipilla": [
        ("ElPaso",      "Meta data center"),
        ("texas",       "El Paso Meta datacenter"),
        ("datacenter",  "Meta El Paso Sopaipilla"),
    ],
    "glxy_helios": [
        ("Lubbock",     "data center Galaxy Helios"),
        ("texas",       "Dickens County Afton data center"),
        ("datacenter",  "Galaxy Helios CoreWeave Texas"),
    ],
    "sbe_milam": [
        ("texas",       "Milam County data center OpenAI"),
        ("OpenAI",      "Stargate Milam Texas data center"),
        ("datacenter",  "OpenAI SBE Milam County Texas"),
    ],
    "qts_magnolia": [
        ("Columbus",    "QTS data center New Albany"),
        ("phoenix",     "QTS data center"),
        ("Dallas",      "QTS data center Irving Wilmer"),
        ("rva",         "QTS data center Henrico"),
        ("datacenter",  "QTS Magnolia Microsoft Meta Oracle"),
    ],
    "cifr_stingray": [
        ("Bitcoin",     "Cipher Mining Stingray Andrews Texas"),
        ("texas",       "Cipher Mining Andrews datacenter Amazon"),
        ("datacenter",  "Cipher Mining Stingray Amazon Andrews"),
    ],
    "apld_eln4": [
        ("northdakota", "Applied Digital ELN4 Ellendale CoreWeave"),
        ("datacenter",  "Applied Digital ELN4 CoreWeave Ellendale"),
    ],
    "yondr_nova": [
        ("NOVA",        "Yondr Aldie data center Loudoun"),
        ("virginia",    "Yondr data center Aldie Loudoun"),
        ("datacenter",  "Yondr NoVa North Aldie Loudoun Oracle"),
    ],
    "zenith": [
        ("oklahoma",    "Zenith Fluidstack Okmulgee data center"),
        ("tulsa",       "data center Okmulgee Fluidstack"),
        ("datacenter",  "Zenith Fluidstack Jane Street Oklahoma"),
    ],
    "digital_drive": [
        ("rva",         "Digital Drive CoreWeave Chesterfield data center"),
        ("virginia",    "CoreWeave Chesterfield data center"),
        ("datacenter",  "Digital Drive CoreWeave Chesterfield Virginia"),
    ],
    "novva_lv_reno": [
        ("vegas",       "Novva data center North Las Vegas CoreWeave"),
        ("reno",        "Novva data center CoreWeave eBay"),
        ("nevada",      "Novva data center CoreWeave"),
        ("datacenter",  "Novva CoreWeave eBay Las Vegas Reno"),
    ],
}

# ── X / Twitter search queries (requires Basic plan bearer token) ─────────────
# Used when TWITTER_BEARER_TOKEN env var is set.
# One query per bond — focused on company/project mentions from credible accounts.
TWITTER_QUERIES = {
    "beignet":          '"Meta" "Richland Parish" OR "Richland data center" -is:retweet lang:en',
    "related_bx":       '"Oracle" "Ann Arbor" OR "Washtenaw" datacenter -is:retweet lang:en',
    "vantage":          '"Vantage Data Centers" OR "Stargate" "Shackelford" -is:retweet lang:en',
    "stack_nm":         '"Stack Infrastructure" "New Mexico" OR "Santa Teresa" datacenter -is:retweet lang:en',
    "tract":            '("NVIDIA" OR "Tract") "Storey County" OR "Tahoe Reno" datacenter -is:retweet lang:en',
    "cifr_black_pearl": '($CIFR OR "Cipher Mining") "Black Pearl" OR "Wink" datacenter -is:retweet lang:en',
    "wulf":             '($WULF OR "TeraWulf") "Lake Mariner" OR "Somerset" datacenter -is:retweet lang:en',
    "flashc":           '("Fluidstack" OR "Aligned") "Abernathy" OR "Hale County" datacenter -is:retweet lang:en',
    "cifr_barber_lake": '($CIFR OR "Cipher Mining") "Barber Lake" OR "Colorado City" -is:retweet lang:en',
    "apld_pf2":         '($APLD OR "Applied Digital") "Harwood" OR "Cass County" datacenter -is:retweet lang:en',
    "apld":             '($APLD OR "Applied Digital") "Ellendale" OR "CoreWeave" datacenter -is:retweet lang:en',
    "voltag":           '("VoltaGrid" OR "Vantage") "Shackelford" OR "Albany" Texas datacenter -is:retweet lang:en',
    "qts":              '("QTS" OR "Project Excalibur") "Fayetteville" OR "Fayette County" Microsoft -is:retweet lang:en',
    "meridian":        '"Meridian" "Sullivan County" OR "New Lebanon" Indiana datacenter -is:retweet lang:en',
    "edged_compute":   '("Edged Compute" OR "Edged Energy") CoreWeave OR Alibaba datacenter -is:retweet lang:en',
    "core_scientific": '($CORZ OR "Core Scientific") CoreWeave datacenter HPC -is:retweet lang:en',
    "hut_google":       '"HUT 8" OR "HUT8" Google datacenter Louisiana -is:retweet lang:en',
    "sbe_softbank":     '"Softbank" OR "SBE" Austin Texas datacenter -is:retweet lang:en',
    "hut_beacon_point": '"HUT 8" OR "HUT8" NVIDIA "Nueces" OR "Corpus Christi" OR "Beacon Point" -is:retweet lang:en',
    "tract_d":          '("NVIDIA" OR "Tract") "Storey County" OR "Tahoe Reno" datacenter -is:retweet lang:en',
    "polar_dc":         '"PolarDC" OR "Polar DC" Norway Crusoe OR CoreWeave datacenter -is:retweet',
    "prime_dc":         '"Prime DC" OR "Prime ORD" CoreWeave "Elk Grove" Illinois -is:retweet lang:en',
    "sopaipilla":       '"Meta" "El Paso" data center OR datacenter -is:retweet lang:en',
    "glxy_helios":      '("Galaxy Digital" OR $GLXY OR "Helios") "CoreWeave" OR "Dickens County" datacenter -is:retweet lang:en',
    "sbe_milam":        '("OpenAI" OR "Stargate" OR "SBE") "Milam" OR "Rosebud" datacenter -is:retweet lang:en',
    "qts_magnolia":     '"QTS" (Magnolia OR "data center") (Columbus OR "New Albany" OR Phoenix OR Glendale OR "Fort Worth" OR Irving) -is:retweet lang:en',
    "cifr_stingray":    '($CIFR OR "Cipher Mining") "Stingray" OR "Andrews" Texas datacenter -is:retweet lang:en',
    "apld_eln4":        '($APLD OR "Applied Digital") "ELN4" OR "Ellendale" CoreWeave datacenter -is:retweet lang:en',
    "yondr_nova":       '"Yondr" "Aldie" OR "Loudoun" Virginia data center -is:retweet lang:en',
    "zenith":           '("Zenith" OR "Fluidstack") "Okmulgee" OR "Jane Street" Oklahoma data center -is:retweet lang:en',
    "digital_drive":    '"CoreWeave" "Chesterfield" OR "Digital Drive" Virginia data center -is:retweet lang:en',
    "novva_lv_reno":    '"Novva" ("North Las Vegas" OR "Reno") CoreWeave OR eBay data center -is:retweet lang:en',
}


# ── Helpers ──────────────────────────────────────────────────────────────────
def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "")[:400]


def _parse_rss_entry(entry, source_label: str) -> dict:
    published = getattr(entry, "published", None) or getattr(entry, "updated", None) or ""
    return {
        "title":     entry.get("title", "").strip(),
        "url":       entry.get("link", ""),
        "source":    source_label,
        "published": published,
        "summary":   _strip_html(entry.get("summary", "")),
        "type":      "news",
    }


async def _fetch_rss(url: str, label: str, limit: int = 6) -> list:
    try:
        loop = asyncio.get_event_loop()
        feed = await loop.run_in_executor(None, feedparser.parse, url)
        return [_parse_rss_entry(e, label) for e in feed.entries[:limit] if e.get("title")]
    except Exception:
        return []


async def _fetch_industry_rss(rss_url: str, keywords: list, label: str, limit: int = 4) -> list:
    try:
        loop = asyncio.get_event_loop()
        feed = await loop.run_in_executor(None, feedparser.parse, rss_url)
        kw = [k.lower() for k in keywords]
        matches = []
        for entry in feed.entries:
            text = (entry.get("title", "") + " " + entry.get("summary", "")).lower()
            if any(k in text for k in kw):
                matches.append(_parse_rss_entry(entry, label))
                if len(matches) >= limit:
                    break
        return matches
    except Exception:
        return []


async def _fetch_newsapi(query: str, limit: int = 6) -> list:
    if not NEWSAPI_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            r = await client.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q":        query,
                    "apiKey":   NEWSAPI_KEY,
                    "language": "en",
                    "sortBy":   "publishedAt",
                    "pageSize": limit,
                },
            )
            items = []
            for a in r.json().get("articles", []):
                if not a.get("title") or a["title"] == "[Removed]":
                    continue
                items.append({
                    "title":     a["title"],
                    "url":       a.get("url", ""),
                    "source":    a.get("source", {}).get("name", "NewsAPI"),
                    "published": a.get("publishedAt", ""),
                    "summary":   (a.get("description") or "")[:400],
                    "type":      "news",
                })
            return items
    except Exception:
        return []


async def _get_reddit_token() -> str:
    """Fetch or return cached Reddit OAuth token (client_credentials flow)."""
    global _reddit_token
    if time.time() < _reddit_token["expires_at"] - 60:
        return _reddit_token["token"]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://www.reddit.com/api/v1/access_token",
                auth=(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET),
                data={"grant_type": "client_credentials"},
                headers={"User-Agent": "DatacenterBondMonitor/1.0"},
            )
        if resp.status_code == 200:
            data = resp.json()
            _reddit_token = {
                "token":      data["access_token"],
                "expires_at": time.time() + data.get("expires_in", 3600),
            }
            return _reddit_token["token"]
        print(f"[reddit-auth] Token request failed: HTTP {resp.status_code}")
    except Exception as e:
        print(f"[reddit-auth] {e}")
    return ""


async def _fetch_reddit_sub(subreddit: str, query: str, limit: int = 6) -> list:
    """
    Search a subreddit via Reddit OAuth API (free, 60 req/min).
    Requires REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET env vars.
    No-ops silently if credentials are not set.
    """
    if not REDDIT_CLIENT_ID:
        return []

    token = await _get_reddit_token()
    if not token:
        return []

    url = (
        f"https://oauth.reddit.com/r/{subreddit}/search.json"
        f"?q={urllib.parse.quote(query)}&sort=new&restrict_sr=on&limit={limit}&t=year"
    )
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={
                "Authorization": f"bearer {token}",
                "User-Agent":    "DatacenterBondMonitor/1.0",
            })
        if resp.status_code != 200:
            print(f"[reddit] r/{subreddit}: HTTP {resp.status_code}")
            return []
        posts = resp.json().get("data", {}).get("children", [])
        items = []
        for post in posts[:limit]:
            p = post.get("data", {})
            title = p.get("title", "").strip()
            if not title:
                continue
            created = p.get("created_utc")
            pub = datetime.fromtimestamp(created, tz=timezone.utc).isoformat() if created else ""
            permalink = p.get("permalink", "")
            post_url = f"https://www.reddit.com{permalink}" if permalink else p.get("url", "")
            selftext = (p.get("selftext") or "")[:300]
            items.append({
                "title":     title,
                "url":       post_url,
                "source":    f"r/{subreddit}",
                "published": pub,
                "summary":   selftext,
                "type":      "reddit",
            })
        return items
    except Exception as e:
        print(f"[reddit] r/{subreddit} '{query[:40]}': {e}")
        return []


async def _fetch_twitter(query: str, limit: int = 10) -> list:
    if not TWITTER_BEARER:
        return []
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            r = await client.get(
                "https://api.twitter.com/2/tweets/search/recent",
                headers={"Authorization": f"Bearer {TWITTER_BEARER}"},
                params={
                    "query":       f"({query}) -is:retweet lang:en",
                    "max_results": min(max(limit, 10), 100),
                    "tweet.fields": "created_at,text",
                    "expansions":  "author_id",
                    "user.fields": "name,username",
                },
            )
            if r.status_code != 200:
                return []
            data  = r.json()
            users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}
            tweets = []
            for t in data.get("data", []):
                user   = users.get(t.get("author_id", ""), {})
                handle = user.get("username", "unknown")
                tweets.append({
                    "title":     f"@{handle}: {t['text'][:120]}",
                    "url":       f"https://twitter.com/{handle}/status/{t['id']}",
                    "source":    f"X / @{handle}",
                    "published": t.get("created_at", ""),
                    "summary":   t.get("text", ""),
                    "type":      "tweet",
                })
            return tweets
    except Exception:
        return []


def _pub_sort_key(item: dict):
    try:
        return datetime.fromisoformat(item["published"].replace("Z", "+00:00"))
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


# ── Portfolio-wide channel checks ────────────────────────────────────────────
# Industry subreddits applied to EVERY bond (on top of its local subs), each
# searched with the bond's own terms so coverage stays relevant per position.
GLOBAL_REDDIT_SUBS = ["datacenter", "hardware", "nvidia", "energy"]


# ── Main entry point ─────────────────────────────────────────────────────────
async def get_news(
    bond_id: str,
    news_queries: list,
    twitter_queries: list,
    bond_name: str = "",
    location: str = "",
    tenant: str = "",
    issuer: str = "",
) -> dict:
    # Return cached result if fresh
    now_ts = time.time()
    cached = _news_cache.get(bond_id)
    if cached and (now_ts - cached["ts"]) < _NEWS_CACHE_TTL:
        return cached["data"]

    local_rss_urls  = LOCAL_OUTLET_RSS.get(bond_id, [])
    na_queries      = NEWSAPI_QUERIES.get(bond_id, news_queries[:3])
    sub_queries     = REDDIT_SUBREDDIT_QUERIES.get(bond_id, [])
    dc_keywords     = DC_INDUSTRY_KEYWORDS.get(bond_id, [])
    x_query         = TWITTER_QUERIES.get(bond_id, "")

    # Portfolio-wide channel checks (applied to every bond, on top of its local
    # channels). Industry subreddits are searched with THIS bond's own terms so
    # the extra coverage stays relevant instead of duplicating generic posts.
    bond_terms      = " ".join(t for t in (bond_name, tenant) if t).strip() \
                      or (news_queries[0] if news_queries else bond_id)
    all_sub_queries = sub_queries + [(sub, bond_terms) for sub in GLOBAL_REDDIT_SUBS]
    # Extra X query on the credit/financing angle (complements the local one).
    x_subject = issuer or bond_name or bond_terms
    x_query2  = (f'("{x_subject}") (bond OR notes OR financing OR lawsuit OR '
                 f'downgrade OR default OR restructuring) -is:retweet lang:en') if x_subject else ""

    # Build task list
    tasks = [
        # NewsAPI (3 targeted queries)
        _fetch_newsapi(na_queries[0], limit=6) if len(na_queries) > 0 else asyncio.sleep(0),
        _fetch_newsapi(na_queries[1], limit=5) if len(na_queries) > 1 else asyncio.sleep(0),
        _fetch_newsapi(na_queries[2], limit=4) if len(na_queries) > 2 else asyncio.sleep(0),
        # Local outlet RSS (site-targeted Google News)
        *[_fetch_rss(url, "Local News", limit=6) for url in local_rss_urls],
        # Industry RSS
        _fetch_industry_rss(DC_DYNAMICS_RSS,  dc_keywords, "DataCenter Dynamics", limit=4),
        _fetch_industry_rss(DC_KNOWLEDGE_RSS, dc_keywords, "DataCenter Knowledge", limit=4),
        # Reddit — per-bond local subs + portfolio-wide industry subs (restrict_sr=on)
        *[_fetch_reddit_sub(sub, q, limit=6) for sub, q in all_sub_queries],
        # X / Twitter (only runs if TWITTER_BEARER_TOKEN is set): local + credit-angle
        _fetch_twitter(x_query, limit=15) if x_query else asyncio.sleep(0),
        _fetch_twitter(x_query2, limit=10) if x_query2 else asyncio.sleep(0),
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    def safe(r):
        return r if isinstance(r, list) else []

    idx = 0
    newsapi_items  = safe(results[idx]); idx += 1
    newsapi_items += safe(results[idx]); idx += 1
    newsapi_items += safe(results[idx]); idx += 1

    local_items = []
    for _ in local_rss_urls:
        local_items += safe(results[idx]); idx += 1

    industry_items  = safe(results[idx]); idx += 1
    industry_items += safe(results[idx]); idx += 1

    reddit_items = []
    for _ in all_sub_queries:
        reddit_items += safe(results[idx]); idx += 1

    x_items  = safe(results[idx]); idx += 1
    x_items += safe(results[idx]); idx += 1

    # Deduplicate by URL
    seen = set()
    all_news = []
    for item in newsapi_items + local_items + industry_items:
        url = item.get("url", "")
        if url and url not in seen and item.get("title"):
            seen.add(url)
            all_news.append(item)

    all_social = []
    for item in reddit_items + x_items:
        url = item.get("url", "")
        if url and url not in seen and item.get("title"):
            seen.add(url)
            all_social.append(item)

    all_news.sort(key=_pub_sort_key, reverse=True)
    all_social.sort(key=_pub_sort_key, reverse=True)

    # ── Relevance scoring ────────────────────────────────────────────────────
    alerts, regular_feed = await score_and_filter(
        items=all_news,
        bond_name=bond_name,
        location=location,
        tenant=tenant,
        issuer=issuer,
    )

    social_alerts, regular_social = await score_and_filter(
        items=all_social,
        bond_name=bond_name,
        location=location,
        tenant=tenant,
        issuer=issuer,
    )

    result = {
        "alerts":   alerts + social_alerts,
        "news":     regular_feed[:25],
        "social":   regular_social[:15],
        "industry": industry_items[:8],
    }

    _news_cache[bond_id] = {"data": result, "ts": now_ts}
    return result
