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
    ],
    "related_bx": [
        {"title": "Washtenaw County Government — Official Portal", "url": "https://www.washtenaw.org/", "source": "Washtenaw County", "type": "pinned"},
        {"title": "Ann Arbor SPARK — Economic Development Portal", "url": "https://jobs.annarborusa.org/jobs", "source": "Ann Arbor SPARK", "type": "pinned"},
    ],
    "vantage": [
        {"title": "Vantage DC — Shackelford County Campus (Official)", "url": "https://vantage-dc.com/data-center-locations/north-america/shackelford-county-tx", "source": "Vantage DC", "type": "pinned"},
        {"title": "Albany News — Shackelford Data Center Named Official Stargate AI Campus", "url": "https://www.thealbanynews.net/news/shackelford-data-center-named-official-stargate-ai-campus", "source": "The Albany News", "type": "pinned"},
        {"title": "Albany News — Data Center Progress: Full Steam Ahead", "url": "https://www.thealbanynews.net/news/data-center-progress-full-steam-ahead", "source": "The Albany News", "type": "pinned"},
    ],
    "stack_nm": [
        {"title": "MVEDA — Mesilla Valley Economic Development Alliance", "url": "https://www.mveda.com/", "source": "MVEDA", "type": "pinned"},
        {"title": "Dona Ana County Government — Official Portal", "url": "https://www.donaanacounty.org/", "source": "Dona Ana County", "type": "pinned"},
    ],
    "tract": [
        {"title": "TRIC — Tahoe Reno Industrial Center Data Center Info", "url": "https://tahoereno.com/data-center/", "source": "TRIC", "type": "pinned"},
        {"title": "PowerHouse Data Centers — Storey County News", "url": "https://www.powerhousedata.com/news/", "source": "PowerHouse DC", "type": "pinned"},
        {"title": "Novva Data Centers — Tahoe Reno Facility", "url": "https://www.novva.com/data-center-facilities/tahoe-reno-nevada/", "source": "Novva DC", "type": "pinned"},
        {"title": "Storey County Business Development", "url": "https://storeycounty.org/277/Business-Development", "source": "Storey County", "type": "pinned"},
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
        {"title": "QTS Data Centers — Fayetteville (Project Excalibur)", "url": "https://q.com/data-centers/fayetteville/", "source": "QTS", "type": "pinned"},
        {"title": "City of Fayetteville — Official Data Center Discussion Page", "url": "https://www.fayetteville-ga.gov/746/Data-Center-Discussion", "source": "City of Fayetteville", "type": "pinned"},
        {"title": "Fayette News — Construction Safety at Georgia Data Center (Suit)", "url": "https://www.fayette-news.net/news/shoddy-construction-at-georgia-data-center-killed-worker-suit-says/article_0f93529a-a41e-4458-b973-18817c8164ce.html", "source": "Fayette County News", "type": "pinned"},
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
        # Richland Beacon-News
        "https://news.google.com/rss/search?q=site:richlandtoday.com&hl=en-US&gl=US&ceid=US:en",
    ],
    "related_bx": [
        # MLive / Ann Arbor News
        "https://news.google.com/rss/search?q=(site:annarbor.com+OR+site:mlive.com)+%22data+center%22+OR+%22Oracle%22+OR+%22Washtenaw%22&hl=en-US&gl=US&ceid=US:en",
        # Patch Ann Arbor + Ypsilanti
        "https://news.google.com/rss/search?q=(site:patch.com/michigan/ann-arbor-mi+OR+site:patch.com/michigan/ypsilanti-mi)+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Michigan Daily
        "https://news.google.com/rss/search?q=site:michigandaily.com+%22data+center%22+OR+%22Oracle%22+OR+%22Washtenaw%22&hl=en-US&gl=US&ceid=US:en",
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
    ],
    "tract": [
        # Comstock Chronicle — most local to Storey County
        "https://news.google.com/rss/search?q=site:thecomstockchronicle.com&hl=en-US&gl=US&ceid=US:en",
        # Reno Gazette-Journal + Nevada Appeal
        "https://news.google.com/rss/search?q=(site:rgj.com+OR+site:nevadaappeal.com)+%22Storey+County%22+OR+%22TRIC%22+OR+%22Tahoe+Reno%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # KOLO TV + KTVN + KRNV
        "https://news.google.com/rss/search?q=(site:kolotv.com+OR+site:2news.com+OR+site:mynews4.com)+%22Storey+County%22+OR+%22TRIC%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "cifr_black_pearl": [
        # Odessa American + Midland Reporter
        "https://news.google.com/rss/search?q=(site:oaoa.com+OR+site:midland-reporter.com)+%22Wink%22+OR+%22Cipher%22+OR+%22data+center%22+OR+%22Winkler%22&hl=en-US&gl=US&ceid=US:en",
        # CBS7 / FirstAlert7 + YourBasin
        "https://news.google.com/rss/search?q=(site:firstalert7.com+OR+site:yourbasin.com)+%22Wink%22+OR+%22Cipher+Mining%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "wulf": [
        # Niagara Gazette — primary local source for Lake Mariner
        "https://news.google.com/rss/search?q=site:niagara-gazette.com+%22data+center%22+OR+%22TeraWulf%22+OR+%22Somerset%22+OR+%22Lake+Mariner%22&hl=en-US&gl=US&ceid=US:en",
        # Buffalo News + Lockport Journal
        "https://news.google.com/rss/search?q=(site:buffalonews.com+OR+site:lockportjournal.com)+%22TeraWulf%22+OR+%22Lake+Mariner%22+OR+%22Somerset%22+data+center&hl=en-US&gl=US&ceid=US:en",
        # WGRZ + WIVB + WKBW
        "https://news.google.com/rss/search?q=(site:wgrz.com+OR+site:wivb.com+OR+site:wkbw.com)+%22TeraWulf%22+OR+%22Lake+Mariner%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "flashc": [
        # Lubbock Avalanche-Journal
        "https://news.google.com/rss/search?q=site:lubbockonline.com+%22Abernathy%22+OR+%22data+center%22+OR+%22Fluidstack%22+OR+%22Hale+County%22&hl=en-US&gl=US&ceid=US:en",
        # KCBD + EverythingLubbock + Fox34
        "https://news.google.com/rss/search?q=(site:kcbd.com+OR+site:everythinglubbock.com+OR+site:fox34lubbock.com)+%22Abernathy%22+OR+%22data+center%22+OR+%22Fluidstack%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "cifr_barber_lake": [
        # Colorado City Record — primary local paper
        "https://news.google.com/rss/search?q=site:coloradocityrecord.com&hl=en-US&gl=US&ceid=US:en",
        # KTAB/KRBC + KTXS
        "https://news.google.com/rss/search?q=(site:bigcountryhomepage.com+OR+site:ktxs.com)+%22Colorado+City%22+OR+%22Mitchell+County%22+OR+%22Cipher%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
    ],
    "apld_pf2": [
        # InForum (Fargo Forum) — primary regional source
        "https://news.google.com/rss/search?q=site:inforum.com+%22Harwood%22+OR+%22Applied+Digital%22+OR+%22APLD%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Valley News Live + KVRR
        "https://news.google.com/rss/search?q=(site:valleynewslive.com+OR+site:kvrr.com)+%22Harwood%22+OR+%22Applied+Digital%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # ND Monitor
        "https://news.google.com/rss/search?q=site:northdakotamonitor.com+%22Harwood%22+OR+%22Applied+Digital%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
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
    ],
    # VOLTAG — identical campus to Vantage (Shackelford Co., TX)
    "voltag": [
        # Albany News — primary local paper, covers the campus extensively
        "https://news.google.com/rss/search?q=site:thealbanynews.net&hl=en-US&gl=US&ceid=US:en",
        # KTAB/KRBC Big Country
        "https://news.google.com/rss/search?q=site:bigcountryhomepage.com+%22Shackelford%22+OR+%22Albany%22+OR+%22Vantage%22+OR+%22data+center%22&hl=en-US&gl=US&ceid=US:en",
        # Broader regional search — Shackelford + Vantage/Oracle/OpenAI/Stargate
        "https://news.google.com/rss/search?q=%22Shackelford+County%22+Texas+%22Vantage%22+OR+%22Oracle%22+OR+%22OpenAI%22+OR+%22Stargate%22+data+center&hl=en-US&gl=US&ceid=US:en",
    ],
}

# ── NewsAPI targeted queries ─────────────────────────────────────────────────
NEWSAPI_QUERIES = {
    "beignet": [
        '"Project Beignet" Meta datacenter Louisiana',
        'Meta "Richland Parish" datacenter hyperscale construction',
        'Meta datacenter Louisiana 2025 OR 2026 construction',
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
        '"Shackelford County" Texas datacenter Oracle OR OpenAI OR Vantage OR Stargate',
    ],
    "qts": [
        '"QTS" Microsoft "Fayetteville" Georgia datacenter "Project Excalibur"',
        '"QTS Realty" Microsoft "Fayette County" Georgia datacenter construction',
        '"QTS" MSFT Microsoft Georgia AI superfactory datacenter 2025 OR 2026',
    ],
}

# ── Industry RSS keywords per bond ───────────────────────────────────────────
DC_INDUSTRY_KEYWORDS = {
    "beignet":          ["meta", "louisiana", "richland", "beignet"],
    "related_bx":       ["related", "oracle", "michigan", "washtenaw", "ann arbor"],
    "vantage":          ["vantage", "oracle", "shackelford", "texas", "stargate"],
    "stack_nm":         ["stack", "oracle", "new mexico", "santa teresa"],
    "tract":            ["tract", "nvidia", "nevada", "storey", "tric", "fleet"],
    "cifr_black_pearl": ["cipher", "cifr", "amazon", "wink", "texas", "black pearl"],
    "wulf":             ["terawulf", "wulf", "lake mariner", "fluidstack", "google", "new york"],
    "flashc":           ["fluidstack", "abernathy", "hypertec", "google", "hale county"],
    "cifr_barber_lake": ["cipher", "cifr", "barber lake", "colorado city", "google", "mitchell"],
    "apld_pf2":         ["applied digital", "apld", "oracle", "harwood", "north dakota", "cass"],
    "apld":             ["applied digital", "apld", "coreweave", "ellendale", "north dakota"],
    "voltag":           ["vantage", "oracle", "shackelford", "albany", "openai", "texas"],
    "qts":              ["qts", "microsoft", "fayetteville", "georgia", "fayette", "excalibur"],
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
        # Reddit — subreddit-specific searches (restrict_sr=on)
        *[_fetch_reddit_sub(sub, q, limit=6) for sub, q in sub_queries],
        # X / Twitter (only runs if TWITTER_BEARER_TOKEN is set)
        _fetch_twitter(x_query, limit=15) if x_query else asyncio.sleep(0),
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
    for _ in sub_queries:
        reddit_items += safe(results[idx]); idx += 1

    x_items = safe(results[idx]); idx += 1

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
