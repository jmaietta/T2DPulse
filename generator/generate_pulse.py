#!/usr/bin/env python3
# generator/generate_pulse.py
# Refactored version with:
# - REMOVED: Google Trends / Trending Chips / Trending Badges
# - Consolidated duplicate functions
# - Dataclasses for type safety
# - Parallel RSS fetching
# - Session with retry adapter
# - Updated for Grid Layout

import os
import re
import json
import html
import time
import random
import hashlib
import math
import urllib.parse
from io import BytesIO
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urljoin
from collections import defaultdict, deque, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import requests
import tldextract
import pytz
import yaml
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from PIL import Image
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================================
# CONFIGURATION
# ============================================================================

ROOT = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(ROOT)

with open(os.path.join(ROOT, "config.yaml"), "r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

TZ = pytz.timezone(CFG.get("timezone", "America/New_York"))
RUN_WINDOW_HOURS = int(CFG.get("run_window_hours", 24))

# Optional caps:
# - MAX_ITEMS caps the in-memory per-category pools (default: unlimited).
#   Set env MAX_ITEMS_PER_CATEGORY to a positive integer to enable.
# - MAX_PAGE_ITEMS caps the number of article cards rendered per page after sorting (default: unlimited).
#   Set env MAX_PAGE_ITEMS to a positive integer to enable.
def _parse_optional_int_env(name: str):
    raw = os.environ.get(name, "").strip()
    if not raw or raw.lower() in ("none", "null", "0"):
        return None
    try:
        v = int(raw)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None

MAX_ITEMS = _parse_optional_int_env("MAX_ITEMS_PER_CATEGORY")
MAX_PAGE_ITEMS = _parse_optional_int_env("MAX_PAGE_ITEMS")

UTM = CFG.get("utm", {"source": "tek2day", "medium": "email"})
BLOCK_SUFFIXES = [s.lower() for s in CFG.get("exclude_domains_suffix", [])]
ALWAYS_BLOCK = {"news.ycombinator.com", "ycombinator.com"}

# Cache configuration
CACHE_DIR = Path(os.path.join(REPO, "cache"))
CACHE_DIR.mkdir(exist_ok=True)
CACHE_FILE = CACHE_DIR / "friday_snapshot.json"

# Source mappings
SOURCE_NAME_MAP = {
    "theverge.com": "The Verge",
    "venturebeat.com": "VentureBeat",
    "pymnts.com": "PYMNTS",
    "arstechnica.com": "Ars Technica",
    "wsj.com": "The Wall Street Journal",
    "nytimes.com": "The New York Times",
    "ft.com": "Financial Times",
    "bloomberg.com": "Bloomberg",
    "techcrunch.com": "TechCrunch",
    "ieee.org": "IEEE Spectrum",
    "theregister.com": "The Register",
    "computerworld.com": "Computerworld",
    "computing.co.uk": "Computing",
    "openai.com": "OpenAI",
    "anthropic.com": "Anthropic",
    "news.google.com": "Google News",
    "youtube.com": "YouTube",
}

FORCE_FINTECH_DOMAINS = {"pymnts.com"}
FORCE_FINTECH_SOURCES = {"pymnts"}
FORCE_AI_SOURCES = {"openai", "anthropic", "claude"}
FORCE_INCLUDE_SOURCES = {"tek2day", "tek2day newsletter"}

# Freshness & diversity settings
FRESH_WINDOW_DAYS = 3
BACKFILL_WINDOW_DAYS = 5
FLOORS = {"ai": 10, "software": 6, "fintech": 6}

# Permalink paths
PERMA_ROOT = os.path.join(REPO, "docs", "p")
PERMA_TPL = os.path.join(ROOT, "templates", "item_template.html")

# ============================================================================
# DATACLASS FOR ARTICLES
# ============================================================================

@dataclass
class Article:
    """Structured representation of a news article."""
    title: str
    url: str
    published_at: str
    source: str
    summary: str = ""
    content_html: str = ""
    image_url: str = ""
    category: str = ""
    summary_text: str = ""
    permalink: str = ""
    abs_permalink: str = ""
    summary_240: str = ""
    thumbnail: str = ""
    backfilled: bool = False
    older_than_fresh_window: bool = False

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        result = {
            "title": self.title,
            "url": self.url,
            "published_at": self.published_at,
            "source": self.source,
        }
        if self.summary:
            result["summary"] = self.summary
        if self.content_html:
            result["content_html"] = self.content_html
        if self.image_url:
            result["image_url"] = self.image_url
        if self.category:
            result["category"] = self.category
        if self.summary_text:
            result["summary_text"] = self.summary_text
        if self.permalink:
            result["_permalink"] = self.permalink
        if self.abs_permalink:
            result["_abs_permalink"] = self.abs_permalink
        if self.summary_240:
            result["_summary_240"] = self.summary_240
        if self.thumbnail:
            result["_thumbnail"] = self.thumbnail
        if self.backfilled:
            result["_backfilled"] = True
        if self.older_than_fresh_window:
            result["_older_than_fresh_window"] = True
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "Article":
        """Create Article from dictionary."""
        return cls(
            title=data.get("title", ""),
            url=data.get("url", ""),
            published_at=data.get("published_at", ""),
            source=data.get("source", ""),
            summary=data.get("summary", ""),
            content_html=data.get("content_html", ""),
            image_url=data.get("image_url", ""),
            category=data.get("category", ""),
            summary_text=data.get("summary_text", ""),
            permalink=data.get("_permalink", ""),
            abs_permalink=data.get("_abs_permalink", ""),
            summary_240=data.get("_summary_240", ""),
            thumbnail=data.get("_thumbnail", ""),
            backfilled=data.get("_backfilled", False),
            older_than_fresh_window=data.get("_older_than_fresh_window", False),
        )


# ============================================================================
# DOMAIN HELPER CLASS
# ============================================================================

class DomainHelper:
    """Centralized domain extraction and checking."""

    @staticmethod
    def extract(url: str) -> str:
        """Get registered domain (e.g., 'nytimes.com')."""
        try:
            ext = tldextract.extract(url or "")
            if not ext.domain:
                return ""
            return f"{ext.domain}.{ext.suffix}".lower() if ext.suffix else ext.domain.lower()
        except Exception:
            return ""

    @staticmethod
    def host(url: str) -> str:
        """Get hostname without www prefix."""
        try:
            h = urlparse(url or "").netloc.lower()
            return h[4:] if h.startswith("www.") else h
        except Exception:
            return ""

    @staticmethod
    def is_blocked(url: str) -> bool:
        """Check if domain should be blocked."""
        d = DomainHelper.extract(url)
        if not d:
            return False
        return d in ALWAYS_BLOCK or any(d.endswith(suf) for suf in BLOCK_SUFFIXES)

    @staticmethod
    def nice_source_for(url: str) -> str:
        """Get human-friendly source name."""
        d = DomainHelper.extract(url)
        if not d:
            return "Google News"
        if d in SOURCE_NAME_MAP:
            return SOURCE_NAME_MAP[d]
        core = d.split(".")[-2] if d.count(".") >= 1 else d
        return core.capitalize()


# Aliases for backward compatibility
domain_of = DomainHelper.extract
_domain = DomainHelper.extract
_host = DomainHelper.host
is_blocked = DomainHelper.is_blocked
nice_source_for = DomainHelper.nice_source_for


# ============================================================================
# HTTP SESSION WITH RETRY ADAPTER
# ============================================================================

def create_session() -> requests.Session:
    """Create session with automatic retries and browser-like headers."""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })
    return session


SESSION = create_session()


def fetch_html(url: str, referer: str | None = None, timeout: int = 15) -> str:
    """Fetch HTML with shared session + optional Referer."""
    headers = {}
    if referer:
        headers["Referer"] = referer
    r = SESSION.get(url, headers=headers, allow_redirects=True, timeout=timeout)
    r.raise_for_status()
    return r.text


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def now_et() -> datetime:
    """Get current time in Eastern timezone."""
    return datetime.now(TZ)


def safe_parse_dt(dt_str: str) -> Optional[datetime]:
    """Safely parse datetime string."""
    if not dt_str:
        return None
    try:
        dt = dtparser.parse(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def clean_text(s: str, limit: int = None) -> str:
    """Clean and optionally truncate text."""
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    if limit and len(s) > limit:
        return s[:limit - 1] + "..."
    return s


def strip_html_to_text(s: str) -> str:
    """Convert HTML to plain text."""
    if not s:
        return ""
    try:
        return BeautifulSoup(s, "html5lib").get_text(" ", strip=True)
    except Exception:
        return s


def add_utm(url: str) -> str:
    """Add UTM parameters to URL."""
    return f"{url}{'&' if '?' in url else '?'}utm_source={UTM['source']}&utm_medium={UTM['medium']}"


def _abs_url(u: str, base: str) -> str:
    """Convert relative URL to absolute."""
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        p = urlparse(base)
        return f"{p.scheme}://{p.netloc}{u}"
    return u


def _stable_id(title: str, url: str, published_at: str) -> str:
    """Generate stable ID for permalink."""
    key = f"{(title or '').strip()}|{(url or '').strip()}|{(published_at or '').strip()}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]


# ============================================================================
# NYT IMAGE HELPERS
# ============================================================================

def _nyt_prefer_super_jumbo(u: str) -> str:
    """Prefer largest NYT size suffix (superJumbo) when present."""
    return re.sub(
        r'-(?:thumbLarge|threeByTwoSmallAt2X|threeByTwoLargeAt2X|articleLeft|articleLarge)\.(jpg|jpeg|png|webp)$',
        r'-superJumbo.\1', u
    )


def _nyt_force_jpeg(u: str) -> str:
    """Coerce NYT image URL to JPEG to avoid AVIF/WEBP incompatibilities."""
    try:
        u = re.sub(r'([?&])(auto|format|fm)=(webp|avif)', r'\1\2=jpg', u, flags=re.I)
        u = re.sub(r'\.(webp|avif)(\?.*)?$', r'.jpg\2', u, flags=re.I)
        if ('format=' not in u.lower()) and ('fm=' not in u.lower()) and ('auto=' not in u.lower()):
            u = u + ('&' if '?' in u else '?') + 'format=jpg'
    except Exception:
        pass
    return u


def _nyt_candidates(u: str) -> list[str]:
    """Return a small ladder of NYT image URLs to try (largest first)."""
    sizes = ["superJumbo", "threeByTwoLargeAt2X", "articleLarge"]
    out = []
    m = re.search(r'-(\w+)\.(jpg|jpeg|png|webp)$', u)
    if m:
        ext = m.group(2)
        base = u[:m.start()]
        for s in sizes:
            out.append(f"{base}-{s}.{ext}")
        out.append(u)
    else:
        out.append(u)
    seen, uniq = set(), []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _pick_from_srcset(srcset: str) -> str:
    """Pick best image from srcset attribute."""
    best_url, best_w = "", -1
    for part in (srcset or "").split(","):
        seg = part.strip().split()
        if not seg:
            continue
        u = seg[0]
        w = 0
        if len(seg) > 1 and seg[1].endswith("w"):
            try:
                w = int(seg[1][:-1])
            except Exception:
                w = 0
        if w > best_w:
            best_url, best_w = u, w
    return best_url


# ============================================================================
# IMAGE EXTRACTION
# ============================================================================

def _extract_json_ld_images(soup: BeautifulSoup) -> list[str]:
    """Extract images from JSON-LD structured data."""
    out = []
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue

        def walk(obj):
            if isinstance(obj, dict):
                if "image" in obj:
                    img = obj["image"]
                    if isinstance(img, str):
                        out.append(img)
                    elif isinstance(img, dict) and "url" in img:
                        out.append(img["url"])
                    elif isinstance(img, list):
                        for i in img:
                            if isinstance(i, str):
                                out.append(i)
                            elif isinstance(i, dict) and "url" in i:
                                out.append(i["url"])
                for v in obj.values():
                    walk(v)
            elif isinstance(obj, list):
                for v in obj:
                    walk(v)

        walk(data)
    return out


def find_best_image_in_soup(soup: BeautifulSoup, page_url: str) -> str:
    """Return a best-guess absolute image URL for a page."""
    # 0) JSON-LD
    try:
        for script in soup.find_all("script", type=lambda v: v and "ld+json" in v):
            try:
                data = json.loads(script.string or "{}")
            except Exception:
                continue
            blocks = data if isinstance(data, list) else [data]
            for block in blocks:
                if not isinstance(block, dict):
                    continue
                if block.get("@type") in ("NewsArticle", "Article", "NewsItem"):
                    imgs = block.get("image")
                    candidates = []
                    if isinstance(imgs, str):
                        candidates = [imgs]
                    elif isinstance(imgs, dict):
                        candidates = [imgs.get("contentUrl") or imgs.get("url") or ""]
                    elif isinstance(imgs, list):
                        for im in imgs:
                            if isinstance(im, str):
                                candidates.append(im)
                            elif isinstance(im, dict):
                                candidates.append(im.get("contentUrl") or im.get("url") or "")
                    for u in [c for c in candidates if c]:
                        u = _abs_url(u, page_url)
                        if "static01.nyt.com" in u:
                            u = _nyt_prefer_super_jumbo(u)
                        if u:
                            return u
    except Exception:
        pass

    # 1) Meta tags (og/twitter)
    for sel in [
        ("meta", {"property": "og:image:secure_url"}),
        ("meta", {"property": "og:image"}),
        ("meta", {"name": "og:image"}),
        ("meta", {"property": "twitter:image:src"}),
        ("meta", {"name": "twitter:image"}),
    ]:
        m = soup.find(*sel)
        if m:
            u = m.get("content") or m.get("value")
            if u:
                u = _abs_url(u, page_url)
                if "static01.nyt.com" in u:
                    u = _nyt_prefer_super_jumbo(u)
                return u

    # 2) link rel=image_src
    l = soup.find("link", rel=lambda v: v and "image_src" in v)
    if l and l.get("href"):
        u = _abs_url(l["href"], page_url)
        if "static01.nyt.com" in u:
            u = _nyt_prefer_super_jumbo(u)
        return u

    # 3) First reasonable <img>
    for img in soup.find_all("img"):
        cand = (img.get("data-src") or img.get("data-original") or img.get("data-url") or
                img.get("data-asset-url") or img.get("src") or "")
        if not cand and img.get("srcset"):
            cand = _pick_from_srcset(img.get("srcset") or "")
        cand = _abs_url(cand, page_url)
        if cand:
            if "static01.nyt.com" in cand:
                cand = _nyt_prefer_super_jumbo(cand)
            return cand

    # 4) AMP page fallback
    amp = soup.find("link", rel="amphtml")
    if amp and amp.get("href"):
        try:
            amp_html = fetch_html(_abs_url(amp["href"], page_url), referer=page_url)
            amp_soup = BeautifulSoup(amp_html, "html.parser")
            aimg = amp_soup.find("meta", {"property": "og:image"}) or amp_soup.find("meta", {"name": "og:image"})
            if aimg and aimg.get("content"):
                u = _abs_url(aimg["content"], page_url)
                if "static01.nyt.com" in u:
                    u = _nyt_prefer_super_jumbo(u)
                return u
        except Exception:
            pass

    return ""


def _download_image_with_retries(img_url: str, referer: str | None, attempts: int = 3, timeout: int = 15) -> bytes:
    """Download image with domain-aware headers and backoff."""
    last_exc = None
    base_sleep = 0.35 + random.random() * 0.2

    for i in range(attempts):
        try:
            hdrs = {
                "Accept": "image/webp,image/jpeg,image/png,image/*;q=0.8",
                "User-Agent": SESSION.headers.get("User-Agent"),
                "Accept-Language": "en-US,en;q=0.9",
            }
            if referer:
                hdrs["Referer"] = referer

            is_nyt = "static01.nyt.com" in img_url
            if is_nyt:
                hdrs["Referer"] = "https://www.nytimes.com/"
                hdrs["Origin"] = "https://www.nytimes.com"

            r = SESSION.get(img_url, headers=hdrs, stream=True, timeout=timeout, allow_redirects=True)
            status = r.status_code
            ct = (r.headers.get("content-type") or "").lower()
            try_sz = int(r.headers.get("content-length", "0") or 0)
            print(f"[IMG] GET {status} {try_sz}B ct={ct} url={img_url[:100]}")

            if status == 429:
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra and ra.isdigit() else (base_sleep * (2 ** i))
                time.sleep(wait)
                continue

            if status >= 500:
                time.sleep(base_sleep * (2 ** i))
                continue

            if status != 200:
                time.sleep(base_sleep * (1 + i * 0.25))
                continue

            data = r.content if r.raw is None else r.raw.read()

            if len(data or b"") < 4096:
                time.sleep(base_sleep * (1 + i * 0.25))
                continue

            try:
                Image.open(BytesIO(data)).convert("RGB")
                return data
            except Exception as decode_err:
                err_str = str(decode_err).lower()
                if "avif" in err_str or "unsupported" in err_str:
                    try_hdrs = dict(hdrs)
                    try_hdrs["Accept"] = "image/jpeg,image/png,image/*"
                    rr = SESSION.get(img_url, headers=try_hdrs, stream=True, timeout=timeout, allow_redirects=True)
                    if rr.status_code == 200:
                        data_retry = rr.content if rr.raw is None else rr.raw.read()
                        try:
                            Image.open(BytesIO(data_retry)).convert("RGB")
                            return data_retry
                        except Exception:
                            pass
                time.sleep(base_sleep * (1 + i * 0.25))
                continue

        except Exception as e:
            last_exc = e
            print(f"[IMG] Error on attempt {i + 1}: {e}")
            time.sleep(base_sleep * (2 ** i))
            continue

    if last_exc:
        raise last_exc
    raise RuntimeError(f"Failed to download image: {img_url}")


# ============================================================================
# CATEGORIZATION (CONSOLIDATED)
# ============================================================================

AI_STRONG = [
    " ai ", "artificial intelligence", "llm", "gpt", "transformer", "diffusion",
    "inference", "fine-tun", "multimodal", "rlhf", "prompting", "agentic",
    "embedding", "vector db", "tokenization", "pretrain", "checkpoint", "weights",
    "npu", "tpu", "cuda", "rocm", "tensor", "accelerator",
    "openai", "anthropic", "deepmind", "mistral", "cohere", "perplexity", "hugging face",
]
AI_WEAK = [
    "model", "models", "neural", "dataset", "benchmark", "hallucination",
    "safety", "guardrail", "alignment", "generation", "genai", "gen ai"
]
AI_NEGATIVE = [
    " deal", " deals", "discount", "sale", "prime day", "coupon", "snag", "lowest price",
    " tv", "headphone", "earbuds", "soundbar", "smartphone", "iphone", "galaxy",
    "movie", "celebrity", "gossip", "trailer"
]
SW_STRONG = [
    "software", "developer", "sdk", "api", "kubernetes", "docker",
    "github", "vscode", "framework", "runtime", "serverless", "cloud", "saas",
    "microservices", "observability", "database", "postgres", "mysql", "redis",
    "code", "programming", "devops", "ci/cd", "deployment"
]
SW_NEGATIVE = [
    "movie", "movies", "film", "show", "shows", "series", "tv", "television",
    "streaming", "netflix", "hulu", "disney+", "marvel", "dc comics",
    "trailer", "premiere", "episode", "season", "actor", "actress"
]
FT_STRONG = [
    "fintech", "payments", "payment", "bank", "banking", "visa", "mastercard", "stripe",
    "paypal", "plaid", "lending", "loan", "crypto", "bitcoin", "ethereum", "stablecoin",
    "defi", "aml", "kyc", "sec", "fdic", "treasury", "card", "tokenization", "coinbase", "merchant"
]


def _count_hits(text: str, terms: list[str]) -> int:
    """Count how many terms appear in text."""
    if not text:
        return 0
    t = f" {text.lower()} "
    return sum(1 for w in terms if w in t)


def compute_scores(title: str, url: str, summary: str = "") -> dict[str, int]:
    """
    Single source of truth for category scoring.
    Returns dict with 'ai', 'software', 'fintech' scores.
    """
    title_l = title or ""
    summary_l = summary or ""
    url_l = url or ""
    combined = f"{title_l} {summary_l} {url_l}"

    ai = (
        3 * _count_hits(title_l, AI_STRONG)
        + 2 * _count_hits(summary_l, AI_STRONG)
        + _count_hits(url_l, AI_STRONG)
        + _count_hits(title_l, AI_WEAK)
        + _count_hits(summary_l, AI_WEAK)
        - min(2, _count_hits(combined, AI_NEGATIVE))
    )

    sw = (
        2 * _count_hits(title_l, SW_STRONG)
        + _count_hits(summary_l, SW_STRONG)
        + _count_hits(url_l, SW_STRONG)
        - min(2, _count_hits(combined, SW_NEGATIVE))
    )

    ft = (
        2 * _count_hits(title_l, FT_STRONG)
        + _count_hits(summary_l, FT_STRONG)
        + _count_hits(url_l, FT_STRONG)
    )

    return {"ai": ai, "software": sw, "fintech": ft}


def categorize_with_score(title: str, url: str, summary: str = "") -> tuple[str, int]:
    """
    Returns (category, score) using compute_scores.
    AI wins if score >= 2 and beats others by at least 1.
    """
    scores = compute_scores(title, url, summary)
    ai, sw, ft = scores["ai"], scores["software"], scores["fintech"]

    if ai >= 2 and ai >= max(sw, ft) + 1:
        return "ai", ai
    return ("software", sw) if sw >= ft else ("fintech", ft)


# ============================================================================
# DEALS/CONSUMER FILTER
# ============================================================================

DEAL_WORDS = [
    "deal", "deals", "discount", "sale", "promo", "coupon", "price",
    "off", "lowest price", "snag", "save", "prime day", "black friday",
    "cyber monday", "preorder", "$"
]
CONSUMER_GADGET_WORDS = [
    "tv", "headphones", "earbuds", "soundbar", "monitor", "iphone",
    "ipad", "apple watch", "pixel", "galaxy", "laptop", "camera",
    "console", "playstation", "xbox", "nintendo", "vacuum", "robot vacuum"
]
DEAL_PATH_HINTS = ["/deals/", "/deal/", "/the-verge-deals", "/coupon", "/shop", "/store"]


def is_deals_or_consumer_shopping(title: str, url: str) -> bool:
    """Check if article is a consumer shopping/deals post."""
    t = f"{title} {url}".lower()
    if any(n in t for n in DEAL_WORDS) or any(n in t for n in CONSUMER_GADGET_WORDS):
        return True
    return any(p in t for p in DEAL_PATH_HINTS)


# ============================================================================
# RSS EXTRACTION
# ============================================================================

def extract_image_url(entry) -> str:
    """Extract image URL from RSS entry."""
    try:
        if hasattr(entry, "media_content") and entry.media_content:
            m = entry.media_content[0]
            if isinstance(m, dict) and m.get("url"):
                return m["url"]
        if hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
            m = entry.media_thumbnail[0]
            if isinstance(m, dict) and m.get("url"):
                return m["url"]
        if hasattr(entry, "enclosures") and entry.enclosures:
            for enc in entry.enclosures:
                url = getattr(enc, "href", None) or enc.get("href") if isinstance(enc, dict) else None
                if url and any(url.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif")):
                    return url
        if hasattr(entry, "content"):
            try:
                html_blob = entry.content[0].value
                m = re.search(r'<img[^>]+src=["\']([^"\']+)', html_blob, re.I)
                if m:
                    return m.group(1)
            except Exception:
                pass
        if hasattr(entry, "summary"):
            m = re.search(r'<img[^>]+src=["\']([^"\']+)', entry.summary, re.I)
            if m:
                return m.group(1)
    except Exception:
        pass
    return ""


def parse_pubdate(entry) -> datetime:
    """Parse publication date from RSS entry."""
    for key in ("published", "updated", "pubDate"):
        if key in entry:
            try:
                dt = dtparser.parse(entry[key])
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(TZ)
            except Exception:
                pass
    return now_et()


def within_window(dt_local: datetime) -> bool:
    """Check if datetime is within backfill window."""
    cutoff = now_et() - timedelta(days=BACKFILL_WINDOW_DAYS)
    return dt_local >= cutoff


def fetch_rss(feed_name: str, url: str) -> list[dict]:
    """Fetch RSS feed with fallback image scraping."""
    try:
        d = feedparser.parse(url)
        items = []
        for e in d.entries[:60]:
            title = clean_text(getattr(e, "title", ""))
            link = getattr(e, "link", "")
            if not title or not link:
                continue
            if is_blocked(link) and "youtube.com/feeds/videos.xml" not in (url or ""):
                continue
            dt_local = parse_pubdate(e)
            is_youtube_feed = 'youtube.com/feeds/videos.xml' in (url or '')
            if (not is_youtube_feed) and (not within_window(dt_local)):
                continue
            raw_sum = getattr(e, "summary", "") or getattr(e, "description", "")
            summary = clean_text(strip_html_to_text(raw_sum), 400)
            content_html = ""
            if hasattr(e, "content"):
                try:
                    content_html = e.content[0].value
                except Exception:
                    pass
            image_url = extract_image_url(e)

            if image_url:
                print(f"[RSS] {feed_name}: found image in entry: {image_url[:80]}")

            if not image_url and link:
                try:
                    print(f"[RSS] {feed_name}: no image in entry, scraping page: {link[:80]}")
                    page_html = fetch_html(link, referer=link, timeout=10)
                    page_soup = BeautifulSoup(page_html, "html.parser")
                    image_url = find_best_image_in_soup(page_soup, link)
                    if image_url:
                        print(f"[RSS] {feed_name}: scraped image: {image_url[:80]}")
                    else:
                        print(f"[RSS] {feed_name}: no image found after scraping")
                except Exception as scrape_err:
                    print(f"[RSS] {feed_name}: scraping failed: {scrape_err}")

            if "youtube.com/feeds/videos.xml" in (url or ""):
                try:
                    video_id = None
                    if "watch?v=" in link:
                        video_id = link.split("watch?v=")[1].split("&")[0]
                    elif "youtu.be/" in link:
                        video_id = link.split("youtu.be/")[1].split("?")[0]
                    if video_id:
                        image_url = f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"
                except Exception:
                    pass

            items.append({
                "title": title,
                "url": link,
                "published_at": dt_local.isoformat(),
                "source": feed_name,
                "summary": summary,
                "content_html": content_html,
                "image_url": image_url,
            })
        return items
    except Exception as e:
        print(f"[RSS] Error fetching {feed_name} from {url}: {e}")
        return []


def fetch_all_rss_parallel(sources: list[dict], max_workers: int = 8) -> list[dict]:
    """Fetch all RSS feeds in parallel for better performance."""
    all_items = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_rss, s["name"], s["url"]): s["name"]
            for s in sources
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                items = future.result()
                print(f"✓ {name}: {len(items)} articles")
                all_items.extend(items)
            except Exception as e:
                print(f"✗ {name}: {e}")

    return all_items


# ============================================================================
# DEDUPLICATION
# ============================================================================

AGG_DOMAINS = {"news.google.com", "news.yahoo.com", "news.ycombinator.com"}


def _strip_publisher_suffix(title: str) -> str:
    """Strip publisher suffix from title."""
    if not title:
        return ""
    t = html.unescape(title)
    t = re.sub("[â€˜â€™]", "'", t)
    t = re.sub('[â€œâ€]', '"', t)
    t = re.sub(r"\s+", " ", t).strip()
    for sep in (" - ", " | "):
        if sep in t:
            left, right = t.rsplit(sep, 1)
            if any(c.isalpha() for c in right) and len(right) <= 40:
                t = left
                break
    return t


def dedupe_story_variants(items: list) -> list:
    """Dedupe similar story variants, preferring original sources."""
    best = {}
    for it in items:
        title = (it.get("title") or it.get("headline") or "").strip()
        key = _strip_publisher_suffix(title).lower()
        if not key:
            key = (it.get("id") or it.get("url") or it.get("permalink") or "").lower()
        if not key:
            continue

        prev = best.get(key)
        if not prev:
            best[key] = it
            continue

        host_new = _host(it.get("permalink") or it.get("url"))
        host_old = _host(prev.get("permalink") or prev.get("url"))

        score_new = (
            (0 if host_new in AGG_DOMAINS else 2) +
            (1 if it.get("image_url") or it.get("_thumbnail") else 0) +
            (1 if (it.get("published_at") or "") > (prev.get("published_at") or "") else 0)
        )
        score_old = (
            (0 if host_old in AGG_DOMAINS else 2) +
            (1 if prev.get("image_url") or prev.get("_thumbnail") else 0)
        )

        if score_new >= score_old:
            best[key] = it

    result = list(best.values())

    def _sort_dt(it):
        try:
            return dtparser.parse(it.get("published_at", ""))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    result.sort(key=_sort_dt, reverse=True)
    return result


def dedupe(items: list) -> list:
    """Deduplicate items by URL and title."""
    out, seen_urls, seen_titles, seen_title_cores = [], set(), set(), set()
    for it in items:
        url = it.get("url", "")
        title = it.get("title", "")
        try:
            parsed = urlparse(url)
            netloc = (parsed.netloc or "").lower()
            path = (parsed.path or "").rstrip("/")
            # Keep YouTube video identity (avoid collapsing all /watch URLs into one).
            if ("youtube.com" in netloc or "m.youtube.com" in netloc) and path == "/watch":
                qs = urllib.parse.parse_qs(parsed.query or "")
                vid = (qs.get("v") or [""])[0]
                if vid:
                    normalized_url = f"youtube.com/watch?v={vid}"
                else:
                    normalized_url = f"{netloc}{path}".lower()
            else:
                normalized_url = f"{netloc}{path}".lower()
        except Exception:
            normalized_url = url.lower()
        if normalized_url in seen_urls:
            continue
        title_key = re.sub(r"[^a-z0-9]+", "", title.lower())
        dom = domain_of(url)
        title_domain_key = f"{title_key}::{dom}"
        if title_domain_key in seen_titles:
            continue
        words = [w for w in re.findall(r'\b[a-z]{3,}\b', title.lower()) if w not in
                 {'the', 'and', 'for', 'with', 'that', 'this', 'from', 'will', 'are', 'was'}]
        if len(words) >= 4:
            core_title = ''.join(sorted(words[:6]))
            if len(core_title) >= 20:
                if core_title in seen_title_cores:
                    continue
                seen_title_cores.add(core_title)
        seen_urls.add(normalized_url)
        seen_titles.add(title_domain_key)
        out.append(it)

    def _dedupe_sort_dt(it):
        try:
            return dtparser.parse(it.get("published_at", ""))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    out.sort(key=_dedupe_sort_dt, reverse=True)
    return out


# ============================================================================
# FRESHNESS & DIVERSITY
# ============================================================================

def is_fresh(item: dict, window_days: int = FRESH_WINDOW_DAYS, now: datetime = None) -> bool:
    """Check if item is within freshness window."""
    now = now or datetime.now(timezone.utc)
    dt = safe_parse_dt(item.get("published_at"))
    if not dt:
        return False
    return (now - dt.astimezone(timezone.utc)) <= timedelta(days=window_days)


def filter_fresh(items: list, window_days: int = FRESH_WINDOW_DAYS, now: datetime = None) -> list:
    """Filter items to only fresh ones."""
    now = now or datetime.now(timezone.utc)
    return [it for it in items if is_fresh(it, window_days, now)]


def prefer_diverse_round_robin(items: list, max_total: int) -> list:
    """Select items with domain diversity using round-robin."""
    if not items:
        return []
    buckets = defaultdict(list)
    for it in items:
        buckets[domain_of(it.get("url", ""))].append(it)
    for d in buckets:
        buckets[d].sort(key=lambda x: safe_parse_dt(x.get("published_at")) or datetime.min.replace(tzinfo=timezone.utc),
                        reverse=True)
        random.shuffle(buckets[d])
    domains = list(buckets.keys())
    soft_cap = max(1, math.ceil(max_total / max(1, len(domains))))
    queues = [deque(v[:soft_cap]) for v in buckets.values() if v]
    out = []
    i = 0
    while queues and len(out) < max_total:
        q = queues[i % len(queues)]
        if q:
            out.append(q.popleft())
        queues = [qq for qq in queues if qq]
        i += 1
    return out


def sort_by_recency(items: list) -> list:
    """Sort items by publication date, newest first."""
    return sorted(items,
                  key=lambda x: safe_parse_dt(x.get("published_at")) or datetime.min.replace(tzinfo=timezone.utc),
                  reverse=True)


def build_preferred_today_with_floors(all_items: list, now_local: datetime = None, floors: dict = None,
                                       backfill_days: int = None) -> dict:
    """Build categorized items with freshness preferences."""
    now_local = now_local or now_et()
    arch_dir = os.path.join(REPO, "docs", "archive", "timestamped")
    cutoff_date = (now_local - timedelta(days=3)).date()
    archived_items = []
    if os.path.exists(arch_dir):
        for filename in os.listdir(arch_dir):
            if not filename.endswith(".json"):
                continue
            try:
                date_part = filename.split("_")[0]
                file_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                if file_date >= cutoff_date:
                    filepath = os.path.join(arch_dir, filename)
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        archived_items.extend(data.get("items", []))
            except Exception:
                continue

    all_combined = archived_items + all_items
    all_combined = dedupe_story_variants(all_combined)
    seen_urls = set()
    unique_items = []
    for it in all_combined:
        url = it.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_items.append(it)

    def _dt_local(it):
        try:
            return dtparser.parse(it["published_at"]).astimezone(TZ)
        except Exception:
            return now_local

    pool_by_cat = {"ai": [], "software": [], "fintech": []}
    for it in unique_items:
        try:
            dtl = _dt_local(it)
        except Exception:
            continue
        if dtl >= now_local - timedelta(days=3):
            cat = it.get("category")
            if cat in pool_by_cat:
                pool_by_cat[cat].append(it)

    for k in pool_by_cat:
        pool_by_cat[k].sort(key=_dt_local, reverse=True)

    out = {}
    for cat in ("ai", "software", "fintech"):
        pool = pool_by_cat.get(cat, [])
        pool.sort(key=_dt_local, reverse=True)
        out[cat] = pool[:MAX_ITEMS] if MAX_ITEMS else pool
        out[cat].sort(key=_dt_local, reverse=True)
    return out


def finalize_section_with_backfill(items: list, section_max: int, now: datetime = None,
                                    max_backfill_days: int = 7) -> list:
    """Finalize section items with backfill if needed."""
    now = now or datetime.now(timezone.utc)
    fresh = filter_fresh(items, FRESH_WINDOW_DAYS, now=now)
    if fresh:
        interleaved = prefer_diverse_round_robin(fresh, max_total=section_max)
        return sort_by_recency(interleaved)[:section_max]
    cutoff = now - timedelta(days=max_backfill_days)
    cands = [it for it in items if (dt := safe_parse_dt(it.get("published_at"))) and dt >= cutoff]
    cands = sort_by_recency(cands)[:section_max]
    for it in cands:
        it["_older_than_fresh_window"] = True
    return cands


# ============================================================================
# SUMMARIZATION
# ============================================================================

def summarize(item: dict) -> str:
    """Get or fetch summary for item."""
    if item.get("summary"):
        return clean_text(item["summary"], 260)
    try:
        r = SESSION.get(item["url"], timeout=12)
        soup = BeautifulSoup(r.text, "html5lib")
        for sel in [("meta", {"property": "og:description"}), ("meta", {"name": "description"})]:
            m = soup.find(*sel)
            if m and m.get("content"):
                return clean_text(m["content"], 260)
    except Exception:
        pass
    return clean_text(item["title"], 200)


def _plain_text_summary(it: dict, limit: int = 180) -> str:
    """Get plain text summary for item."""
    raw = it.get("summary_text") or it.get("summary") or it.get("description") or it.get("content_html") or it.get(
        "title") or ""
    txt = strip_html_to_text(raw)
    txt = re.sub(r"\s+", " ", txt).strip()
    if len(txt) <= limit:
        return txt
    cut = txt[:limit - 1]
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return cut + "..."


# ============================================================================
# PERMALINK & OG IMAGE GENERATION
# ============================================================================

def create_branded_og_image(source_url: str, permalink_dir: str, pre_extracted_image_url: str = "") -> tuple[str, str]:
    """Create branded OG image and thumbnail."""
    logo_path = os.path.join(REPO, "docs", "icons", "T2D_Pulse_Logo_2.png")
    banner_path = os.path.join(REPO, "docs", "icons", "T2D_Pulse_Banner.png")
    output_path = os.path.join(permalink_dir, "og-image.png")
    thumbnail_path = os.path.join(permalink_dir, "thumbnail.png")

    TARGET_WIDTH, TARGET_HEIGHT = 1200, 630
    # Increased THUMB_WIDTH for grid layout (was 240)
    THUMB_WIDTH, THUMB_HEIGHT = 400, 225
    LOGO_SIZE, PADDING = 120, 20

    def _compose_and_save(base_img: Image.Image) -> tuple[str, str]:
        img_aspect = base_img.width / base_img.height
        target_aspect = TARGET_WIDTH / TARGET_HEIGHT
        if img_aspect > target_aspect:
            new_height = TARGET_HEIGHT
            new_width = int(new_height * img_aspect)
            resized = base_img.resize((new_width, new_height), Image.LANCZOS)
            left = (new_width - TARGET_WIDTH) // 2
            og_img = resized.crop((left, 0, left + TARGET_WIDTH, TARGET_HEIGHT))
        else:
            new_width = TARGET_WIDTH
            new_height = int(new_width / img_aspect)
            resized = base_img.resize((new_width, new_height), Image.LANCZOS)
            top = (new_height - TARGET_HEIGHT) // 2
            og_img = resized.crop((0, top, TARGET_WIDTH, top + TARGET_HEIGHT))

        try:
            if os.path.exists(logo_path):
                logo = Image.open(logo_path).convert("RGBA")
                logo = logo.resize((LOGO_SIZE, LOGO_SIZE), Image.LANCZOS)
                logo_x = TARGET_WIDTH - LOGO_SIZE - PADDING
                logo_y = PADDING
                og_img.paste(logo, (logo_x, logo_y), logo)
        except Exception:
            pass

        og_img.save(output_path, "PNG", optimize=True)

        thumb = base_img.copy()
        t_as = thumb.width / thumb.height
        tgt_as = THUMB_WIDTH / THUMB_HEIGHT
        if t_as > tgt_as:
            nh = THUMB_HEIGHT * 2
            nw = int(nh * t_as)
            thumb = thumb.resize((nw, nh), Image.LANCZOS)
            left = (nw - THUMB_WIDTH * 2) // 2
            thumb = thumb.crop((left, 0, left + THUMB_WIDTH * 2, THUMB_HEIGHT * 2))
        else:
            nw = THUMB_WIDTH * 2
            nh = int(nw / t_as)
            thumb = thumb.resize((nw, nh), Image.LANCZOS)
            top = (nh - THUMB_HEIGHT * 2) // 2
            thumb = thumb.crop((0, top, THUMB_WIDTH * 2, top + THUMB_HEIGHT * 2))
        thumb = thumb.resize((THUMB_WIDTH, THUMB_HEIGHT), Image.LANCZOS)
        thumb.save(thumbnail_path, "PNG", optimize=True)

        pid = os.path.basename(permalink_dir)
        return (f"/p/{pid}/og-image.png", f"/p/{pid}/thumbnail.png")

    # Priority 1: Use pre-extracted image from RSS if provided
    if pre_extracted_image_url:
        try:
            print(f"[OG] Using pre-extracted image: {pre_extracted_image_url[:80]}")
            img_bytes = _download_image_with_retries(pre_extracted_image_url, referer=source_url, attempts=3,
                                                      timeout=25)
            base_img = Image.open(BytesIO(img_bytes)).convert("RGB")
            return _compose_and_save(base_img)
        except Exception as e:
            print(f"[OG] Pre-extracted image failed: {e}, trying page scrape")

    # Priority 2: Try to fetch and parse the article page
    try:
        html_text = fetch_html(source_url, referer=source_url, timeout=10)
        soup = BeautifulSoup(html_text, "html5lib")
        source_img_url = find_best_image_in_soup(soup, source_url)
        if source_img_url:
            print(f"[OG] Scraped image from page: {source_img_url[:80]}")
            img_bytes = _download_image_with_retries(source_img_url, referer=source_url, attempts=3, timeout=25)
            base_img = Image.open(BytesIO(img_bytes)).convert("RGB")
            return _compose_and_save(base_img)
    except Exception as e:
        print(f"[OG] Could not build branded image from page: {e}")

    # Priority 3: Fallback to placeholder banner
    try:
        if os.path.exists(banner_path):
            print(f"[OG] Using fallback banner")
            banner = Image.open(banner_path).convert("RGB")
            return _compose_and_save(banner)
    except Exception as e:
        print(f"[OG] Fallback banner failed: {e}")

    return ("", "")


def _render_template_string(tpl: str, **kv) -> str:
    """Simple template rendering."""
    html_out = tpl
    for k, v in kv.items():
        html_out = html_out.replace(f"{{{{{k}}}}}", v or "")
    return html_out


def write_permalink_page(it: dict) -> str:
    """Write permalink page for an item."""
    site_base = os.environ.get("SITE_BASE_URL") or (CFG.get("site_base") or "")
    site_base = site_base.rstrip("/")

    title = (it.get("title") or "").strip()
    url = (it.get("url") or "").strip()
    src = (it.get("source") or "").strip()
    dtstr = it.get("published_at") or ""
    dom = domain_of(url)
    try:
        date_fmt = dtparser.parse(dtstr).astimezone(TZ).strftime("%b %-d, %Y") if dtstr else ""
    except Exception:
        date_fmt = ""

    pid = _stable_id(title, url, dtstr)
    perma_dir = os.path.join(PERMA_ROOT, pid)
    os.makedirs(perma_dir, exist_ok=True)

    rel_permalink = f"/p/{pid}/"
    abs_permalink = f"{site_base}{rel_permalink}" if site_base else rel_permalink

    summary = _plain_text_summary(it, limit=180)
    og_image_rel, thumbnail_rel = create_branded_og_image(url, perma_dir, pre_extracted_image_url=it.get("image_url", ""))
    og_image_abs = f"{site_base}{og_image_rel}" if og_image_rel and site_base else og_image_rel
    thumbnail_abs = f"{site_base}{thumbnail_rel}" if thumbnail_rel and site_base else thumbnail_rel

    with open(PERMA_TPL, "r", encoding="utf-8") as f:
        tpl = f.read()

    page = _render_template_string(
        tpl,
        TITLE=title,
        SOURCE=src,
        SOURCE_URL=url,
        DATE=date_fmt,
        DOMAIN=dom,
        ABS_PERMALINK=abs_permalink,
        SUMMARY=summary,
        OG_IMAGE=og_image_abs,
    )
    with open(os.path.join(perma_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(page)

    it["_permalink"] = rel_permalink
    it["_abs_permalink"] = abs_permalink
    it["_summary_240"] = summary
    it["_thumbnail"] = thumbnail_abs
    return abs_permalink


# ============================================================================
# HTML GENERATION
# ============================================================================


# ============================================================================
# PULSE BRIEF (Expandable Daily Summary) - Heuristic / No-LLM
# ============================================================================
#
# Summarizes the *subject matter* of the day's items using only what we have in
# RSS (titles + snippets). No source weighting and no scraping required.
# Deterministic, fast, and zero-cost.
#

_BRIEF_STOPWORDS = {
    "a","an","and","are","as","at","be","been","but","by","can","could","did","do","does","for","from",
    "had","has","have","he","her","hers","him","his","how","i","if","in","into","is","it","its","just",
    "may","more","most","new","no","not","of","on","or","our","out","over","s","she","so","that","the",
    "their","them","then","there","these","they","this","those","to","too","under","up","us","was","we",
    "were","what","when","where","which","who","why","will","with","you","your",
    # news boilerplate
    "today","yesterday","week","year","years","month","months","daily","report","reports","update","updates",
    "breaking","live","read","watch","video","podcast","exclusive","analysis","opinion",
}

_BRIEF_IMPACT_PATTERNS = [
    r"\b(acquire|acquires|acquired|acquisition|merge|merger)\b",
    r"\b(ipo|public offering|files for ipo)\b",
    r"\b(raises|raised|funding|round|seed|series\s?[a-e])\b",
    r"\b(layoff|layoffs|cuts|cutting|job cuts)\b",
    r"\b(lawsuit|sues|suing|settlement|antitrust|doj|ftc)\b",
    r"\b(sec|regulator|regulatory|ban|banned)\b",
    r"\b(release|releases|launch|launches|introduces|unveils|ships)\b",
    r"\b(earnings|guidance|revenue|profit|loss|forecast)\b",
    r"\b(chip|gpu|semiconductor|nvidia|amd|intel)\b",
    r"\b(model|llm|chatgpt|gpt-?4|gpt-?5|claude|gemini)\b",
    r"\b(cyber|breach|hack|ransomware|outage)\b",
]

_BRIEF_BIG_NAMES = [
    "openai","anthropic","nvidia","microsoft","apple","google","alphabet","meta","amazon","aws",
    "tesla","oracle","ibm","salesforce","adobe","intel","amd","tencent","samsung","tsmc",
    "coinbase","stripe","paypal","visa","mastercard",
]

def _brief_clean_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _brief_tokens(s: str) -> list:
    s = _brief_clean_text(s).lower()
    toks = re.split(r"[^a-z0-9]+", s)
    out = []
    for t in toks:
        if not t or len(t) < 3:
            continue
        if t in _BRIEF_STOPWORDS:
            continue
        if t.isdigit() and len(t) <= 2:
            continue
        out.append(t)
    return out



def _brief_extract_keywords(text_blob: str) -> list[str]:
    """Extract candidate *display* keywords for the Daily Brief chips.

    We purposely keep this lightweight and deterministic (no LLM calls).
    It looks at titles + RSS summaries and tries to surface repeated proper nouns,
    acronyms, and a small set of high-signal topic words.
    """
    if not text_blob:
        return []

    blob = text_blob
    out: list[str] = []
    seen = set()

    def _add(tok: str):
        t = (tok or "").strip()
        if not t:
            return
        key = t.lower()
        if key in _BRIEF_STOPWORDS:
            return
        if len(key) < 2:
            return
        if key in seen:
            return
        seen.add(key)
        out.append(t)

    # 1) Proper nouns / product names (Anthropic, OpenAI, Microsoft, Blue Prism, etc.)
    # Keep single tokens; multi-word entities will often show up as repeated single tokens ("Microsoft").
    for m in re.finditer(r"\b[A-Z][A-Za-z0-9&.+_-]{2,}\b", blob):
        w = m.group(0)
        if w.lower() in _BRIEF_STOPWORDS:
            continue
        if w in {"The", "A", "An"}:
            continue
        _add(w)

    # 2) Acronyms (AI, GPU, SEC, ETF, etc.)
    for m in re.finditer(r"\b[A-Z]{2,}\b", blob):
        _add(m.group(0))

    # 3) High-signal topic words from normalized tokens
    topic_map = {
        "ai": "AI",
        "crypto": "Crypto",
        "fintech": "FinTech",
        "security": "Security",
        "privacy": "Privacy",
        "payments": "Payments",
        "banking": "Banking",
        "stablecoins": "Stablecoins",
        "regulation": "Regulation",
        "policy": "Policy",
        "models": "Models",
        "model": "Model",
        "llm": "LLM",
        "gpu": "GPU",
        "chips": "Chips",
        "cloud": "Cloud",
    }
    toks = _brief_tokens(blob)
    for t in toks:
        if t in topic_map:
            _add(topic_map[t])

    return out

def _brief_jaccard_sim(a: str, b: str) -> float:
    """Jaccard similarity of token sets for two strings (0..1)."""
    sa = set(_brief_tokens(a or ""))
    sb = set(_brief_tokens(b or ""))
    if not sa or not sb:
        return 0.0
    inter = sa.intersection(sb)
    union = sa.union(sb)
    if not union:
        return 0.0
    return len(inter) / len(union)


def _brief_top_keyword(s: str) -> str:
    toks = _brief_tokens(s)
    if not toks:
        return ""
    freq = defaultdict(int)
    for t in toks:
        freq[t] += 1
    return max(freq.items(), key=lambda kv: kv[1])[0]

def _brief_global_topics(items: list, top_n: int = 6) -> list:
    freq = defaultdict(int)
    for it in items:
        txt = f"{it.get('title','')} {it.get('summary_text','')} {it.get('summary','')}"
        for t in _brief_tokens(txt):
            freq[t] += 1

    def _score(kv):
        k, v = kv
        bonus = 2 if k in _BRIEF_BIG_NAMES else 0
        penalty = 2 if k in ("ai","amp","via","new") else 0
        return (v + bonus - penalty, len(k))

    ranked = sorted(freq.items(), key=_score, reverse=True)
    return [k for k, _ in ranked[:top_n]]

def _brief_article_score(it: dict, now_local: datetime) -> float:
    title = _brief_clean_text(it.get("title",""))
    summ = _brief_clean_text(it.get("summary_text","") or it.get("summary",""))
    blob = f"{title} {summ}".lower()

    dt = safe_parse_dt(it.get("published_at"))
    if dt:
        try:
            dt_l = dt.astimezone(TZ)
            hours = max(0.0, (now_local - dt_l).total_seconds() / 3600.0)
        except Exception:
            hours = 24.0
    else:
        hours = 24.0

    recency = math.exp(-hours / 18.0)  # ~0.25 after ~25h

    impact = 0.0
    for pat in _BRIEF_IMPACT_PATTERNS:
        if re.search(pat, blob):
            impact += 1.0

    for nm in _BRIEF_BIG_NAMES:
        if nm in blob:
            impact += 0.35

    if re.search(r"[$€£]\s?\d", title) or re.search(r"\b\d+(\.\d+)?\s?(billion|million|bn|m)\b", blob):
        impact += 0.6

    if len(title) < 18:
        impact -= 0.4

    return (1.15 * recency) + (0.25 * impact)

def _brief_word_shingles(text: str, k: int = 3) -> set:
    text = (text or "").lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    words = [w for w in text.split() if len(w) > 2]
    if len(words) < k:
        return set(words)
    return {" ".join(words[i:i+k]) for i in range(0, len(words) - k + 1)}

def _brief_title_similarity(a: str, b: str) -> float:
    sa = _brief_word_shingles(a, 3)
    sb = _brief_word_shingles(b, 3)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)

def _brief_domain(url: str) -> str:
    try:
        ext = tldextract.extract(url or "")
        dom = ".".join([p for p in [ext.domain, ext.suffix] if p])
        return (dom or "").lower()
    except Exception:
        return ""

def _brief_pick_diverse(items: list, max_n: int, now_local: datetime, max_per_domain: int = 1, sim_threshold: float = 0.55) -> list:
    """Pick items with high score but avoid near-duplicates + domain clustering."""
    chosen = []
    domain_counts = {}

    scored = []
    for it in (items or []):
        scored.append((_brief_article_score(it, now_local), it))
    scored.sort(key=lambda x: x[0], reverse=True)

    for score, it in scored:
        if len(chosen) >= max_n:
            break

        url = (it.get("url") or "").strip()
        dom = _brief_domain(url)
        if dom and domain_counts.get(dom, 0) >= max_per_domain:
            continue

        title = it.get("title", "") or ""
        if any(_brief_title_similarity(title, c.get("title","")) >= sim_threshold for c in chosen):
            continue

        chosen.append(it)
        if dom:
            domain_counts[dom] = domain_counts.get(dom, 0) + 1

    return chosen

def _brief_strip_tags(s: str) -> str:
    """Best-effort HTML tag stripper for RSS summaries."""
    if not s:
        return ""
    # Remove tags, collapse whitespace.
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _brief_first_sentence(s: str, max_chars: int = 160) -> str:
    """Grab a readable first sentence (or clause) from a summary/snippet."""
    s = _brief_strip_tags(s)
    if not s:
        return ""
    # Split on sentence-ish boundaries.
    parts = re.split(r"(?<=[\.\?\!])\s+", s)
    first = parts[0].strip() if parts else s.strip()
    if len(first) < 60 and len(parts) > 1:
        # Sometimes the first fragment is too short; include the next sentence.
        first = (first + " " + parts[1].strip()).strip()
    if len(first) > max_chars:
        first = first[:max_chars].rsplit(" ", 1)[0] + "…"
    return first

def _brief_theme_list(keywords: list[str], fallback_tokens: list[str] | None = None, max_themes: int = 3) -> list[str]:
    """Pick 2–3 human-friendly theme words for the header line."""
    themes: list[str] = []
    banned = {"ai", "software", "fintech"}
    for k in (keywords or []):
        kk = (k or "").strip()
        if not kk:
            continue
        if kk.lower() in banned:
            continue
        if kk.lower() in {t.lower() for t in themes}:
            continue
        themes.append(kk)
        if len(themes) >= max_themes:
            break
    if themes:
        return themes

    # Fallback to token list (already lowercased). Title-case for display.
    out = []
    for t in (fallback_tokens or []):
        t = (t or "").strip()
        if not t or t in banned:
            continue
        disp = t.upper() if len(t) <= 4 else t.title()
        if disp.lower() in {x.lower() for x in out}:
            continue
        out.append(disp)
        if len(out) >= max_themes:
            break
    return out

def _brief_make_lede(themes: list[str]) -> str:
    """A single-sentence lede that feels edited but stays deterministic."""
    if not themes:
        return "Today’s Pulse highlights the most important developments across AI, Software, and FinTech."
    if len(themes) == 1:
        return f"Today’s Pulse centers on {themes[0]}, with key developments spanning AI, Software, and FinTech."
    if len(themes) == 2:
        return f"Today’s Pulse is led by {themes[0]} and {themes[1]}, with notable developments across AI, Software, and FinTech."
    return f"Today’s Pulse is led by {themes[0]}, with {themes[1]} and {themes[2]} shaping the agenda across AI, Software, and FinTech."

def _brief_watch_candidate(items: list[dict]) -> dict | None:
    """Pick a single 'watch' item: high-impact / forward-looking / time-sensitive."""
    if not items:
        return None

    # Prefer items that match impact patterns or common catalyst words.
    catalyst_re = re.compile(r"\b(earnings|results|guidance|sec|cfpb|fed|bank|lawsuit|court|hearing|vote|deadline|ban|approval|etf|ipo|merger|acquisition|breach|hack|exploit|patch|launch|release)\b", re.I)
    ranked = []
    for it in items:
        title = _brief_clean_text(it.get("title",""))
        summ = _brief_clean_text(it.get("summary_text","") or it.get("summary",""))
        blob = f"{title} {summ}"
        score = 0.0
        if catalyst_re.search(blob):
            score += 2.0
        for pat in _BRIEF_IMPACT_PATTERNS:
            if re.search(pat, blob.lower()):
                score += 0.8
        # Recency boost
        dt = safe_parse_dt(it.get("published_at"))
        if dt:
            try:
                hours = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)
                score += math.exp(-hours / 18.0)
            except Exception:
                pass
        ranked.append((score, it))
    ranked.sort(key=lambda kv: kv[0], reverse=True)
    return ranked[0][1] if ranked else items[0]

def compute_pulse_brief(by_cat: dict, now_local, max_items: int = 6) -> dict:
    """
    Editorial Brief (deterministic, no LLM calls):
    - Themes (2–3) derived from repeated keywords
    - One lede sentence
    - 3–6 takeaway bullets (title + first sentence of summary/snippet)
    - One 'Watch' line (single catalyst item)
    - Keyword chips retained as topic tags
    """
    if not by_cat:
        return {}

    # Flatten items (ensure category is present)
    all_items: list[dict] = []
    for cat, items in (by_cat or {}).items():
        for it in (items or []):
            if not isinstance(it, dict):
                continue
            if "category" not in it and cat:
                it = dict(it)
                it["category"] = cat
            all_items.append(it)

    if not all_items:
        return {}

    # Build keyword chips from titles + summaries
    rows = []
    for it in all_items:
        title = (it.get("title") or "").strip()
        summ = (it.get("summary") or it.get("summary_text") or it.get("description") or it.get("snippet") or "").strip()
        if title or summ:
            rows.append(f"{title}\n{summ}")

    counts = Counter()
    for blob in rows:
        counts.update(_brief_extract_keywords(blob))

    # Remove globally-common topics (helps reduce "AI" / "users" type noise)
    global_topics = set(_brief_global_topics(all_items, top_n=6))
    if global_topics:
        for k in list(counts.keys()):
            if (k or "").lower() in global_topics:
                counts.pop(k, None)

    keywords = [w for (w, _n) in counts.most_common(12) if w]
    themes = _brief_theme_list(keywords, fallback_tokens=list(global_topics) if global_topics else None, max_themes=3)
    lede = _brief_make_lede(themes)

    # Pick takeaway items: score + de-dupe + domain diversity
    picked = _brief_pick_diverse(all_items, now_local=now_local, max_n=max_items, max_per_domain=1, sim_threshold=0.55)
    if not picked:
        picked = sort_by_recency(all_items)[:max_items]

    takeaways = []
    for it in (picked or [])[:max_items]:
        title = _brief_clean_text(it.get("title",""))
        summ = it.get("summary_text") or it.get("summary") or it.get("description") or it.get("snippet") or ""
        impact = _brief_first_sentence(summ, max_chars=170)

        # If the summary is basically a duplicate of the title, fall back to a shorter clause.
        if impact and title and impact.lower().startswith(title.lower()[: min(18, len(title))]):
            impact = _brief_first_sentence(re.sub(re.escape(title), "", summ, flags=re.I), max_chars=140)

        if not impact:
            # Deterministic fallback: category + top theme.
            cat = (it.get("category") or "").upper()
            topic = themes[0] if themes else "today’s themes"
            impact = f"Key development in {cat or 'today’s feed'} tied to {topic}."

        takeaways.append({
            "title": title,
            "impact": impact,
            "url": it.get("url") or "",
            "source": it.get("source") or "",
            "published_at": it.get("published_at") or "",
            "category": it.get("category") or "",
            "image_url": it.get("image_url") or "",
        })

        return {
        "generated_at": now_local.isoformat() if hasattr(now_local, "isoformat") else "",
        "themes": themes,
        "lede": lede,
        "takeaways": takeaways,
    }


def render_pulse_brief_html(brief: dict, date_str: str = "") -> str:
    """Render the DAILY_BRIEF block.

    Design intent:
    - A compact, editor-style summary of the most important stories.
    - Avoid redundancy/noisy “topics chips” and “watch” callouts.
    - Each takeaway includes: headline + 1-sentence impact + Read link + source.
    """

    if not brief:
        return ""

    themes = brief.get("themes") or []
    lede = (brief.get("lede") or "").strip()
    takeaways = brief.get("takeaways") or []

    themes_html = ""
    if themes:
        themes_html = " · ".join(html.escape(str(t)) for t in themes[:4])

    li_parts = []
    for t in takeaways:
        title = (t.get("title") or "").strip()
        if not title:
            continue

        impact = (t.get("impact") or "").strip()
        url = (t.get("url") or "").strip()
        source = (t.get("source") or "").strip()

        title_txt = html.escape(title)
        impact_txt = html.escape(impact) if impact else ""
        source_txt = html.escape(source) if source else ""

        if url:
            safe_url = html.escape(url)
            title_html = f'<a class="pb-title" href="{safe_url}" target="_blank" rel="noopener noreferrer">{title_txt}</a>'
            read_html = f'<a class="pb-read" href="{safe_url}" target="_blank" rel="noopener noreferrer">Read</a>'
        else:
            title_html = title_txt
            read_html = ""

        tail = ""
        if read_html and source_txt:
            tail = f' {read_html} · <span class="pb-source">{source_txt}</span>'
        elif read_html:
            tail = f' {read_html}'
        elif source_txt:
            tail = f' <span class="pb-source">{source_txt}</span>'

        if impact_txt:
            li_parts.append(f"<li><strong>{title_html}</strong> — <span class='pb-impact'>{impact_txt}</span>{tail}</li>")
        else:
            li_parts.append(f"<li><strong>{title_html}</strong>{tail}</li>")

    takeaways_html = "<ul class='pb-takeaways'>" + "".join(li_parts) + "</ul>" if li_parts else ""

    lede_html = html.escape(lede) if lede else ""

    return f"""
<div class="pb-card" data-brief>
  <div class="pb-head">
    <div class="pb-kicker">
      <span class="pb-pill">BRIEF</span>
      <span class="pb-themes">{themes_html}</span>
    </div>
    <button class="pb-toggle" type="button" aria-label="Collapse brief" title="Collapse brief">▲</button>
  </div>
  <div class="pb-body">
    <p class="pb-lede">{lede_html}</p>
    {takeaways_html}
  </div>
</div>
"""

def build_section(date_str: str, by_cat: dict, brief: dict = None) -> str:
    """Build HTML section from categorized items."""
    with open(os.path.join(ROOT, "templates/section_template.html"), "r", encoding="utf-8") as f:
        tpl = f.read()

    # Build the daily brief HTML for this date (used by {{DAILY_BRIEF}})
    if brief is None:
        brief = compute_pulse_brief(by_cat, now_local=now_et())
    brief_html = render_pulse_brief_html(brief, date_str=date_str)

    def render_item_badges(it: dict, now: datetime = None) -> str:
        now = now or datetime.now(timezone.utc)
        dt = safe_parse_dt(it.get("published_at"))
        if not dt:
            return ""
        if it.get("_backfilled"):
            return ""
        if it.get("_older_than_fresh_window"):
            return '<span class="badge muted">Older</span>'
        try:
            age = (now - dt.astimezone(timezone.utc)).total_seconds()
            if age < 24 * 3600:
                return '<span class="badge">New</span>'
        except Exception:
            pass
        return ""

    def render_items(items: list) -> str:
        parts = []
        for idx, it in enumerate(items):
            # REMOVED: Trending Badge Logic
            # REMOVED: Trending Chips Logic

            title_raw = it["title"]
            title = html.escape(title_raw)
            url_raw = it["url"]
            url = add_utm(url_raw)
            permalink = it.get("_abs_permalink", "")

            if "youtube.com" in url_raw or "youtu.be" in url_raw:
                thumbnail = ""
                try:
                    video_id = None
                    if "watch?v=" in url_raw:
                        video_id = url_raw.split("watch?v=")[1].split("&")[0]
                    elif "youtu.be/" in url_raw:
                        video_id = url_raw.split("youtu.be/")[1].split("?")[0]
                    if video_id:
                        thumbnail = f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg"
                except Exception:
                    pass
            else:
                thumbnail = it.get("_thumbnail") or it.get("image_url") or ""

            src = html.escape(it["source"])
            try:
                dt_local = dtparser.parse(it["published_at"]).astimezone(TZ)
                dt_str = dt_local.strftime("%b %-d, %Y")
            except Exception:
                dt_str = date_str
            summary_txt = clean_text(strip_html_to_text(it.get("summary_text", "")), 180)
            summary_html = html.escape(summary_txt)
            thumb_html = f'<img src="{thumbnail}" alt="{html.escape(title_raw, quote=True)}" class="article-thumb" loading="lazy">' if thumbnail else ''

            # CLEANED UP HTML STRUCTURE (No trends)
            parts.append(f"""<article data-card data-url="{url}" data-permalink="{permalink}" data-title="{html.escape(title_raw, quote=True)}" data-summary="{summary_html}" data-source="{src}">
  {thumb_html}
  <div class="article-content">
    <h3><a data-title-link href="{url}">{title}</a></h3>
    <div class="meta"><span class="src">{src}</span> · {dt_str} {render_item_badges(it)}</div>
    <p data-summary>{summary_html}</p>
  </div>
</article>""")
        return "\n".join(parts)

    html_out = tpl.replace("{{DATE_STR}}", date_str)
    total_count = sum(len(by_cat.get(k, [])) for k in ("ai", "software", "fintech"))

    def _final_sort_dt(it):
        try:
            return dtparser.parse(it["published_at"]).astimezone(TZ)
        except Exception:
            return datetime.min.replace(tzinfo=TZ)

    # Combine all articles into one chronological list
    all_articles = []
    for cat_key in ("ai", "software", "fintech"):
        items = by_cat.get(cat_key, [])
        all_articles.extend(items)

    all_articles.sort(key=_final_sort_dt, reverse=True)
    if MAX_PAGE_ITEMS:
        all_articles = all_articles[:MAX_PAGE_ITEMS]
    all_items_html = render_items(all_articles) if all_articles else "<p>No items today.</p>"

    html_out = html_out.replace("{{DAILY_BRIEF}}", brief_html)
    html_out = html_out.replace("{{AI_ITEMS}}", all_items_html)
    html_out = html_out.replace("{{SW_ITEMS}}", "")
    html_out = html_out.replace("{{FT_ITEMS}}", "")
    html_out = html_out.replace("{{AI_COUNT}}", str(len(all_articles)))
    html_out = html_out.replace("{{SW_COUNT}}", "0")
    html_out = html_out.replace("{{FT_COUNT}}", "0")
    html_out = html_out.replace("{{TOTAL_COUNT}}", str(total_count))
    html_out = html_out.replace("{{COUNT}}", str(total_count))
    return html_out


# ============================================================================
# WEEKEND CACHE
# ============================================================================

def _last_friday(d: datetime) -> datetime:
    """Get last Friday's date."""
    wd = d.weekday()
    delta = (wd - 4) % 7
    return d - timedelta(days=delta)


def _weekend_use_friday_payload_if_available() -> Optional[bool]:
    """Reuse Friday's payload on weekends if available."""
    today = now_et().date()
    wd = today.weekday()
    if wd not in (5, 6):
        return None
    want_friday = _last_friday(today)
    if CACHE_FILE.exists():
        try:
            cached = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            if cached.get("ref_date") == want_friday.strftime("%Y-%m-%d"):
                by_cat = cached.get("by_cat", {})
                date_str = today.strftime("%b %-d, %Y")
                brief = compute_pulse_brief(by_cat, now_local=now_et()); section = build_section(date_str, by_cat, brief)
                docs = os.path.join(REPO, "docs")
                os.makedirs(docs, exist_ok=True)
                with open(os.path.join(docs, "index.html"), "w", encoding="utf-8") as f:
                    f.write(section)
                with open(os.path.join(docs, "pulse.json"), "w", encoding="utf-8") as f:
                    json.dump(cached.get("all_items", []), f, indent=2)
                return True
        except Exception:
            pass
    return False


def _save_friday_snapshot_if_today(all_items: list, by_cat: dict) -> None:
    """Save Friday snapshot for weekend reuse."""
    today = now_et().date()
    if today.weekday() == 4:
        payload = {"ref_date": today.strftime("%Y-%m-%d"), "all_items": all_items, "by_cat": by_cat}
        try:
            CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass


# ============================================================================
# MAIN
# ============================================================================

def main():
    # Weekend: reuse Friday snapshot if available
    if os.environ.get('DISABLE_WEEKEND_CACHE', '0') != '1':
        if _weekend_use_friday_payload_if_available():
            return

    print("=== Starting T2D Pulse Generation ===")

    # REMOVED: Fetching trending keywords logic

    # Fetch RSS feeds in parallel (major performance improvement)
    print(f"\n--- Fetching {len(CFG['sources']['rss'])} RSS feeds (parallel) ---")
    all_items = fetch_all_rss_parallel(CFG["sources"]["rss"], max_workers=8)

    # Dedupe
    print(f"\n--- Before dedupe: {len(all_items)} total articles ---")
    all_items = dedupe(all_items)
    print(f"--- After dedupe: {len(all_items)} unique articles ---")

    # Final filtering, relevance, enrichment
    pruned = []
    for it in all_items:
        if is_blocked(it["url"]):
            continue
        if is_deals_or_consumer_shopping(it["title"], it["url"]):
            continue
        it["summary_text"] = summarize(it)
        cat, score = categorize_with_score(it["title"], it["url"], it.get("summary_text", ""))
        src_norm = (it.get("source") or "").strip().lower()

        # Debug logging for TEK2day articles
        if "tek2day" in src_norm:
            print(f"[DEBUG] TEK2day article found: '{it.get('title', 'Unknown')}' | Source: '{it.get('source')}' | Score: {score}")

        is_youtube_src = ("youtube" in src_norm)
        is_force_included = any(term.lower() in src_norm for term in FORCE_INCLUDE_SOURCES)

        if "tek2day" in src_norm:
            print(f"[DEBUG] TEK2day article - is_force_included: {is_force_included}, will keep: {score > 0 or is_force_included}")

        if score == 0 and (src_norm not in FORCE_AI_SOURCES) and (not is_youtube_src) and (not is_force_included):
            continue
        it["category"] = cat

        # REMOVED: Tagging with trending keywords

        # Force-routing for certain sources/domains
        try:
            d = domain_of(it["url"])
        except Exception:
            d = ""
        is_force_included_src = any(term.lower() in src_norm for term in FORCE_INCLUDE_SOURCES)

        if ("youtube" in src_norm) or (src_norm in FORCE_AI_SOURCES) or is_force_included_src:
            it["category"] = "ai"
        elif d in FORCE_FINTECH_DOMAINS or src_norm in FORCE_FINTECH_SOURCES:
            scores = compute_scores(it["title"], it["url"], it.get("summary_text", ""))
            if not (scores["ai"] >= 3 and scores["ai"] >= scores["fintech"] + 1):
                it["category"] = "fintech"

        pruned.append(it)

    all_items = pruned

    # Sort newest first
    def parsed_dt(it):
        try:
            return dtparser.parse(it["published_at"]).astimezone(TZ)
        except Exception:
            return now_et()

    all_items.sort(key=parsed_dt, reverse=True)

    # Bucket
    by_cat = build_preferred_today_with_floors(
        all_items,
        now_local=now_et(),
        floors=None,
        backfill_days=BACKFILL_WINDOW_DAYS
    )

    # REMOVED: Loop to ensure chips are present

    # Generate permalinks and concise summaries for today's items (PARALLEL for speed)
    unique_items, _seen = [], set()
    items_to_process = []
    
    for cat in ("ai", "software", "fintech"):
        for it in by_cat.get(cat, []):
            key = f"{re.sub(r'[^a-z0-9]+', '', (it.get('title') or '').lower())}::{domain_of(it.get('url', ''))}"
            if key in _seen:
                continue
            _seen.add(key)
            items_to_process.append(it)
            unique_items.append(it)
    
    # Parallel permalink generation (OG images) - major speed improvement
    print(f"\n--- Generating {len(items_to_process)} permalink pages (parallel, 6 workers) ---")
    failed_count = 0
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(write_permalink_page, it): it for it in items_to_process}
        for future in as_completed(futures):
            it = futures[future]
            try:
                future.result()
            except Exception as e:
                failed_count += 1
                print(f"Warning: Failed to create permalink for '{it.get('title', 'Unknown')[:50]}': {e}")
    
    if failed_count:
        print(f"--- Permalink generation complete: {len(items_to_process) - failed_count} succeeded, {failed_count} failed ---")
    else:
        print(f"--- Permalink generation complete: {len(items_to_process)} pages created ---")

    # Render
    date_str = now_et().strftime("%b %-d, %Y")
    brief = compute_pulse_brief(by_cat, now_local=now_et())
    section = build_section(date_str, by_cat, brief)

    # Save Friday snapshot for weekend reuse
    _save_friday_snapshot_if_today(all_items, by_cat)

    # Write outputs
    docs = os.path.join(REPO, "docs")
    os.makedirs(docs, exist_ok=True)
    with open(os.path.join(docs, "index.html"), "w", encoding="utf-8") as f:
        f.write(section)
    with open(os.path.join(docs, "pulse.json"), "w", encoding="utf-8") as f:
        json.dump(all_items, f, indent=2)

    # Timestamped snapshot
    try:
        ts_dir = os.path.join(docs, "archive", "timestamped")
        os.makedirs(ts_dir, exist_ok=True)
        ts_name = now_et().strftime("%Y-%m-%d_%H%M%S") + ".json"
        ts_path = os.path.join(ts_dir, ts_name)
        with open(ts_path, "w", encoding="utf-8") as tf:
            json.dump({"items": all_items, "brief": brief}, tf, indent=2)
    except FileExistsError:
        pass
    except Exception:
        pass

    # REMOVED: Trending analytics JSON

    # Daily snapshot (overwrite each run so the homepage + brief stay consistent)
    try:
        arch_dir = os.path.join(docs, "archive", "json")
        os.makedirs(arch_dir, exist_ok=True)
        snap_name = now_et().strftime("%Y-%m-%d") + ".json"
        arch_path = os.path.join(arch_dir, snap_name)
        with open(arch_path, "w", encoding="utf-8") as f:
            json.dump({"date": now_et().strftime("%Y-%m-%d"), "by_cat": by_cat, "brief": brief}, f, indent=2)
    except Exception:
        pass

    # REMOVED: Backfill retro-tagging logic

    print(f"\n=== T2D Pulse Generation Complete ===")
    print(f"Total articles: {len(all_items)}")
    print(f"By category: AI={len(by_cat.get('ai', []))}, Software={len(by_cat.get('software', []))}, FinTech={len(by_cat.get('fintech', []))}")


if __name__ == "__main__":
    main()
