#!/usr/bin/env python3
# generator/generate_pulse.py
#
# Pipeline: fetch RSS in parallel -> dedupe -> filter/categorize -> generate
# permalink pages + social images -> render index.html + pulse.json.
#
# Generated output under docs/ (p/, archive/, index.html, pulse.json) is not
# tracked in git; the workflow restores p/ and archive/ from the Actions cache
# so unchanged articles are not re-downloaded and re-encoded on every run.

import os
import re
import json
import html
import time
import random
import hashlib
import math
import shutil
import threading
import urllib.parse
from io import BytesIO
from typing import Optional
from datetime import datetime, timedelta, timezone, date
from urllib.parse import urlparse
from collections import defaultdict
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

_raw_max = CFG.get("max_items_per_category", 150)
if (_raw_max is None) or (isinstance(_raw_max, str) and _raw_max.strip().lower() in ("none", "null", "")):
    MAX_ITEMS = None
else:
    try:
        MAX_ITEMS = int(_raw_max)
        if MAX_ITEMS <= 0:
            MAX_ITEMS = None
    except (TypeError, ValueError):
        MAX_ITEMS = None

UTM = CFG.get("utm", {"source": "tek2day", "medium": "email"})
BLOCK_SUFFIXES = [s.lower() for s in CFG.get("exclude_domains_suffix", [])]
ALWAYS_BLOCK = {"news.ycombinator.com", "ycombinator.com"}
SITE_BASE = (os.environ.get("SITE_BASE_URL") or CFG.get("site_base") or "").rstrip("/")

# Newest-first cap per source so one high-volume feed cannot dominate the page.
# Sources in FORCE_INCLUDE_SOURCES are exempt.
_raw_source_cap = CFG.get("max_items_per_source", 10)
try:
    MAX_ITEMS_PER_SOURCE = int(_raw_source_cap) if _raw_source_cap not in (None, "", "none", "null") else None
    if MAX_ITEMS_PER_SOURCE is not None and MAX_ITEMS_PER_SOURCE <= 0:
        MAX_ITEMS_PER_SOURCE = None
except (TypeError, ValueError):
    MAX_ITEMS_PER_SOURCE = None

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
RETENTION_DAYS = int(CFG.get("retention_days", 7))

# Permalink paths
PERMA_ROOT = os.path.join(REPO, "docs", "p")
PERMA_TPL = os.path.join(ROOT, "templates", "item_template.html")

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
FEED_STATUS = []


def fetch_html(url: str, referer: str | None = None, timeout: int = 15) -> str:
    """Fetch HTML with shared session + optional Referer."""
    headers = {}
    if referer:
        headers["Referer"] = referer
    r = SESSION.get(url, headers=headers, allow_redirects=True, timeout=timeout)
    r.raise_for_status()
    return r.text


# Article pages are needed by up to three steps (summary fallback, image
# scraping, social-card generation). Fetch each URL at most once per run.
_PAGE_CACHE: dict[str, str] = {}
_PAGE_CACHE_LOCK = threading.Lock()
IMAGE_STATS = {"reused": 0, "generated": 0}


def fetch_page_cached(url: str, timeout: int = 10) -> str:
    """Fetch an article page once per run; later callers get the cached HTML.

    Failures are cached too (as an empty string) so a dead page is not retried
    by every downstream step.
    """
    with _PAGE_CACHE_LOCK:
        if url in _PAGE_CACHE:
            return _PAGE_CACHE[url]
    try:
        text = fetch_html(url, referer=url, timeout=timeout)
    except Exception as e:
        print(f"[PAGE] fetch failed: {url[:80]}: {e}")
        text = ""
    with _PAGE_CACHE_LOCK:
        _PAGE_CACHE[url] = text
    return text


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def now_et() -> datetime:
    """Get current time in Eastern timezone."""
    return datetime.now(TZ)


def display_date(value: datetime) -> str:
    """Portable date formatting for Windows previews and Linux publishing."""
    return f"{value:%b} {value.day}, {value:%Y}"


def safe_web_url(value: str) -> str:
    """Only allow HTTP(S) links in feed-derived HTML attributes."""
    value = (value or "").strip()
    try:
        parsed = urlparse(value)
        return value if parsed.scheme.lower() in ("http", "https") and parsed.hostname else ""
    except ValueError:
        return ""


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
        return BeautifulSoup(s, "html.parser").get_text(" ", strip=True)
    except Exception:
        return s


def add_utm(url: str) -> str:
    """Add UTM parameters to URL."""
    return f"{url}{'&' if '?' in url else '?'}utm_source={UTM['source']}&utm_medium={UTM['medium']}"


TRACKING_QUERY_KEYS = {
    "fbclid", "gclid", "igshid", "mc_cid", "mc_eid", "mkt_tok", "ref", "ref_src",
    "source", "src", "s", "si", "spm", "trk", "utm_campaign", "utm_content",
    "utm_id", "utm_medium", "utm_name", "utm_source", "utm_term", "ved", "wpisrc",
}
IDENTITY_QUERY_KEYS = {"id", "p", "post", "story", "article", "item", "v"}


def canonicalize_url(url: str) -> str:
    """Normalize URL for de-duplication while preserving identity query params."""
    if not url:
        return ""
    try:
        parsed = urlparse(url.strip())
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = (parsed.path or "/").strip()
        if not path.startswith("/"):
            path = "/" + path
        path = path if path == "/" else path.rstrip("/")

        # YouTube watch URLs must preserve video ID.
        if "youtube.com" in host and path == "/watch":
            qs = urllib.parse.parse_qs(parsed.query or "")
            vid = (qs.get("v") or [""])[0]
            return f"{host}{path}?v={vid}".rstrip("?")

        query_pairs = urllib.parse.parse_qsl(parsed.query or "", keep_blank_values=False)
        kept = []
        for k, v in query_pairs:
            key = (k or "").strip().lower()
            if not key:
                continue
            if key in TRACKING_QUERY_KEYS or key.startswith("utm_"):
                continue
            if key in IDENTITY_QUERY_KEYS:
                kept.append((key, v))
        query = urllib.parse.urlencode(sorted(kept)) if kept else ""
        return f"{host}{path}{('?' + query) if query else ''}"
    except Exception:
        return (url or "").strip().lower()


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


def permalink_id_for_item(item: dict) -> str:
    """Derive permalink ID exactly as permalink page generation does."""
    return _stable_id(item.get("title", ""), item.get("url", ""), item.get("published_at", ""))


def is_within_retention(item: dict, now: datetime, retention_days: int = RETENTION_DAYS) -> bool:
    """True when item timestamp is within retention window."""
    dt = safe_parse_dt(item.get("published_at"))
    if not dt:
        return False
    return dt.astimezone(timezone.utc) >= (now.astimezone(timezone.utc) - timedelta(days=retention_days))


def retain_recent_items(items: list, now: datetime, retention_days: int = RETENTION_DAYS) -> list:
    """Keep only items within retention window."""
    return [it for it in items if is_within_retention(it, now=now, retention_days=retention_days)]


_CONFIG_CAP = object()  # sentinel: resolve MAX_ITEMS_PER_SOURCE at call time, not def time


def cap_per_source(items: list, cap=_CONFIG_CAP) -> list:
    """Keep at most `cap` items per source, preserving order (call on a newest-first list)."""
    if cap is _CONFIG_CAP:
        cap = MAX_ITEMS_PER_SOURCE
    if not cap:
        return list(items)
    counts: dict[str, int] = defaultdict(int)
    out = []
    for it in items:
        src = (it.get("source") or "").strip().lower()
        if any(term in src for term in FORCE_INCLUDE_SOURCES):
            out.append(it)
            continue
        if counts[src] >= cap:
            continue
        counts[src] += 1
        out.append(it)
    return out


def public_item(it: dict) -> dict:
    """Shape an item for pulse.json / archives: drop raw feed HTML that no consumer uses."""
    return {k: v for k, v in it.items() if k != "content_html"}


def _extract_date_from_filename(filename: str, fmt: str) -> Optional[date]:
    try:
        date_part = filename.split("_")[0] if "_" in filename else filename.replace(".json", "")
        return datetime.strptime(date_part, fmt).date()
    except Exception:
        return None


def _extract_permalink_id(item: dict) -> str:
    perma = (item.get("_permalink") or "").strip()
    m = re.search(r"/p/([a-f0-9]{10})/?", perma)
    if m:
        return m.group(1)
    return permalink_id_for_item(item)


def purge_old_outputs(docs_dir: str, now_local: datetime, retention_days: int, seed_items: list) -> None:
    """Delete old snapshots and orphaned permalink folders outside retention window."""
    cutoff_date = (now_local - timedelta(days=retention_days)).date()
    ts_dir = os.path.join(docs_dir, "archive", "timestamped")
    day_dir = os.path.join(docs_dir, "archive", "json")

    for base_dir, date_fmt in ((ts_dir, "%Y-%m-%d"), (day_dir, "%Y-%m-%d")):
        if not os.path.isdir(base_dir):
            continue
        for name in os.listdir(base_dir):
            if not name.endswith(".json"):
                continue
            file_date = _extract_date_from_filename(name, date_fmt)
            if file_date and file_date < cutoff_date:
                try:
                    os.remove(os.path.join(base_dir, name))
                except Exception:
                    pass

    keep_ids = {_extract_permalink_id(it) for it in seed_items if it.get("title") and it.get("url")}

    def _load_and_collect(path: str):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return

        if isinstance(data, dict):
            if isinstance(data.get("items"), list):
                for it in data["items"]:
                    keep_ids.add(_extract_permalink_id(it))
            by_cat = data.get("by_cat")
            if isinstance(by_cat, dict):
                for cat_items in by_cat.values():
                    if isinstance(cat_items, list):
                        for it in cat_items:
                            keep_ids.add(_extract_permalink_id(it))

    for base_dir in (ts_dir, day_dir):
        if not os.path.isdir(base_dir):
            continue
        for name in os.listdir(base_dir):
            if name.endswith(".json"):
                _load_and_collect(os.path.join(base_dir, name))

    perma_root = os.path.join(docs_dir, "p")
    if os.path.isdir(perma_root):
        for pid in os.listdir(perma_root):
            perma_dir = os.path.join(perma_root, pid)
            if not os.path.isdir(perma_dir):
                continue
            if not re.fullmatch(r"[a-f0-9]{10}", pid):
                continue
            if pid not in keep_ids:
                try:
                    shutil.rmtree(perma_dir)
                except Exception:
                    pass


# ============================================================================
# NYT IMAGE HELPERS
# ============================================================================

def _nyt_prefer_super_jumbo(u: str) -> str:
    """Prefer largest NYT size suffix (superJumbo) when present."""
    return re.sub(
        r'-(?:thumbLarge|threeByTwoSmallAt2X|threeByTwoLargeAt2X|articleLeft|articleLarge)\.(jpg|jpeg|png|webp)$',
        r'-superJumbo.\1', u
    )


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
    current = now_et()
    cutoff = current - timedelta(days=BACKFILL_WINDOW_DAYS)
    return cutoff <= dt_local <= current + timedelta(minutes=15)


def fetch_rss(feed_name: str, url: str) -> list[dict]:
    """Fetch RSS feed with fallback image scraping."""
    try:
        # Fetch through the retrying HTTP client; feedparser's URL path has no timeout.
        with create_session() as session:
            response = session.get(url, timeout=(10, 30))
            response.raise_for_status()
            d = feedparser.parse(response.content, response_headers={
                "content-location": response.url,
                "content-type": response.headers.get("content-type", ""),
            })
        if not d.entries and (d.bozo or not d.get("version")):
            raise ValueError("Response is not a usable RSS or Atom feed")
        items = []
        for e in d.entries[:60]:
            title = clean_text(getattr(e, "title", ""))
            link = safe_web_url(getattr(e, "link", ""))
            if not title or not link:
                continue
            if is_blocked(link) and "youtube.com/feeds/videos.xml" not in (url or ""):
                continue
            dt_local = parse_pubdate(e)
            is_youtube_feed = 'youtube.com/feeds/videos.xml' in (url or '')
            if dt_local > now_et() + timedelta(minutes=15):
                continue
            if not is_youtube_feed and not within_window(dt_local):
                continue
            raw_sum = getattr(e, "summary", "") or getattr(e, "description", "")
            summary = clean_text(strip_html_to_text(raw_sum), 400)
            content_html = ""
            if hasattr(e, "content"):
                try:
                    content_html = e.content[0].value
                except Exception:
                    pass
            # Only what the feed itself carries. Entries without an image are
            # scraped later, after dedupe/filtering, in create_branded_og_image.
            image_url = extract_image_url(e)

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
        raise RuntimeError(f"Feed failed: {feed_name}") from e


def fetch_all_rss_parallel(sources: list[dict], max_workers: int = 8) -> list[dict]:
    """Fetch all RSS feeds in parallel for better performance."""
    all_items = []
    FEED_STATUS.clear()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_rss, s["name"], s["url"]): s["name"]
            for s in sources
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                items = future.result()
                print(f"[OK] {name}: {len(items)} articles")
                all_items.extend(items)
                FEED_STATUS.append({"source": name, "status": "ok", "articles": len(items)})
            except Exception as e:
                print(f"[ERROR] {name}: {e}")
                FEED_STATUS.append({"source": name, "status": "error", "articles": 0})

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
            key = canonicalize_url(it.get("url") or it.get("permalink") or "")
        if not key:
            key = (it.get("id") or "").lower()
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
        normalized_url = canonicalize_url(url)
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
# BUCKETING (merge archive + fresh fetch, keep the last FRESH_WINDOW_DAYS)
# ============================================================================

def _load_archived_items(now_local: datetime) -> list:
    """Items from timestamped archives within the fresh window (restored by the Actions cache)."""
    arch_dir = os.path.join(REPO, "docs", "archive", "timestamped")
    cutoff_date = (now_local - timedelta(days=FRESH_WINDOW_DAYS)).date()
    archived = []
    if not os.path.isdir(arch_dir):
        return archived
    for filename in os.listdir(arch_dir):
        if not filename.endswith(".json"):
            continue
        try:
            file_date = datetime.strptime(filename.split("_")[0], "%Y-%m-%d").date()
            if file_date < cutoff_date:
                continue
            with open(os.path.join(arch_dir, filename), "r", encoding="utf-8") as f:
                archived.extend(json.load(f).get("items", []))
        except Exception:
            continue
    return archived


def bucket_recent_by_category(all_items: list, now_local: datetime = None) -> dict:
    """Merge archived + freshly fetched items, dedupe, cap per source, bucket by category."""
    now_local = now_local or now_et()

    all_combined = dedupe_story_variants(_load_archived_items(now_local) + all_items)
    seen_urls = set()
    unique_items = []
    for it in all_combined:
        url_key = canonicalize_url(it.get("url", ""))
        if url_key and url_key not in seen_urls:
            seen_urls.add(url_key)
            unique_items.append(it)

    def _dt_local(it):
        try:
            return dtparser.parse(it["published_at"]).astimezone(TZ)
        except Exception:
            return now_local

    unique_items.sort(key=_dt_local, reverse=True)
    unique_items = cap_per_source(unique_items)

    pool_by_cat = {"ai": [], "software": [], "fintech": []}
    for it in unique_items:
        if _dt_local(it) >= now_local - timedelta(days=FRESH_WINDOW_DAYS):
            cat = it.get("category")
            if cat in pool_by_cat:
                pool_by_cat[cat].append(it)

    return {cat: pool_by_cat[cat][:MAX_ITEMS] for cat in ("ai", "software", "fintech")}


# ============================================================================
# SUMMARIZATION
# ============================================================================

def summarize(item: dict) -> str:
    """Get or fetch summary for item."""
    if item.get("summary"):
        return clean_text(item["summary"], 260)
    try:
        page_html = fetch_page_cached(item["url"], timeout=12)
        if page_html:
            soup = BeautifulSoup(page_html, "html.parser")
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

OG_IMAGE_NAME = "og-image.jpg"
THUMBNAIL_NAME = "thumbnail.jpg"
# JPEG instead of PNG: photos compress ~6-10x smaller with no visible loss,
# and every card on the homepage loads a thumbnail.
OG_JPEG_QUALITY = 82
THUMB_JPEG_QUALITY = 80


def create_branded_og_image(source_url: str, permalink_dir: str, pre_extracted_image_url: str = "") -> tuple[str, str, str]:
    """Create branded OG image + thumbnail.

    Returns (og_image_rel_path, thumbnail_rel_path, source_image_url). The
    source image URL is whichever image was actually used (RSS-provided or
    scraped), so callers can record it on the item.
    """
    logo_path = os.path.join(REPO, "docs", "icons", "t2d-pulse-512.png")
    banner_path = os.path.join(REPO, "docs", "icons", "T2D_Pulse_Banner.png")
    output_path = os.path.join(permalink_dir, OG_IMAGE_NAME)
    thumbnail_path = os.path.join(permalink_dir, THUMBNAIL_NAME)

    TARGET_WIDTH, TARGET_HEIGHT = 1200, 630
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

        og_img.save(output_path, "JPEG", quality=OG_JPEG_QUALITY, optimize=True, progressive=True)

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
        thumb.save(thumbnail_path, "JPEG", quality=THUMB_JPEG_QUALITY, optimize=True, progressive=True)

        pid = os.path.basename(permalink_dir)
        return (f"/p/{pid}/{OG_IMAGE_NAME}", f"/p/{pid}/{THUMBNAIL_NAME}")

    # Priority 1: Use pre-extracted image from RSS if provided
    if pre_extracted_image_url:
        try:
            print(f"[OG] Using pre-extracted image: {pre_extracted_image_url[:80]}")
            img_bytes = _download_image_with_retries(pre_extracted_image_url, referer=source_url, attempts=3,
                                                      timeout=25)
            base_img = Image.open(BytesIO(img_bytes)).convert("RGB")
            return (*_compose_and_save(base_img), pre_extracted_image_url)
        except Exception as e:
            print(f"[OG] Pre-extracted image failed: {e}, trying page scrape")

    # Priority 2: Try to fetch and parse the article page (fetched at most once per run)
    try:
        html_text = fetch_page_cached(source_url, timeout=10)
        source_img_url = find_best_image_in_soup(BeautifulSoup(html_text, "html.parser"), source_url) if html_text else ""
        if source_img_url:
            print(f"[OG] Scraped image from page: {source_img_url[:80]}")
            img_bytes = _download_image_with_retries(source_img_url, referer=source_url, attempts=3, timeout=25)
            base_img = Image.open(BytesIO(img_bytes)).convert("RGB")
            return (*_compose_and_save(base_img), source_img_url)
    except Exception as e:
        print(f"[OG] Could not build branded image from page: {e}")

    # Priority 3: Fallback to placeholder banner
    try:
        if os.path.exists(banner_path):
            print("[OG] Using fallback banner")
            banner = Image.open(banner_path).convert("RGB")
            return (*_compose_and_save(banner), "")
    except Exception as e:
        print(f"[OG] Fallback banner failed: {e}")

    return ("", "", "")


def _render_template_string(tpl: str, **kv) -> str:
    """Escape feed text once without interpreting placeholders inside that text."""
    return re.sub(r"\{\{(\w+)\}\}", lambda match: html.escape(str(kv.get(match[1]) or ""), quote=True), tpl)


def write_permalink_page(it: dict) -> str:
    """Write permalink page for an item.

    The page itself is always re-rendered (cheap, and picks up template
    changes). The social image + thumbnail are reused when a previous run
    already produced them for this permalink id, which is the expensive part.
    """
    site_base = SITE_BASE

    title = (it.get("title") or "").strip()
    url = safe_web_url(it.get("url"))
    if not url:
        raise ValueError("Article URL must use HTTP or HTTPS")
    src = (it.get("source") or "").strip()
    dtstr = it.get("published_at") or ""
    dom = domain_of(url)
    try:
        date_fmt = display_date(dtparser.parse(dtstr).astimezone(TZ)) if dtstr else ""
    except Exception:
        date_fmt = ""

    pid = _stable_id(title, url, dtstr)
    perma_dir = os.path.join(PERMA_ROOT, pid)
    os.makedirs(perma_dir, exist_ok=True)

    rel_permalink = f"/p/{pid}/"
    abs_permalink = f"{site_base}{rel_permalink}" if site_base else rel_permalink

    summary = _plain_text_summary(it, limit=180)
    if os.path.isfile(os.path.join(perma_dir, OG_IMAGE_NAME)) and os.path.isfile(os.path.join(perma_dir, THUMBNAIL_NAME)):
        og_image_rel, thumbnail_rel = f"/p/{pid}/{OG_IMAGE_NAME}", f"/p/{pid}/{THUMBNAIL_NAME}"
        with _PAGE_CACHE_LOCK:
            IMAGE_STATS["reused"] += 1
    else:
        with _PAGE_CACHE_LOCK:
            IMAGE_STATS["generated"] += 1
        og_image_rel, thumbnail_rel, used_image_url = create_branded_og_image(
            url, perma_dir, pre_extracted_image_url=it.get("image_url", ""))
        if used_image_url and not it.get("image_url"):
            it["image_url"] = used_image_url
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



def _brief_article_score(it: dict, now_local: datetime) -> float:
    """Score articles for the brief — topicality/impact first, recency second."""
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

    # Recency: gentle decay — keeps recent articles competitive but doesn't dominate
    recency = math.exp(-hours / 24.0)  # ~0.37 after 24h

    # Impact: primary driver of score
    impact = 0.0
    for pat in _BRIEF_IMPACT_PATTERNS:
        if re.search(pat, blob):
            impact += 1.0

    for nm in _BRIEF_BIG_NAMES:
        if nm in blob:
            impact += 0.5

    # Monetary figures signal hard news
    if re.search(r"[$€£]\s?\d", title) or re.search(r"\b\d+(\.\d+)?\s?(billion|million|bn|m)\b", blob):
        impact += 0.8

    # Substantive summary bonus — articles with real content rank higher
    if len(summ) > 120:
        impact += 0.4
    elif len(summ) > 60:
        impact += 0.2

    # Penalize very short/vague titles
    if len(title) < 18:
        impact -= 0.5

    # Impact-first weighting: topicality drives selection, recency breaks ties
    return (0.40 * recency) + (1.0 * impact)

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

def _brief_pick_diverse(items: list, max_n: int = None, now_local=None, max_per_domain: int = 2, sim_threshold: float = 0.55, max_total: int = None) -> list:
    """Pick items with high score but avoid near-duplicates + domain clustering."""
    # Backward-compatible arg name: allow max_total (older call sites)
    if max_total is not None:
        max_n = max_total
    if max_n is None:
        max_n = 6
    if now_local is None:
        try:
            now_local = now_et()
        except Exception:
            now_local = datetime.now(timezone.utc)

    scored = []
    for it in (items or []):
        try:
            s = _brief_article_score(it, now_local)
        except Exception:
            s = 0.0
        scored.append((s, it))
    scored.sort(key=lambda x: x[0], reverse=True)

    chosen = []
    domain_counts = {}
    for _score, it in scored:
        if len(chosen) >= max_n:
            break

        url = it.get("link") or it.get("url") or ""
        dom = _brief_domain(url) if url else ""
        if dom and domain_counts.get(dom, 0) >= max_per_domain:
            continue

        title = it.get("title", "") or ""
        if any(_brief_title_similarity(title, (c.get("title", "") or "")) >= sim_threshold for c in chosen):
            continue

        chosen.append(it)
        if dom:
            domain_counts[dom] = domain_counts.get(dom, 0) + 1

    return chosen


def _brief_first_sentence(s: str) -> str:
    """Return a clean first sentence for brief 'impact' lines."""
    if not s:
        return ""
    s = clean_text(strip_html_to_text(s), 240).strip()
    if not s:
        return ""
    parts = re.split(r'(?<=[.!?])\s+', s, maxsplit=1)
    out = (parts[0] or "").strip()
    if len(out) < 40 and len(parts) > 1:
        out2 = (out + " " + parts[1][:140]).strip()
        out = re.split(r'(?<=[.!?])\s+', out2, maxsplit=1)[0].strip()
    return out

_BRIEF_THEME_RULES = [
    ("Crypto markets", [r"\bcrypto\b", r"\bbitcoin\b", r"\bethereum\b", r"\bstablecoin\b", r"\bdefi\b", r"\bmining\b"]),
    ("AI agents & coding", [r"\bagent\b", r"\bagents\b", r"\bagentic\b", r"\bcodex\b", r"\bcopilot\b", r"\bcoding\b", r"\bdeveloper\b", r"\bcode\b"]),
    ("IPO watch", [r"\bipo\b", r"\bfiling\b", r"\bs-1\b", r"\bprospectus\b", r"\bpublic\b"]),
    ("Earnings", [r"\bearnings\b", r"\bresults\b", r"\bguidance\b", r"\brevenue\b", r"\bprofit\b"]),
    ("Regulation", [r"\bsec\b", r"\bcfpb\b", r"\bftc\b", r"\bregulator\b", r"\bban\b", r"\blaw\b"]),
    ("Security", [r"\bhack\b", r"\bbreach\b", r"\bransom\b", r"\bmalware\b", r"\bsecurity\b"]),
    ("Efficiency cuts", [r"\blayoff\b", r"\bcuts\b", r"\bheadcount\b", r"\befficiency\b", r"\bjob\b", r"\beliminated\b"]),
]

def _brief_infer_themes(items: list, max_themes: int = 3) -> list:
    """Infer editorial themes from the picked items using simple rules."""
    if not items:
        return []
    blob = " ".join([
        ((it.get("title") or "") + " " + (it.get("summary") or it.get("description") or it.get("snippet") or it.get("summary_text") or "")).lower()
        for it in (items or [])
    ])
    themes = []
    for label, pats in _BRIEF_THEME_RULES:
        if any(re.search(p, blob, flags=re.I) for p in pats):
            themes.append(label)
        if len(themes) >= max_themes:
            break
    return themes

def _brief_lede(themes: list) -> str:
    if not themes:
        return ""
    if len(themes) == 1:
        t = themes[0]
    elif len(themes) == 2:
        t = f"{themes[0]} and {themes[1]}"
    else:
        t = f"{themes[0]}, {themes[1]}, and {themes[2]}"
    # Rotate through editorial phrasings for variety
    templates = [
        f"Today's signal: {t} dominating the conversation across AI, Software, and FinTech.",
        f"What's moving: {t} — here's what matters across AI, Software, and FinTech.",
        f"The headlines converge on {t} across AI, Software, and FinTech today.",
        f"Pulse check: {t} leading the day across AI, Software, and FinTech.",
    ]
    # Deterministic pick based on day of year
    try:
        idx = now_et().timetuple().tm_yday % len(templates)
    except Exception:
        idx = 0
    return templates[idx]

def compute_pulse_brief(by_cat: dict, now_local, max_items: int = 5) -> dict:
    """Build an editorial brief (themes + takeaways) from today's articles."""
    if not by_cat:
        return {}

    # Tag each item with its category before merging
    all_items = []
    for cat_key, items in (by_cat or {}).items():
        for it in (items or []):
            it_copy = dict(it)
            it_copy["_brief_cat"] = cat_key
            all_items.append(it_copy)
    if not all_items:
        return {}

    picked = _brief_pick_diverse(all_items, now_local=now_local, max_n=max_items, max_per_domain=2, sim_threshold=0.55)
    if not picked:
        picked = all_items[:max_items]

    themes = _brief_infer_themes(picked, max_themes=3)
    lede = _brief_lede(themes)

    takeaways = []
    for it in picked[:max_items]:
        title = (it.get("title") or "").strip()
        if not title:
            continue

        summary = (it.get("summary") or it.get("description") or it.get("snippet") or it.get("summary_text") or "").strip()
        impact = _brief_first_sentence(summary) if summary else ""

        url = (it.get("url") or "").strip()
        src = (it.get("source") or it.get("domain") or "").strip()
        cat = (it.get("_brief_cat") or it.get("category") or "ai").strip().lower()

        takeaways.append({
            "title": title,
            "impact": impact,
            "url": url,
            "source": src,
            "category": cat,
        })

    return {
        "generated_at": now_local.isoformat() if hasattr(now_local, "isoformat") else "",
        "themes": themes,
        "lede": lede,
        "takeaways": takeaways,
        "story_count": len(takeaways),
    }


def _brief_impact_is_useful(impact: str, title: str) -> bool:
    """[B] Quality gate: suppress impact lines that are too short or just repeat the title."""
    if not impact:
        return False
    impact_clean = impact.strip()
    # Too short to be informative
    if len(impact_clean) < 40:
        return False
    # Check if impact mostly repeats the title's nouns
    title_tokens = set(_brief_tokens(title))
    impact_tokens = set(_brief_tokens(impact_clean))
    if not impact_tokens:
        return False
    overlap = title_tokens & impact_tokens
    # If >70% of impact words already appear in title, it's filler
    if len(overlap) / len(impact_tokens) > 0.70:
        return False
    return True


def _brief_editorial_hook(takeaways: list, max_items: int = 3) -> str:
    """[A] Generate a dynamic one-line hook from the top article titles for the summary bar."""
    if not takeaways:
        return ""
    # Extract short fragments from the top titles
    fragments = []
    for t in takeaways[:max_items]:
        title = (t.get("title") or "").strip()
        if not title:
            continue
        # Use first ~35 chars, break at word boundary
        if len(title) > 38:
            cut = title[:38].rsplit(" ", 1)[0]
            fragments.append(cut + "…")
        else:
            fragments.append(title)
    if not fragments:
        return ""
    if len(fragments) == 1:
        return fragments[0]
    elif len(fragments) == 2:
        return f"{fragments[0]}, {fragments[1]}"
    else:
        return f"{fragments[0]}, {fragments[1]}, {fragments[2]}"


def render_pulse_brief_html(brief: dict, date_str: str = "") -> str:
    """Render the Brief card with engagement-optimized layout.

    Implements:
    [A] Dynamic editorial hook in collapsed summary bar
    [B] Impact quality gate — suppress weak/redundant impact lines
    [C] Hover preview — impact shown on hover via CSS (no JS needed)
    [D] Right-aligned Read CTA — vertically centered in each row
    [E] Category text labels — "AI"/"SW"/"FT" replace color dots
    """
    if not brief:
        return ""

    takeaways = brief.get("takeaways") or []
    story_count = brief.get("story_count") or len(takeaways)

    # Story count badge
    count_html = f"<span class='pb-count'>{story_count} stories</span>" if story_count else ""

    # [A] Editorial hook for the summary bar
    hook_text = _brief_editorial_hook(takeaways)
    hook_html = f"<span class='pb-hook'>{html.escape(hook_text)}</span>" if hook_text else ""

    # Category label map [E]
    cat_label = {"ai": "AI", "software": "SW", "fintech": "FT"}
    cat_class = {"ai": "ai", "software": "sw", "fintech": "ft"}

    # Build takeaway items
    items_html = []
    for idx, t in enumerate(takeaways[:8], start=1):
        title = html.escape((t.get("title") or "").strip())
        raw_impact = (t.get("impact") or "").strip()
        url = safe_web_url(t.get("url"))
        src = html.escape((t.get("source") or "").strip())
        cat = (t.get("category") or "ai").strip().lower()
        label = cat_label.get(cat, "AI")
        cls = cat_class.get(cat, "ai")

        if not title:
            continue

        # [B] Impact quality gate
        if _brief_impact_is_useful(raw_impact, t.get("title", "")):
            impact_html = f"<span class='pb-impact'>{html.escape(raw_impact)}</span>"
        else:
            impact_html = ""

        # Source line (always visible)
        src_html = f"<span class='pb-src'>{src}</span>" if src else ""

        # [D] Read CTA — right-aligned, vertically centered
        read_html = f"<a class='pb-read' href='{html.escape(url)}' target='_blank' rel='noopener' aria-label='Read {title} (opens in a new tab)'>Read</a>" if url else ""

        items_html.append(
            f"<li>"
            f"<span class='pb-rank'>{idx}</span>"
            f"<span class='pb-cat pb-cat--{cls}'>{label}</span>"
            f"<div class='pb-takeaway-body'>"
            f"<strong>{title}</strong>"
            f"{impact_html}"
            f"{src_html}"
            f"</div>"
            f"{read_html}"
            f"</li>"
        )

    takeaways_html = "<ul class='pb-takeaways'>" + "".join(items_html) + "</ul>" if items_html else ""

    return (
        "<details class='pb' open>"
        "<summary>"
        "<span class='pb-pill'>BRIEF</span>"
        f"{count_html}"
        f"{hook_html}"
        "<span class='pb-chev' aria-hidden='true'>▲</span>"
        "</summary>"
        "<div class='pb-body'>"
        f"{takeaways_html}"
        "</div>"
        "</details>"
    )


def build_section(date_str: str, by_cat: dict, brief: dict = None, generated_at: datetime = None) -> str:
    """Build HTML section from categorized items."""
    with open(os.path.join(ROOT, "templates/section_template.html"), "r", encoding="utf-8") as f:
        tpl = f.read()
    generated_at = generated_at or now_et()

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
            if 0 <= age < 24 * 3600:
                return '<span class="badge">New</span>'
        except Exception:
            pass
        return ""

    CATEGORY_LABEL = {"ai": "AI", "software": "Software", "fintech": "FinTech"}
    today_local = generated_at.astimezone(TZ).date()

    def _when(it: dict) -> tuple[str, str]:
        """(display text, ISO datetime): clock time for today's stories, month + day otherwise."""
        try:
            dt_local = dtparser.parse(it["published_at"]).astimezone(TZ)
        except Exception:
            return date_str, ""
        if dt_local.date() == today_local:
            disp = f"{dt_local:%I:%M %p}".lstrip("0")
        else:
            disp = f"{dt_local:%b} {dt_local.day}"
        return disp, dt_local.isoformat()

    def _thumbnail_for(it: dict, url_raw: str) -> str:
        if "youtube.com" in url_raw or "youtu.be" in url_raw:
            video_id = None
            if "watch?v=" in url_raw:
                video_id = url_raw.split("watch?v=")[1].split("&")[0]
            elif "youtu.be/" in url_raw:
                video_id = url_raw.split("youtu.be/")[1].split("?")[0]
            return f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg" if video_id else ""
        thumbnail = it.get("_thumbnail") or it.get("image_url") or ""
        # Our own generated thumbnails are referenced same-origin (root-relative)
        # so the page works in local previews and the service worker treats
        # them like any other site asset. External images must be http(s).
        if SITE_BASE and thumbnail.startswith(f"{SITE_BASE}/p/"):
            return thumbnail[len(SITE_BASE):]
        if thumbnail.startswith("/p/"):
            return thumbnail
        return safe_web_url(thumbnail)

    def render_card(it: dict, lead: bool = False, in_grid: bool = False) -> str:
        """One story card. The lead renders large in the hero; its grid copy is
        marked data-lead so the page can hide it while the hero is visible."""
        title_raw = it["title"]
        title = html.escape(title_raw)
        url_raw = safe_web_url(it["url"])
        if not url_raw:
            return ""
        url = html.escape(add_utm(url_raw), quote=True)
        permalink = html.escape(safe_web_url(it.get("_abs_permalink")), quote=True)
        thumbnail = _thumbnail_for(it, url_raw)
        src = html.escape(it["source"])
        when, when_iso = _when(it)
        summary_txt = clean_text(strip_html_to_text(it.get("summary_text", "")), 260 if lead else 180)
        summary_html = html.escape(summary_txt)
        category = it.get("category", "ai")
        if category not in CATEGORY_LABEL:
            category = "ai"

        # width/height match the 16:9 thumbnails; CSS controls the displayed size.
        thumb_html = (f'<img src="{html.escape(thumbnail, quote=True)}" alt="{html.escape(title_raw, quote=True)}" '
                      f'class="article-thumb" width="400" height="225" loading="{"eager" if lead else "lazy"}" '
                      f'decoding="async">') if thumbnail else ''
        eyebrow = f'<span class="topic topic--{category}">{CATEGORY_LABEL[category]}</span>'
        if lead:
            eyebrow += '<span class="eyebrow-note">Lead story</span>'
        classes = " ".join(c for c in ("lead" if lead else "",) if c)
        lead_attr = " data-lead" if in_grid else ""
        heading = "h2" if lead else "h3"

        return f"""<article{' class="' + classes + '"' if classes else ''} data-card data-category="{category}" data-url="{url}" data-permalink="{permalink}" data-title="{html.escape(title_raw, quote=True)}" data-summary="{summary_html}" data-source="{src}"{lead_attr}>
  {thumb_html}
  <div class="article-content">
    <div class="eyebrow">{eyebrow}</div>
    <{heading}><a data-title-link href="{url}">{title}</a></{heading}>
    <div class="meta"><span class="src">{src}</span> · <time datetime="{html.escape(when_iso, quote=True)}">{when}</time> {render_item_badges(it, generated_at)}</div>
    <p data-summary>{summary_html}</p>
  </div>
</article>"""

    def _final_sort_dt(it):
        try:
            return dtparser.parse(it["published_at"]).astimezone(TZ)
        except Exception:
            return datetime.min.replace(tzinfo=TZ)

    # One chronological grid across all categories; the tabs filter it client-side.
    all_articles = []
    for cat_key in ("ai", "software", "fintech"):
        all_articles.extend(by_cat.get(cat_key, [])[:MAX_ITEMS])
    all_articles.sort(key=_final_sort_dt, reverse=True)

    lead = pick_lead_story(all_articles, brief)
    lead_key = canonicalize_url(lead.get("url", "")) if lead else ""
    grid_html = "\n".join(
        render_card(it, in_grid=bool(lead_key) and canonicalize_url(it.get("url", "")) == lead_key)
        for it in all_articles
    ) or "<p>No items today.</p>"
    lead_html = render_card(lead, lead=True) if lead else ""

    counts = {k: len(by_cat.get(k, [])[:MAX_ITEMS]) for k in ("ai", "software", "fintech")}
    updated_label = f"{display_date(generated_at)} · {generated_at:%I:%M %p %Z}".replace("· 0", "· ")

    html_out = tpl.replace("{{DATE_STR}}", html.escape(date_str))
    html_out = html_out.replace("{{DAILY_BRIEF}}", brief_html)
    html_out = html_out.replace("{{LEAD}}", lead_html)
    html_out = html_out.replace("{{ITEMS}}", grid_html)
    html_out = html_out.replace("{{COUNT_ALL}}", str(len(all_articles)))
    html_out = html_out.replace("{{COUNT_AI}}", str(counts["ai"]))
    html_out = html_out.replace("{{COUNT_SW}}", str(counts["software"]))
    html_out = html_out.replace("{{COUNT_FT}}", str(counts["fintech"]))
    html_out = html_out.replace("{{MORE_COUNT}}", str(max(0, len(all_articles) - (1 if lead else 0))))
    html_out = html_out.replace("{{UPDATED_ISO}}", html.escape(generated_at.isoformat(), quote=True))
    html_out = html_out.replace("{{UPDATED_LABEL}}", html.escape(updated_label))
    return html_out


def pick_lead_story(articles: list, brief: Optional[dict]) -> Optional[dict]:
    """The Brief's top pick becomes the lead story when it has a real image;
    otherwise the first Brief pick with any thumbnail, then the newest story."""
    if not articles:
        return None
    by_url = {canonicalize_url(a.get("url", "")): a for a in articles}
    picks = [by_url.get(canonicalize_url(t.get("url", ""))) for t in (brief or {}).get("takeaways", [])]
    picks = [p for p in picks if p]
    for candidate in picks:
        if candidate.get("image_url") and candidate.get("_thumbnail"):
            return candidate
    for candidate in picks:
        if candidate.get("_thumbnail"):
            return candidate
    for candidate in articles:
        if candidate.get("_thumbnail"):
            return candidate
    return articles[0]


# ============================================================================
# SITEMAP
# ============================================================================

def write_sitemap(docs_dir: str, lastmod_by_pid: Optional[dict] = None) -> None:
    """Rewrite sitemap.xml each build: homepage + every article permalink page.

    `lastmod` for an article is its publication date (stable across rebuilds);
    file mtime is only a fallback for pages whose item is no longer in memory.
    """
    base = SITE_BASE or "https://pulse.tek2dayholdings.com"
    lastmod_by_pid = lastmod_by_pid or {}
    today = now_et().strftime("%Y-%m-%d")
    entries = [
        f"  <url>\n    <loc>{base}/</loc>\n    <lastmod>{today}</lastmod>\n"
        f"    <changefreq>hourly</changefreq>\n    <priority>1.0</priority>\n  </url>"
    ]
    perma_root = os.path.join(docs_dir, "p")
    if os.path.isdir(perma_root):
        for pid in sorted(os.listdir(perma_root)):
            page = os.path.join(perma_root, pid, "index.html")
            if not os.path.isfile(page):
                continue
            mod = lastmod_by_pid.get(pid) or datetime.fromtimestamp(os.path.getmtime(page)).strftime("%Y-%m-%d")
            entries.append(f"  <url>\n    <loc>{base}/p/{pid}/</loc>\n    <lastmod>{mod}</lastmod>\n  </url>")
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(entries)
        + "\n</urlset>\n"
    )
    with open(os.path.join(docs_dir, "sitemap.xml"), "w", encoding="utf-8") as f:
        f.write(xml)
    print(f"Sitemap written: {len(entries)} URLs")


def _lastmod_map(items: list) -> dict:
    """pid -> YYYY-MM-DD publication date, for sitemap lastmod."""
    out = {}
    for it in items:
        dt = safe_parse_dt(it.get("published_at"))
        if not dt:
            continue
        out[_extract_permalink_id(it)] = dt.astimezone(TZ).strftime("%Y-%m-%d")
    return out


# ============================================================================
# MAIN
# ============================================================================

def main():
    now_local = now_et()
    print("=== Starting T2D Pulse Generation ===")

    print(f"\n--- Fetching {len(CFG['sources']['rss'])} RSS feeds (parallel) ---")
    all_items = fetch_all_rss_parallel(CFG["sources"]["rss"], max_workers=8)

    print(f"\n--- Before dedupe: {len(all_items)} total articles ---")
    all_items = dedupe(all_items)
    print(f"--- After dedupe: {len(all_items)} unique articles ---")

    all_items = retain_recent_items(all_items, now=now_local, retention_days=RETENTION_DAYS)
    print(f"--- Within {RETENTION_DAYS}-day retention: {len(all_items)} articles ---")

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

        is_youtube_src = ("youtube" in src_norm)
        is_force_included = any(term.lower() in src_norm for term in FORCE_INCLUDE_SOURCES)
        if is_force_included:
            print(f"[INCLUDE] {it.get('source')}: '{it.get('title', '')[:70]}' score={score}")

        if score == 0 and (src_norm not in FORCE_AI_SOURCES) and (not is_youtube_src) and (not is_force_included):
            continue
        it["category"] = cat

        # Force-routing for certain sources/domains
        d = domain_of(it["url"])
        if is_youtube_src or (src_norm in FORCE_AI_SOURCES) or is_force_included:
            it["category"] = "ai"
        elif d in FORCE_FINTECH_DOMAINS or src_norm in FORCE_FINTECH_SOURCES:
            scores = compute_scores(it["title"], it["url"], it.get("summary_text", ""))
            if not (scores["ai"] >= 3 and scores["ai"] >= scores["fintech"] + 1):
                it["category"] = "fintech"

        pruned.append(it)

    all_items = pruned
    if not all_items:
        raise RuntimeError("No publishable articles fetched. Keeping the existing deployment intact.")

    def parsed_dt(it):
        try:
            return dtparser.parse(it["published_at"]).astimezone(TZ)
        except Exception:
            return now_local

    all_items.sort(key=parsed_dt, reverse=True)
    before_cap = len(all_items)
    all_items = cap_per_source(all_items)
    if len(all_items) != before_cap:
        print(f"--- Per-source cap ({MAX_ITEMS_PER_SOURCE}): {before_cap} -> {len(all_items)} articles ---")

    # Bucket for the homepage: merges archived items restored from the Actions cache.
    by_cat = bucket_recent_by_category(all_items, now_local=now_local)

    # Permalink pages + social images for everything on the page (parallel).
    items_to_process, _seen = [], set()
    for cat in ("ai", "software", "fintech"):
        for it in by_cat.get(cat, []):
            key = f"{re.sub(r'[^a-z0-9]+', '', (it.get('title') or '').lower())}::{domain_of(it.get('url', ''))}"
            if key in _seen:
                continue
            _seen.add(key)
            items_to_process.append(it)

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

    print(f"--- Permalinks: {len(items_to_process) - failed_count} ok, {failed_count} failed; "
          f"images reused={IMAGE_STATS['reused']} generated={IMAGE_STATS['generated']}; "
          f"pages fetched={len(_PAGE_CACHE)} ---")

    # Render
    date_str = display_date(now_local)
    brief = compute_pulse_brief(by_cat, now_local=now_local)
    section = build_section(date_str, by_cat, brief, generated_at=now_local)

    # Public feed = everything on the page, plus fetched items older than the
    # fresh window but still within retention. Newest first.
    page_items = [it for cat in ("ai", "software", "fintech") for it in by_cat.get(cat, [])]
    seen_urls = {canonicalize_url(it.get("url", "")) for it in page_items}
    feed_items = page_items + [it for it in all_items if canonicalize_url(it.get("url", "")) not in seen_urls]
    feed_items.sort(key=parsed_dt, reverse=True)
    public_feed = [public_item(it) for it in feed_items]

    # Write outputs
    docs = os.path.join(REPO, "docs")
    os.makedirs(docs, exist_ok=True)
    with open(os.path.join(docs, "index.html"), "w", encoding="utf-8") as f:
        f.write(section)
    with open(os.path.join(docs, "pulse.json"), "w", encoding="utf-8") as f:
        json.dump(public_feed, f, ensure_ascii=False, separators=(",", ":"))
    with open(os.path.join(docs, "build-status.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": now_local.isoformat(), "articles": len(public_feed),
                   "by_category": {k: len(by_cat.get(k, [])) for k in ("ai", "software", "fintech")},
                   "sources": sorted(FEED_STATUS, key=lambda s: s["source"]),
                   "permalink_failures": failed_count,
                   "images": dict(IMAGE_STATS)}, f, indent=2)

    # Timestamped snapshot (read back by the next run's archive merge)
    try:
        ts_dir = os.path.join(docs, "archive", "timestamped")
        os.makedirs(ts_dir, exist_ok=True)
        ts_path = os.path.join(ts_dir, now_local.strftime("%Y-%m-%d_%H%M%S") + ".json")
        with open(ts_path, "w", encoding="utf-8") as tf:
            json.dump({"items": public_feed, "brief": brief}, tf, ensure_ascii=False, separators=(",", ":"))
    except Exception as e:
        print(f"Warning: timestamped snapshot not written: {e}")

    # Daily snapshot (overwritten each run so the homepage + brief stay consistent)
    try:
        arch_dir = os.path.join(docs, "archive", "json")
        os.makedirs(arch_dir, exist_ok=True)
        arch_path = os.path.join(arch_dir, now_local.strftime("%Y-%m-%d") + ".json")
        public_by_cat = {k: [public_item(it) for it in v] for k, v in by_cat.items()}
        with open(arch_path, "w", encoding="utf-8") as f:
            json.dump({"date": now_local.strftime("%Y-%m-%d"), "by_cat": public_by_cat, "brief": brief},
                      f, ensure_ascii=False, separators=(",", ":"))
    except Exception as e:
        print(f"Warning: daily snapshot not written: {e}")

    purge_old_outputs(docs, now_local=now_local, retention_days=RETENTION_DAYS, seed_items=feed_items)
    write_sitemap(docs, lastmod_by_pid=_lastmod_map(feed_items))

    print("\n=== T2D Pulse Generation Complete ===")
    print(f"Total articles: {len(public_feed)}")
    print(f"By category: AI={len(by_cat.get('ai', []))}, Software={len(by_cat.get('software', []))}, FinTech={len(by_cat.get('fintech', []))}")


if __name__ == "__main__":
    main()
