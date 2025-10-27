#!/usr/bin/env python3
# generator/generate_pulse.py
# (excerpted + maintained whole file)
# - Robust static image extraction (no Selenium) with NYTimes-friendly headers and retries
# - Canonical trending keyword chips (aliases) + backfill into archive
# - In-memory chip apply so older items in today's view show chips immediately

import os, re, json, html, time, random
import urllib.parse
import feedparser, requests, tldextract
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtparser
import pytz, yaml
import hashlib
from PIL import Image
from io import BytesIO
from urllib.parse import urlparse, urljoin
from pathlib import Path

# Google Trends (pytrends) optional import with fallback
try:
    from pytrends.request import TrendReq
except Exception:
    TrendReq = None  # pytrends not installed; trending will gracefully degrade

# ---- Global config/paths
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

# ---- Cache file for weekend reuse
CACHE_DIR = Path(os.path.join(REPO, "cache"))
CACHE_DIR.mkdir(exist_ok=True)
CACHE_FILE = CACHE_DIR / "friday_snapshot.json"

# ---- Shared HTTP session (browser-y headers) ----
SESSION = requests.Session()
SESSION.headers.update({
    # Use a modern desktop Chrome UA
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
})

def fetch_html(url: str, referer: str | None = None, timeout: int = 15) -> str:
    """Fetch HTML with shared session + optional Referer."""
    headers = {}
    if referer:
        headers["Referer"] = referer
    r = SESSION.get(url, headers=headers, allow_redirects=True, timeout=timeout)
    r.raise_for_status()
    return r.text

def _abs_url(u: str, base: str) -> str:
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        p = urlparse(base)
        return f"{p.scheme}://{p.netloc}{u}"
    return u

# ---- Helper functions (early definitions to avoid forward references) ----
def _domain(u: str) -> str:
    try:
        return tldextract.extract(u).registered_domain.lower()
    except Exception:
        return ""

def domain_of(url):
    ext = tldextract.extract(url or "")
    if not ext.domain:
        return ""
    return f"{ext.domain}.{ext.suffix}".lower() if ext.suffix else ext.domain.lower()

def _host(u: str) -> str:
    try:
        h = urlparse(u or "").netloc.lower()
        return h[4:] if h.startswith("www.") else h
    except Exception:
        return ""

# ---- NYTimes helpers ----
def _nyt_prefer_super_jumbo(u: str) -> str:
    """Prefer largest NYT size suffix (superJumbo) when present."""
    return re.sub(r'-(?:thumbLarge|threeByTwoSmallAt2X|threeByTwoLargeAt2X|articleLeft|articleLarge)\.(jpg|jpeg|png|webp)$',
                  r'-superJumbo.\1', u)

def _nyt_force_jpeg(u: str) -> str:
    """Coerce NYT image URL to JPEG to avoid AVIF/WEBP incompatibilities in CI."""
    try:
        # Normalize common query params
        u = re.sub(r'([?&])(auto|format|fm)=(webp|avif)', r'\1\2=jpg', u, flags=re.I)
        # Convert extension
        u = re.sub(r'\.(webp|avif)(\?.*)?$', r'.jpg\2', u, flags=re.I)
        # If no explicit format param present, add one (harmless if already jpg)
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
        out.append(u)  # original as last resort
    else:
        out.append(u)
    # de-dup preserve order
    seen, uniq = set(), []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq

def _pick_from_srcset(srcset: str) -> str:
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

def _extract_json_ld_images(soup: BeautifulSoup) -> list[str]:
    import json as _json
    out = []
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = _json.loads(tag.string or "")
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
    """Return a best-guess absolute image URL for a page, with NYT-friendly adjustments."""
    # 0) JSON-LD (often richest on NYT)
    try:
        for script in soup.find_all("script", type=lambda v: v and "ld+json" in v):
            try:
                data = json.loads(script.string or "{}")
            except Exception:
                continue
            # Normalize to list
            blocks = data if isinstance(data, list) else [data]
            for block in blocks:
                if not isinstance(block, dict):
                    continue
                # The NewsArticle/image field can be a URL string, list, or ImageObject(s)
                if block.get("@type") in ("NewsArticle", "Article", "NewsItem"):
                    imgs = block.get("image")
                    candidates = []
                    if isinstance(imgs, str):
                        candidates = [imgs]
                    elif isinstance(imgs, dict):
                        # ImageObject
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

    # 3) First reasonable <img> with width hints/srcset
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

    # 4) AMP page as a fallback
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
    """Download image with domain-aware headers and backoff. Handles AVIF gracefully."""
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

            if len(data or b"") < 4096:  # too small
                time.sleep(base_sleep * (1 + i * 0.25))
                continue

            # Try to decode the image; if AVIF fails, retry with stricter Accept
            try:
                img = Image.open(BytesIO(data)).convert("RGB")
                return data
            except Exception as decode_err:
                err_str = str(decode_err).lower()
                if "avif" in err_str or "unsupported" in err_str:
                    # Retry with stricter Accept, asking for JPEG/PNG only
                    try_hdrs = dict(hdrs)
                    try_hdrs["Accept"] = "image/jpeg,image/png,image/*"
                    rr = SESSION.get(img_url, headers=try_hdrs, stream=True, timeout=timeout, allow_redirects=True)
                    if rr.status_code == 200:
                        data_retry = rr.content if rr.raw is None else rr.raw.read()
                        try:
                            img_retry = Image.open(BytesIO(data_retry)).convert("RGB")
                            return data_retry
                        except Exception:
                            pass
                # Decode failed; sleep and retry
                time.sleep(base_sleep * (1 + i * 0.25))
                continue

        except Exception as e:
            last_exc = e
            print(f"[IMG] Error on attempt {i+1}: {e}")
            time.sleep(base_sleep * (2 ** i))
            continue

    if last_exc:
        raise last_exc
    raise RuntimeError(f"Failed to download image: {img_url}")

def now_et():
    return datetime.now(TZ)

def _now_et():
    return now_et()

def _last_friday(d):
    wd = d.weekday()
    delta = (wd - 4) % 7
    return d - timedelta(days=delta)

def _weekend_use_friday_payload_if_available():
    today = _now_et().date()
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
                section = build_section(date_str, by_cat)
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

def _save_friday_snapshot_if_today(all_items, by_cat):
    today = _now_et().date()
    if today.weekday() == 4:
        payload = {"ref_date": today.strftime("%Y-%m-%d"), "all_items": all_items, "by_cat": by_cat}
        try:
            CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

# ---- Source name mapping, etc.
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
FORCE_INCLUDE_SOURCES = {"tek2day", "tek2day newsletter"}  # Support both variants

# --- Freshness & diversity helpers
FRESH_WINDOW_DAYS = 3
BACKFILL_WINDOW_DAYS = 5
FLOORS = {"ai": 10, "software": 6, "fintech": 6}

def safe_parse_dt(dt_str):
    if not dt_str:
        return None
    try:
        dt = dtparser.parse(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def is_fresh(item, window_days=FRESH_WINDOW_DAYS, now=None):
    now = now or datetime.now(timezone.utc)
    dt = safe_parse_dt(item.get("published_at"))
    if not dt:
        return False
    return (now - dt.astimezone(timezone.utc)) <= timedelta(days=window_days)

def filter_fresh(items, window_days=FRESH_WINDOW_DAYS, now=None):
    now = now or datetime.now(timezone.utc)
    return [it for it in items if is_fresh(it, window_days, now)]

from collections import defaultdict, deque
import math, random as _random

def prefer_diverse_round_robin(items, max_total):
    if not items:
        return []
    buckets = defaultdict(list)
    for it in items:
        buckets[domain_of(it.get("url",""))].append(it)
    for d in buckets:
        buckets[d].sort(key=lambda x: safe_parse_dt(x.get("published_at")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        _random.shuffle(buckets[d])
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

def sort_by_recency(items):
    return sorted(items, key=lambda x: safe_parse_dt(x.get("published_at")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

def build_preferred_today_with_floors(all_items, now_local=None, floors=None, backfill_days=None):
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
        # Ensure chronological order before slicing
        pool.sort(key=_dt_local, reverse=True)
        out[cat] = pool[:MAX_ITEMS]
        # Sort again after slicing to be absolutely sure
        out[cat].sort(key=_dt_local, reverse=True)
    return out

def finalize_section_with_backfill(items, section_max, now=None, max_backfill_days=7):
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

def within_window(dt_local):
    cutoff = now_et() - timedelta(days=BACKFILL_WINDOW_DAYS)
    return dt_local >= cutoff

def add_utm(url):
    return f"{url}{'&' if '?' in url else '?'}utm_source={UTM['source']}&utm_medium={UTM['medium']}"

def nice_source_for(url):
    d = domain_of(url)
    if not d:
        return "Google News"
    if d in SOURCE_NAME_MAP:
        return SOURCE_NAME_MAP[d]
    core = d.split(".")[-2] if d.count(".") >= 1 else d
    return core.capitalize()

def is_blocked(url):
    d = domain_of(url)
    if not d:
        return False
    return (d in ALWAYS_BLOCK) or any(d.endswith(suf) for suf in BLOCK_SUFFIXES)

def clean_text(s, limit=None):
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    if limit and len(s) > limit:
        return s[:limit - 1] + "..."
    return s

def strip_html_to_text(s):
    if not s:
        return ""
    try:
        return BeautifulSoup(s, "html5lib").get_text(" ", strip=True)
    except Exception:
        return s

def parse_pubdate(entry):
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

# ---- Image extraction helper (RSS) ----
def extract_image_url(entry) -> str:
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
                if url and any(url.lower().endswith(ext) for ext in (".jpg",".jpeg",".png",".webp",".gif")):
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

# ---- Permalink & summary helpers ----
PERMA_ROOT = os.path.join(REPO, "docs", "p")  # docs/p/<id>/
PERMA_TPL  = os.path.join(ROOT, "templates", "item_template.html")

def _stable_id(title: str, url: str, published_at: str) -> str:
    key = f"{(title or '').strip()}|{(url or '').strip()}|{(published_at or '').strip()}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]

def _plain_text_summary(it: dict, limit: int = 180) -> str:
    raw = it.get("summary_text") or it.get("summary") or it.get("description") or it.get("content_html") or it.get("title") or ""
    txt = strip_html_to_text(raw)
    txt = re.sub(r"\s+", " ", txt).strip()
    if len(txt) <= limit:
        return txt
    cut = txt[:limit - 1]
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return cut + "..."

def _render_template_string(tpl: str, **kv) -> str:
    html_out = tpl
    for k, v in kv.items():
        html_out = html_out.replace(f"{{{{{k}}}}}", v or "")
    return html_out

# ---- Branded OG image + thumbnail (robust fetch for NYT/CDNs) ----
def create_branded_og_image(source_url: str, permalink_dir: str, pre_extracted_image_url: str = "") -> tuple[str, str]:
    """
    Creates a branded OG image (1200x630) and thumbnail (240x135):
    - Use pre-extracted image if provided (from RSS)
    - Otherwise try to extract from source HTML
    - Download with domain-aware headers/retries
    - Overlay T2D logo top-right; fallback to banner if anything fails
    Returns (og_rel_path, thumb_rel_path) or ("","") on failure.
    """
    logo_path = os.path.join(REPO, "docs", "icons", "T2D_Pulse_Logo_2.png")
    banner_path = os.path.join(REPO, "docs", "icons", "T2D_Pulse_Banner.png")
    output_path = os.path.join(permalink_dir, "og-image.png")
    thumbnail_path = os.path.join(permalink_dir, "thumbnail.png")

    TARGET_WIDTH, TARGET_HEIGHT = 1200, 630
    THUMB_WIDTH, THUMB_HEIGHT = 240, 135
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
            img_bytes = _download_image_with_retries(pre_extracted_image_url, referer=source_url, attempts=3, timeout=25)
            base_img = Image.open(BytesIO(img_bytes)).convert("RGB")
            return _compose_and_save(base_img)
        except Exception as e:
            print(f"[OG] Pre-extracted image failed: {e}, trying page scrape")

    # Priority 2: Try to fetch and parse the article page (may fail on 403/429)
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

def write_permalink_page(it: dict) -> str:
    site_base = os.environ.get("SITE_BASE_URL") or (CFG.get("site_base") or "")
    site_base = site_base.rstrip("/")

    title = (it.get("title") or "").strip()
    url   = (it.get("url")   or "").strip()
    src   = (it.get("source") or "").strip()
    dtstr = it.get("published_at") or ""
    dom   = domain_of(url)
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

# ---- Deals/consumer filter ----
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

def contains_any(haystack, needles):
    h = haystack.lower()
    return any(n in h for n in needles)

def is_deals_or_consumer_shopping(title, url):
    t = f"{title} {url}".lower()
    if contains_any(t, DEAL_WORDS) or contains_any(t, CONSUMER_GADGET_WORDS):
        return True
    return any(p in t for p in DEAL_PATH_HINTS)

# ---- Fetch RSS with fallback image scraping ----
def fetch_rss(feed_name, url):
    """Direct outlet RSS with aggressive fallback page scraping for missing images."""
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={'User-Agent': SESSION.headers["User-Agent"]})
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
            
            # Debug: log what we found from RSS
            if image_url:
                print(f"[RSS] {feed_name}: found image in entry: {image_url[:80]}")
            
            # Fallback: if no image in RSS entry, try scraping the article page
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

# ---- Google Trends integration ----
def get_trending_tech_keywords(max_keywords: int = 25) -> list[str]:
    fallback = [
        "ai","openai","anthropic","claude","chatgpt","gpt","llm","deepmind","mistral",
        "nvidia","h100","gpu","cuda","rocm","tensor","transformer","agent","genai",
        "google","microsoft","apple","meta","amazon","cloud","saas","kubernetes","k8s",
        "stripe","paypal","fintech","bitcoin","ethereum","stablecoin","vector db","embedding","llama"
    ]
    try:
        if TrendReq is None:
            return fallback[:max_keywords]
        pytrends = TrendReq(hl="en-US", tz=360)
        df = pytrends.trending_searches(pn="united_states")
        trends = [str(x).strip() for x in df.iloc[:,0].dropna().tolist()]
        seeds = ["AI","OpenAI","Nvidia","Claude","ChatGPT","Llama","GPU","Cloud","Fintech","Stripe","Microsoft","Google","Apple"]
        enriched = []
        for seed in seeds:
            try:
                pytrends.build_payload([seed], timeframe="now 7-d", geo="US")
                rq = pytrends.related_queries()
                for _, v in (rq or {}).items():
                    if not v: 
                        continue
                    for col in ("rising","top"):
                        if v.get(col) is not None:
                            enriched += [str(x).strip() for x in v[col]["query"].dropna().tolist()]
            except Exception:
                continue
        tech_hints = set([
            "ai","gpt","llm","model","chip","gpu","npu","cuda","rocm","nvidia","openai","anthropic","claude",
            "deepmind","mistral","llama","meta","microsoft","azure","google","cloud","saas",
            "apple","iphone","mac","silicon","tensorflow","pytorch","transformer","diffusion",
            "agent","prompt","vector","database","fintech","payments","stripe","paypal","bitcoin","ethereum","stablecoin"
        ])
        def is_techy(q: str) -> bool:
            ql = q.lower()
            return any(h in ql for h in tech_hints)
        merged = [t for t in (trends + enriched) if t and is_techy(t)]
        seen = set()
        uniq = []
        for t in merged:
            tl = t.lower()
            if tl not in seen:
                seen.add(tl)
                uniq.append(tl)
        if not uniq:
            return fallback[:max_keywords]
        return uniq[:max_keywords]
    except Exception:
        return fallback[:max_keywords]

# === Canonical chip aliases (case-insensitive; punctuation-insensitive) ===
def _norm_keyword(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

TRENDING_KEYWORD_ALIASES: dict[str, list[str]] = {
    "ai": [
        "AI", "A.I.", "A I", "a.i.", "a i",
        "artificial intelligence", "artificial-intelligence", "artificial_intelligence",
        "Artificial Intelligence"
    ],
    "chatgpt": ["ChatGPT", "Chat GPT", "chat gpt"],
    "openai": ["OpenAI", "Open AI", "open ai"],
    "deepseek": ["DeepSeek", "Deep Seek", "deep seek"],
}

ALIAS_TO_CANON: dict[str, str] = {
    _norm_keyword(variant): canon.lower()
    for canon, variants in TRENDING_KEYWORD_ALIASES.items()
    for variant in variants + [canon]
}

def _match_trending_keywords(text: str, keywords: list[str]) -> list[str]:
    if not text:
        return []
    tl = (text or "").lower()
    tn = _norm_keyword(text)

    out: set[str] = set()
    for kw in (keywords or []):
        if not kw:
            continue
        kw_lower = kw.lower().strip()
        kw_norm = _norm_keyword(kw)
        matched = (kw_lower and kw_lower in tl) or (kw_norm and kw_norm in tn)
        if not matched:
            continue
        canonical = ALIAS_TO_CANON.get(kw_norm, kw_lower)
        out.add(canonical)
    return sorted(out)

# ---- Categorization (scores) ----
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
    "defi", "aml", "kyc", "sec", "fdic", "treasury", "card", "tokenization", "stablecoin", "coinbase", "merchant"
]

def _count_hits(text: str, terms: list[str]) -> int:
    if not text:
        return 0
    t = f" {text.lower()} "
    return sum(1 for w in terms if w in t)

def categorize_with_score(title: str, url: str, summary: str = ""):
    title_l = title or ""
    summary_l = summary or ""
    url_l = url or ""

    ai = (
        3 * _count_hits(title_l, AI_STRONG)
      + 2 * _count_hits(summary_l, AI_STRONG)
      + 1 * _count_hits(url_l, AI_STRONG)
      + 1 * _count_hits(title_l, AI_WEAK)
      + 1 * _count_hits(summary_l, AI_WEAK)
    )
    ai -= min(2, _count_hits(f"{title_l} {summary_l} {url_l}", AI_NEGATIVE))

    sw = (
        2 * _count_hits(title_l, SW_STRONG)
      + 1 * _count_hits(summary_l, SW_STRONG)
      + 1 * _count_hits(url_l, SW_STRONG)
    )
    sw -= min(2, _count_hits(f"{title_l} {summary_l} {url_l}", SW_NEGATIVE))
    
    ft = (
        2 * _count_hits(title_l, FT_STRONG)
      + 1 * _count_hits(summary_l, FT_STRONG)
      + 1 * _count_hits(url_l, FT_STRONG)
    )

    other_max = max(sw, ft)
    if ai >= 2 and ai >= other_max + 1:
        return "ai", ai
    if sw >= ft:
        return "software", sw
    else:
        return "fintech", ft

def compute_scores(title: str, url: str, summary: str = "") -> dict:
    title_l = title or ""
    summary_l = summary or ""
    url_l = url or ""

    ai = (
        3 * _count_hits(title_l, AI_STRONG)
      + 2 * _count_hits(summary_l, AI_STRONG)
      + 1 * _count_hits(url_l, AI_STRONG)
      + 1 * _count_hits(title_l, AI_WEAK)
      + 1 * _count_hits(summary_l, AI_WEAK)
      - min(2, _count_hits(f"{title_l} {summary_l} {url_l}", AI_NEGATIVE))
    )
    sw = (
        2 * _count_hits(title_l, SW_STRONG)
      + 1 * _count_hits(summary_l, SW_STRONG)
      + 1 * _count_hits(url_l, SW_STRONG)
      - min(2, _count_hits(f"{title_l} {summary_l} {url_l}", SW_NEGATIVE))
    )
    ft = (
        2 * _count_hits(title_l, FT_STRONG)
      + 1 * _count_hits(summary_l, FT_STRONG)
      + 1 * _count_hits(url_l, FT_STRONG)
    )
    return {"ai": ai, "software": sw, "fintech": ft}

# ---- Cross-run duplicate collapse ----
AGG_DOMAINS = {"news.google.com", "news.yahoo.com", "news.ycombinator.com"}

def _strip_publisher_suffix(title: str) -> str:
    if not title: return ""
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

    # Sort by published date (newest first) to maintain chronological order
    result = list(best.values())
    def _sort_dt(it):
        try:
            return dtparser.parse(it.get("published_at", ""))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)
    result.sort(key=_sort_dt, reverse=True)
    return result

def dedupe(items):
    out, seen_urls, seen_titles, seen_title_cores = [], set(), set(), set()
    for it in items:
        url = it.get("url", "")
        title = it.get("title", "")
        try:
            parsed = urlparse(url)
            normalized_url = f"{parsed.netloc}{parsed.path}".lower().rstrip("/")
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
    
    # Sort by published date to maintain chronological order
    def _dedupe_sort_dt(it):
        try:
            return dtparser.parse(it.get("published_at", ""))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)
    out.sort(key=_dedupe_sort_dt, reverse=True)
    return out

def summarize(item):
    if item.get("summary"):
        return clean_text(item["summary"], 260)
    try:
        r = SESSION.get(item["url"], timeout=12)
        soup = BeautifulSoup(r.text, "html5lib")
        for sel in [("meta", {"property":"og:description"}), ("meta", {"name":"description"})]:
            m = soup.find(*sel)
            if m and m.get("content"):
                return clean_text(m["content"], 260)
    except Exception:
        pass
    return clean_text(item["title"], 200)

def build_section(date_str, by_cat):
    with open(os.path.join(ROOT, "templates/section_template.html"), "r", encoding="utf-8") as f:
        tpl = f.read()

    def render_item_badges(it, now=None):
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
            if age < 24*3600:
                return '<span class="badge">New</span>'
        except Exception:
            pass
        return ""

    def render_items(items):
        parts = []
        for idx, it in enumerate(items):
            trending_badge = '<span class="trending-indicator">Trending</span>' if it.get("_trending_tags") else ''
            trending_chips_html = ''
            if it.get("_trending_tags"):
                try:
                    trending_chips_html = ' '.join([
                        f'<span class="trend-chip" style="display:inline-block;padding:2px 8px;border-radius:999px;background:#eef3ff;border:1px solid #cfd8ff;font-size:12px;margin-right:6px;">{html.escape(tag)}</span>'
                        for tag in it.get("_trending_tags", [])
                    ])
                except Exception:
                    trending_chips_html = ''

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
            summary_txt = clean_text(strip_html_to_text(it.get("summary_text","")), 180)
            summary_html = html.escape(summary_txt)
            top_cls = " top" if idx == 0 else ""
            thumb_html = f'<img src="{thumbnail}" alt="{html.escape(title_raw, quote=True)}" class="article-thumb">' if thumbnail else ''
            parts.append(f"""<article class="{top_cls.strip()}" data-card data-url="{url}" data-permalink="{permalink}" data-title="{html.escape(title_raw, quote=True)}" data-summary="{summary_html}" data-source="{src}">
  {thumb_html}
  <div class="article-content">
    <h3><a data-title-link href="{url}">{title}</a></h3>
    <div class="meta"><span class="src">{src}</span> - {dt_str} {render_item_badges(it)} {trending_badge}</div>
    <div class="trend-chips">{trending_chips_html}</div>
    <p data-summary>{summary_html}</p>
  </div>
</article>""")
        return "\n".join(parts)

    html_out = tpl.replace("{{DATE_STR}}", date_str)
    total_count = sum(len(by_cat.get(k, [])) for k in ("ai", "software", "fintech"))

    # Helper to parse dates for final sort
    def _final_sort_dt(it):
        try:
            return dtparser.parse(it["published_at"]).astimezone(TZ)
        except Exception:
            return datetime.min.replace(tzinfo=TZ)

    # COMBINE ALL ARTICLES FROM ALL CATEGORIES INTO ONE CHRONOLOGICAL LIST
    all_articles = []
    for cat_key in ("ai", "software", "fintech"):
        items = by_cat.get(cat_key, [])[:MAX_ITEMS]
        all_articles.extend(items)
    
    # Sort the combined list chronologically (newest first)
    all_articles.sort(key=_final_sort_dt, reverse=True)
    
    # Render all articles as one unified list
    all_items_html = render_items(all_articles) if all_articles else "<p>No items today.</p>"
    
    # Replace placeholders - use the combined list for all three
    html_out = html_out.replace("{{AI_ITEMS}}", all_items_html)
    html_out = html_out.replace("{{SW_ITEMS}}", "")  # Empty - already in AI_ITEMS
    html_out = html_out.replace("{{FT_ITEMS}}", "")  # Empty - already in AI_ITEMS
    html_out = html_out.replace("{{AI_COUNT}}", str(len(all_articles)))
    html_out = html_out.replace("{{SW_COUNT}}", "0")
    html_out = html_out.replace("{{FT_COUNT}}", "0")

    html_out = html_out.replace("{{TOTAL_COUNT}}", str(total_count))
    html_out = html_out.replace("{{COUNT}}", str(total_count))
    return html_out

def _apply_trend_chips_inplace(it: dict, trending_keywords: list[str]) -> None:
    title = it.get("title", "") or ""
    summary_raw = it.get("summary_text") or it.get("summary") or it.get("description") or ""
    summary_txt = strip_html_to_text(summary_raw)
    tags = set(_match_trending_keywords(title, trending_keywords))
    tags.update(_match_trending_keywords(summary_txt, trending_keywords))
    if tags:
        it["_trending_tags"] = sorted(tags)

def main():
    # Weekend: reuse Friday snapshot if available
    if os.environ.get('DISABLE_WEEKEND_CACHE', '0') != '1':
        if _weekend_use_friday_payload_if_available():
            return
    
    print("=== Starting T2D Pulse Generation ===")
    all_items = []

    trending_keywords = get_trending_tech_keywords(max_keywords=30)
    print(f"Fetched {len(trending_keywords)} trending tech keywords from Google Trends.")

    # Direct RSS
    print(f"\n--- Fetching {len(CFG['sources']['rss'])} RSS feeds ---")
    for s in CFG["sources"]["rss"]:
        print(f"Fetching: {s['name']} from {s['url']}")
        fetched = fetch_rss(s["name"], s["url"])
        print(f"  → Got {len(fetched)} articles")
        all_items.extend(fetched)

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
        # Drop unrelated items unless forced include
        src_norm = (it.get("source") or "").strip().lower()
        
        # Debug logging for TEK2day articles
        if "tek2day" in src_norm:
            print(f"[DEBUG] TEK2day article found: '{it.get('title', 'Unknown')}' | Source: '{it.get('source')}' | Score: {score}")
        
        is_youtube_src = ("youtube" in src_norm)
        # Check if any FORCE_INCLUDE_SOURCES term appears in the source name
        is_force_included = any(term.lower() in src_norm for term in FORCE_INCLUDE_SOURCES)
        
        if "tek2day" in src_norm:
            print(f"[DEBUG] TEK2day article - is_force_included: {is_force_included}, will keep: {score > 0 or is_force_included}")
        
        if score == 0 and (src_norm not in FORCE_AI_SOURCES) and (not is_youtube_src) and (not is_force_included):
            continue
        it["category"] = cat

        # Tag with canonical trending chips
        try:
            tags = set(_match_trending_keywords(it.get("title",""), trending_keywords))
            tags.update(_match_trending_keywords(it.get("summary_text",""), trending_keywords))
            if tags:
                it["_trending_tags"] = sorted(tags)
        except Exception:
            pass

        # Force-routing for certain sources/domains
        try:
            d = domain_of(it["url"])
        except Exception:
            d = ""
        # Check if source should be force-included (case-insensitive substring match)
        is_force_included_src = any(term.lower() in src_norm for term in FORCE_INCLUDE_SOURCES)
        
        if ("youtube" in src_norm) or (src_norm in FORCE_AI_SOURCES) or is_force_included_src:
            it["category"] = "ai"
        elif d in FORCE_FINTECH_DOMAINS or src_norm in FORCE_FINTECH_SOURCES:
            scores = compute_scores(it["title"], it["url"], it.get("summary_text",""))
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

    # Ensure chips are present for items being rendered now (older archive items)
    for cat in ("ai", "software", "fintech"):
        for it in by_cat.get(cat, []):
            if not it.get("_trending_tags"):
                _apply_trend_chips_inplace(it, trending_keywords)

    # Generate permalinks and concise summaries for today's items
    unique_items, _seen = [], set()
    for cat in ("ai", "software", "fintech"):
        for it in by_cat.get(cat, []):
            key = f"{re.sub(r'[^a-z0-9]+', '', (it.get('title') or '').lower())}::{domain_of(it.get('url',''))}"
            if key in _seen:
                continue
            _seen.add(key)
            try:
                write_permalink_page(it)
            except Exception as e:
                print(f"Warning: Failed to create permalink for '{it.get('title', 'Unknown')}': {e}")
            unique_items.append(it)

    # Render
    date_str = now_et().strftime("%b %-d, %Y")
    section = build_section(date_str, by_cat)

    # Save Friday snapshot for weekend reuse
    _save_friday_snapshot_if_today(all_items, by_cat)

    # Write outputs
    docs = os.path.join(REPO, "docs")
    os.makedirs(docs, exist_ok=True)
    with open(os.path.join(docs, "index.html"), "w", encoding="utf-8") as f:
        f.write(section)
    with open(os.path.join(docs, "pulse.json"), "w", encoding="utf-8") as f:
        json.dump(all_items, f, indent=2)
        # Timestamped snapshot so items persist across runs (3-day retention)
        try:
            ts_dir = os.path.join(docs, "archive", "timestamped")
            os.makedirs(ts_dir, exist_ok=True)
            ts_name = now_et().strftime("%Y-%m-%d_%H%M%S") + ".json"
            ts_path = os.path.join(ts_dir, ts_name)
            with open(ts_path, "x", encoding="utf-8") as tf:
                json.dump({"items": all_items}, tf, indent=2)
        except FileExistsError:
            pass
        except Exception:
            pass

    # Trending analytics JSON (optional)
    try:
        analytics = {
            "generated_at": now_et().isoformat(),
            "total_keywords": len(trending_keywords),
            "keyword_counts": {},
            "sources_per_keyword": {},
            "top_articles_per_keyword": {},
        }
        items_for_stats = unique_items
        kw_to_items = {}
        for it in items_for_stats:
            for kw in (it.get("_trending_tags") or []):
                kw_to_items.setdefault(kw, []).append(it)
        for kw, items in kw_to_items.items():
            analytics["keyword_counts"][kw] = len(items)
        nonzero = {k:v for k,v in analytics["keyword_counts"].items() if v > 0}
        analytics["keyword_counts"] = dict(sorted(nonzero.items(), key=lambda kv: (-kv[1], kv[0])))
        with open(os.path.join(docs, "trending_analytics.json"), "w", encoding="utf-8") as f:
            json.dump(analytics, f, indent=2)
    except Exception as e:
        print(f"Warning: failed to write trending_analytics.json: {e}")

    # Daily snapshot
    try:
        arch_dir = os.path.join(docs, "archive", "json")
        os.makedirs(arch_dir, exist_ok=True)
        snap_name = now_et().strftime("%Y-%m-%d") + ".json"
        arch_path = os.path.join(arch_dir, snap_name)
        if not os.path.exists(arch_path):
            with open(arch_path, "x", encoding="utf-8") as f:
                json.dump({"date": now_et().strftime("%Y-%m-%d"), "by_cat": by_cat}, f, indent=2)
    except Exception:
        pass

    # === Backfill: retro-tag archived items with canonical trending chips ===
    try:
        ts_dir = os.path.join(docs, "archive", "timestamped")
        if os.path.isdir(ts_dir):
            updated_files = 0
            for filename in os.listdir(ts_dir):
                if not filename.endswith(".json"):
                    continue
                path = os.path.join(ts_dir, filename)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except Exception:
                    continue

                items = data.get("items") or []
                changed = False

                for it in items:
                    title = it.get("title", "")
                    summary_raw = it.get("summary_text") or it.get("summary") or it.get("description") or ""
                    summary_txt = strip_html_to_text(summary_raw)
                    tags = set(_match_trending_keywords(title, trending_keywords))
                    tags.update(_match_trending_keywords(summary_txt, trending_keywords))
                    if tags:
                        new_tags = sorted(tags)
                        if new_tags != (it.get("_trending_tags") or []):
                            it["_trending_tags"] = new_tags
                            changed = True

                if changed:
                    try:
                        with open(path, "w", encoding="utf-8") as f:
                            json.dump({"items": items}, f, indent=2, ensure_ascii=False)
                        updated_files += 1
                    except Exception as e:
                        print(f"Backfill write failed for {filename}: {e}")

            if updated_files:
                print(f"Backfill: updated trending tags in {updated_files} archived snapshot file(s).")
            else:
                print("Backfill: no archived snapshots needed updates.")
    except Exception as e:
        print(f"Backfill error: {e}")

if __name__ == "__main__":
    main()
