
#!/usr/bin/env python3
# generator/generate_pulse.py
# Website-only generator for TEK2day Pulse (AI -> Software -> FinTech)

import os, re, json, html, urllib.parse
import feedparser, requests, tldextract
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtparser
import pytz, yaml
import hashlib
from PIL import Image
from io import BytesIO

# Google Trends (pytrends) optional import with fallback
try:
    from pytrends.request import TrendReq
except Exception:
    TrendReq = None  # pytrends not installed; trending will gracefully degrade

ROOT = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(ROOT)

with open(os.path.join(ROOT, "config.yaml"), "r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

# ---- Weekend snapshot cache (reuse Friday content on Sat/Sun) ----
from pathlib import Path as _Path

CACHE_FILE = _Path(REPO) / "docs" / ".cache" / "friday_snapshot.json"
CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)

def _now_et():
    return now_et()

def _last_friday(d):
    wd = d.weekday()
    delta = (wd - 4) % 7  # days since Friday
    return d - timedelta(days=delta)

def _weekend_use_friday_payload_if_available():
    """On Sat/Sun, use the cached Friday snapshot if available.
    Writes docs/index.html and docs/pulse.json from cache and exits."""
    today = _now_et().date()
    wd = today.weekday()
    if wd not in (5, 6):  # only weekends
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
                    try:
                        ts_dir = os.path.join(docs, "archive", "timestamped")
                        os.makedirs(ts_dir, exist_ok=True)
                        ts_name = now_et().strftime("%Y-%m-%d_%H%M%S") + ".json"
                        ts_path = os.path.join(ts_dir, ts_name)
                        with open(ts_path, "x", encoding="utf-8") as tf:
                            json.dump({"items": cached.get("all_items", [])}, tf, indent=2)
                    except FileExistsError:
                        pass
                    except Exception:
                        pass
                return True
        except Exception:
            pass
    return False

def _save_friday_snapshot_if_today(all_items, by_cat):
    today = _now_et().date()
    if today.weekday() == 4:  # Friday
        payload = {
            "ref_date": today.strftime("%Y-%m-%d"),
            "all_items": all_items,
            "by_cat": by_cat
        }
        try:
            CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

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
FORCE_INCLUDE_SOURCES = {"tek2day"}

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
import math, random

def prefer_diverse_round_robin(items, max_total):
    if not items:
        return []
    buckets = defaultdict(list)
    for it in items:
        buckets[domain_of(it.get("url",""))].append(it)
    for d in buckets:
        buckets[d].sort(key=lambda x: safe_parse_dt(x.get("published_at")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
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

def sort_by_recency(items):
    return sorted(
        items,
        key=lambda x: safe_parse_dt(x.get("published_at")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True
    )

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
        out[cat] = pool[:MAX_ITEMS]
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

def now_et():
    return datetime.now(TZ)

def within_window(dt_local):
    cutoff = now_et() - timedelta(days=BACKFILL_WINDOW_DAYS)
    result = dt_local >= cutoff
    return result

def add_utm(url):
    return f"{url}{'&' if '?' in url else '?'}utm_source={UTM['source']}&utm_medium={UTM['medium']}"

def domain_of(url):
    ext = tldextract.extract(url or "")
    if not ext.domain:
        return ""
    return f"{ext.domain}.{ext.suffix}".lower() if ext.suffix else ext.domain.lower()

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
                dt_tz = dt.astimezone(TZ)
                return dt_tz
            except Exception:
                pass
    return now_et()

# ---- Image extraction helper (RSS) ----
def extract_image_url(entry) -> str:
    try:
        # media:content / media:thumbnail
        if hasattr(entry, "media_content") and entry.media_content:
            m = entry.media_content[0]
            if isinstance(m, dict) and m.get("url"):
                return m["url"]
        if hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
            m = entry.media_thumbnail[0]
            if isinstance(m, dict) and m.get("url"):
                return m["url"]
        # enclosures
        if hasattr(entry, "enclosures") and entry.enclosures:
            for enc in entry.enclosures:
                url = getattr(enc, "href", None) or enc.get("href") if isinstance(enc, dict) else None
                if url and any(url.lower().endswith(ext) for ext in (".jpg",".jpeg",".png",".webp",".gif")):
                    return url
        # content html image
        if hasattr(entry, "content"):
            try:
                html_blob = entry.content[0].value
                m = re.search(r'<img[^>]+src=["\']([^"\']+)', html_blob, re.I)
                if m:
                    return m.group(1)
            except Exception:
                pass
        # summary image
        if hasattr(entry, "summary"):
            m = re.search(r'<img[^>]+src=["\']([^"\']+)', entry.summary, re.I)
            if m:
                return m.group(1)
    except Exception:
        pass
    return ""

# ---- Permalink & summary helpers ----
PERMA_ROOT = os.path.join(REPO, "docs", "p")  # docs/p/<id>/
PERMA_TPL  = os.path.join(ROOT, "templates", "item_template.html")  # generator/templates/item_template.html

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

def create_branded_og_image(source_url: str, permalink_dir: str) -> tuple[str, str]:
    """
    Creates a branded OG image (1200x630) and thumbnail (240x135):
    - Primary: Fetch source article's og:image and overlay T2D logo (120x120, top-right, 20px padding)
    - Fallback: Use T2D banner if source image unavailable
    Returns tuple of (og_image_rel_path, thumbnail_rel_path) or empty strings on failure.
    """
    logo_path = os.path.join(REPO, "docs", "icons", "T2D_Pulse_Logo_2.png")
    banner_path = os.path.join(REPO, "docs", "icons", "T2D_Pulse_Banner.png")
    output_path = os.path.join(permalink_dir, "og-image.png")
    thumbnail_path = os.path.join(permalink_dir, "thumbnail.png")
    
    TARGET_WIDTH = 1200
    TARGET_HEIGHT = 630
    THUMB_WIDTH = 240
    THUMB_HEIGHT = 135
    LOGO_SIZE = 120
    PADDING = 20
    
    try:
        source_og_url = None
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            resp = requests.get(source_url, timeout=10, headers=headers, allow_redirects=True)
            soup = BeautifulSoup(resp.text, "html5lib")
            og_img = (soup.find("meta", property="og:image") or 
                     soup.find("meta", attrs={"name": "og:image"}) or
                     soup.find("meta", property="twitter:image") or
                     soup.find("meta", attrs={"name": "twitter:image"}))
            if og_img:
                img_url = og_img.get("content") or og_img.get("value")
                if img_url:
                    if img_url.startswith("//"):
                        img_url = "https:" + img_url
                    elif img_url.startswith("/"):
                        from urllib.parse import urlparse
                        parsed = urlparse(source_url)
                        img_url = f"{parsed.scheme}://{parsed.netloc}{img_url}"
                    source_og_url = img_url
        except Exception as e:
            print(f"Could not fetch og:image from {source_url}: {e}")
        
        base_img = None
        if source_og_url:
            try:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Referer": source_url
                }
                img_resp = requests.get(source_og_url, timeout=10, headers=headers)
                base_img = Image.open(BytesIO(img_resp.content)).convert("RGB")
                
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
                
                if os.path.exists(logo_path):
                    logo = Image.open(logo_path).convert("RGBA")
                    logo = logo.resize((LOGO_SIZE, LOGO_SIZE), Image.LANCZOS)
                    logo_x = TARGET_WIDTH - LOGO_SIZE - PADDING
                    logo_y = PADDING
                    og_img.paste(logo, (logo_x, logo_y), logo)
                
                og_img.save(output_path, "PNG", optimize=True)
                
                thumb = base_img.copy()
                thumb_aspect = thumb.width / thumb.height
                target_thumb_aspect = THUMB_WIDTH / THUMB_HEIGHT
                
                if thumb_aspect > target_thumb_aspect:
                    new_height = THUMB_HEIGHT * 2
                    new_width = int(new_height * thumb_aspect)
                    thumb = thumb.resize((new_width, new_height), Image.LANCZOS)
                    left = (new_width - THUMB_WIDTH * 2) // 2
                    thumb = thumb.crop((left, 0, left + THUMB_WIDTH * 2, THUMB_HEIGHT * 2))
                else:
                    new_width = THUMB_WIDTH * 2
                    new_height = int(new_width / thumb_aspect)
                    thumb = thumb.resize((new_width, new_height), Image.LANCZOS)
                    top = (new_height - THUMB_HEIGHT * 2) // 2
                    thumb = thumb.crop((0, top, THUMB_WIDTH * 2, top + THUMB_HEIGHT * 2))
                
                thumb = thumb.resize((THUMB_WIDTH, THUMB_HEIGHT), Image.LANCZOS)
                thumb.save(thumbnail_path, "PNG", optimize=True)
                
                pid = os.path.basename(permalink_dir)
                return (f"/p/{pid}/og-image.png", f"/p/{pid}/thumbnail.png")
            except Exception as e:
                print(f"Could not process image from {source_og_url}: {e}")
        
        if os.path.exists(banner_path):
            banner = Image.open(banner_path).convert("RGB")
            og_img = banner.resize((TARGET_WIDTH, TARGET_HEIGHT), Image.LANCZOS)
            og_img.save(output_path, "PNG", optimize=True)
            
            thumb = banner.copy()
            thumb_aspect = thumb.width / thumb.height
            target_thumb_aspect = THUMB_WIDTH / THUMB_HEIGHT
            
            if thumb_aspect > target_thumb_aspect:
                new_height = THUMB_HEIGHT * 2
                new_width = int(new_height * thumb_aspect)
                thumb = thumb.resize((new_width, new_height), Image.LANCZOS)
                left = (new_width - THUMB_WIDTH * 2) // 2
                thumb = thumb.crop((left, 0, left + THUMB_WIDTH * 2, THUMB_HEIGHT * 2))
            else:
                new_width = THUMB_WIDTH * 2
                new_height = int(new_width / thumb_aspect)
                thumb = thumb.resize((new_width, new_height), Image.LANCZOS)
                top = (new_height - THUMB_HEIGHT * 2) // 2
                thumb = thumb.crop((0, top, THUMB_WIDTH * 2, top + THUMB_HEIGHT * 2))
            
            thumb = thumb.resize((THUMB_WIDTH, THUMB_HEIGHT), Image.LANCZOS)
            thumb.save(thumbnail_path, "PNG", optimize=True)
            
            pid = os.path.basename(permalink_dir)
            return (f"/p/{pid}/og-image.png", f"/p/{pid}/thumbnail.png")
        
    except Exception as e:
        print(f"Failed to create branded image: {e}")
    
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
    
    article_url = url
    og_image_rel, thumbnail_rel = create_branded_og_image(article_url, perma_dir)
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

# ----------------- fetchers -----------------
def fetch_rss(feed_name, url):
    """Direct outlet RSS (Verge, VB, PYMNTS, OpenAI, Anthropic)."""
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        
        is_tek2day = "tek2day" in feed_name.lower()
        
        if is_tek2day:
            print(f"  [DEBUG] Fetching URL: {url}")
        
        try:
            response = urllib.request.urlopen(req, timeout=15)
            feed_content = response.read()
            if is_tek2day:
                print(f"  [DEBUG] Downloaded {len(feed_content)} bytes")
            d = feedparser.parse(feed_content)
        except Exception as e:
            if is_tek2day:
                print(f"  [DEBUG] Download failed, trying direct parse: {e}")
            d = feedparser.parse(url)
        
        items = []
        
        if is_tek2day:
            print(f"  [DEBUG] TEK2day feed has {len(d.entries)} total entries")
            print(f"  [DEBUG] Feed status: {d.get('status', 'unknown')}")
            print(f"  [DEBUG] Feed bozo: {d.get('bozo', False)}")
            if d.get('bozo'):
                print(f"  [DEBUG] Feed exception: {d.get('bozo_exception', 'none')}")
        
        for e in d.entries[:60]:
            title = clean_text(getattr(e, "title", ""))
            link = getattr(e, "link", "")
            
            if is_tek2day:
                print(f"  [DEBUG] Entry: {title[:50]}")
                print(f"  [DEBUG] Link: {link}")
            
            if not title or not link:
                if is_tek2day:
                    print(f"  [DEBUG] ❌ Skipped: No title or link")
                continue
                
            if is_blocked(link) and "youtube.com/feeds/videos.xml" not in (url or ""):
                if is_tek2day:
                    print(f"  [DEBUG] ❌ Skipped: Blocked domain")
                continue
                
            dt_local = parse_pubdate(e)
            
            if is_tek2day:
                print(f"  [DEBUG] Parsed date: {dt_local}")
                print(f"  [DEBUG] within_window check: {within_window(dt_local)}")
            
            is_youtube_feed = 'youtube.com/feeds/videos.xml' in (url or '')
            if (not is_youtube_feed) and (not within_window(dt_local)):
                if is_tek2day:
                    print(f"  [DEBUG] ❌ Skipped: Outside {BACKFILL_WINDOW_DAYS}-day window")
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
            
            if is_tek2day:
                print(f"  [DEBUG] ✅ Added to items!")
            
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
        if "tek2day" in feed_name.lower():
            print(f"  [DEBUG] ❌ EXCEPTION: {e}")
            import traceback
            traceback.print_exc()
        return []

# ----------------- Google Trends integration -----------------
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
    """
    Return canonical trending chips that appear in text.
    - Case-insensitive.
    - Punctuation-insensitive (e.g., 'A.I.' matches 'ai').
    - Variations collapse to a single canonical chip via ALIAS_TO_CANON.
    """
    if not text:
        return []
    tl = (text or "").lower()
    tn = _norm_keyword(text)

    out: set[str] = set()
    for kw in keywords or []:
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

# ----------------- categorization -----------------
CATEGORY_KEYWORDS = {
    "ai": [
        "ai","artificial intelligence","large language model","llm","gpt","openai",
        "anthropic","deepmind","sora","transformer","diffusion","ml","machine learning",
        "npu","tpu","cuda","rocm","inference","llama","mistral"
    ],
    "software": [
        "software","developer","devops","platform","sdk","api","apps","app","release",
        "github","cloud","saas","microservices","kubernetes","langchain","runtime","framework"
    ],
    "fintech": [
        "fintech","payments","payment","bank","banking","crypto","blockchain","defi",
        "lending","card","visa","mastercard","stripe","paypal","square","nubank","aml","kyc"
    ],
}

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
from urllib.parse import urlparse

AGG_DOMAINS = {"news.google.com", "news.yahoo.com", "news.ycombinator.com"}

def _strip_publisher_suffix(title: str) -> str:
    if not title: return ""
    t = html.unescape(title)
    t = re.sub("[‘’]", "'", t)  # curly -> straight
    t = re.sub('[“”]', '"', t)
    t = re.sub(r"\s+", " ", t).strip()
    for sep in (" - ", " | "):
        if sep in t:
            left, right = t.rsplit(sep, 1)
            if any(c.isalpha() for c in right) and len(right) <= 40:
                t = left
                break
    return t

def _host(u: str) -> str:
    try:
        h = urlparse(u or "").netloc.lower()
        return h[4:] if h.startswith("www.") else h
    except Exception:
        return ""

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

    return list(best.values())

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
    return out

def summarize(item):
    if item.get("summary"):
        return clean_text(item["summary"], 260)
    try:
        r = requests.get(item["url"], timeout=12, headers={"User-Agent":"Mozilla/5.0"})
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
            
            parts.append(f"""<article class="{top_cls.strip()}" data-card data-url="{url}" data-permalink="{permalink}" data-title="{html.escape(title_raw, quote=True)}" data-summary="{summary_html}">
  {thumb_html}
  <div class="article-content">
    <h3><a data-title-link href="{url}">{title}</a></h3>
    <div class="meta">{src} - {dt_str} {render_item_badges(it)} {trending_badge}</div>
    <div class="trend-chips">{trending_chips_html}</div>
    <p data-summary>{summary_html}</p>
  </div>
</article>""")
        return "\n".join(parts)

    html_out = tpl.replace("{{DATE_STR}}", date_str)
    total_count = sum(len(by_cat.get(k, [])) for k in ("ai", "software", "fintech"))

    for cat_key, ph in (("ai", "AI"), ("software", "SW"), ("fintech", "FT")):
        items = by_cat.get(cat_key, [])[:MAX_ITEMS]
        html_out = html_out.replace(f"{{{{{ph}_COUNT}}}}", str(len(items)))
        html_out = html_out.replace(
            f"{{{{{ph}_ITEMS}}}}",
            render_items(items) if items else "<p>No items today.</p>"
        )

    html_out = html_out.replace("{{TOTAL_COUNT}}", str(total_count))
    html_out = html_out.replace("{{COUNT}}", str(total_count))
    return html_out

# ---- Helper: ensure chips exist for items being rendered right now ----
def _apply_trend_chips_inplace(it: dict, trending_keywords: list[str]) -> None:
    title = it.get("title", "") or ""
    summary_raw = it.get("summary_text") or it.get("summary") or it.get("description") or ""
    summary_txt = strip_html_to_text(summary_raw)
    tags = set(_match_trending_keywords(title, trending_keywords))
    tags.update(_match_trending_keywords(summary_txt, trending_keywords))
    if tags:
        it["_trending_tags"] = sorted(tags)

def main():
    if os.environ.get('DISABLE_WEEKEND_CACHE', '0') != '1':
        if _weekend_use_friday_payload_if_available():
            return
    
    print("=== Starting T2D Pulse Generation ===")
    all_items = []

    trending_keywords = get_trending_tech_keywords(max_keywords=30)
    print(f"Fetched {len(trending_keywords)} trending tech keywords from Google Trends.")

    print(f"\n--- Fetching {len(CFG['sources']['rss'])} RSS feeds ---")
    for s in CFG["sources"]["rss"]:
        print(f"Fetching: {s['name']} from {s['url']}")
        fetched = fetch_rss(s["name"], s["url"])
        print(f"  → Got {len(fetched)} articles")
        all_items.extend(fetched)

    print(f"\n--- Before dedupe: {len(all_items)} total articles ---")
    all_items = dedupe(all_items)
    print(f"--- After dedupe: {len(all_items)} unique articles ---")

    pruned = []
    tek2day_count = 0
    tek2day_filtered = 0
    
    for it in all_items:
        is_tek2day = "tek2day" in (it.get("source") or "").lower()
        
        if is_tek2day:
            tek2day_count += 1
            print(f"\n[TEK2DAY] Processing article: {it.get('title', 'NO TITLE')[:60]}")
            print(f"  Source: {it.get('source')}")
            print(f"  URL: {it.get('url')}")
        
        if is_blocked(it["url"]):
            if is_tek2day:
                print(f"  ❌ BLOCKED by domain filter")
                tek2day_filtered += 1
            continue
            
        if is_deals_or_consumer_shopping(it["title"], it["url"]):
            if is_tek2day:
                print(f"  ❌ BLOCKED by deals/shopping filter")
                tek2day_filtered += 1
            continue
            
        src_norm = (it.get("source") or "").strip().lower()
        
        if is_tek2day:
            print(f"  Normalized source: '{src_norm}'")
            print(f"  FORCE_INCLUDE_SOURCES: {FORCE_INCLUDE_SOURCES}")
        
        it["summary_text"] = summarize(it)
        cat, score = categorize_with_score(it["title"], it["url"], it.get("summary_text", ""))
        
        if is_tek2day:
            print(f"  Category: {cat}, Score: {score}")
        
        is_youtube_src = ("youtube" in src_norm)
        is_force_included = any(term in src_norm for term in FORCE_INCLUDE_SOURCES)
        
        if is_tek2day:
            print(f"  is_force_included: {is_force_included}")
            print(f"  Checking if 'tek2day' in '{src_norm}': {'tek2day' in src_norm}")
        
        if score == 0 and (src_norm not in FORCE_AI_SOURCES) and (not is_youtube_src) and (not is_force_included):
            if is_tek2day:
                print(f"  ❌ BLOCKED by zero-score filter")
                tek2day_filtered += 1
            continue
            
        it["category"] = cat

        try:
            tags = set(_match_trending_keywords(it.get("title",""), trending_keywords))
            tags.update(_match_trending_keywords(it.get("summary_text",""), trending_keywords))
            if tags:
                it["_trending_tags"] = sorted(tags)
        except Exception:
            pass

        if is_tek2day:
            print(f"  ✅ PASSED all filters!")

        try:
            d = domain_of(it["url"])
        except Exception:
            d = ""
        src_norm = (it.get("source") or "").strip().lower()
        
        if ("youtube" in src_norm) or (src_norm in FORCE_AI_SOURCES) or (src_norm in FORCE_INCLUDE_SOURCES):
            it["category"] = "ai"
        elif d in FORCE_FINTECH_DOMAINS or src_norm in FORCE_FINTECH_SOURCES:
            scores = compute_scores(it["title"], it["url"], it.get("summary_text",""))
            if not (scores["ai"] >= 3 and scores["ai"] >= scores["fintech"] + 1):
                it["category"] = "fintech"

        pruned.append(it)
    
    print(f"\n=== TEK2DAY SUMMARY ===")
    print(f"TEK2day articles found: {tek2day_count}")
    print(f"TEK2day articles filtered out: {tek2day_filtered}")
    print(f"TEK2day articles passed: {tek2day_count - tek2day_filtered}")
    
    all_items = pruned

    def parsed_dt(it):
        try:
            return dtparser.parse(it["published_at"]).astimezone(TZ)
        except Exception:
            return now_et()
    all_items.sort(key=parsed_dt, reverse=True)

    by_cat = {"ai": [], "software": [], "fintech": []}
    for it in all_items:
        by_cat[it["category"]].append(it)

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

    date_str = now_et().strftime("%b %-d, %Y")
    section = build_section(date_str, by_cat)

    _save_friday_snapshot_if_today(all_items, by_cat)

    docs = os.path.join(REPO, "docs")
    os.makedirs(docs, exist_ok=True)
    with open(os.path.join(docs, "index.html"), "w", encoding="utf-8") as f:
        f.write(section)
    with open(os.path.join(docs, "pulse.json"), "w", encoding="utf-8") as f:
        json.dump(all_items, f, indent=2)
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

    try:
        analytics = {
            "generated_at": now_et().isoformat(),
            "total_keywords": len(trending_keywords),
            "keyword_counts": {},
            "sources_per_keyword": {},
            "top_articles_per_keyword": {},
        }
        items_for_stats = unique_items
        kw_to_items = {kw: [] for kw in trending_keywords}
        for it in items_for_stats:
            for kw in (it.get("_trending_tags") or []):
                kw_to_items.setdefault(kw, []).append(it)
        for kw, items in kw_to_items.items():
            if not items:
                continue
            analytics["keyword_counts"][kw] = len(items)
            srcs = sorted({(it.get("source") or "").strip() for it in items if it.get("source")})
            doms = sorted({domain_of(it.get("url","")) for it in items if it.get("url")})
            analytics["sources_per_keyword"][kw] = {"sources": srcs, "domains": doms}
            def _dt(it):
                try:
                    return dtparser.parse(it.get("published_at","")).astimezone(TZ)
                except Exception:
                    return now_et()
            top = sorted(items, key=_dt, reverse=True)[:5]
            analytics["top_articles_per_keyword"][kw] = [
                {
                    "title": it.get("title"),
                    "url": it.get("url"),
                    "source": it.get("source"),
                    "published_at": it.get("published_at"),
                    "category": it.get("category"),
                    "_permalink": it.get("_abs_permalink") or it.get("_permalink"),
                } for it in top
            ]
        nonzero = {k:v for k,v in analytics["keyword_counts"].items() if v > 0}
        analytics["keyword_counts"] = dict(sorted(nonzero.items(), key=lambda kv: (-kv[1], kv[0])))
        with open(os.path.join(docs, "trending_analytics.json"), "w", encoding="utf-8") as f:
            json.dump(analytics, f, indent=2)
    except Exception as e:
        print(f"Warning: failed to write trending_analytics.json: {e}")

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
