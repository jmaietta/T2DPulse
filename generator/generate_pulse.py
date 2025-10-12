#!/usr/bin/env python3
# generator/generate_pulse.py
# Website-only generator for TEK2day Pulse (AI → Software → FinTech)
# ✅ No Hacker News (blocked at domain level; GN redirects resolved)
# ✅ Google News links resolved to real publisher domains
# ✅ Clean titles/summaries (HTML stripped, entities decoded)
# ✅ Filters out deals/consumer shopping posts (e.g., TV discounts)
# ✅ Requires at least one AI/Software/FinTech keyword match

import os, re, json, html, urllib.parse
import feedparser, requests, tldextract
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtparser
import pytz, yaml
import hashlib
from PIL import Image
from io import BytesIO

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
    # Return date of the most recent Friday relative to date d (d is ET date).
    # Monday=0 ... Sunday=6; we want Friday=4
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
    # read cache
    if CACHE_FILE.exists():
        try:
            cached = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            if cached.get("ref_date") == want_friday.strftime("%Y-%m-%d"):
                # Re-render using cached by_cat and Friday header date
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
    # If today is Friday, cache the snapshot for weekend reuse.
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
MAX_ITEMS = int(CFG.get("max_items_per_category", 15))
UTM = CFG.get("utm", {"source": "tek2day", "medium": "email"})
BLOCK_SUFFIXES = [s.lower() for s in CFG.get("exclude_domains_suffix", [])]

# Always block these (HN sometimes arrives via Google News)
ALWAYS_BLOCK = {"news.ycombinator.com", "ycombinator.com"}

# Optional: map domains → clean brand names
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
}

# --- Force-category overrides (base) ---
FORCE_FINTECH_DOMAINS = {"pymnts.com"}     # normalized by domain_of()
FORCE_FINTECH_SOURCES = {"pymnts"}         # lowercased source label

# ----------------- helpers -----------------
# --- Freshness & diversity helpers (news-first policy) ---
FRESH_WINDOW_DAYS = 3  # hard freshness window

# --- Backfill floors & window (quick hot-fix) ---
BACKFILL_WINDOW_DAYS = 5  # backfill pool window
FLOORS = {"ai": 10, "software": 6, "fintech": 6}  # minimum per category


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
    """Interleave by domain among fresh items without forcing stale content."""
    if not items:
        return []
    buckets = defaultdict(list)
    for it in items:
        buckets[domain_of(it.get("url",""))].append(it)
    # newest first inside each bucket; randomize ties
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

def _enforce_floors_with_backfill(by_cat: dict, all_items: list, now_local=None, floors=None, backfill_days=None):
    # Ensure at least N items per category by topping up from the last X days.
    # Today's items are preferred. Backfilled items get `_backfilled=True` (no badges).
    now_local = now_local or now_et()
    floors = floors or FLOORS
    backfill_days = backfill_days or BACKFILL_WINDOW_DAYS

    def _dt_local(it):
        try:
            return dtparser.parse(it["published_at"]).astimezone(TZ)
        except Exception:
            return now_local

    def _is_today(dt):
        return dt.date() == now_local.date()

    pool_by_cat = {"ai": [], "software": [], "fintech": []}
    for it in all_items:
        try:
            dtl = _dt_local(it)
        except Exception:
            continue
        if dtl >= now_local - timedelta(days=backfill_days):
            pool_by_cat.get(it.get("category"), []).append(it)

    for k in pool_by_cat:
        pool_by_cat[k].sort(key=_dt_local, reverse=True)

    out = {}
    for cat in ("ai", "software", "fintech"):
        floor = int(floors.get(cat, 0))
        pool = pool_by_cat.get(cat, [])
        todays = [it for it in pool if _is_today(_dt_local(it))]

        chosen = todays[:]
        target = max(floor, min(MAX_ITEMS, len(todays)))

        if len(chosen) < target:
            backfill = [it for it in pool if it not in chosen]
            need = target - len(chosen)
            for it in backfill:
                if len(chosen) >= target:
                    break
                it["_backfilled"] = True  # suppress badges
                chosen.append(it)

        out[cat] = chosen[:MAX_ITEMS]
    return out


def finalize_section_with_backfill(items, section_max, now=None, max_backfill_days=7):
    now = now or datetime.now(timezone.utc)
    fresh = filter_fresh(items, FRESH_WINDOW_DAYS, now=now)
    if fresh:
        interleaved = prefer_diverse_round_robin(fresh, max_total=section_max)
        return sort_by_recency(interleaved)[:section_max]
    # graceful backfill: up to N days older, clearly labeled later
    cutoff = now - timedelta(days=max_backfill_days)
    cands = [it for it in items if (dt := safe_parse_dt(it.get("published_at"))) and dt >= cutoff]
    cands = sort_by_recency(cands)[:section_max]
    for it in cands:
        it["_older_than_fresh_window"] = True
    return cands

def now_et():
    return datetime.now(TZ)

def within_window(dt_local):
    # Accept items up to BACKFILL_WINDOW_DAYS old so we can top up thin sections.
    return dt_local >= (now_et() - timedelta(days=BACKFILL_WINDOW_DAYS))

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
        return s[:limit - 1] + "…"
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


# ---- Permalink & summary helpers ----
PERMA_ROOT = os.path.join(REPO, "docs", "p")  # docs/p/<id>/
PERMA_TPL  = os.path.join(ROOT, "templates", "item_template.html")  # generator/templates/item_template.html

def _stable_id(title: str, url: str, published_at: str) -> str:
    key = f"{(title or '').strip()}|{(url or '').strip()}|{(published_at or '').strip()}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]

def _plain_text_summary(it: dict, limit: int = 240) -> str:
    raw = it.get("summary_text") or it.get("summary") or it.get("description") or it.get("content_html") or it.get("title") or ""
    txt = strip_html_to_text(raw)
    txt = re.sub(r"\s+", " ", txt).strip()
    if len(txt) <= limit:
        return txt
    cut = txt[:limit - 1]
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return cut + "…"

def _render_template_string(tpl: str, **kv) -> str:
    html_out = tpl
    for k, v in kv.items():
        html_out = html_out.replace(f"{{{{{k}}}}}", v or "")
    return html_out

def create_branded_og_image(source_url: str, permalink_dir: str) -> str:
    """
    Creates a branded OG image (1200x630):
    - Primary: Fetch source article's og:image and overlay T2D logo (80x80, top-right, 20px padding)
    - Fallback: Use T2D banner if source image unavailable
    Returns relative path to the image or empty string on failure.
    """
    logo_path = os.path.join(REPO, "docs", "icons", "T2D_Pulse_Logo_2.png")
    banner_path = os.path.join(REPO, "docs", "icons", "T2D_Pulse_Banner.png")
    output_path = os.path.join(permalink_dir, "og-image.png")
    
    # Target dimensions
    TARGET_WIDTH = 1200
    TARGET_HEIGHT = 630
    LOGO_SIZE = 80
    PADDING = 20
    
    try:
        # Try to fetch source article's og:image
        source_og_url = None
        try:
            resp = requests.get(source_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(resp.text, "html5lib")
            og_img = soup.find("meta", property="og:image")
            if og_img and og_img.get("content"):
                source_og_url = og_img["content"]
        except Exception:
            pass
        
        # Try to create image from source
        base_img = None
        if source_og_url:
            try:
                img_resp = requests.get(source_og_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
                base_img = Image.open(BytesIO(img_resp.content)).convert("RGB")
                
                # Resize to 1200x630, maintaining aspect ratio and cropping
                img_aspect = base_img.width / base_img.height
                target_aspect = TARGET_WIDTH / TARGET_HEIGHT
                
                if img_aspect > target_aspect:
                    # Image is wider, fit height and crop width
                    new_height = TARGET_HEIGHT
                    new_width = int(new_height * img_aspect)
                    base_img = base_img.resize((new_width, new_height), Image.LANCZOS)
                    # Crop center
                    left = (new_width - TARGET_WIDTH) // 2
                    base_img = base_img.crop((left, 0, left + TARGET_WIDTH, TARGET_HEIGHT))
                else:
                    # Image is taller, fit width and crop height
                    new_width = TARGET_WIDTH
                    new_height = int(new_width / img_aspect)
                    base_img = base_img.resize((new_width, new_height), Image.LANCZOS)
                    # Crop center
                    top = (new_height - TARGET_HEIGHT) // 2
                    base_img = base_img.crop((0, top, TARGET_WIDTH, top + TARGET_HEIGHT))
                
                # Overlay logo in top right
                if os.path.exists(logo_path):
                    logo = Image.open(logo_path).convert("RGBA")
                    logo = logo.resize((LOGO_SIZE, LOGO_SIZE), Image.LANCZOS)
                    
                    # Position: top-right with padding
                    logo_x = TARGET_WIDTH - LOGO_SIZE - PADDING
                    logo_y = PADDING
                    
                    # Paste with alpha channel
                    base_img.paste(logo, (logo_x, logo_y), logo)
                
                base_img.save(output_path, "PNG", optimize=True)
                return "/p/" + os.path.basename(permalink_dir) + "/og-image.png"
            except Exception:
                pass
        
        # Fallback: use banner
        if os.path.exists(banner_path):
            banner = Image.open(banner_path).convert("RGB")
            banner = banner.resize((TARGET_WIDTH, TARGET_HEIGHT), Image.LANCZOS)
            banner.save(output_path, "PNG", optimize=True)
            return "/p/" + os.path.basename(permalink_dir) + "/og-image.png"
        
    except Exception:
        pass
    
    return ""

def write_permalink_page(it: dict) -> str:
    """Writes docs/p/<id>/index.html and returns the ABSOLUTE permalink URL."""
    site_base = (CFG.get("site_base") or "").rstrip("/")

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

    summary = _plain_text_summary(it, limit=240)
    
    # Create branded OG image
    og_image_rel = create_branded_og_image(url, perma_dir)
    og_image_abs = f"{site_base}{og_image_rel}" if og_image_rel and site_base else ""

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
    return abs_permalink


# ---- "deals/consumer shopping" filter ----
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
        d = feedparser.parse(url)
        items = []
        for e in d.entries[:60]:
            title = clean_text(getattr(e, "title", ""))
            link = getattr(e, "link", "")
            if not title or not link:
                continue
            if is_blocked(link):
                continue
            dt_local = parse_pubdate(e)
            if not within_window(dt_local):
                continue
            raw_sum = getattr(e, "summary", "") or getattr(e, "description", "")
            summary = clean_text(strip_html_to_text(raw_sum), 400)
            content_html = ""
            if hasattr(e, "content"):
                try:
                    content_html = e.content[0].value
                except Exception:
                    pass
            items.append({
                "title": title,
                "url": link,
                "published_at": dt_local.isoformat(),
                "source": feed_name,  # trust the configured brand for direct feeds
                "summary": summary,
                "content_html": content_html,
            })
        return items
    except Exception:
        return []

def google_news_rss(query):
    """Google News RSS → resolve to publisher URL, drop blocked domains,
       fix summaries, and label with real publisher (never HN/Google News)."""
    q = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    d = feedparser.parse(url)
    out = []
    for e in d.entries[:60]:
        title = clean_text(getattr(e, "title", ""))
        link = getattr(e, "link", "")
        if not title or not link:
            continue
        dt_local = parse_pubdate(e)
        if not within_window(dt_local):
            continue

        raw_sum = getattr(e, "summary", "") or getattr(e, "description", "")
        summary = clean_text(strip_html_to_text(raw_sum), 400)

        # Resolve redirect to final publisher
        final_url = link
        try:
            resp = requests.get(link, timeout=15, allow_redirects=True, headers={"User-Agent":"Mozilla/5.0"})
            if resp.url:
                final_url = resp.url
        except Exception:
            pass

        if is_blocked(final_url):
            continue
        if domain_of(final_url) == "news.google.com":
            continue

        out.append({
            "title": title,
            "url": final_url,
            "published_at": dt_local.isoformat(),
            "source": nice_source_for(final_url),
            "summary": summary,
            "content_html": "",
        })
    return out

# ----------------- categorization -----------------
# (Legacy reference; new logic below does not rely on it directly.)
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

# --- Improved categorization: stricter AI, weighted by field ---
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
    "software", "developer", "sdk", "api", "release", "version", "kubernetes", "docker",
    "github", "vscode", "framework", "runtime", "serverless", "cloud", "saas",
    "microservices", "observability", "database", "postgres", "mysql", "redis"
]
FT_STRONG = [
    "fintech", "payments", "payment", "bank", "banking", "visa", "mastercard", "stripe",
    "paypal", "plaid", "lending", "loan", "crypto", "bitcoin", "ethereum", "stablecoin",
    "defi", "aml", "kyc", "sec", "fdic", "treasury", "card", "tokenization", "stablecoin", "coinbase", "merchant"
]

def _count_hits(text: str, terms: list[str]) -> int:
    if not text:
        return 0
    t = f" {text.lower()} "  # pad to catch word-ish boundaries
    return sum(1 for w in terms if w in t)

def categorize_with_score(title: str, url: str, summary: str = ""):
    """
    Weighted scoring:
      - Title has 3x weight, Summary 2x, URL 1x.
      - AI uses strong + weak lists; negatives subtract.
      - To label as AI: ai_score >= 2 and ai_score >= max(other) + 1
    """
    title_l = title or ""
    summary_l = summary or ""
    url_l = url or ""

    # AI scoring
    ai = (
        3 * _count_hits(title_l, AI_STRONG)
      + 2 * _count_hits(summary_l, AI_STRONG)
      + 1 * _count_hits(url_l, AI_STRONG)
      + 1 * _count_hits(title_l, AI_WEAK)
      + 1 * _count_hits(summary_l, AI_WEAK)
    )
    ai -= min(2, _count_hits(f"{title_l} {summary_l} {url_l}", AI_NEGATIVE))  # cap penalty at 2

    # Software and FinTech
    sw = (
        2 * _count_hits(title_l, SW_STRONG)
      + 1 * _count_hits(summary_l, SW_STRONG)
      + 1 * _count_hits(url_l, SW_STRONG)
    )
    ft = (
        2 * _count_hits(title_l, FT_STRONG)
      + 1 * _count_hits(summary_l, FT_STRONG)
      + 1 * _count_hits(url_l, FT_STRONG)
    )

    # Decide with threshold and margin
    other_max = max(sw, ft)
    if ai >= 2 and ai >= other_max + 1:
        return "ai", ai
    # else pick between software/fintech
    if sw >= ft:
        return "software", sw
    else:
        return "fintech", ft

# ---- Score exposer (for routing decisions) ----
def compute_scores(title: str, url: str, summary: str = "") -> dict:
    """Return raw category scores using the same logic as above (no thresholding)."""
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
    )
    ft = (
        2 * _count_hits(title_l, FT_STRONG)
      + 1 * _count_hits(summary_l, FT_STRONG)
      + 1 * _count_hits(url_l, FT_STRONG)
    )
    return {"ai": ai, "software": sw, "fintech": ft}

# ----------------- pipeline -----------------

def dedupe(items):
    out, seen = [], set()
    for it in items:
        title_key = re.sub(r"[^a-z0-9]+", "", (it.get("title") or "").lower())
        dom = domain_of(it.get("url", ""))
        key = f"{title_key}::{dom}"
        if key in seen:
            continue
        seen.add(key)
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
        # Backfilled items: suppress 'New' and 'Older' badges
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
            title_raw = it["title"]
            title = html.escape(title_raw)
            url = add_utm(it["url"])
            permalink = it.get("_abs_permalink", "")
            src = html.escape(it["source"])
            try:
                dt_local = dtparser.parse(it["published_at"]).astimezone(TZ)
                dt_str = dt_local.strftime("%b %-d, %Y")
            except Exception:
                dt_str = date_str
            summary_txt = clean_text(strip_html_to_text(it.get("summary_text","")), 240)
            summary_html = html.escape(summary_txt)
            top_cls = " top" if idx == 0 else ""
            parts.append(f'''<article class="{top_cls.strip()}" data-card data-url="{url}" data-permalink="{permalink}" data-title="{html.escape(title_raw, quote=True)}" data-summary="{summary_html}">
  <h3><a data-title-link href="{url}">{title}</a></h3>
  <div class="meta">{src} - {dt_str} {render_item_badges(it)}</div>
  <p data-summary>{summary_html}</p>
</article>''')
        return "\n".join(parts)

    html_out = tpl.replace("{{DATE_STR}}", date_str)
    for cat_key, ph in (("ai","AI"),("software","SW"),("fintech","FT")):
        items = by_cat.get(cat_key, [])[:MAX_ITEMS]
        html_out = html_out.replace(f"{{{{{ph}_COUNT}}}}", str(len(items)))
        html_out = html_out.replace(f"{{{{{ph}_ITEMS}}}}", render_items(items) if items else "<p>No items today.</p>")
    return html_out

def main():
    # Weekend: reuse Friday snapshot if available
    if _weekend_use_friday_payload_if_available():
        return
    all_items = []

    # Direct RSS
    for s in CFG["sources"]["rss"]:
        all_items.extend(fetch_rss(s["name"], s["url"]))

    # Google News queries
    for _, queries in CFG["sources"]["google_news_queries"].items():
        for q in queries:
            all_items.extend(google_news_rss(q))

    # Dedupe
    all_items = dedupe(all_items)

    # Final filtering, relevance, enrichment
    pruned = []
    for it in all_items:
        if is_blocked(it["url"]):
            continue
        if is_deals_or_consumer_shopping(it["title"], it["url"]):
            continue
        # Compute summary first so categorizer can use it
        it["summary_text"] = summarize(it)
        cat, score = categorize_with_score(it["title"], it["url"], it.get("summary_text", ""))
        if score == 0:
            continue  # drop unrelated items
        it["category"] = cat

        # --- PYMNTS routing: FinTech unless clearly AI ---
        try:
            d = domain_of(it["url"])  # e.g., "pymnts.com"
        except Exception:
            d = ""
        src_norm = (it.get("source") or "").strip().lower()  # e.g., "pymnts"
        if d in FORCE_FINTECH_DOMAINS or src_norm in FORCE_FINTECH_SOURCES:
            scores = compute_scores(it["title"], it["url"], it.get("summary_text",""))
            # Only let PYMNTS land in AI if it's clearly AI (AI ≥ 3 and AI ≥ FinTech + 1)
            if not (scores["ai"] >= 3 and scores["ai"] >= scores["fintech"] + 1):
                it["category"] = "fintech"
        # --- end routing ---

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
    by_cat = {"ai": [], "software": [], "fintech": []}
    for it in all_items:
        by_cat[it["category"]].append(it)

    # Fresh-first + graceful backfill (never force a stale source)
    for _k in ("ai", "software", "fintech"):
        by_cat[_k] = finalize_section_with_backfill(
            by_cat.get(_k, []),
            section_max=MAX_ITEMS,
            now=now_et(),
            max_backfill_days=7
        )


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
            except Exception:
                pass
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

    # Write daily snapshot to docs/archive/json/YYYY-MM-DD.json
    try:
        arch_dir = os.path.join(docs, "archive", "json")
        os.makedirs(arch_dir, exist_ok=True)
        snap_name = now_et().strftime("%Y-%m-%d") + ".json"
        with open(os.path.join(arch_dir, snap_name), "w", encoding="utf-8") as f:
            json.dump({"date": now_et().strftime("%Y-%m-%d"), "by_cat": by_cat}, f, indent=2)
    except Exception:
        pass

if __name__ == "__main__":
    main()
