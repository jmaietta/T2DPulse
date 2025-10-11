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

ROOT = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(ROOT)

with open(os.path.join(ROOT, "config.yaml"), "r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

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
    return dt_local >= (now_et() - timedelta(hours=RUN_WINDOW_HOURS))

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

# ---- “deals/consumer shopping” filter ----
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
        key = re.sub(r"[^a-z0-9]+", "", it["title"].lower())
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
        if it.get("_older_than_fresh_window"):
            return '<span class="badge muted">Older</span>'
        # mark <24h items as New
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
            title = html.escape(it["title"])
            url = add_utm(it["url"])
            src = html.escape(it["source"])
            try:
                dt_local = dtparser.parse(it["published_at"]).astimezone(TZ)
                dt_str = dt_local.strftime("%b %-d, %Y")
            except Exception:
                dt_str = date_str
            summary = html.escape(it.get("summary_text",""))
            top_cls = " top" if idx == 0 else ""
            parts.append(f'''<article class="{top_cls.strip()}">
  <h3><a href="{url}">{title}</a></h3>
  <div class="meta">{src} - {dt_str} {render_item_badges(it)}</div>
  <p>{summary}</p>
</article>''')
        return "\n        ".join(parts)

    html_out = tpl.replace("{{DATE_STR}}", date_str)
    for cat_key, ph in (("ai","AI"),("software","SW"),("fintech","FT")):
        items = by_cat.get(cat_key, [])[:MAX_ITEMS]
        html_out = html_out.replace(f"{{{{{ph}_COUNT}}}}", str(len(items)))
        html_out = html_out.replace(f"{{{{{ph}_ITEMS}}}}", render_items(items) if items else "<p>No items today.</p>")
    return html_out

def main():
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

    # Render

    date_str = now_et().strftime("%b %-d, %Y")
    section = build_section(date_str, by_cat)

    # Write outputs
    docs = os.path.join(REPO, "docs")
    os.makedirs(docs, exist_ok=True)
    with open(os.path.join(docs, "index.html"), "w", encoding="utf-8") as f:
        f.write(section)
    with open(os.path.join(docs, "pulse.json"), "w", encoding="utf-8") as f:
        json.dump(all_items, f, indent=2)

if __name__ == "__main__":
    main()

