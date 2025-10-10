#!/usr/bin/env python3
# generator/generate_pulse.py
#
# Website-only generator for TEK2day Pulse (AI → Software → FinTech)
# – NO Hacker News (blocked at fetch + at Google News resolution)
# – Google News links resolved to real publisher domains
# – Clean titles/summaries (HTML stripped, entities decoded)
# – Consistent, human-friendly source names

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

# Always block these (Hacker News arrives via Google News sometimes)
ALWAYS_BLOCK = {"news.ycombinator.com", "ycombinator.com"}

# Map domains → clean outlet names
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
    "news.google.com": "Google News",  # used only if resolution fails
}

# ----------------- helpers -----------------
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
    # Fallback: title-case core domain (e.g., computerworld → Computerworld)
    core = d.split(".")[-2] if d.count(".") >= 1 else d
    return core.capitalize()

def is_blocked(url):
    d = domain_of(url)
    if not d:
        return False
    if d in ALWAYS_BLOCK:
        return True
    if any(d.endswith(suf) for suf in BLOCK_SUFFIXES):
        return True
    return False

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

# ----------------- fetchers -----------------
def fetch_rss(feed_name, url):
    """For direct outlet RSS (Verge, VB, PYMNTS, OpenAI, Anthropic).
       We trust the feed; we still clean summaries and block domains for safety.
    """
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
                "source": feed_name,  # keep brand name from config for direct feeds
                "summary": summary,
                "content_html": content_html,
            })
        return items
    except Exception:
        return []

def google_news_rss(query):
    """Fetch Google News RSS, resolve to canonical, drop blocked domains,
       and set source to the final publisher (never HN/Google News unless resolution fails).
    """
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

        # Clean the feed-provided summary
        raw_sum = getattr(e, "summary", "") or getattr(e, "description", "")
        summary = clean_text(strip_html_to_text(raw_sum), 400)

        # Resolve the Google News redirect to the publisher URL
        final_url = link
        try:
            resp = requests.get(link, timeout=15, allow_redirects=True, headers={"User-Agent":"Mozilla/5.0"})
            if resp.url:
                final_url = resp.url
        except Exception:
            pass

        # Block unwanted domains (HN/Substack/X/etc.)
        if is_blocked(final_url):
            continue

        # If we somehow failed to resolve and still sit on news.google.com, skip it (messy source)
        if domain_of(final_url) == "news.google.com":
            continue

        out.append({
            "title": title,
            "url": final_url,
            "published_at": dt_local.isoformat(),
            "source": nice_source_for(final_url),  # publisher name, not "Hacker News"
            "summary": summary,
            "content_html": "",
        })
    return out

# ----------------- categorization -----------------
CATEGORY_KEYWORDS = {
    "ai": [
        "ai","artificial intelligence","large language model","llm","gpt","openai",
        "anthropic","deepmind","sora","transformer","diffusion","ml","machine learning",
        "neural","chip","npu"
    ],
    "software": [
        "software","developer","devops","platform","sdk","api","apps","app","release",
        "github","cloud","saas","microservices","kubernetes","langchain"
    ],
    "fintech": [
        "fintech","payments","payment","bank","banking","crypto","blockchain","defi",
        "lending","card","visa","mastercard","stripe","paypal","square","nubank"
    ],
}

def categorize(title, url):
    t = f"{title} {url}".lower()
    score = {"ai":0,"software":0,"fintech":0}
    for cat, kws in CATEGORY_KEYWORDS.items():
        for w in kws:
            if w in t:
                score[cat] += 1
    cat = max(score, key=score.get)
    return cat if score[cat] > 0 else "ai"

# ----------------- pipeline -----------------
def dedupe(items):
    out, seen = [], set()
    for it in items:
        key = re.sub(r"[^a-z0-9]+","", it["title"].lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

def summarize(item):
    # Prefer feed summary; otherwise try OG/description at the page
    if item.get("summary"):
        return clean_text(item["summary"], 260)
    try:
        r = requests.get(item["url"], timeout=12, headers={"User-Agent":"Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html5lib")
        for sel in [
            ("meta", {"property":"og:description"}),
            ("meta", {"name":"description"}),
        ]:
            m = soup.find(*sel)
            if m and m.get("content"):
                return clean_text(m["content"], 260)
    except Exception:
        pass
    return clean_text(item["title"], 200)

def build_section(date_str, by_cat):
    with open(os.path.join(ROOT, "templates/section_template.html"), "r", encoding="utf-8") as f:
        tpl = f.read()

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
            quote = html.escape(it.get("quote","")) if it.get("quote") else ""
            top_cls = " top" if idx == 0 else ""
            quote_html = f'<p class="quote">“{quote}”</p>' if quote else ""
            parts.append(f'''<article class="{top_cls.strip()}">
  <h3><a href="{url}">{title}</a></h3>
  <div class="meta">{src} - {dt_str}</div>
  <p>{summary}</p>
  {quote_html}
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

    # Direct RSS feeds (trusted brands)
    for s in CFG["sources"]["rss"]:
        all_items.extend(fetch_rss(s["name"], s["url"]))

    # Google News bundles (resolve to publishers, block HN/Substack/X)
    for _, queries in CFG["sources"]["google_news_queries"].items():
        for q in queries:
            all_items.extend(google_news_rss(q))

    # Global dedupe
    all_items = dedupe(all_items)

    # Summarize, categorize, final domain block
    pruned = []
    for it in all_items:
        if is_blocked(it["url"]):
            continue
        it["summary_text"] = summarize(it)
        it["category"] = categorize(it["title"], it["url"])
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
    by_cat = {"ai":[], "software":[], "fintech":[]}
    for it in all_items:
        try:
            if within_window(dtparser.parse(it["published_at"]).astimezone(TZ)):
                by_cat[it["category"]].append(it)
        except Exception:
            by_cat[it["category"]].append(it)

    # Render HTML + JSON
    date_str = now_et().strftime("%b %-d, %Y")
    section = build_section(date_str, by_cat)

    docs = os.path.join(REPO, "docs")
    os.makedirs(docs, exist_ok=True)
    with open(os.path.join(docs, "index.html"), "w", encoding="utf-8") as f:
        f.write(section)
    with open(os.path.join(docs, "pulse.json"), "w", encoding="utf-8") as f:
        json.dump(all_items, f, indent=2)

if __name__ == "__main__":
    main()
