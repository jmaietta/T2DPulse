#!/usr/bin/env python3
# generator/generate_pulse.py
#
# Builds a vertically stacked AI → Software → FinTech HTML section (docs/index.html)
# from your configured RSS feeds + Google News queries. Hacker News REMOVED.
#
# Changes in this version:
# - Titles/summaries now decode HTML entities first (fixes &#8217; → ’).
# - Dropped Hacker News completely
# - Optional domain blocklist applied to *all* items (e.g., substack.com).

import os, re, json, html
import feedparser, requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtparser
import pytz, yaml

ROOT = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(ROOT)

# ----- Load config -----
with open(os.path.join(ROOT, "config.yaml"), "r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

TZ = pytz.timezone(CFG.get("timezone", "America/New_York"))
RUN_WINDOW_HOURS = int(CFG.get("run_window_hours", 24))
MAX_ITEMS = int(CFG.get("max_items_per_category", 15))
UTM = CFG.get("utm", {"source": "tek2day", "medium": "email"})
BLOCK_SUFFIXES = [s.lower() for s in CFG.get("exclude_domains_suffix", [])]

# ----- Helpers -----
def now_et():
    return datetime.now(TZ)

def within_window(dt_local: datetime) -> bool:
    start = now_et() - timedelta(hours=RUN_WINDOW_HOURS)
    return dt_local >= start

def add_utm(url: str) -> str:
    return f"{url}{'&' if '?' in url else '?'}utm_source={UTM['source']}&utm_medium={UTM['medium']}"

def is_blocked(url: str) -> bool:
    u = (url or "").lower()
    return any(u.endswith(suf) or suf in u for suf in BLOCK_SUFFIXES)

def clean_text(s: str, limit: int | None = None) -> str:
    """
    Fix: decode HTML entities first so smart quotes render correctly.
    Then normalize whitespace. Optionally truncate.
    """
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    if limit and len(s) > limit:
        return s[:limit - 1] + "…"
    return s

def parse_pubdate(entry) -> datetime:
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

# ----- Fetchers -----
def fetch_rss(name: str, url: str) -> list[dict]:
    try:
        d = feedparser.parse(url)
        items = []
        for e in d.entries[:50]:
            title = clean_text(getattr(e, "title", ""))
            link = getattr(e, "link", "")
            if not title or not link:
                continue
            if is_blocked(link):
                continue

            dt_local = parse_pubdate(e)
            if not within_window(dt_local):
                continue

            summary = clean_text(
                getattr(e, "summary", "") or getattr(e, "description", ""),
                400,
            )
            content_html = ""
            if hasattr(e, "content"):
                try:
                    content_html = e.content[0].value
                except Exception:
                    pass

            items.append(
                {
                    "title": title,
                    "url": link,
                    "published_at": dt_local.isoformat(),
                    "source": name,
                    "summary": summary,
                    "content_html": content_html,
                }
            )
        return items
    except Exception:
        return []

def google_news_rss(query: str) -> list[dict]:
    import urllib.parse
    q = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    return fetch_rss("Google News", url)

# ----- Classification -----
CATEGORY_KEYWORDS = {
    "ai": [
        "ai", "artificial intelligence", "large language model", "llm", "gpt",
        "openai", "anthropic", "deepmind", "sora", "transformer", "diffusion",
        "ml", "machine learning", "neural", "chip", "npu"
    ],
    "software": [
        "software", "developer", "devops", "platform", "sdk", "api", "apps",
        "app", "release", "github", "cloud", "saas", "microservices",
        "kubernetes", "langchain"
    ],
    "fintech": [
        "fintech", "payments", "payment", "bank", "banking", "crypto",
        "blockchain", "defi", "lending", "card", "visa", "mastercard",
        "stripe", "paypal", "square", "nubank"
    ],
}

def categorize(title: str, url: str) -> str:
    t = f"{title} {url}".lower()
    score = {"ai": 0, "software": 0, "fintech": 0}
    for cat, kws in CATEGORY_KEYWORDS.items():
        for w in kws:
            if w in t:
                score[cat] += 1
    cat = max(score, key=score.get)
    if score[cat] == 0:
        cat = "ai"  # default
    return cat

# ----- Processing -----
def dedupe(items: list[dict]) -> list[dict]:
    out, seen = [], set()
    for it in items:
        key = re.sub(r"[^a-z0-9]+", "", it["title"].lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

def summarize(item: dict) -> str:
    # Use feed summary when present; otherwise attempt to pull OG/description from the page.
    if item.get("summary"):
        return clean_text(item["summary"], 260)
    try:
        r = requests.get(item["url"], timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html5lib")
        og = soup.find("meta", attrs={"property": "og:description"})
        if og and og.get("content"):
            return clean_text(og["content"], 260)
        desc = soup.find("meta", attrs={"name": "description"})
        if desc and desc.get("content"):
            return clean_text(desc["content"], 260)
    except Exception:
        pass
    return clean_text(item["title"], 200)

# ----- Render -----
def build_section(date_str: str, by_cat: dict) -> str:
    with open(os.path.join(ROOT, "templates/section_template.html"), "r", encoding="utf-8") as f:
        tpl = f.read()

    def render_items(items: list[dict]) -> str:
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
            summary = html.escape(it.get("summary_text", ""))
            quote = html.escape(it.get("quote", "")) if it.get("quote") else ""
            top_cls = " top" if idx == 0 else ""
            quote_html = f'<p class="quote">“{quote}”</p>' if quote else ""
            parts.append(
                f'''<article class="{top_cls.strip()}">
  <h3><a href="{url}">{title}</a></h3>
  <div class="meta">{src} - {dt_str}</div>
  <p>{summary}</p>
  {quote_html}
</article>'''
            )
        return "\n        ".join(parts)

    html_out = tpl.replace("{{DATE_STR}}", date_str)
    for cat_key, ph in (("ai", "AI"), ("software", "SW"), ("fintech", "FT")):
        items = by_cat.get(cat_key, [])[:MAX_ITEMS]
        html_out = html_out.replace(f"{{{{{ph}_COUNT}}}}", str(len(items)))
        html_out = html_out.replace(
            f"{{{{{ph}_ITEMS}}}}",
            render_items(items) if items else "<p>No items today.</p>",
        )
    return html_out

# ----- Main -----
def main():
    all_items = []

    # RSS sources
    for s in CFG["sources"]["rss"]:
        all_items.extend(fetch_rss(s["name"], s["url"]))

    # Google News bundles
    for _cat, queries in CFG["sources"]["google_news_queries"].items():
        for q in queries:
            all_items.extend(google_news_rss(q))

    # Global dedupe
    all_items = dedupe(all_items)

    # Summarize + categorize + final block
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
    by_cat = {"ai": [], "software": [], "fintech": []}
    for it in all_items:
        # Only keep items within window (in case downstream parsers added items)
        try:
            if not within_window(dtparser.parse(it["published_at"]).astimezone(TZ)):
                continue
        except Exception:
            pass
        by_cat[it["category"]].append(it)

    # Render HTML
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
