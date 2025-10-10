#!/usr/bin/env python3
import os, re, json
import feedparser, requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtparser
import pytz, html, yaml

ROOT = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(ROOT)

with open(os.path.join(ROOT, "config.yaml"), "r") as f:
    CFG = yaml.safe_load(f)

TZ = pytz.timezone(CFG.get("timezone","America/New_York"))
RUN_WINDOW_HOURS = int(CFG.get("run_window_hours", 24))
MAX_ITEMS = int(CFG.get("max_items_per_category", 15))
UTM = CFG.get("utm", {"source":"tek2day","medium":"email"})

def now_et():
    return datetime.now(TZ)

def within_window(dt):
    start = now_et() - timedelta(hours=RUN_WINDOW_HOURS)
    return dt >= start

def add_utm(url):
    return f"{url}{'&' if '?' in url else '?'}utm_source={UTM['source']}&utm_medium={UTM['medium']}"

def clean_text(s, limit=None):
    s = re.sub(r"\s+", " ", s or "").strip()
    if limit and len(s) > limit:
        return s[:limit-1] + "…"
    return s

def parse_pubdate(entry):
    for key in ["published", "updated", "pubDate"]:
        if key in entry:
            try:
                dt = dtparser.parse(entry[key])
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(TZ)
            except Exception:
                pass
    return now_et()

def fetch_rss(name, url):
    try:
        d = feedparser.parse(url)
        items = []
        for e in d.entries[:50]:
            title = clean_text(getattr(e, "title", ""))
            link = getattr(e, "link", "")
            if not title or not link:
                continue
            dt = parse_pubdate(e)
            if not within_window(dt):
                continue
            summary = clean_text(getattr(e, "summary", "") or getattr(e, "description",""), 400)
            content_html = ""
            if hasattr(e, "content"):
                try:
                    content_html = e.content[0].value
                except Exception:
                    pass
            items.append({
                "title": title,
                "url": link,
                "published_at": dt.isoformat(),
                "source": name,
                "summary": summary,
                "content_html": content_html
            })
        return items
    except Exception:
        return []

def google_news_rss(query):
    import urllib.parse
    q = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    return fetch_rss("Google News", url)

def hn_items():
    base = "https://hacker-news.firebaseio.com/v0"
    def get_json(path):
        return requests.get(f"{base}/{path}.json", timeout=15).json()

    ids = []
    try:
        top = get_json("topstories") or []
        new = get_json("newstories") or []
        ids = list(dict.fromkeys((top[:100] + new[:200])))
    except Exception:
        return []

    items = []
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=RUN_WINDOW_HOURS)).timestamp()
    for sid in ids:
        try:
            it = get_json(f"item/{sid}")
            if not it or it.get("type") != "story":
                continue
            t = it.get("time", 0)
            if t < cutoff:
                continue
            url = it.get("url") or ""
            title = it.get("title") or ""
            if not url or not title:
                continue
            lowered = url.lower()
            if any(suf in lowered for suf in CFG.get("exclude_domains_suffix", [])):
                continue
            dt = datetime.fromtimestamp(t, timezone.utc).astimezone(TZ)
            items.append({
                "title": clean_text(title),
                "url": url,
                "published_at": dt.isoformat(),
                "source": "Hacker News",
                "summary": ""
            })
        except Exception:
            continue
    return items

CATEGORY_KEYWORDS = {
    "ai": ["ai","artificial intelligence","large language model","llm","gpt","openai","anthropic","deepmind","sora","transformer","diffusion","ml","machine learning","neural","chip","npu"],
    "software": ["software","developer","devops","platform","sdk","api","apps","app","release","github","cloud","saas","microservices","kubernetes","langchain"],
    "fintech": ["fintech","payments","payment","bank","banking","crypto","blockchain","defi","lending","card","visa","mastercard","stripe","paypal","square","nubank"]
}

def categorize(title, url):
    t = f"{title} {url}".lower()
    score = {"ai":0,"software":0,"fintech":0}
    for cat, kws in CATEGORY_KEYWORDS.items():
        for w in kws:
            if w in t:
                score[cat]+=1
    cat = max(score, key=score.get)
    if score[cat]==0:
        cat = "ai"
    return cat

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
    if item.get("summary"):
        return clean_text(item["summary"], 260)
    try:
        r = requests.get(item["url"], timeout=10, headers={"User-Agent":"Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html5lib")
        og = soup.find("meta", attrs={"property":"og:description"})
        if og and og.get("content"):
            return clean_text(og["content"], 260)
        desc = soup.find("meta", attrs={"name":"description"})
        if desc and desc.get("content"):
            return clean_text(desc["content"], 260)
    except Exception:
        pass
    return clean_text(item["title"], 200)

def build_section(date_str, by_cat):
    with open(os.path.join(ROOT, "templates/section_template.html"), "r") as f:
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
    for cat_key, placeholder in [("ai","AI"),("software","SW"),("fintech","FT")]:
        items = by_cat.get(cat_key, [])[:MAX_ITEMS]
        html_out = html_out.replace(f"{{{{{placeholder}_COUNT}}}}", str(len(items)))
        html_out = html_out.replace(f"{{{{{placeholder}_ITEMS}}}}", render_items(items) if items else "<p>No items today.</p>")
    return html_out

def main():
    all_items = []
    for s in CFG["sources"]["rss"]:
        all_items.extend(fetch_rss(s["name"], s["url"]))
    for cat, queries in CFG["sources"]["google_news_queries"].items():
        for q in queries:
            all_items.extend(google_news_rss(q))
    all_items.extend(hn_items())
    all_items = dedupe(all_items)
    for it in all_items:
        it["summary_text"] = summarize(it)
        it["category"] = categorize(it["title"], it["url"])
    def parsed_dt(it):
        try:
            return dtparser.parse(it["published_at"]).astimezone(TZ)
        except Exception:
            return now_et()
    all_items.sort(key=parsed_dt, reverse=True)
    by_cat = {"ai":[], "software":[], "fintech":[]}
    for it in all_items:
        by_cat[it["category"]].append(it)
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
