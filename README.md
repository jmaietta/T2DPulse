# TEK2day Pulse

  Read TEK2day Pulse for the latest Technology news.

  **Live site:** [pulse.tek2dayholdings.com](https://pulse.tek2dayholdings.com)

  ![TEK2day Pulse](docs/screenshots/readme-home.png)

  Automated technology news aggregation platform published by [TEK2day Holdings](https://tek2dayholdings.com). Pulls from 20+ curated RSS sources,
  deduplicates, categorizes, and publishes a clean JSON feed and static site — updated five times per weekday.

  **JSON feed:** [pulse.tek2dayholdings.com/pulse.json](https://pulse.tek2dayholdings.com/pulse.json)

  ---

  ## What It Does

  TEK2day Pulse ingests RSS feeds from high-quality tech and finance publishers, applies multi-layer deduplication, categorizes articles into three verticals, and publishes:

  - A responsive static web page updated throughout the day
  - A structured JSON API (`pulse.json`) consumed by Kilby AI and other tools
  - Timestamped archives for historical reference
  - Individual article permalink pages with Open Graph social cards

  ---

  ## Categories

  | Category | Color | Focus |
  |----------|-------|-------|
  | **AI** | Indigo | Models, research, infrastructure, agents |
  | **Software** | Orange | Dev tools, platforms, enterprise software |
  | **FinTech** | Emerald | Payments, banking, crypto, capital markets |

  ---

  ## Sources

  20 RSS feeds including:

  - VentureBeat, TechCrunch, The Verge, Ars Technica
  - Bloomberg Technology, WSJ Technology
  - NVIDIA, Microsoft, Apple newsrooms
  - Anthropic, OpenAI announcements
  - PYMNTS, Finextra (fintech)
  - Curated YouTube channels

  Hacker News, X/Twitter, and shopping/deals content are deliberately excluded to avoid aggregator noise.

  ---

  ## Update Schedule

  Runs via GitHub Actions on a cron schedule:

  - **Frequency:** Five weekday runs (`23 13,15,17,19,21 * * 1-5`) — roughly 9:23 AM, 11:23 AM, 1:23 PM, 3:23 PM and 5:23 PM Eastern in summer, one hour earlier in winter. Minute 23 avoids GitHub's congested on-the-hour cron slots.
  - **Weekends:** No scheduled builds; Friday's edition stays live. `generator/check_schedule.py` enforces this and writes a step output the workflow gates on. Set `PULSE_WEEKDAYS_ONLY` to `"0"` in the workflow to publish seven days a week.
  - **Manual trigger:** `workflow_dispatch` always builds, any day.
  - **Incremental builds:** Permalink pages, social images and JSON archives persist between runs via the Actions cache, so only new articles are fetched and encoded. A typical run reuses everything from the previous run.
  - **Failures:** An empty result fails the build before upload, preserving the deployed edition. Individual source failures are logged and recorded in `build-status.json` while healthy sources continue.

  ---

  ## JSON Feed Structure

  ```json
  [
    {
      "title": "Article headline",
      "url": "https://...",
      "published_at": "2026-03-18T10:30:00-04:00",
      "source": "VentureBeat",
      "summary_text": "Plain text excerpt",
      "category": "ai",
      "image_url": "https://...",
      "_summary_240": "Truncated summary (max 240 chars)"
    }
  ]
  ```

  Category values: "ai" · "software" · "fintech"

  ---
  How It Works

  RSS Feeds (20 sources)
          ↓
    Parallel fetch (8 workers)
          ↓
    Deduplication (URL + title similarity)
          ↓
    Categorization (keyword scoring, 3× title weight)
          ↓
    Image processing + OG card generation
          ↓
    Render HTML + pulse.json
          ↓
    Deploy to GitHub Pages → pulse.tek2dayholdings.com

  ---
  Project Structure

  generator/
    generate_pulse.py     # Main pipeline script
    check_schedule.py     # Weekday publishing gate for the workflow
    preview_pulse.py      # Render a saved pulse.json without network access
    config.yaml           # Sources, exclusions, per-source cap, settings
    requirements.txt      # Python dependencies
    templates/
      section_template.html   # Main index page
      item_template.html      # Article permalink pages
  tests/
    test_pulse.py         # Regression suite (unittest)
  .github/
    workflows/
      tek2day_pulse.yml       # Scheduled build + Pages deploy
      checks.yml              # Tests on push / pull request
  docs/                   # GitHub Pages site
    pulse.css, pulse.js   # Homepage styles and behaviour (tracked)
    sw.js, offline.html   # Service worker and offline fallback (tracked)
    index.html            # Generated (not tracked)
    pulse.json            # Generated (not tracked)
    build-status.json     # Generated: per-source health, counts, image reuse
    p/                    # Generated article pages + og-image.jpg / thumbnail.jpg
    archive/              # Generated daily and timestamped JSON snapshots
    og/                   # Site social card

  ---
  Tech Stack

  - Python — pipeline, deduplication, categorization, image processing
  - feedparser — RSS/Atom ingestion (fetched through a retrying HTTP session with timeouts)
  - BeautifulSoup4 — HTML parsing
  - Pillow — image resizing; social cards and thumbnails are progressive JPEG
  - GitHub Actions — scheduling, incremental cache, deployment
  - GitHub Pages — static hosting

  ---
  Using the JSON Feed

  The pulse.json endpoint is public, requires no authentication, and is suitable for direct programmatic access:

  import httpx

  resp = httpx.get("https://pulse.tek2dayholdings.com/pulse.json")
  articles = resp.json()

  # Filter by category
  ai_news = [a for a in articles if a["category"] == "ai"]

  Kilby AI uses this feed via the search_t2d_pulse tool to answer tech, AI, and fintech news queries with curated, source-verified results.

  ---
  License

  Proprietary — © TEK2day Holdings. All rights reserved.

## Website maintenance and local previews

The homepage is `generator/templates/section_template.html` plus `docs/pulse.css`
and `docs/pulse.js`. Edit these sources, then regenerate the homepage.
Generated `docs/index.html` and `docs/pulse.json` are intentionally not tracked.

Layout, top to bottom: header (logo, wordmark, date, search); section tabs
(All / AI / Software / FinTech with live counts, plus the build time); a hero
with the **lead story** beside the Brief; then the chronological card grid with
a category eyebrow on each card. The lead is the Brief's top pick when it has a
real image (`pick_lead_story` in the generator), otherwise the first pick with
any thumbnail, otherwise the newest story. Tabs and search share one client-side
filter: while nothing is filtered the hero shows and the lead's grid copy
(`data-lead`) stays hidden; a tab or a query hides the hero and filters the
grid, lead included.

The service worker checks the network first, with a five-second timeout and a
bounded cache of previously loaded content. Cached headlines and summaries are
available offline (`docs/offline.html` is the fallback); original publisher
articles and external images may require a connection. Only Pulse's own obsolete
caches are removed during upgrades.

Use Python 3.11 and install `generator/requirements.txt` in a virtual environment.
For a full build (fetches publishers and generates images; works on Windows too):

```sh
python generator/generate_pulse.py
python -m http.server 8765 --bind 127.0.0.1 --directory docs
```

A second run is near-instant: images already under `docs/p/` are reused and
article pages are only fetched when needed. Delete `docs/p/` to force a full
regeneration. `docs/build-status.json` shows which feeds succeeded and how many
images were reused versus generated.

Editorial knobs live in `generator/config.yaml`: the source list,
`max_items_per_source` (default 10, newest kept; the TEK2day newsletter is
exempt) and `max_items_per_category`.

For UI work, save the public JSON feed to `.tmp/live-pulse.json`, then render it
without fetching publishers or regenerating images:

```sh
python -m generator.preview_pulse --feed .tmp/live-pulse.json
python -m http.server 8765 --bind 127.0.0.1 --directory docs
```

This preview displays the render time as its update time; article publication
dates still come from the saved feed. Open `http://127.0.0.1:8765/`.

## Validation

```sh
python -m unittest discover -s tests -v
node --check docs/pulse.js
node --check docs/sw.js
```

The regression suite covers feed errors and timeouts, safe HTML rendering,
future publication dates, the weekday gate, per-source capping, page-fetch
memoization, image reuse, sitemap dates, archive merging, preservation of an
existing edition when feeds fail, and a complete build with mocked network
input. GitHub Actions runs these checks before publishing and on pull requests.
