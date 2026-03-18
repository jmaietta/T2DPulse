# T2D Pulse

  Automated tech, AI, and fintech news aggregation platform published by [Tek2Day Holdings](https://tek2dayholdings.com). Pulls from 20+ curated RSS sources,
  deduplicates, categorizes, and publishes a clean JSON feed and static site — updated five times per day on weekdays.

  **Live site:** [pulse.tek2dayholdings.com](https://pulse.tek2dayholdings.com)
  **JSON feed:** [pulse.tek2dayholdings.com/pulse.json](https://pulse.tek2dayholdings.com/pulse.json)

  ---

  ## What It Does

  T2D Pulse ingests RSS feeds from high-quality tech and finance publishers, applies multi-layer deduplication, categorizes articles into three verticals, and publishes:

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

  - **Frequency:** Every 5 hours (`0 */5 * * *`)
  - **Window:** Monday–Friday, 7:30 AM – 6:00 PM ET
  - **Weekends:** Reuses Friday's payload (no redundant processing)
  - **Manual trigger:** Available via `workflow_dispatch`

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
    config.yaml           # Sources, exclusions, settings
    requirements.txt      # Python dependencies
    templates/
      section_template.html   # Main index page
      item_template.html      # Article permalink pages
  .github/
    workflows/
      tek2day_pulse.yml       # Main automation workflow
  docs/                   # GitHub Pages output (auto-generated)
    index.html
    pulse.json
    p/                    # Article permalink pages
    archive/              # Timestamped JSON archives
    og/                   # Open Graph social cards

  ---
  Tech Stack

  - Python — pipeline, deduplication, categorization, image processing
  - feedparser — RSS/Atom ingestion
  - BeautifulSoup4 — HTML parsing
  - Pillow — image resizing and OG card generation
  - GitHub Actions — scheduling and deployment
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

  Proprietary — © Tek2Day Holdings. All rights reserved.
