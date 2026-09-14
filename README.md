# TEK2day Pulse

Read TEK2day Pulse for the latest technology news.

**Live site:** [pulse.tek2dayholdings.com](https://pulse.tek2dayholdings.com)
· **JSON feed:** [pulse.tek2dayholdings.com/pulse.json](https://pulse.tek2dayholdings.com/pulse.json)

TEK2day Pulse is an automated technology news site published by
[TEK2day Holdings](https://tek2dayholdings.com). It pulls from 21 curated RSS
feeds, removes duplicates, keeps the stories that are about AI, software or
fintech, and publishes a static site and a JSON feed hourly during weekday
business hours.

## What it publishes

- A single chronological front page: a lead story, a five-story Brief, and a
  grid of the latest stories with search
- `pulse.json`, a public JSON feed of the same stories
- A permalink page for every story, with a social-sharing image
- Daily and timestamped JSON archives

## What gets in

The feeds are general technology publishers, so not everything they print
belongs here. Each story is checked against vocabularies for AI, software and
fintech; a story that matches none of them is dropped. On a typical day that
removes car launches, game releases, gadget reviews, conference promotions and
entertainment pieces. Everything dropped is listed as `[DROP]` in the build log
so the filter can be audited.

Matching is on whole words (with plural and verb endings), weighted title 3×,
summary 2×, URL 1×, and a story needs a score of at least 2. Everything from
PYMNTS, Cointelegraph, OpenAI, Anthropic, the TEK2day newsletter and the
configured YouTube channels is kept without scoring. Shopping and deals posts
are removed separately, and Hacker News and X/Twitter links are blocked.

Each edition carries at most 10 stories per source (`max_items_per_source` in
`generator/config.yaml`; the TEK2day newsletter is exempt), so no single feed
can dominate the page.

## Sources

Configured in `generator/config.yaml`:

- TechCrunch, The Verge, Ars Technica, Wired, VentureBeat
- Bloomberg Technology, WSJ Technology, The New York Times Technology,
  Financial Times Technology
- NVIDIA, Microsoft and Apple newsrooms; OpenAI News; Anthropic News
- PYMNTS and Cointelegraph
- The TEK2day newsletter
- YouTube channels: T2D Pulse, OpenAI, Anthropic, SS&C Blue Prism

Feeds that fail on a run are logged and recorded in `build-status.json`; the
healthy ones publish as normal. (Anthropic's news feed and two of the YouTube
feeds currently return errors and contribute nothing.)

## The page

Header with logo, date and search. Below it, a hero with the **lead story** —
the Brief's top pick when it has a real image, otherwise the first pick with a
thumbnail, otherwise the newest story — beside the **Brief**, five stories
ranked by impact. Then **Latest**: every story, newest first, with the build
time in the section head. Typing in the search box filters the grid and hides
the hero; clearing it brings the hero back.

## Update schedule

GitHub Actions builds and deploys the site:

- **Weekdays:** hourly runs (`23 8-17 * * 1-5`, timezone `America/New_York`)
  from 8:23 AM through 5:23 PM Eastern, with automatic daylight saving
  adjustment. Minute 23 avoids GitHub's congested on-the-hour slots. GitHub
  schedules are best-effort: runs can be delayed or dropped, so these are
  target times rather than guaranteed publication times. Hourly attempts
  provide another opportunity to refresh after a missed trigger.
- **Weekends:** no scheduled builds; Friday's edition stays live.
  `generator/check_schedule.py` enforces this. Set `PULSE_WEEKDAYS_ONLY` to
  `"0"` in the workflow to publish seven days a week.
- **On merge:** every push to `main` builds and deploys immediately.
- **Manual:** *Actions → Build TEK2day Pulse → Run workflow* builds any day.

To check freshness, compare `generated_at` in the live
[`build-status.json`](https://pulse.tek2dayholdings.com/build-status.json) with
the [workflow run history](https://github.com/jmaietta/T2DPulse/actions/workflows/tek2day_pulse.yml).
A green push or manual run verifies the build and deployment, but does not
verify that scheduled triggers are firing. If no scheduled run appears for
more than two hours during the publishing window, trigger a manual build and
investigate the scheduler.

Builds are incremental: story pages, images and archives persist between runs
in the Actions cache, so a run only fetches and encodes new stories (typically
well under a minute). A run that yields no stories fails before deploying, so
the previous edition stays up.

## JSON feed

`pulse.json` is public and needs no authentication. It is an array of stories,
newest first:

```json
[
  {
    "title": "Article headline",
    "url": "https://publisher.example/story",
    "published_at": "2026-09-12T20:15:00-04:00",
    "source": "The New York Times - Technology",
    "summary_text": "Plain-text excerpt",
    "image_url": "https://publisher.example/image.jpg",
    "category": "ai",
    "_permalink": "/p/3cf421aa7d/",
    "_abs_permalink": "https://pulse.tek2dayholdings.com/p/3cf421aa7d/",
    "_summary_240": "Excerpt trimmed for cards",
    "_thumbnail": "https://pulse.tek2dayholdings.com/p/3cf421aa7d/thumbnail.jpg"
  }
]
```

`category` records which vocabulary the story matched (`ai`, `software` or
`fintech`). It is kept for API consumers and is not shown on the site.

```python
import httpx

stories = httpx.get("https://pulse.tek2dayholdings.com/pulse.json").json()
latest = stories[:10]
```

Kilby AI reads this feed through its `search_t2d_pulse` tool.

## Project structure

```
generator/
  generate_pulse.py       Pipeline: fetch, dedupe, filter, render, archive
  check_schedule.py       Weekday gate for scheduled workflow runs
  preview_pulse.py        Render a saved pulse.json without network access
  config.yaml             Feeds, blocked domains, per-source cap
  requirements.txt        Python dependencies
  templates/
    section_template.html Front page
    item_template.html    Story permalink page
tests/
  test_pulse.py           Regression suite (unittest)
.github/workflows/
  tek2day_pulse.yml       Build + deploy (schedule, push to main, manual)
  checks.yml              Tests on push and pull requests
docs/                     GitHub Pages site
  pulse.css, pulse.js     Front-page styles and behaviour
  sw.js, offline.html     Service worker and offline fallback
  index.html, pulse.json  Generated each build (not tracked)
  build-status.json       Generated: feed health, counts, image reuse
  p/                      Generated story pages, og-image.jpg, thumbnail.jpg
  archive/                Generated daily and timestamped JSON snapshots
```

## Tech stack

- Python — pipeline, deduplication, relevance filter, image processing
- feedparser — RSS/Atom parsing (fetched through a retrying HTTP session with timeouts)
- BeautifulSoup4 — HTML parsing
- Pillow — image resizing; social cards and thumbnails are progressive JPEG
- GitHub Actions — scheduling, incremental cache, deployment
- GitHub Pages — static hosting

## Local development

Python 3.11+ with `generator/requirements.txt` installed in a virtual
environment. A full build fetches the feeds and generates images; it runs on
Windows, macOS and Linux:

```sh
python generator/generate_pulse.py
python -m http.server 8765 --bind 127.0.0.1 --directory docs
```

A second run is near-instant because images already under `docs/p/` are
reused; delete `docs/p/` to force a full regeneration. `docs/build-status.json`
shows which feeds succeeded and how many images were reused versus generated.

To work on the page without fetching anything, save the public feed to
`.tmp/live-pulse.json` and render it:

```sh
python -m generator.preview_pulse --feed .tmp/live-pulse.json
python -m http.server 8765 --bind 127.0.0.1 --directory docs
```

Tests:

```sh
python -m unittest discover -s tests -v
node --check docs/pulse.js
node --check docs/sw.js
```

The suite covers the relevance filter (whole-word matching, off-topic drops,
publication routing), feed errors and timeouts, safe rendering of feed content,
the weekday gate, per-source capping, image reuse, sitemap dates, archive
merging, preservation of the last edition when feeds fail, and a complete build
with mocked network input. GitHub Actions runs it before every deploy and on
every pull request.

## License

Proprietary — © TEK2day Holdings. All rights reserved.
