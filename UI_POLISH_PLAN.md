# T2D Pulse — UI/UX Polish Plan (Editorial Premium)

Goal: make the front page feel like a premium editorial briefing (FT/NYT-clean, modern)
without changing the publishing pipeline, feed contract, or test selectors.

Truth repo: https://github.com/jmaietta/T2DPulse (local `main` in sync).
Preview workflow: `python -m generator.preview_pulse --feed .tmp/live-pulse.json`,
serve `docs/` on `:8765`. `docs/index.html` is gitignored; only `docs/pulse.css`
(plus templates/JS in later batches) is tracked.

Markup/JS contracts to preserve (asserted in `tests/test_pulse.py`):
`article[data-card]`, `data-title/summary/source/permalink`, `h3` grid / `h2` lead,
`.hero article.lead`, `.items article[data-lead]`, `.eyebrow`, `.meta time`,
`.badge`, `.article-thumb`, `#t2d-q`, `#result-count`, `#last-updated`, `.hero`,
`body.is-filtered`, `#search-empty`, `.pb*`, `.share`.

---

## Batch 1 — CSS-only polish (items 1–6, zero pipeline risk)

File: `docs/pulse.css` only. No template, JS, or Python changes.

1. Broken-image resilience
   - Keep `aspect-ratio: 16/9` boxes with warm paper/skeleton background so a 404
     still occupies a calm rectangle instead of collapsing.
   - Tone down broken-alt text (muted color, smaller size, padding) and hide
     empty `img[src="" / no-src]`.
   - Wrap thumbs in `overflow:hidden`; add a subtle inner border via box-shadow.
   - Accept: delete/404 a thumbnail in preview → card keeps height, no alt wall.

2. Retire Orbitron from the wordmark
   - `header.site h1`: Inter 800, tight tracking (`-0.02em`), ink color.
   - Keep Orbitron import untouched (no template change); just stop using it.
   - Keep Orbitron only on `.pb-pill` if a techy accent is wanted — otherwise
     Inter 800 everywhere.
   - Accept: header reads as editorial, not sci-fi; no layout shift.

3. Quiet the `NEW` badges
   - CSS-only step: shrink badge (10–11px, uppercase, letter-spaced), soften to
     `ai-light` background + `ai-deep` text instead of solid indigo pill.
   - Keep `.badge.muted` for `Older`; no Python logic change in this batch.
   - Full fix (Batch 2): narrow `render_item_badges()` window 24h → 6h.
   - Accept: grid scans without 90 loud pills competing with headlines.

4. Card de-clutter: share row reveal-on-interest
   - Default `.share` is visually quiet (smaller, muted, hairline top border).
   - Reveal full opacity on `article:hover / :focus-within`; always visible on
     touch (`@media (hover:none)`) and when keyboard focused.
   - Do NOT `display:none` (keeps layout stable, no CLS; share buttons stay
     tabbable for a11y).
   - Accept: cards read headline → meta → summary; actions appear on intent.

5. Type scale + rhythm
   - Grid `h3`: ~20px serif, `line-height 1.3`, `text-wrap:pretty`.
   - Lead `h2`: 34–40px desktop, 28px ≤960px, 24px ≤520px, serif 800.
   - Meta 12px uppercase-ish source; summary 15px/1.6; consistent
     `margin: 0 0 .625rem` stack.
   - Accept: clear headline hierarchy lead > card; no orphan stacking.

6. Visited state
   - `h3 a:visited, .lead h2 a:visited { color: #526078 }`, hover restores ink.
   - Brief `.pb-takeaway-body strong a:visited` equivalent if links appear.
   - Accept: scanned stories visibly dim; unvisited stay ink.

Batch-1 verify: `unittest discover -s tests` (25 ok), `node --check pulse.js/sw.js`,
brace balance, `preview_pulse` render, desktop+mobile screenshots, no horizontal
overflow at 320/390/768/1024/1440.

## Batch 2 — Template + JS affordances (small, test-safe) — IMPLEMENTED

- `generator/templates/section_template.html` + `generator/generate_pulse.py`
  (`render_card`, `render_pulse_brief_html`, `render_item_badges`, `build_section`)
  + `docs/pulse.js` + `docs/pulse.css`.
- 7. Lead meta row: source · date · favicon · read time · relative age. DONE —
     favicon via Google S2 (`domain_of`), `_read_time` (200wpm), `_relative_time`
     (`5m/3h/1d ago`); CSS wraps to two lines on narrow cards.
- 8. Brief header: `The Brief — 5 need-to-knows · Updated 1:52 AM` sub-line in
     every rail; chevron corrected to `▾` (down when open, rotates on close).
- 9. Search UX: clear `×` button, `/` focuses `#t2d-q`, Esc clears,
     live `result-count` (`Showing N matches · M of T — press Esc to clear`),
     `aria-controls="news-items"`; JS stays in `pulse.js` filter block.
- 10. Paginate Latest: 24 cards + `Load more` pager (`#pager`, `hidden`-based,
     tests' `.items [data-card]` count untouched); search bypasses paging.
- 11. Header slim-down: tagline moved to `.edition-line` under the header;
     date pill only in header; sticky header shrinks via existing `.shadow`
     hook (40px logo, tighter padding). Brand-mark gets width/height attrs.

Batch-2 verify: 25 tests ok, `node --check` both JS, CSS braces balanced,
`preview_pulse` 129 articles, desktop+mobile screenshots, no overflow.

## Batch 3 — Trust, brand, platform — IMPLEMENTED

- 12. Permalink unified: `item_template.html` now uses front-page tokens
      (paper bg, indigo `--ai`, serif `h1`, `--muted #5b6b7f`), 52px rounded
      logo + wordmark, pill CTA (`ai → ai-deep` hover, 44px), same footer
      copy. OG/Twitter/canonical placeholders untouched.
- 13. Fallback art + favicons: no-thumbnail cards render `.thumb-fallback`
      (serif source initial on indigo wash); broken remote thumbs get
      `.is-broken` via `pulse.js` listeners (no inline handlers — the
      escaping test rejects `on*` attributes); favicons remove themselves
      on error via JS. Lead keeps `fetchpriority="high"` + eager.
- 14. PWA: manifest icon paths fixed to real files
      (`t2d-pulse-192/256/512/180`; 384 kept as `icon-384x384.png` which
      exists); `sw.js CACHE v3 → v4` so clients fetch the new CSS/JS.
- 15. A11y: `--muted #64748b → #5b6b7f`; `.badge.muted` is now an outlined
      chip (no more white-on-muted washout); `.share-btn`/`.pb-read` 44px
      targets; `.pb-cat` inks deepened to `ai-deep/sw/ft`; focus-visible
      rings already present.
- 16. Perf: lead `fetchpriority="high"`, grid stays `lazy` + `async`;
      Batch-2 paging (24 + Load more) caps initial image weight.

Batch-3 verify: 25 tests ok (after moving inline handlers to JS listeners),
`node --check` both JS, CSS balanced, manifest icon files exist, 129-article
preview, desktop+mobile screenshots.

---

## Ship order

1. Batch 1 now (this change): `docs/pulse.css` only → preview → tests → commit.
2. Batch 2 next: template + JS, one PR with test updates.
3. Batch 3 last: permalink + manifest + SW bump, one PR, force-verify share cards.
