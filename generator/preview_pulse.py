"""Render an existing pulse.json for UI work without fetching publishers or images."""
import argparse
import json
from pathlib import Path

from generator.generate_pulse import build_section, display_date, now_et


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--feed', type=Path, required=True)
    parser.add_argument('--output', type=Path, default=Path('docs/index.html'))
    args = parser.parse_args()
    items = json.loads(args.feed.read_text(encoding='utf-8'))
    if not isinstance(items, list) or not items:
        parser.error('The feed must contain a nonempty JSON array of articles.')
    by_category = {key: [item for item in items if item.get('category') == key]
                   for key in ('ai', 'software', 'fintech')}
    now = now_et()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(build_section(display_date(now), by_category, generated_at=now), encoding='utf-8')
    print(f'Preview rendered: {args.output} ({len(items)} articles). Publication dates come from the saved feed.')


if __name__ == '__main__':
    main()
