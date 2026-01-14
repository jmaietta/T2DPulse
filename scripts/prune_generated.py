#!/usr/bin/env python3
"""
prune_generated.py
===================

This utility script prunes generated site artifacts for the TEK2day Pulse
repository. It is designed to help control disk usage on GitHub Actions
runners by removing old or unused files in the `docs` directory prior to
and after running the site generator.  The script supports two stages,
`pre` and `post`, which are used in the workflow to clean the working
directory before the build and again afterward.

Features
--------

* **Daily archive retention**: keeps only a configurable number of days of
  `docs/archive/json/YYYY-MM-DD.json` files.
* **Timestamped snapshot retention**: optionally cleans
  `docs/archive/timestamped/*.json` files that are older than a configured
  number of days.
* **Permalink pruning**: removes directories under `docs/p/` that are not
  referenced by the latest `docs/pulse.json` file or recent daily
  archives. This prevents the `docs/p` folder from growing without bound.
* **Hard cap on permalink directories**: enforces a maximum number of
  permalink directories to retain, dropping the oldest ones if necessary.

Configuration is provided through environment variables or command-line
options. Environment variables take precedence over default values but
will be overridden by explicit command-line arguments if provided.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator, Optional, Set
from zoneinfo import ZoneInfo

# Regular expressions for parsing permalink identifiers and daily archive
PERMALINK_RE = re.compile(r"/p/([^/]+)/?")
DATEFILE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})\.json$")

# Time zone for computing cutoffs based on Eastern Time
ET = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class Settings:
    """Aggregated configuration settings for pruning."""

    docs_dir: Path
    keep_archive_days: int
    keep_permalink_days: int
    max_permalink_dirs: int
    keep_timestamped_days: int
    dry_run: bool


def _env_int(name: str, default: int) -> int:
    """Fetch an integer from the environment or return the default."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def parse_settings(args: argparse.Namespace) -> Settings:
    """Translate command-line arguments into a Settings instance."""
    docs_dir = Path(args.docs_dir)
    return Settings(
        docs_dir=docs_dir,
        keep_archive_days=args.keep_archive_days,
        keep_permalink_days=args.keep_permalink_days,
        max_permalink_dirs=args.max_permalink_dirs,
        keep_timestamped_days=args.keep_timestamped_days,
        dry_run=args.dry_run,
    )


def parse_archive_date(filename: str) -> Optional[date]:
    """Extract a date object from an archive filename such as YYYY-MM-DD.json."""
    m = DATEFILE_RE.match(filename)
    if not m:
        return None
    y, mo, d = map(int, m.groups())
    try:
        return date(y, mo, d)
    except ValueError:
        return None


def iter_daily_archives(archive_dir: Path) -> list[Path]:
    """Return sorted list of daily archive files within the given directory."""
    if not archive_dir.exists():
        return []
    files: list[Path] = []
    for p in archive_dir.iterdir():
        if p.is_file() and parse_archive_date(p.name):
            files.append(p)
    return sorted(files, key=lambda p: p.name)


def prune_daily_archives(s: Settings) -> list[Path]:
    """Prune `docs/archive/json` to retain only the last N days of files.

    Returns a list of the archive files that remain (are kept).
    """
    archive_dir = s.docs_dir / "archive" / "json"
    files = iter_daily_archives(archive_dir)
    if not files:
        return []

    today_et = datetime.now(ET).date()
    cutoff = today_et - timedelta(days=max(s.keep_archive_days, 0))
    kept: list[Path] = []
    removed: list[Path] = []
    for p in files:
        d = parse_archive_date(p.name)
        if d is None:
            continue
        if d >= cutoff:
            kept.append(p)
        else:
            removed.append(p)

    if removed:
        print(f"Pruning {len(removed)} old daily archive files (< {cutoff.isoformat()})")
        for p in removed:
            if s.dry_run:
                print(f"  [dry-run] rm {p}")
            else:
                p.unlink(missing_ok=True)
    print(f"Daily archives kept: {len(kept)}")
    return kept


def prune_timestamped(s: Settings) -> None:
    """Delete timestamped snapshot files older than a configured number of days."""
    ts_dir = s.docs_dir / "archive" / "timestamped"
    if not ts_dir.exists():
        return
    if s.keep_timestamped_days < 0:
        return

    cutoff_dt = datetime.now() - timedelta(days=s.keep_timestamped_days)
    removed = 0
    for p in ts_dir.glob("*.json"):
        try:
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
        except FileNotFoundError:
            continue
        if mtime < cutoff_dt:
            removed += 1
            if s.dry_run:
                print(f"  [dry-run] rm {p}")
            else:
                p.unlink(missing_ok=True)
    print(
        f"Timestamped snapshots removed: {removed} (kept last {s.keep_timestamped_days} days)"
    )


def extract_permalink_id(value: str) -> Optional[str]:
    """Extract a permalink identifier from a URL or path."""
    if not value:
        return None
    m = PERMALINK_RE.search(value)
    if not m:
        return None
    return m.group(1)


def collect_permalinks_from_pulse_json(pulse_path: Path) -> Set[str]:
    """Collect permalink identifiers from the latest pulse JSON file."""
    if not pulse_path.exists():
        return set()
    try:
        obj = json.loads(pulse_path.read_text(encoding="utf-8"))
    except Exception:
        return set()

    keep: Set[str] = set()
    # The pulse.json may be a list of items or a dict keyed by category.
    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                pid = extract_permalink_id(str(item.get("_permalink", "")))
                if pid:
                    keep.add(pid)
    elif isinstance(obj, dict):
        for val in obj.values():
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        pid = extract_permalink_id(str(item.get("_permalink", "")))
                        if pid:
                            keep.add(pid)
    return keep


def iter_items_from_daily_archive(doc: object) -> Iterator[dict]:
    """Yield individual article objects from a daily archive document."""
    if not isinstance(doc, dict):
        return
    by_cat = doc.get("by_cat")
    if not isinstance(by_cat, dict):
        return
    for items in by_cat.values():
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    yield it


def collect_permalinks_from_archives(
    archives: Iterable[Path], keep_days: int
) -> Set[str]:
    """Collect permalink identifiers referenced in the last `keep_days` of archives."""
    archives = sorted(list(archives), key=lambda p: p.name)
    if keep_days <= 0 or not archives:
        return set()

    today_et = datetime.now(ET).date()
    cutoff = today_et - timedelta(days=keep_days)
    selected = []
    for p in archives:
        d = parse_archive_date(p.name)
        if d and d >= cutoff:
            selected.append(p)

    keep: Set[str] = set()
    for p in selected:
        try:
            doc = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        for item in iter_items_from_daily_archive(doc):
            pid = extract_permalink_id(str(item.get("_permalink", "")))
            if pid:
                keep.add(pid)
    return keep


def prune_permalinks(s: Settings, kept_archives: list[Path]) -> None:
    """Prune the `docs/p` directory based on references in pulse.json and recent archives."""
    p_dir = s.docs_dir / "p"
    p_dir.mkdir(parents=True, exist_ok=True)

    # Determine which permalinks to keep.
    keep: Set[str] = set()
    keep |= collect_permalinks_from_pulse_json(s.docs_dir / "pulse.json")
    keep |= collect_permalinks_from_archives(kept_archives, s.keep_permalink_days)

    # Safety: if we found nothing to keep, abort pruning to avoid deleting everything.
    if not keep:
        print(
            "No permalinks discovered to keep (pulse.json and archives yielded none). "
            "Skipping docs/p prune."
        )
        return

    # Enforce a hard maximum number of permalink directories, if configured
    if s.max_permalink_dirs > 0 and len(keep) > s.max_permalink_dirs:
        # Sort deterministically and keep only the most recent IDs
        keep = set(sorted(keep)[-s.max_permalink_dirs :])

    removed = 0
    scanned = 0
    for child in p_dir.iterdir():
        if not child.is_dir():
            continue
        scanned += 1
        if child.name not in keep:
            removed += 1
            if s.dry_run:
                print(f"  [dry-run] rmtree {child}")
            else:
                shutil.rmtree(child, ignore_errors=True)

    print(f"Permalink dirs scanned: {scanned}")
    print(f"Permalink dirs kept (target): {len(keep)}")
    print(f"Permalink dirs removed: {removed}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prune generated TEK2day Pulse artifacts"
    )
    parser.add_argument(
        "--docs-dir",
        default="docs",
        help="Path to docs directory (default: docs)",
    )
    parser.add_argument(
        "--keep-archive-days",
        type=int,
        default=_env_int("KEEP_ARCHIVE_DAYS", 30),
        help="Keep this many days of docs/archive/json/YYYY-MM-DD.json files "
             "(default: env KEEP_ARCHIVE_DAYS or 30)",
    )
    parser.add_argument(
        "--keep-permalink-days",
        type=int,
        default=_env_int("KEEP_PERMALINK_DAYS", 7),
        help="Keep permalinks referenced by the last N days of daily archives "
             "(default: env KEEP_PERMALINK_DAYS or 7)",
    )
    parser.add_argument(
        "--max-permalink-dirs",
        type=int,
        default=_env_int("MAX_PERMALINK_DIRS", 1200),
        help="Hard cap on number of permalink dirs to keep (0 disables). "
             "Default env MAX_PERMALINK_DIRS or 1200.",
    )
    parser.add_argument(
        "--keep-timestamped-days",
        type=int,
        default=_env_int("KEEP_TIMESTAMPED_DAYS", 2),
        help="Keep this many days of docs/archive/timestamped/*.json files "
             "(default: env KEEP_TIMESTAMPED_DAYS or 2)",
    )
    parser.add_argument(
        "--stage",
        choices=["pre", "post"],
        default="post",
        help="Stage label for logging (pre or post). Does not affect behavior.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print deletions without actually deleting anything",
    )
    args = parser.parse_args()

    s = parse_settings(args)
    print(f"== prune_generated.py ({args.stage}) ==")
    print(
        "Settings: "
        f"keep_archive_days={s.keep_archive_days}, "
        f"keep_permalink_days={s.keep_permalink_days}, "
        f"max_permalink_dirs={s.max_permalink_dirs}, "
        f"keep_timestamped_days={s.keep_timestamped_days}, "
        f"dry_run={s.dry_run}"
    )

    kept_archives = prune_daily_archives(s)
    prune_timestamped(s)
    prune_permalinks(s, kept_archives)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
