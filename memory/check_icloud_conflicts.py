#!/usr/bin/env python3
# ============================================================
# check_icloud_conflicts.py  —  Ombre Brain iCloud Conflict Detector
# iCloud 冲突文件检测器
#
# Scans the configured bucket directory for iCloud sync conflict
# artefacts and duplicate bucket IDs, then prints a report.
# 扫描配置的桶目录，发现 iCloud 同步冲突文件及重复桶 ID，输出报告。
#
# Usage:
#   python check_icloud_conflicts.py
#   python check_icloud_conflicts.py --buckets-dir /path/to/dir
#   python check_icloud_conflicts.py --quiet      # exit-code only (0=clean)
# ============================================================

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

# ──────────────────────────────────────────────────────────────
# iCloud conflict file patterns
# Pattern 1 (macOS classic): "filename 2.md", "filename 3.md"
# Pattern 2 (iCloud Drive):  "filename (Device's conflicted copy YYYY-MM-DD).md"
# ──────────────────────────────────────────────────────────────
_CONFLICT_SUFFIX   = re.compile(r"^(.+?)\s+\d+\.md$")
_CONFLICT_ICLOUD   = re.compile(r"^(.+?)\s+\(.+conflicted copy .+\)\.md$", re.IGNORECASE)
# Bucket ID pattern: 12 hex chars at end of stem before extension
_BUCKET_ID_PATTERN = re.compile(r"_([0-9a-f]{12})$")


def resolve_buckets_dir() -> Path:
    """Resolve bucket directory: env var → config.yaml → ./buckets fallback."""
    env_dir = os.environ.get("OMBRE_BUCKETS_DIR", "").strip()
    if env_dir:
        return Path(env_dir)

    config_path = Path(__file__).parent / "config.yaml"
    if config_path.exists():
        try:
            import yaml  # type: ignore
            with open(config_path, encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            if cfg.get("buckets_dir"):
                return Path(cfg["buckets_dir"])
        except Exception:
            pass

    return Path(__file__).parent / "buckets"


def scan(buckets_dir: Path) -> tuple[list[Path], dict[str, list[Path]]]:
    """
    Returns:
        conflict_files  — list of files that look like iCloud conflict artefacts
        dup_ids         — dict of bucket_id -> [list of files sharing that id]
                          (only entries with 2+ files)
    """
    if not buckets_dir.exists():
        return [], {}

    conflict_files: list[Path] = []
    id_to_files: dict[str, list[Path]] = defaultdict(list)

    for md_file in buckets_dir.rglob("*.md"):
        name = md_file.name

        # --- Conflict file detection ---
        if _CONFLICT_SUFFIX.match(name) or _CONFLICT_ICLOUD.match(name):
            conflict_files.append(md_file)
            continue  # don't register conflicts in the ID map

        # --- Duplicate ID detection ---
        stem = md_file.stem
        m = _BUCKET_ID_PATTERN.search(stem)
        if m:
            id_to_files[m.group(1)].append(md_file)

    dup_ids = {bid: paths for bid, paths in id_to_files.items() if len(paths) > 1}
    return conflict_files, dup_ids


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Detect iCloud conflict files and duplicate bucket IDs."
    )
    parser.add_argument(
        "--buckets-dir",
        metavar="PATH",
        help="Override bucket directory (default: from config.yaml / OMBRE_BUCKETS_DIR)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress output; exit 0 = clean, 1 = problems found",
    )
    args = parser.parse_args()

    buckets_dir = Path(args.buckets_dir) if args.buckets_dir else resolve_buckets_dir()

    if not args.quiet:
        print(f"Scanning: {buckets_dir}")
        if not buckets_dir.exists():
            print("  ✗ Directory does not exist.")
            return 1
        print()

    conflict_files, dup_ids = scan(buckets_dir)
    problems = bool(conflict_files or dup_ids)

    if args.quiet:
        return 1 if problems else 0

    # ── Report ─────────────────────────────────────────────────
    if not problems:
        print("✓ No iCloud conflicts or duplicate IDs found.")
        return 0

    if conflict_files:
        print(f"⚠ iCloud conflict files ({len(conflict_files)} found):")
        for f in sorted(conflict_files):
            rel = f.relative_to(buckets_dir) if f.is_relative_to(buckets_dir) else f
            print(f"  {rel}")
        print()

    if dup_ids:
        print(f"⚠ Duplicate bucket IDs ({len(dup_ids)} ID(s) shared by multiple files):")
        for bid, paths in sorted(dup_ids.items()):
            print(f"  ID: {bid}")
            for p in sorted(paths):
                rel = p.relative_to(buckets_dir) if p.is_relative_to(buckets_dir) else p
                print(f"    {rel}")
        print()

    print(
        "NOTE: This script is report-only. No files are modified or deleted.\n"
        "注意：本脚本仅报告，不删除或修改任何文件。"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
