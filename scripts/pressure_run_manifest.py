#!/usr/bin/env python3
"""Write a machine-readable manifest for a live pressure-run report."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stock_universe.ops import build_pressure_run_manifest, list_pressure_cohort_plans


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", default=None, help="Pressure-run JSON report path.")
    parser.add_argument(
        "--cohort", default=None, help="Stable cohort name, such as baseline-50."
    )
    parser.add_argument(
        "--command", default=None, help="Command that produced the report."
    )
    parser.add_argument(
        "--db", default=None, help="SQLite database path, if not present in the report."
    )
    parser.add_argument(
        "--out", default=None, help="Optional manifest output path. Defaults to stdout."
    )
    parser.add_argument(
        "--list-cohorts", action="store_true", help="List known pressure cohort plans."
    )
    args = parser.parse_args(argv)

    if args.list_cohorts:
        return _emit({"ok": True, "cohorts": list_pressure_cohort_plans()}, args.out)
    missing = [
        name for name in ("report", "cohort", "command") if getattr(args, name) is None
    ]
    if missing:
        parser.error(
            "--report, --cohort, and --command are required unless --list-cohorts is used"
        )

    manifest = build_pressure_run_manifest(
        report_path=args.report,
        cohort=args.cohort,
        command=args.command,
        db_path=args.db,
        repo_root=REPO_ROOT,
    )
    return _emit(manifest, args.out)


def _emit(manifest: dict, out_path: str | None) -> int:
    payload = json.dumps(manifest, indent=2, sort_keys=True)
    if out_path:
        out = Path(out_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(payload + "\n", encoding="utf-8")
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
