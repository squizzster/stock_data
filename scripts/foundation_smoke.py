#!/usr/bin/env python3
"""Foundation smoke harness for the new stock universe engine slice.

This script is intentionally offline. It checks Python compilation, runs the
fixture parity suite, and emits a compact machine-readable summary.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run_step(name: str, command: list[str]) -> dict:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return {
        "name": name,
        "command": command,
        "returncode": completed.returncode,
        "ok": completed.returncode == 0,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def compile_command() -> list[str]:
    files = sorted(
        str(path.relative_to(ROOT)) for path in (ROOT / "stock_universe").rglob("*.py")
    )
    files.extend(
        str(path.relative_to(ROOT)) for path in sorted((ROOT / "scripts").glob("*.py"))
    )
    return [sys.executable, "-m", "py_compile", *files]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Emit JSON only.")
    args = parser.parse_args(argv)

    steps = [
        run_step("compile", compile_command()),
        run_step("pytest", [sys.executable, "-m", "pytest", "-q"]),
    ]
    summary = {
        "ok": all(step["ok"] for step in steps),
        "root": str(ROOT),
        "steps": steps,
    }

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        for step in steps:
            status = "PASS" if step["ok"] else "FAIL"
            print(f"{status} {step['name']}: {' '.join(step['command'])}")
            if step["stdout"].strip():
                print(step["stdout"].rstrip())
            if step["stderr"].strip():
                print(step["stderr"].rstrip(), file=sys.stderr)
        print("PASS foundation smoke" if summary["ok"] else "FAIL foundation smoke")

    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
