#!/usr/bin/env python3
"""Run named validation gates for the stock universe engine."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "gate",
        choices=("compile", "offline", "xctx", "sqlite"),
        help="Named validation gate to run.",
    )
    parser.add_argument("--json", action="store_true", help="Emit a JSON summary.")
    args = parser.parse_args(argv)

    steps = [_run_step(name, command) for name, command in _gate_steps(args.gate)]
    summary = {
        "ok": all(step["ok"] for step in steps),
        "gate": args.gate,
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
        print(f"{'PASS' if summary['ok'] else 'FAIL'} {args.gate} gate")
    return 0 if summary["ok"] else 1


def _gate_steps(gate: str) -> list[tuple[str, list[str]]]:
    compile_step = ("compile", _compile_command())
    if gate == "compile":
        return [compile_step]
    if gate == "offline":
        return [compile_step, ("pytest", [sys.executable, "-m", "pytest", "-q"])]
    if gate == "xctx":
        return [
            compile_step,
            (
                "pytest-xctx",
                [sys.executable, "-m", "pytest", "-q", "tests", "-k", "xctx"],
            ),
        ]
    if gate == "sqlite":
        return [
            compile_step,
            (
                "pytest-sqlite-executor",
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "-q",
                    "tests",
                    "-k",
                    "sqlite or live_bar_executor or executor_contract",
                ],
            ),
        ]
    raise ValueError(f"unknown gate: {gate}")


def _compile_command() -> list[str]:
    files = sorted(
        str(path.relative_to(ROOT)) for path in (ROOT / "stock_universe").rglob("*.py")
    )
    files.extend(
        str(path.relative_to(ROOT)) for path in sorted((ROOT / "scripts").glob("*.py"))
    )
    return [sys.executable, "-m", "py_compile", *files]


def _run_step(name: str, command: list[str]) -> dict[str, Any]:
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


if __name__ == "__main__":
    raise SystemExit(main())
