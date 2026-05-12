"""Shared command-line runtime helpers."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, NoReturn


INTERRUPTED_EXIT = 130


def emit_json(payload: Any) -> bool:
    """Write a JSON payload and return false when stdout is closed by a pipe."""
    try:
        sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        sys.stdout.flush()
    except BrokenPipeError:
        silence_stdout()
        return False
    return True


def silence_stdout() -> None:
    """Redirect stdout to devnull so Python shutdown does not report EPIPE."""
    try:
        stdout_fd = sys.stdout.fileno()
    except (AttributeError, OSError, ValueError):
        return
    try:
        devnull_fd = os.open(os.devnull, os.O_WRONLY)
    except OSError:
        return
    try:
        os.dup2(devnull_fd, stdout_fd)
    except OSError:
        pass
    finally:
        try:
            os.close(devnull_fd)
        except OSError:
            pass


def interrupted_exit(prog: str) -> int:
    try:
        sys.stderr.write(f"{prog}: interrupted\n")
        sys.stderr.flush()
    except OSError:
        pass
    return INTERRUPTED_EXIT


def print_help_for_missing_command(parser: argparse.ArgumentParser) -> NoReturn:
    parser.print_help(sys.stderr)
    parser.exit(
        2, f"{parser.prog}: error: the following arguments are required: command\n"
    )
