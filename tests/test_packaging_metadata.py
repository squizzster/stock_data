from __future__ import annotations

import tomllib
import os
from pathlib import Path

import stock_universe


def test_package_metadata_declares_build_backend_and_packages() -> None:
    pyproject = _pyproject()

    assert pyproject["build-system"]["build-backend"] == "setuptools.build_meta"
    assert "setuptools>=68" in pyproject["build-system"]["requires"]
    assert pyproject["tool"]["setuptools"]["packages"]["find"]["include"] == [
        "stock_universe*"
    ]
    assert pyproject["tool"]["setuptools"]["package-data"]["stock_universe"] == [
        "us_market_hours.json"
    ]


def test_project_scripts_match_public_cli_surfaces() -> None:
    scripts = _pyproject()["project"]["scripts"]

    assert scripts == {
        "stock-universe": "stock_universe.cli:main",
        "xctx": "stock_universe.xctx.cli:main",
    }


def test_source_checkout_stock_universe_wrapper_is_executable() -> None:
    wrapper = Path("stock_universe.cli")

    assert wrapper.exists()
    assert os.access(wrapper, os.X_OK)
    assert wrapper.read_text(encoding="utf-8").startswith("#!/usr/bin/env python\n")


def test_project_version_matches_package_version() -> None:
    pyproject = _pyproject()

    assert pyproject["project"]["version"] == stock_universe.__version__


def _pyproject() -> dict:
    return tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
