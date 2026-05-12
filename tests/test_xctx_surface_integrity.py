from __future__ import annotations

import argparse
import json
import shlex
from pathlib import Path

import pytest

from stock_universe import cli as stock_cli_module
from stock_universe.storage import SQLiteStockUniverseRepository
from stock_universe.xctx.cli import main as xctx_main
from stock_universe.xctx import cli as xctx_cli_module
from stock_universe.xctx import (
    xctx_binding_maps,
    xctx_command_schemas,
    xctx_recipes,
    xctx_transition_graph,
)


PUBLIC_REPORT_SURFACE_FILES = (
    "stock_universe/ops/pressure_manifest.py",
    "scripts/fixture_matrix.py",
    "scripts/live_sqlite_backfill.py",
)


def test_public_report_surfaces_emit_ohlcv_series_id_not_legacy_series_id() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    for relative_path in PUBLIC_REPORT_SURFACE_FILES:
        text = (repo_root / relative_path).read_text(encoding="utf-8")
        assert '"series_id":' not in text, relative_path


PRIMARY_AGENT_SURFACE_COMMANDS = {
    "stock-universe inspect-plan",
    "stock-universe identity-search",
    "stock-universe update-reference-universe",
    "stock-universe dry-run",
    "stock-universe backfill",
    "stock-universe backfill-reference-batch",
    "stock-universe catch-up",
    "stock-universe catch-up-stop",
    "stock-universe catch-up-reconcile",
    "stock-universe validate-db",
    "stock-universe universe-status",
    "stock-universe quality-audit",
    "stock-universe repair-missing-receipts",
    "stock-universe audit-executions",
    "stock-universe doctor",
}

STOCK_UNIVERSE_ARGPARSE_COMMANDS = {
    "stock-universe inspect-plan": "inspect-plan",
    "stock-universe identity-search": "identity-search",
    "stock-universe update-reference-universe": "update-reference-universe",
    "stock-universe dry-run": "dry-run",
    "stock-universe backfill": "backfill",
    "stock-universe backfill-reference-batch": "backfill-reference-batch",
    "stock-universe catch-up": "catch-up",
    "stock-universe catch-up-stop": "catch-up-stop",
    "stock-universe catch-up-reconcile": "catch-up-reconcile",
    "stock-universe validate-db": "validate-db",
    "stock-universe universe-status": "universe-status",
    "stock-universe quality-audit": "quality-audit",
    "stock-universe repair-missing-receipts": "repair-missing-receipts",
    "stock-universe audit-executions": "audit-executions",
    "stock-universe doctor": "doctor",
}

XCTX_ARGPARSE_COMMANDS = {
    "xctx tree": "tree",
    "xctx capabilities": "capabilities",
    "xctx doctor": "doctor",
    "xctx examples": "examples",
    "xctx schema": "schema",
    "xctx validate": "validate",
    "xctx dry-run": "dry-run",
    "xctx resolve-identity": "resolve-identity",
    "xctx bars": "bars",
    "xctx next": "next",
    "xctx repair": "repair",
    "xctx universe-status": "universe-status",
    "xctx quality-audit": "quality-audit",
    "xctx catch-up-plan": "catch-up-plan",
    "xctx catch-up-runs": "catch-up-runs",
    "xctx catch-up-status": "catch-up-status",
    "xctx observe": "observe",
    "xctx compose": "compose",
}

ARG_DEST_ALIASES = {
    "query_arg": "query",
    "query_option": "query",
}


def test_xctx_schema_and_bindings_cover_primary_agent_surface() -> None:
    schemas = xctx_command_schemas()
    bindings = xctx_binding_maps()

    assert PRIMARY_AGENT_SURFACE_COMMANDS <= set(schemas)
    assert PRIMARY_AGENT_SURFACE_COMMANDS <= set(bindings)


def test_xctx_binding_structured_inputs_are_schema_args() -> None:
    schemas = xctx_command_schemas()

    for command, binding in xctx_binding_maps().items():
        schema_args = set(schemas[command]["args"])
        structured_keys = set(binding.get("structured_input") or {})

        assert structured_keys <= schema_args


def test_xctx_schemas_cover_argparse_help_surface() -> None:
    schemas = xctx_command_schemas()
    stock_subparsers = _subparsers(stock_cli_module._parser(prog="stock-universe"))
    xctx_subparsers = _subparsers(xctx_cli_module._parser(prog="xctx"))

    for schema_command, argparse_command in STOCK_UNIVERSE_ARGPARSE_COMMANDS.items():
        assert _arg_dests(stock_subparsers[argparse_command]) <= set(
            schemas[schema_command]["args"]
        )

    for schema_command, argparse_command in XCTX_ARGPARSE_COMMANDS.items():
        assert _arg_dests(xctx_subparsers[argparse_command]) <= set(
            schemas[schema_command]["args"]
        )


def test_xctx_recipe_steps_are_discoverable_and_schema_backed() -> None:
    schemas = xctx_command_schemas()
    bindings = xctx_binding_maps()
    transition_names = {transition["name"] for transition in xctx_transition_graph()}

    for recipe in xctx_recipes():
        for step in recipe["steps"]:
            command_key = _schema_key(step.get("logical_command", step["command"]))

            assert step["transition"] in transition_names
            assert command_key in schemas
            assert command_key in bindings


def test_xctx_binding_argv_fields_are_source_checkout_runnable() -> None:
    for command, binding in xctx_binding_maps().items():
        for key, value in binding.items():
            if not (key == "argv" or key.endswith("_argv")):
                continue
            if key.startswith("logical_"):
                continue

            assert value[0] == "./stock_universe.cli", (command, key, value)


def test_xctx_recipes_expose_runnable_commands_with_logical_keys() -> None:
    for recipe in xctx_recipes():
        for step in recipe["steps"]:
            assert step["command"].startswith("./stock_universe.cli "), (
                recipe["name"],
                step["command"],
            )
            assert "logical_command" in step
            assert not step["logical_command"].startswith("./stock_universe.cli")


def test_xctx_recipes_expose_long_running_reporting_policy() -> None:
    recipes = {recipe["name"]: recipe for recipe in xctx_recipes()}
    catch_up = recipes["database-catch-up"]
    data_not_loaded = recipes["data-not-loaded-catch-up"]
    reference = recipes["reference-universe-maintenance"]
    provenance = recipes["bar-provenance-audit"]

    assert catch_up["agent_reporting"]["routine"]["system_poll_seconds"] == 60
    assert catch_up["agent_reporting"]["routine"]["first_update_seconds"] == 180
    assert reference["agent_reporting"]["routine"]["default_update_seconds"] == 300

    catch_up_steps = {
        step["transition"]: step
        for step in catch_up["steps"]
        if "agent_reporting" in step
    }
    assert (
        catch_up_steps["catch-up-run"]["agent_reporting"]["applies_when"]
        == "commit=true and expected_duration_seconds >= 10"
    )
    assert (
        "regular status paths"
        in catch_up_steps["catch-up-run"]["agent_reporting"]["monitoring_guidance"]
    )
    assert catch_up_steps["validate-db"]["agent_reporting"]["immediate_update_on"] == [
        "validation_failure",
        "hard_error",
        "nonzero_exit",
        "no_progress_or_stall",
    ]
    catch_up_commands = [step["command"] for step in catch_up["steps"]]
    data_not_loaded_commands = [step["command"] for step in data_not_loaded["steps"]]
    assert any(
        "--category data_not_loaded" in command for command in data_not_loaded_commands
    )
    assert any(
        "--workers 10 --batch-size 25 --category data_not_loaded" in command
        for command in data_not_loaded_commands
    )
    assert not any(
        "--category data_not_loaded" in command for command in catch_up_commands
    )
    provenance_commands = [step["command"] for step in provenance["steps"]]
    assert any("--view extra_detail" in command for command in provenance_commands)
    assert any("--bar-grain {bar_grain}" in command for command in provenance_commands)
    assert provenance["steps"][-1]["transition"] == "validate-db"


def test_xctx_catch_up_stop_schema_exposes_stop_modes() -> None:
    schema = xctx_command_schemas()["stock-universe catch-up-stop"]
    binding = xctx_binding_maps()["stock-universe catch-up-stop"]

    assert schema["args"]["mode"]["enum"] == ["drain", "quiesce", "abort"]
    assert schema["args"]["mode"]["default"] == "drain"
    assert binding["structured_input"]["mode"] == "drain|quiesce|abort"
    assert "--mode" in binding["mode_argv"]


def test_xctx_dry_run_schema_includes_cli_help_arguments() -> None:
    dry_run_args = xctx_command_schemas()["xctx dry-run"]["args"]

    assert dry_run_args["omit_kind"]["items"] == "evidence_kind"
    assert dry_run_args["defer_kind"]["items"] == "evidence_kind"
    assert dry_run_args["api_key"]["type"] == "string"
    assert dry_run_args["base_url"]["type"] == "url"


def test_xctx_schema_declares_actual_capability_result_type() -> None:
    assert xctx_command_schemas()["xctx capabilities"]["returns"] == "CapabilityList"


def test_xctx_schema_declares_actual_observe_result_type() -> None:
    assert xctx_command_schemas()["xctx observe"]["returns"] == "ExecutionAudit"


def test_xctx_validate_db_schema_declares_conditional_write() -> None:
    schema = xctx_command_schemas()["stock-universe validate-db"]

    assert schema["mutates"] is True
    assert schema["write_condition"] == "db_missing_or_schema_missing"


def test_xctx_identity_protocol_declares_series_reporting_contract() -> None:
    schema = xctx_command_schemas()["xctx resolve-identity"]
    binding = xctx_binding_maps()["xctx resolve-identity"]

    assert (
        "ohlcv_series_id as the canonical OHLCV reporting key" in schema["description"]
    )
    assert "agent_ohlcv_reporting_policy" in schema["returns"]
    assert "reporting_policy" in schema["returns"]
    assert binding["result_contract"]["canonical_ohlcv_field"] == "ohlcv_series_id"
    assert "ohlcv_series_id" in binding["result_contract"]["agent_rule"]
    assert "ticker label" in binding["result_contract"]["agent_rule"]


def test_xctx_manifest_recommended_loop_uses_source_checkout_execution() -> None:
    from stock_universe.xctx import xctx_tool_manifest

    loop = xctx_tool_manifest()["recommended_agent_loop"]
    assert "./stock_universe.cli backfill --fixture <fixture> --strict" in loop
    assert './stock_universe.cli xctx schema --command "xctx bars"' in loop
    assert "./stock_universe.cli xctx compose --recipe bar-provenance-audit" in loop


def test_xctx_examples_cover_core_agent_workflows(capsys) -> None:
    assert xctx_main(["examples"]) == 0
    payload = json.loads(capsys.readouterr().out)
    example_commands = {example["command"] for example in payload["examples"]}

    assert {
        "xctx resolve-identity",
        "xctx bars",
        "xctx quality-audit",
        "xctx observe",
        "xctx catch-up-plan",
        "xctx catch-up-runs",
        "xctx catch-up-status",
        "stock-universe update-reference-universe",
        "stock-universe backfill-reference-batch",
        "stock-universe catch-up",
        "stock-universe catch-up-stop",
        "stock-universe catch-up-reconcile",
        "stock-universe validate-db",
    } <= example_commands
    examples = {example["name"]: example for example in payload["examples"]}
    assert (
        "session/UTC/direct-lineage/raw-sidecar provenance"
        in examples["observe-canonical-bars"]["what_it_teaches"]
    )
    assert examples["audit-bar-provenance"]["structured_input"]["view"] == "extra_detail"
    assert set(payload["known_example_commands"]) == example_commands
    assert PRIMARY_AGENT_SURFACE_COMMANDS <= set(payload["known_schema_commands"])
    for example in payload["examples"]:
        assert example["argv"][0] == "./stock_universe.cli", (
            example["name"],
            example["argv"],
        )


def test_xctx_schema_resolves_stock_universe_aliases(capsys) -> None:
    assert xctx_main(["schema", "--command", "stock-universe search"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["alias_of"] == "stock-universe identity-search"
    assert "stock-universe identity-search" in payload["command_schema"]


def test_xctx_compose_unknown_recipe_lists_known_recipes(capsys) -> None:
    assert xctx_main(["compose", "--recipe", "does-not-exist"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["result_type"] == "RepairError"
    assert "ticker-live-backfill" in payload["known_recipes"]
    assert "bar-provenance-audit" in payload["known_recipes"]
    assert payload["next_actions"][0]["name"] == "list-workflow-recipes"


def test_xctx_quality_audit_empty_db_teaches_reference_setup(tmp_path, capsys) -> None:
    db = tmp_path / "stock_universe.sqlite"
    SQLiteStockUniverseRepository(db).ensure_schema()

    assert xctx_main(["quality-audit", "--db", str(db), "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["active_reference_series"] == 0
    assert {action["name"] for action in payload["next_moves"]} >= {
        "inspect-universe-status",
        "dry-run-reference-universe-update",
        "commit-reference-universe-update",
    }
    assert "issues" not in payload


def test_xctx_quality_audit_missing_db_returns_repair_error(tmp_path, capsys) -> None:
    db = tmp_path / "missing.sqlite"

    assert xctx_main(["quality-audit", "--db", str(db), "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["result_type"] == "RepairError"
    assert payload["repairs"][0]["name"] == "provide-existing-sqlite-db"
    assert db.exists() is False


def test_xctx_validate_invalid_json_returns_repair_error(tmp_path, capsys) -> None:
    fixture = tmp_path / "bad.json"
    fixture.write_text("{", encoding="utf-8")

    assert xctx_main(["validate", "--fixture", str(fixture)]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["result_type"] == "RepairError"
    assert payload["errors"][0]["code"] == "fixture_json_invalid"
    assert payload["effects"]["will_read"] == [str(fixture)]


def test_xctx_dry_run_missing_series_id_returns_repair_error(tmp_path, capsys) -> None:
    db = tmp_path / "stock_universe.sqlite"
    SQLiteStockUniverseRepository(db).ensure_schema()

    assert (
        xctx_main(
            [
                "dry-run",
                "--ohlcv-series-id",
                "1",
                "--db",
                str(db),
                "--api-key",
                "secret",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["result_type"] == "RepairError"
    assert payload["errors"][0]["code"] == "ohlcv_series_id_not_found"
    assert {repair["name"] for repair in payload["repairs"]} >= {
        "dry-run-reference-universe-update",
        "commit-reference-universe-update",
    }


def test_stock_universe_quality_audit_missing_db_returns_repair_json(
    tmp_path, capsys
) -> None:
    db = tmp_path / "missing.sqlite"

    assert (
        stock_cli_module.main(["quality-audit", "--db", str(db), "--limit", "5"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["result_type"] == "RepairError"
    assert payload["repairs"][0]["command"]["name"] == "stock-universe validate-db"
    assert db.exists() is False


def test_stock_universe_dry_run_missing_series_id_exits_without_traceback(
    tmp_path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    SQLiteStockUniverseRepository(db).ensure_schema()

    with pytest.raises(SystemExit) as exc:
        stock_cli_module.main(
            [
                "dry-run",
                "--ohlcv-series-id",
                "1",
                "--db",
                str(db),
                "--api-key",
                "secret",
            ]
        )

    assert exc.value.code == 2
    captured = capsys.readouterr()
    assert "ohlcv_series_id not found in reference universe: 1" in captured.err
    assert "Traceback" not in captured.err


def _schema_key(command: str) -> str:
    tokens = shlex.split(command)
    if tokens[:2] == ["xctx", "describe"] and len(tokens) >= 3:
        return " ".join(tokens[:3])
    if len(tokens) >= 2 and tokens[0] in {"stock-universe", "xctx"}:
        return " ".join(tokens[:2])
    return command


def _subparsers(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action.choices
    raise AssertionError("parser has no subparsers")


def _arg_dests(parser: argparse.ArgumentParser) -> set[str]:
    dests = set()
    for action in parser._actions:
        if action.dest == "help":
            continue
        if not action.option_strings and action.nargs == 0:
            continue
        dests.add(ARG_DEST_ALIASES.get(action.dest, action.dest))
    return dests
