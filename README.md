# stock_data

Production workspace for the stock-universe data engine.

The current foundation is intentionally centered on one canonical SQLite
database and the executable-context (`xctx`) interface:

```text
discover via xctx -> plan with evidence -> approve effects -> execute -> audit from bar to lineage
```

## Storage Foundation

- Canonical DB: `production_build/stock_universe.sqlite`
- Market calendar: `us_market_hours.json`
- Hot OHLCV tables store compact facts only: scope id, market session id,
  session start time, UTC start timestamp, lineage id, and OHLCV fields.
- `market_sessions` is seeded from `us_market_hours.json` for the supported US
  equity calendar ids.
- Every bar points directly to `ohlcv_bar_lineage`.
- Raw provider bars are stored in `ohlcv_bar_raw_payloads`, outside the hot
  tables.
- Evidence fact identity is separate from evidence-ledger membership.
- Read-only SQLite access uses `mode=ro` plus `PRAGMA query_only = ON`.

Runtime databases, WAL files, caches, and `production_build/` are ignored by
git.

## Agent Workflow

This repository is an Executable Context workspace. Start with:

```bash
./stock_universe.cli xctx doctor
./stock_universe.cli xctx tree
```

Then follow returned schemas, recipes, examples, repair envelopes, and
`next_actions`. See `AGENTS.md` for the bootloader rules.

Useful discovery commands:

```bash
./stock_universe.cli xctx schema --command "xctx bars"
./stock_universe.cli xctx compose --recipe bar-provenance-audit
./stock_universe.cli xctx examples
```

## Local Gates

```bash
python -m py_compile stock_universe/storage/sqlite_repo.py stock_universe/quality_audit.py
pytest tests/test_sqlite_foundation.py tests/test_market_calendar.py tests/test_sqlite_access.py -q
pytest tests/test_xctx_surface_integrity.py tests/test_xctx_bar_observation.py tests/test_xctx_v2_bar_observation.py -q
```

Broader non-fixture gate used for the current foundation:

```bash
pytest tests/test_market_calendar.py tests/test_sqlite_foundation.py tests/test_sqlite_access.py tests/test_xctx_bar_observation.py tests/test_xctx_v2_bar_observation.py tests/test_packaging_metadata.py tests/test_xctx_surface_integrity.py tests/test_catch_up_workflow.py tests/test_massive_alias_history.py tests/test_massive_reference_and_probes.py tests/test_massive_ticker_replacement.py tests/test_reference_universe.py tests/test_ticker_seed.py tests/test_pressure_manifest.py -q
```

Some legacy fixture tests require `tests/fixtures/legacy_plans/`, which is not
present in this checkout.

## Live Smoke

Requires `MASSIVE_API_KEY`.

```bash
./stock_universe.cli validate-db
./stock_universe.cli update-reference-universe --limit 1000 --max-pages 100 --commit
./stock_universe.cli backfill --ticker NVDA --bar-grain 1d --strict
./stock_universe.cli backfill --ticker NVDA --bar-grain 30m --strict
./stock_universe.cli validate-db
./stock_universe.cli xctx bars --ohlcv-series-id 7964 --date 2024-06-10 --bar-grain 1d --view extra_detail
```
