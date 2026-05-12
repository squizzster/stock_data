"""Legacy JSON views over typed backfill plans."""

from __future__ import annotations

import json

from stock_universe.domain import BackfillPlan


def legacy_plan_dict(plan: BackfillPlan) -> dict:
    return plan.to_legacy_dict()


def legacy_plan_json(plan: BackfillPlan, *, indent: int = 2) -> str:
    return json.dumps(legacy_plan_dict(plan), indent=indent, sort_keys=True) + "\n"
