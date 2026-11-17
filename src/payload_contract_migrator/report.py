"""Stable migration report envelope.

This is a *contract migration report*, not a generic dead-letter envelope:
every quarantined event carries its rule-hit trajectory, the compatibility
check result, machine-readable reasons and re-executable evidence (original
payload + payload digest + ruleset version + rule digest).  A generic DLQ
entry (message + error string) cannot prove any of that, which is why the
report deliberately uses its own ``report_type`` and structure instead of a
shared dead-letter shape.
"""
from typing import Any, Dict, List

from .models import Job

REPORT_TYPE = "contract_migration_report"
REPORT_VERSION = "1"
PROJECT = "payload-contract-migrator"


def build_report(
    job: Job,
    status: str,
    counts: Dict[str, int],
    outcomes: List[Dict[str, Any]],
    shards: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "report_type": REPORT_TYPE,
        "report_version": REPORT_VERSION,
        "project": PROJECT,
        "run_id": job.run_id,
        "mode": job.mode,
        "batch_policy": job.batch_policy,
        "status": status,
        "counts": counts,
        "tenants": {
            tenant_id: {
                "ruleset_version": contract.ruleset_version,
                "rule_digest": contract.rule_digest,
                "source_schema": contract.source.label,
                "target_schema": contract.target.label,
            }
            for tenant_id, contract in sorted(job.contracts.items())
        },
        "contract_errors": list(job.contract_errors),
        "errors": [],
        "warnings": list(job.warnings),
        "shards": shards,
        "outcomes": outcomes,
    }


def build_failure_report(payload: Any, error: str) -> Dict[str, Any]:
    """Report for a job that could not even be parsed: loud, never silent."""
    run_id = "run-unknown"
    if (
        isinstance(payload, dict)
        and isinstance(payload.get("run_id"), str)
        and payload["run_id"]
    ):
        run_id = payload["run_id"]
    return {
        "report_type": REPORT_TYPE,
        "report_version": REPORT_VERSION,
        "project": PROJECT,
        "run_id": run_id,
        "mode": None,
        "batch_policy": None,
        "status": "failed",
        "counts": {
            "received": 0,
            "migrated": 0,
            "bypassed": 0,
            "quarantined": 0,
            "duplicate": 0,
            "skipped_resume": 0,
        },
        "tenants": {},
        "contract_errors": [],
        "errors": [error],
        "warnings": [],
        "shards": [],
        "outcomes": [],
    }
