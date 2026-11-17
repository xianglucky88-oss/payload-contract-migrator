"""Rebuild a replay job from quarantine evidence.

Every quarantined outcome carries re-executable evidence: the original
payload, its digest, and the schema/ruleset versions in force when it failed.
``build_replay_job`` turns those entries back into a job of original-payload
events, each pinned to its evidence digest so tampering is detected
(``replay_evidence_mismatch``) instead of silently migrating the wrong data.
"""
from typing import Any, Dict, Iterable, Optional


def build_replay_job(
    report: Dict[str, Any],
    tenant_contracts: Dict[str, Any],
    event_ids: Optional[Iterable[str]] = None,
    mode: str = "batch",
) -> Dict[str, Any]:
    """Build a job that retries the quarantined events of a previous *report*
    from their original payloads, under the (possibly fixed) *tenant_contracts*."""
    wanted = set(event_ids) if event_ids is not None else None
    events = []
    for outcome in report.get("outcomes", []):
        if outcome.get("status") != "quarantined":
            continue
        if wanted is not None and outcome["event_id"] not in wanted:
            continue
        evidence = outcome["evidence"]
        events.append(
            {
                "tenant_id": outcome["tenant_id"],
                "event_id": outcome["event_id"],
                "schema_version": evidence["source_schema_version"],
                "payload": evidence["original_payload"],
                "replay": {
                    "expected_payload_digest": evidence["payload_digest"],
                    "previous_ruleset_version": evidence["ruleset_version"],
                    "previous_run_id": report.get("run_id"),
                },
            }
        )
    return {"mode": mode, "tenant_contracts": tenant_contracts, "events": events}
