"""Migration engine: per-event migration, batch boundary and resumable sharding.

Execution semantics:

* **Idempotent bypass** — an event already stamped with the target schema
  version and the current ``ruleset_version`` is passed through untouched, so
  re-running a batch never re-transforms (e.g. never re-converts units).
  A matching schema version with a *different* ruleset version is a
  ``ruleset_conflict`` quarantine, never a silent pass.
* **Quarantine boundary** — every failure (unknown field, missing required,
  illegal unit, unmapped enum, rule failure, target incompatibility) routes
  the single event to quarantine with machine-readable reasons and full
  replay evidence; the rest of the batch is unaffected (``isolate`` policy).
  With ``transactional`` policy any quarantine aborts the whole batch and no
  outcome is committed.
* **Deterministic sharding** — events are assigned to shards by content hash,
  so input order never changes shard layout; each shard emits a checkpoint
  with its event keys, and a later run can resume past completed shards.
* **Deduplication** — the first arrival of ``(tenant_id, event_id)`` wins;
  later arrivals are reported as duplicates (flagged ``conflicting`` when the
  payload differs) and are never re-transformed.
"""
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from .contracts import value_matches_type
from .digest import sha256_hex, tagged
from .models import Event, Job, Schema, TenantContract
from .report import build_report
from .rules import RuleFailure, apply_rule


def validate_against_schema(payload: Dict[str, Any], schema: Schema) -> List[Dict[str, str]]:
    issues: List[Dict[str, str]] = []
    for fname, fspec in schema.fields.items():
        if fname not in payload:
            if fspec.required:
                issues.append(
                    {
                        "code": "missing_required",
                        "field": fname,
                        "detail": "required field %r is missing" % fname,
                    }
                )
            continue
        value = payload[fname]
        if value is None:
            if not fspec.nullable:
                issues.append(
                    {
                        "code": "null_not_allowed",
                        "field": fname,
                        "detail": "field %r is null" % fname,
                    }
                )
            continue
        if not value_matches_type(value, fspec.type):
            issues.append(
                {
                    "code": "type_mismatch",
                    "field": fname,
                    "detail": "field %r is %s, expected %s"
                    % (fname, type(value).__name__, fspec.type),
                }
            )
            continue
        if fspec.enum is not None and value not in fspec.enum:
            issues.append(
                {
                    "code": "enum_violation",
                    "field": fname,
                    "detail": "field %r value %r is not in %s"
                    % (fname, value, list(fspec.enum)),
                }
            )
    if not schema.allow_unknown:
        for fname in sorted(set(payload) - set(schema.fields)):
            issues.append(
                {
                    "code": "unknown_field",
                    "field": fname,
                    "detail": "field %r is not declared in schema %s"
                    % (fname, schema.label),
                }
            )
    return issues


def _evidence(
    event: Event, contract: Optional[TenantContract], replay_verified: bool
) -> Dict[str, Any]:
    return {
        "source_schema_version": event.schema_version,
        "target_schema_version": contract.target.version if contract else None,
        "ruleset_version": contract.ruleset_version if contract else None,
        "rule_digest": contract.rule_digest if contract else None,
        "payload_digest": tagged(event.payload),
        "original_payload": deepcopy(event.payload),
        "replay_verified": replay_verified,
    }


def _outcome(
    event: Event,
    status: str,
    output: Optional[Dict[str, Any]],
    rule_trace: Optional[List[Dict[str, Any]]],
    compatibility: Optional[Dict[str, Any]],
    reasons: Optional[List[Dict[str, Any]]],
    evidence: Dict[str, Any],
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    outcome = {
        "event_id": event.event_id,
        "tenant_id": event.tenant_id,
        "seq": event.seq,
        "status": status,
        "output": output,
        "rule_trace": rule_trace,
        "compatibility": compatibility,
        "reasons": reasons,
        "evidence": evidence,
        "committed": True,
    }
    if extra:
        outcome.update(extra)
    return outcome


def _quarantine(
    event: Event,
    contract: Optional[TenantContract],
    reasons: List[Dict[str, Any]],
    rule_trace: Optional[List[Dict[str, Any]]] = None,
    compatibility: Optional[Dict[str, Any]] = None,
    replay_verified: bool = False,
) -> Dict[str, Any]:
    return _outcome(
        event,
        "quarantined",
        None,
        rule_trace or [],
        compatibility,
        reasons,
        _evidence(event, contract, replay_verified),
    )


def migrate_event(event: Event, job: Job) -> Dict[str, Any]:
    contract = job.contracts.get(event.tenant_id)

    # Replay evidence check happens first: a tampered retry must never migrate
    # data that is not the payload the earlier quarantine recorded.
    replay_verified = False
    if event.replay is not None:
        expected = event.replay["expected_payload_digest"]
        actual = tagged(event.payload)
        if actual != expected:
            return _quarantine(
                event,
                contract,
                [
                    {
                        "code": "replay_evidence_mismatch",
                        "detail": f"payload digest {actual} does not match replay"
                        f" evidence {expected}",
                    }
                ],
            )
        replay_verified = True

    if event.tenant_id in job.invalid_tenants:
        return _quarantine(
            event,
            None,
            [
                {
                    "code": "invalid_contract",
                    "detail": "tenant %r contract failed validation; see contract_errors"
                    % event.tenant_id,
                }
            ],
            replay_verified=replay_verified,
        )

    if contract is None:
        return _quarantine(
            event,
            None,
            [
                {
                    "code": "unknown_tenant",
                    "detail": "no contract declared for tenant %r" % event.tenant_id,
                }
            ],
            replay_verified=replay_verified,
        )

    if event.schema_version == contract.target.version:
        return _bypass_or_conflict(event, contract, replay_verified)
    if event.schema_version == contract.source.version:
        return _migrate_from_source(event, contract, replay_verified)
    return _quarantine(
        event,
        contract,
        [
            {
                "code": "unsupported_schema_version",
                "detail": "schema_version %r is neither source %r nor target %r"
                % (
                    event.schema_version,
                    contract.source.version,
                    contract.target.version,
                ),
            }
        ],
        replay_verified=replay_verified,
    )


def _bypass_or_conflict(
    event: Event, contract: TenantContract, replay_verified: bool
) -> Dict[str, Any]:
    """Event already claims the target schema version: pass through untouched
    only when it can prove it was migrated by the current ruleset."""
    if event.ruleset_version is None:
        return _quarantine(
            event,
            contract,
            [
                {
                    "code": "missing_ruleset_proof",
                    "detail": "event claims target schema %r but carries no"
                    " ruleset_version proof" % contract.target.version,
                }
            ],
            replay_verified=replay_verified,
        )
    if event.ruleset_version != contract.ruleset_version:
        return _quarantine(
            event,
            contract,
            [
                {
                    "code": "ruleset_conflict",
                    "detail": "event was migrated with ruleset %r but the contract now"
                    " requires %r; replay from the original payload instead of"
                    " re-transforming"
                    % (event.ruleset_version, contract.ruleset_version),
                }
            ],
            replay_verified=replay_verified,
        )

    issues = validate_against_schema(event.payload, contract.target)
    compatibility = {"stage": "bypass", "compatible": not issues, "issues": issues}
    if issues:
        return _quarantine(
            event,
            contract,
            [
                {
                    "code": "bypass_validation_failed",
                    "detail": "already-migrated payload fails the current target schema",
                }
            ]
            + list(issues),
            compatibility=compatibility,
            replay_verified=replay_verified,
        )
    return _outcome(
        event,
        "bypassed",
        deepcopy(event.payload),
        [
            {
                "seq": None,
                "rule_id": None,
                "op": "bypass",
                "result": "applied",
                "detail": "already at target schema with current ruleset;"
                " not re-transformed",
            }
        ],
        compatibility,
        [],
        _evidence(event, contract, replay_verified),
    )


def _migrate_from_source(
    event: Event, contract: TenantContract, replay_verified: bool
) -> Dict[str, Any]:
    source_issues = validate_against_schema(event.payload, contract.source)
    if source_issues:
        return _quarantine(
            event,
            contract,
            list(source_issues),
            compatibility={
                "stage": "source",
                "compatible": False,
                "issues": source_issues,
            },
            replay_verified=replay_verified,
        )

    doc = deepcopy(event.payload)
    trace: List[Dict[str, Any]] = []
    for rule in contract.rules:
        try:
            apply_rule(doc, rule, trace)
        except RuleFailure as failure:
            return _quarantine(
                event,
                contract,
                [
                    {
                        "code": "rule_failed",
                        "rule_id": rule.rule_id,
                        "op": rule.op,
                        "failure": failure.code,
                        "detail": failure.detail,
                    }
                ],
                rule_trace=trace,
                compatibility={"stage": "rules", "compatible": False, "issues": []},
                replay_verified=replay_verified,
            )

    target_issues = validate_against_schema(doc, contract.target)
    compatibility = {
        "stage": "target",
        "compatible": not target_issues,
        "issues": target_issues,
    }
    if target_issues:
        return _quarantine(
            event,
            contract,
            [{"code": "incompatible_target",
              "detail": "migrated document fails the target schema"}]
            + list(target_issues),
            rule_trace=trace,
            compatibility=compatibility,
            replay_verified=replay_verified,
        )
    return _outcome(
        event,
        "migrated",
        doc,
        trace,
        compatibility,
        [],
        _evidence(event, contract, replay_verified),
    )


def shard_of(event: Event, shard_count: int) -> int:
    """Deterministic shard assignment by content hash — input order independent."""
    return int(sha256_hex({"tenant_id": event.tenant_id, "event_id": event.event_id}), 16) % shard_count


def _duplicate_outcome(
    event: Event, canonical_outcome: Dict[str, Any], job: Job
) -> Dict[str, Any]:
    contract = job.contracts.get(event.tenant_id)
    conflicting = tagged(event.payload) != canonical_outcome["evidence"]["payload_digest"]
    return _outcome(
        event,
        "duplicate",
        None,
        [],
        None,
        [
            {
                "code": "duplicate_delivery",
                "detail": "event %r already processed at seq %d with status %r"
                % (
                    event.event_id,
                    canonical_outcome["seq"],
                    canonical_outcome["status"],
                ),
            }
        ],
        _evidence(event, contract, False),
        {
            "duplicate_of": {
                "event_id": canonical_outcome["event_id"],
                "seq": canonical_outcome["seq"],
                "status": canonical_outcome["status"],
            },
            "conflicting": conflicting,
        },
    )


def run_job(job: Job) -> Dict[str, Any]:
    # Sorting by (tenant, event, seq) makes the outcome stream independent of
    # the order events appeared in the input file.
    ordered = sorted(job.events, key=lambda e: (e.tenant_id, e.event_id, e.seq))

    completed_shards: Dict[int, Dict[str, Any]] = {}
    if job.resume:
        completed_shards = {entry["shard"]: entry for entry in job.resume["completed"]}

    outcomes: List[Dict[str, Any]] = []
    canonical_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    shard_members: Dict[int, List[Dict[str, Any]]] = {}

    for event in ordered:
        key = (event.tenant_id, event.event_id)
        shard_idx = shard_of(event, job.shard_count) if job.mode == "sharded" else None

        if shard_idx is not None and shard_idx in completed_shards:
            if event.key in completed_shards[shard_idx]["event_keys"]:
                outcome = _outcome(
                    event,
                    "skipped_resume",
                    None,
                    [],
                    None,
                    [
                        {
                            "code": "resumed_from_checkpoint",
                            "detail": "shard %d was completed under checkpoint %s"
                            % (shard_idx, completed_shards[shard_idx]["checkpoint"]),
                        }
                    ],
                    _evidence(event, job.contracts.get(event.tenant_id), False),
                    {"shard": shard_idx},
                )
            elif key in canonical_by_key:
                outcome = _duplicate_outcome(event, canonical_by_key[key], job)
                outcome["shard"] = shard_idx
            else:
                outcome = _quarantine(
                    event,
                    job.contracts.get(event.tenant_id),
                    [
                        {
                            "code": "shard_sealed",
                            "detail": "event arrived for shard %d which was already"
                            " completed; late arrivals must be replayed explicitly"
                            % shard_idx,
                        }
                    ],
                )
                outcome["shard"] = shard_idx
                canonical_by_key[key] = outcome
        else:
            if key in canonical_by_key:
                outcome = _duplicate_outcome(event, canonical_by_key[key], job)
                if shard_idx is not None:
                    outcome["shard"] = shard_idx
            else:
                outcome = migrate_event(event, job)
                if shard_idx is not None:
                    outcome["shard"] = shard_idx
                canonical_by_key[key] = outcome

        outcomes.append(outcome)
        if shard_idx is not None:
            shard_members.setdefault(shard_idx, []).append(outcome)

    quarantined = [o for o in outcomes if o["status"] == "quarantined"]
    aborted = job.batch_policy == "transactional" and bool(quarantined)
    if aborted:
        for outcome in outcomes:
            outcome["committed"] = False

    counts = {
        "received": len(outcomes),
        "migrated": sum(1 for o in outcomes if o["status"] == "migrated"),
        "bypassed": sum(1 for o in outcomes if o["status"] == "bypassed"),
        "quarantined": len(quarantined),
        "duplicate": sum(1 for o in outcomes if o["status"] == "duplicate"),
        "skipped_resume": sum(1 for o in outcomes if o["status"] == "skipped_resume"),
    }

    shards: List[Dict[str, Any]] = []
    if job.mode == "sharded":
        for idx in sorted(completed_shards):
            entry = completed_shards[idx]
            shards.append(
                {
                    "shard": idx,
                    "checkpoint": entry["checkpoint"],
                    "event_keys": sorted(entry["event_keys"]),
                    "counts": entry.get("counts"),
                    "resumed": True,
                }
            )
        for idx in sorted(shard_members):
            members = shard_members[idx]
            keys = sorted(f"{o['tenant_id']}:{o['event_id']}" for o in members)
            statuses = {f"{o['tenant_id']}:{o['event_id']}": o["status"] for o in members}
            shards.append(
                {
                    "shard": idx,
                    "checkpoint": tagged(
                        {"shard": idx, "event_keys": keys, "statuses": statuses}
                    ),
                    "event_keys": keys,
                    "counts": {
                        s: sum(1 for o in members if o["status"] == s)
                        for s in ("migrated", "bypassed", "quarantined", "duplicate", "skipped_resume")
                    },
                    "resumed": False,
                }
            )

    if job.contract_errors:
        status = "failed"
    elif aborted:
        status = "aborted"
    elif counts["quarantined"] or any(o.get("conflicting") for o in outcomes):
        status = "partial"
    else:
        status = "ok"

    return build_report(job, status, counts, outcomes, shards)
