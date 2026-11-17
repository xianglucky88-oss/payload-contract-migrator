"""Input validation: turn the raw job payload into a typed :class:`Job`.

Anything structurally wrong with the job envelope raises :class:`JobError`
(the run then reports ``status: failed`` — structural problems must never be
silently absorbed).  Per-tenant contract problems are collected separately so
one broken tenant cannot hide behind, or be hidden by, the others.
"""
from typing import Any, Dict, List, Optional, Tuple

from .contracts import parse_contracts
from .digest import sha256_hex
from .models import BATCH_POLICIES, MODES, Event, Job

_JOB_KEYS = {"run_id", "mode", "batch_policy", "shard_count", "tenant_contracts", "events", "resume"}
_EVENT_KEYS = {"tenant_id", "event_id", "schema_version", "payload", "ruleset_version", "replay"}


class JobError(Exception):
    """Raised when the migration job envelope itself is invalid."""


def _parse_event(seq: int, raw: Any, warnings: List[str]) -> Event:
    ctx = "event #%d" % seq
    if not isinstance(raw, dict):
        raise JobError("%s must be an object" % ctx)
    unknown = sorted(set(raw) - _EVENT_KEYS)
    if unknown:
        warnings.append(f"{ctx} has unknown keys {unknown}")
    tenant_id = raw.get("tenant_id")
    event_id = raw.get("event_id")
    schema_version = raw.get("schema_version")
    payload = raw.get("payload")
    if not isinstance(tenant_id, str) or not tenant_id:
        raise JobError("%s: tenant_id must be a non-empty string" % ctx)
    if not isinstance(event_id, str) or not event_id:
        raise JobError("%s: event_id must be a non-empty string" % ctx)
    if not isinstance(schema_version, str) or not schema_version:
        raise JobError("%s: schema_version must be a non-empty string" % ctx)
    if not isinstance(payload, dict):
        raise JobError("%s: payload must be an object" % ctx)
    ruleset_version = raw.get("ruleset_version")
    if ruleset_version is not None and not isinstance(ruleset_version, str):
        raise JobError("%s: ruleset_version must be a string" % ctx)
    replay = raw.get("replay")
    if replay is not None:
        if not isinstance(replay, dict) or not isinstance(
            replay.get("expected_payload_digest"), str
        ):
            raise JobError(
                "%s: replay must be an object with expected_payload_digest" % ctx
            )
    return Event(
        seq=seq,
        tenant_id=tenant_id,
        event_id=event_id,
        schema_version=schema_version,
        payload=payload,
        ruleset_version=ruleset_version,
        replay=replay,
    )


def _parse_resume(raw: Any, shard_count: int) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise JobError("resume must be an object")
    if raw.get("shard_count") != shard_count:
        raise JobError(
            "shard_layout_changed: resume token declares shard_count="
            f"{raw.get('shard_count')!r} but the job declares {shard_count!r};"
            " re-sharding mid-stream would silently reorder the batch boundary"
        )
    completed = raw.get("completed")
    if not isinstance(completed, list):
        raise JobError("resume.completed must be a list of shard checkpoints")
    seen = set()
    for entry in completed:
        if not isinstance(entry, dict) or not isinstance(entry.get("shard"), int):
            raise JobError(
                "resume.completed entries must be objects with an integer shard"
            )
        idx = entry["shard"]
        if not (0 <= idx < shard_count):
            raise JobError(
                "resume.completed shard %d is outside shard_count=%d" % (idx, shard_count)
            )
        if idx in seen:
            raise JobError("resume.completed lists shard %d twice" % idx)
        seen.add(idx)
        if not isinstance(entry.get("checkpoint"), str):
            raise JobError(
                "resume.completed shard %d is missing its checkpoint digest" % idx
            )
        if not isinstance(entry.get("event_keys"), list):
            raise JobError("resume.completed shard %d is missing event_keys" % idx)
    return {"shard_count": shard_count, "completed": completed}


def parse_job(payload: Any) -> Job:
    if not isinstance(payload, dict):
        raise JobError("job must be a JSON object")
    warnings: List[str] = []
    unknown = sorted(set(payload) - _JOB_KEYS)
    if unknown:
        warnings.append("job has unknown keys %s" % unknown)

    mode = payload.get("mode", "batch")
    if mode not in MODES:
        raise JobError(f"mode must be one of {list(MODES)}")
    batch_policy = payload.get("batch_policy", "isolate")
    if batch_policy not in BATCH_POLICIES:
        raise JobError(f"batch_policy must be one of {list(BATCH_POLICIES)}")

    shard_count = payload.get("shard_count")
    if mode == "sharded":
        if (
            not isinstance(shard_count, int)
            or isinstance(shard_count, bool)
            or shard_count < 1
        ):
            raise JobError("sharded mode requires an integer shard_count >= 1")
    else:
        if shard_count is not None:
            raise JobError("shard_count is only valid in sharded mode")
        shard_count = 1

    resume = payload.get("resume")
    if resume is not None:
        if mode != "sharded":
            raise JobError("resume is only valid in sharded mode")
        resume = _parse_resume(resume, shard_count)

    events_raw = payload.get("events")
    if not isinstance(events_raw, list):
        raise JobError("events must be a list")
    events = tuple(_parse_event(i, raw, warnings) for i, raw in enumerate(events_raw))
    if mode == "single" and len(events) != 1:
        raise JobError("single mode requires exactly one event, got %d" % len(events))

    try:
        contracts, contract_errors, invalid_tenants = parse_contracts(
            payload.get("tenant_contracts")
        )
    except ValueError as exc:
        raise JobError(str(exc))

    run_id = payload.get("run_id")
    if run_id is None:
        run_id = "run-" + sha256_hex(
            {k: v for k, v in payload.items() if k != "run_id"}
        )[:12]
    elif not isinstance(run_id, str) or not run_id:
        raise JobError("run_id must be a non-empty string")

    return Job(
        run_id=run_id,
        mode=mode,
        batch_policy=batch_policy,
        shard_count=shard_count,
        contracts=contracts,
        contract_errors=tuple(contract_errors),
        invalid_tenants=tuple(invalid_tenants),
        events=events,
        resume=resume,
        warnings=tuple(warnings),
    )
