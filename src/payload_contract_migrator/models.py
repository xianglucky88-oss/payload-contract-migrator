"""Immutable domain contracts for the migration pipeline.

These types describe the *inputs* of a migration run: tenant contracts
(source/target schema + ordered, versioned rules) and the raw events to be
migrated.  Per-event outcomes are report-shaped dicts built by the engine.
"""
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

SCHEMA_TYPES = ("string", "number", "integer", "boolean", "object", "array", "any")
RULE_OPS = ("copy", "split", "merge", "enum_map", "unit_convert", "default")
MODES = ("single", "batch", "sharded")
BATCH_POLICIES = ("isolate", "transactional")


@dataclass(frozen=True)
class FieldSpec:
    type: str = "any"
    required: bool = False
    enum: Optional[Tuple[Any, ...]] = None
    nullable: bool = False


@dataclass(frozen=True)
class Schema:
    name: str
    version: str
    fields: Dict[str, FieldSpec]
    allow_unknown: bool = False

    @property
    def label(self) -> str:
        return f"{self.name}@{self.version}"


@dataclass(frozen=True)
class Rule:
    seq: int
    rule_id: str
    op: str
    params: Dict[str, Any]


@dataclass(frozen=True)
class TenantContract:
    tenant_id: str
    source: Schema
    target: Schema
    ruleset_version: str
    rules: Tuple[Rule, ...]
    rule_digest: str


@dataclass(frozen=True)
class Event:
    seq: int
    tenant_id: str
    event_id: str
    schema_version: str
    payload: Dict[str, Any]
    ruleset_version: Optional[str] = None
    replay: Optional[Dict[str, Any]] = None

    @property
    def key(self) -> str:
        return f"{self.tenant_id}:{self.event_id}"


@dataclass(frozen=True)
class Job:
    run_id: str
    mode: str
    batch_policy: str
    shard_count: int
    contracts: Dict[str, TenantContract]
    contract_errors: Tuple[str, ...]
    invalid_tenants: Tuple[str, ...]
    events: Tuple[Event, ...]
    resume: Optional[Dict[str, Any]]
    warnings: Tuple[str, ...]
