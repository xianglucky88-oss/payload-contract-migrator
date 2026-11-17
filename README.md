# Payload Contract Migrator

Migrate versioned tenant payloads between a declared source schema and target
schema with explicit quarantine and replay semantics.

Each tenant declares its own contract: a source schema, a target schema, a
pinned `ruleset_version` and an **ordered** list of migration rules. Rule order
is semantic, so the contract is parsed and statically proven before any event is
touched. Every event produces one outcome with its migrated (or quarantined)
payload, the rule-hit trace, the compatibility check, machine-readable reasons
and re-executable evidence.

## Modules

- `models.py`: immutable input contracts (`FieldSpec`, `Schema`, `Rule`,
  `TenantContract`, `Event`, `Job`).
- `digest.py`: canonical JSON digests for rule summaries, payload fingerprints
  and shard checkpoints.
- `contracts.py`: contract parsing plus the static source→target dataflow proof.
- `parser.py`: job envelope validation; structural errors fail loudly, per-tenant
  contract errors are collected separately.
- `rules.py`: the ordered rule engine (`copy`, `split`, `merge`, `enum_map`,
  `unit_convert`, `default`) and the per-rule trace.
- `engine.py`: per-event migration, idempotent bypass, dedup, batch boundary and
  deterministic sharding.
- `report.py`: the migration report envelope (deliberately **not** a generic
  dead-letter shape).
- `replay.py`: rebuild a replay job from quarantine evidence.
- `pipeline.py` / `cli.py`: orchestration and the deterministic JSON CLI.

## Rule operations

| op | purpose | failure codes |
| --- | --- | --- |
| `copy` | rename / pass through a field | contract proof rejects type changes |
| `split` | one string → N fields, `on_arity_mismatch: quarantine\|pad` | `not_a_string`, `arity_mismatch` |
| `merge` | N fields → one string | `merge_source_missing` |
| `enum_map` | rename enum values, `on_unmapped: quarantine\|default` | `unmapped_enum` |
| `unit_convert` | scale a number, decimal precision, optional unit guard | `not_numeric`, `illegal_unit` |
| `default` | fill a missing/null target field | skipped when already present |

## Guarantees

- **Static compatibility proof.** Parsing rejects rules whose source is not
  available at that point in the order, targets not declared in the target
  schema, enum/default values outside the target type/enum, unconsumed source
  fields (unless `allow_unknown`), and required target fields with no producer.
- **Order is semantic.** The `rule_digest` covers the ruleset version, the
  ordered rules and both schemas, so reordering rules or tightening the target
  schema changes the digest attached to every outcome.
- **Every failure is quarantined, never silently dropped.** Unknown fields,
  missing required fields, type mismatches, enum violations, illegal units,
  unmapped enums, rule failures and target incompatibilities all produce
  machine-readable `reasons` and quarantine the *single* event.
- **Re-executable evidence.** Every outcome (success, bypass, duplicate or
  quarantine) carries `original_payload`, `payload_digest`,
  `source_schema_version`, `target_schema_version`, `ruleset_version` and
  `rule_digest`. `build_replay_job` retries quarantined events from the original
  payload pinned to that digest; a tampered payload yields
  `replay_evidence_mismatch`.
- **No double transformation.** An event already at the target schema with the
  current `ruleset_version` is `bypassed` untouched. A different ruleset is a
  `ruleset_conflict`; a missing proof is `missing_ruleset_proof`; an
  already-migrated payload that no longer satisfies the target schema is
  `bypass_validation_failed`.
- **Deterministic batches.** Outcomes are ordered by `(tenant_id, event_id,
  seq)`, so input order never changes results. Identical redelivery is a
  `duplicate`; a differing payload is flagged `conflicting` and makes the run
  `partial`.

## Execution modes and batch boundary

| mode | meaning |
| --- | --- |
| `single` | exactly one event; anything else is a structural failure |
| `batch` | all events in one boundary (`isolate` default) |
| `sharded` | events assigned to shards by content hash; resumable |

`batch_policy: isolate` commits every surviving event and quarantines only the
failures. `batch_policy: transactional` aborts the whole batch if any event
quarantines: every outcome is marked `committed: false` and the run status is
`aborted`.

Run status is `failed` for structural or contract errors, `aborted` for a
transactional rollback, `partial` when anything was quarantined or a duplicate
conflicted, and `ok` otherwise.

## Sharding and replay

Events are assigned with `int(sha256({"tenant_id","event_id"}), 16) % shard_count`
so the layout is independent of input order. Each shard reports a `checkpoint`
digest over its event keys and statuses. A later run may pass
`resume.completed` checkpoints: completed shards are skipped (`skipped_resume`),
a late arrival to a sealed shard is `shard_sealed`, and a changed `shard_count`
is a structural `shard_layout_changed` failure rather than a silent re-shard.

## Report vs. generic dead letter

The report is `report_type: contract_migration_report` with its own version. It
intentionally has no `dead_letter`/`dead_letters` fields: a generic DLQ row
(message + error string) cannot prove the rule trajectory, compatibility check
or original-payload digest that replay and audit require.

## Run

```bash
PYTHONPATH=src python3 -m payload_contract_migrator.cli fixtures/input.json
PYTHONPATH=src python3 -m payload_contract_migrator.cli fixtures/replay.json
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

CLI exit codes: `0` ok, `1` partial/aborted, `2` failed.

The fixtures are local and deterministic; no network or credentials are required.
