"""Tenant contract parsing and the static compatibility proof.

A tenant contract declares a source schema, a target schema and an *ordered*
list of migration rules pinned to a ``ruleset_version``.  Parsing a contract
proves, before any event is seen, that:

* every rule source is available at the point the rule runs (order is
  semantic: each rule consumes and produces fields of a working document);
* every rule target exists in the target schema;
* enum maps and default values satisfy the target field's type and enum;
* every required target field is produced by the rules or carried over;
* no source field is left over that the target schema would reject.

The ``rule_digest`` covers the ruleset version, the ordered rules and both
schema definitions, so any change to the contract — including a target schema
tightening — is detectable from the evidence attached to each outcome.
"""
from typing import Any, Dict, List, Tuple

from .digest import tagged
from .models import SCHEMA_TYPES, FieldSpec, Rule, Schema, TenantContract

_FIELD_SPEC_KEYS = {"type", "required", "enum", "nullable"}
_SCHEMA_KEYS = {"name", "version", "fields", "allow_unknown"}
_CONTRACT_KEYS = {"source_schema", "target_schema", "ruleset_version", "rules"}
_RULE_PARAM_KEYS = {
    "copy": {"source", "target"},
    "split": {"source", "targets", "delimiter", "on_arity_mismatch"},
    "merge": {"sources", "target", "delimiter"},
    "enum_map": {"source", "target", "map", "on_unmapped", "default"},
    "unit_convert": {
        "source",
        "target",
        "from_unit",
        "to_unit",
        "unit_field",
        "rate",
        "precision",
    },
    "default": {"target", "value"},
}


class ContractError(Exception):
    """Raised when a tenant contract is internally inconsistent."""


def _fail(ctx: str, msg: str) -> None:
    raise ContractError(f"{ctx}: {msg}")


def value_matches_type(value: Any, ftype: str) -> bool:
    if ftype == "any":
        return True
    if ftype == "string":
        return isinstance(value, str)
    if ftype == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if ftype == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if ftype == "boolean":
        return isinstance(value, bool)
    if ftype == "object":
        return isinstance(value, dict)
    if ftype == "array":
        return isinstance(value, list)
    return False


def _parse_field_spec(fname: str, raw: Any, ctx: str) -> FieldSpec:
    if not isinstance(raw, dict):
        _fail(ctx, "field %r spec must be an object" % fname)
    unknown = sorted(set(raw) - _FIELD_SPEC_KEYS)
    if unknown:
        _fail(ctx, f"field {fname!r} has unknown spec keys {unknown}")
    ftype = raw.get("type", "any")
    if ftype not in SCHEMA_TYPES:
        _fail(ctx, f"field {fname!r} has unknown type {ftype!r}")
    enum = raw.get("enum")
    if enum is not None:
        if not isinstance(enum, list) or not enum:
            _fail(ctx, "field %r enum must be a non-empty list" % fname)
        if len({repr(v) for v in enum}) != len(enum):
            _fail(ctx, "field %r enum has duplicate values" % fname)
        enum = tuple(enum)
    return FieldSpec(
        type=ftype,
        required=bool(raw.get("required", False)),
        enum=enum,
        nullable=bool(raw.get("nullable", False)),
    )


def parse_schema(raw: Any, kind: str, tenant_id: str) -> Schema:
    ctx = f"tenant {tenant_id!r} {kind}_schema"
    if not isinstance(raw, dict):
        _fail(ctx, "must be an object")
    unknown = sorted(set(raw) - _SCHEMA_KEYS)
    if unknown:
        _fail(ctx, "unknown keys %s" % unknown)
    name = raw.get("name")
    version = raw.get("version")
    if not isinstance(name, str) or not name:
        _fail(ctx, "name must be a non-empty string")
    if not isinstance(version, str) or not version:
        _fail(ctx, "version must be a non-empty string")
    fields_raw = raw.get("fields")
    if not isinstance(fields_raw, dict) or not fields_raw:
        _fail(ctx, "fields must be a non-empty object")
    fields = {
        fname: _parse_field_spec(fname, spec, ctx)
        for fname, spec in fields_raw.items()
    }
    return Schema(
        name=name,
        version=version,
        fields=fields,
        allow_unknown=bool(raw.get("allow_unknown", False)),
    )


def _require_str(params: Dict[str, Any], key: str, ctx: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        _fail(ctx, "%r must be a non-empty string" % key)
    return value


def _parse_rule(seq: int, raw: Any, ctx: str) -> Rule:
    rctx = "%s rule #%d" % (ctx, seq)
    if not isinstance(raw, dict):
        _fail(rctx, "must be an object")
    rule_id = raw.get("id")
    op = raw.get("op")
    if not isinstance(rule_id, str) or not rule_id:
        _fail(rctx, "id must be a non-empty string")
    rctx = f"{ctx} rule {rule_id!r}"
    if op not in _RULE_PARAM_KEYS:
        _fail(rctx, f"unknown op {op!r} (supported: {sorted(_RULE_PARAM_KEYS)})")
    unknown = sorted(set(raw) - _RULE_PARAM_KEYS[op] - {"id", "op"})
    if unknown:
        _fail(rctx, f"unknown params {unknown} for op {op!r}")

    params: Dict[str, Any] = {}
    if op == "copy":
        params["source"] = _require_str(raw, "source", rctx)
        params["target"] = raw.get("target", params["source"])
        if not isinstance(params["target"], str) or not params["target"]:
            _fail(rctx, "'target' must be a non-empty string")
    elif op == "split":
        params["source"] = _require_str(raw, "source", rctx)
        targets = raw.get("targets")
        if (
            not isinstance(targets, list)
            or len(targets) < 2
            or not all(isinstance(t, str) for t in targets)
        ):
            _fail(rctx, "'targets' must be a list of at least two field names")
        params["targets"] = list(targets)
        params["delimiter"] = raw.get("delimiter", " ")
        if not isinstance(params["delimiter"], str):
            _fail(rctx, "'delimiter' must be a string")
        params["on_arity_mismatch"] = raw.get("on_arity_mismatch", "quarantine")
        if params["on_arity_mismatch"] not in ("quarantine", "pad"):
            _fail(rctx, "'on_arity_mismatch' must be 'quarantine' or 'pad'")
    elif op == "merge":
        sources = raw.get("sources")
        if (
            not isinstance(sources, list)
            or len(sources) < 2
            or not all(isinstance(s, str) for s in sources)
        ):
            _fail(rctx, "'sources' must be a list of at least two field names")
        params["sources"] = list(sources)
        params["target"] = _require_str(raw, "target", rctx)
        if params["target"] in params["sources"]:
            _fail(rctx, "'target' must not overlap 'sources'")
        params["delimiter"] = raw.get("delimiter", " ")
        if not isinstance(params["delimiter"], str):
            _fail(rctx, "'delimiter' must be a string")
    elif op == "enum_map":
        params["source"] = _require_str(raw, "source", rctx)
        params["target"] = raw.get("target", params["source"])
        if not isinstance(params["target"], str) or not params["target"]:
            _fail(rctx, "'target' must be a non-empty string")
        mapping = raw.get("map")
        if not isinstance(mapping, dict) or not mapping:
            _fail(rctx, "'map' must be a non-empty object")
        params["map"] = dict(mapping)
        params["on_unmapped"] = raw.get("on_unmapped", "quarantine")
        if params["on_unmapped"] not in ("quarantine", "default"):
            _fail(rctx, "'on_unmapped' must be 'quarantine' or 'default'")
        if params["on_unmapped"] == "default":
            if "default" not in raw:
                _fail(rctx, "'default' value is required when on_unmapped='default'")
            params["default"] = raw["default"]
    elif op == "unit_convert":
        params["source"] = _require_str(raw, "source", rctx)
        params["target"] = _require_str(raw, "target", rctx)
        params["from_unit"] = _require_str(raw, "from_unit", rctx)
        params["to_unit"] = _require_str(raw, "to_unit", rctx)
        unit_field = raw.get("unit_field")
        if unit_field is not None and (
            not isinstance(unit_field, str) or not unit_field
        ):
            _fail(rctx, "'unit_field' must be a non-empty string")
        params["unit_field"] = unit_field
        rate = raw.get("rate")
        if not isinstance(rate, (int, float)) or isinstance(rate, bool):
            _fail(rctx, "'rate' must be a number")
        params["rate"] = rate
        precision = raw.get("precision")
        if precision is not None and (
            not isinstance(precision, int)
            or isinstance(precision, bool)
            or precision < 0
        ):
            _fail(rctx, "'precision' must be a non-negative integer")
        params["precision"] = precision
    elif op == "default":
        params["target"] = _require_str(raw, "target", rctx)
        if "value" not in raw:
            _fail(rctx, "'value' is required")
        params["value"] = raw["value"]
    return Rule(seq=seq, rule_id=rule_id, op=op, params=params)


def _rule_sources(rule: Rule) -> List[str]:
    p = rule.params
    if rule.op == "merge":
        return list(p["sources"])
    if rule.op == "default":
        return []
    return [p["source"]]


def _rule_targets(rule: Rule) -> List[str]:
    p = rule.params
    if rule.op == "split":
        return list(p["targets"])
    if rule.op == "default":
        return [p["target"]]
    return [p["target"]]


def _check_value_against_field(value: Any, fspec: FieldSpec, ctx: str) -> None:
    if not value_matches_type(value, fspec.type):
        _fail(ctx, f"value {value!r} does not match target type {fspec.type!r}")
    if fspec.enum is not None and value not in fspec.enum:
        _fail(ctx, f"value {value!r} is not in the target enum {list(fspec.enum)}")


def _prove_dataflow(
    source: Schema, target: Schema, rules: Tuple[Rule, ...], ctx: str
) -> None:
    """Static compatibility proof: rules, in declared order, must transform the
    source document into exactly the target document shape."""
    available = set(source.fields)
    for rule in rules:
        rctx = f"{ctx} rule {rule.rule_id!r}"
        for src in _rule_sources(rule):
            if src not in available:
                _fail(
                    rctx,
                    "source field %r is not available at this point in the rule order"
                    % src,
                )
        if rule.op == "unit_convert" and rule.params["unit_field"] is not None:
            if rule.params["unit_field"] not in available:
                _fail(rctx, "unit_field %r is not available" % rule.params["unit_field"])
        for tgt in _rule_targets(rule):
            if tgt not in target.fields:
                _fail(rctx, "target field %r is not declared in the target schema" % tgt)

        tspecs = {t: target.fields[t] for t in _rule_targets(rule)}
        if rule.op == "copy":
            sspec = source.fields.get(rule.params["source"], FieldSpec())
            tspec = tspecs[rule.params["target"]]
            if (
                tspec.type != "any"
                and sspec.type != "any"
                and tspec.type != sspec.type
            ):
                _fail(
                    rctx,
                    f"cannot copy {sspec.type!r} field {rule.params['source']!r}"
                    f" into {tspec.type!r} field {rule.params['target']!r}",
                )
        elif rule.op == "split":
            for t, tspec in tspecs.items():
                if tspec.type not in ("string", "any"):
                    _fail(
                        rctx,
                        f"split target {t!r} must be a string field, got {tspec.type!r}",
                    )
        elif rule.op == "merge":
            tspec = tspecs[rule.params["target"]]
            if tspec.type not in ("string", "any"):
                _fail(
                    rctx,
                    f"merge target {rule.params['target']!r} must be a string field,"
                    f" got {tspec.type!r}",
                )
        elif rule.op == "enum_map":
            tspec = tspecs[rule.params["target"]]
            mapped_values = list(rule.params["map"].values())
            if "default" in rule.params:
                mapped_values = mapped_values + [rule.params["default"]]
            for mapped in mapped_values:
                _check_value_against_field(mapped, tspec, rctx)
        elif rule.op == "unit_convert":
            tspec = tspecs[rule.params["target"]]
            if tspec.type not in ("number", "integer", "any"):
                _fail(
                    rctx,
                    f"unit_convert target {rule.params['target']!r} must be numeric,"
                    f" got {tspec.type!r}",
                )
        elif rule.op == "default":
            _check_value_against_field(
                rule.params["value"], tspecs[rule.params["target"]], rctx
            )

        targets = set(_rule_targets(rule))
        if rule.op == "copy" and rule.params["source"] == rule.params["target"]:
            pass
        else:
            for src in _rule_sources(rule):
                if src not in targets:
                    available.discard(src)
        available.update(targets)

    leftover = sorted(available - set(target.fields))
    if leftover and not target.allow_unknown:
        _fail(
            ctx,
            "fields %s are not consumed by any rule and not declared in the target"
            " schema; add a rule or set allow_unknown" % leftover,
        )
    missing = sorted(
        f for f, spec in target.fields.items() if spec.required and f not in available
    )
    if missing:
        _fail(ctx, "required target fields %s are never produced by the rules" % missing)


def parse_contract(tenant_id: str, raw: Any) -> TenantContract:
    ctx = "tenant %r" % tenant_id
    if not isinstance(raw, dict):
        _fail(ctx, "contract must be an object")
    unknown = sorted(set(raw) - _CONTRACT_KEYS)
    if unknown:
        _fail(ctx, "unknown keys %s" % unknown)
    source = parse_schema(raw.get("source_schema"), "source", tenant_id)
    target = parse_schema(raw.get("target_schema"), "target", tenant_id)
    ruleset_version = raw.get("ruleset_version")
    if not isinstance(ruleset_version, str) or not ruleset_version:
        _fail(ctx, "ruleset_version must be a non-empty string")
    rules_raw = raw.get("rules")
    if not isinstance(rules_raw, list):
        _fail(ctx, "rules must be a list")
    rules = tuple(_parse_rule(i, r, ctx) for i, r in enumerate(rules_raw))
    seen = set()
    for rule in rules:
        if rule.rule_id in seen:
            _fail(ctx, "duplicate rule id %r" % rule.rule_id)
        seen.add(rule.rule_id)
    _prove_dataflow(source, target, rules, ctx)
    digest = tagged(
        {
            "ruleset_version": ruleset_version,
            "source_schema": {
                "name": source.name,
                "version": source.version,
                "fields": {k: v.__dict__ for k, v in sorted(source.fields.items())},
                "allow_unknown": source.allow_unknown,
            },
            "target_schema": {
                "name": target.name,
                "version": target.version,
                "fields": {k: v.__dict__ for k, v in sorted(target.fields.items())},
                "allow_unknown": target.allow_unknown,
            },
            "rules": rules_raw,
        }
    )
    return TenantContract(
        tenant_id=tenant_id,
        source=source,
        target=target,
        ruleset_version=ruleset_version,
        rules=rules,
        rule_digest=digest,
    )


def parse_contracts(raw: Any) -> Tuple[Dict[str, TenantContract], List[str], List[str]]:
    """Parse every tenant contract, collecting per-tenant errors instead of
    failing the whole job: events of a broken tenant are quarantined with
    ``invalid_contract`` by the engine and the run status becomes ``failed``.

    Returns ``(contracts, errors, invalid_tenant_ids)``.
    """
    if not isinstance(raw, dict) or not raw:
        raise ValueError("tenant_contracts must be a non-empty object")
    contracts: Dict[str, TenantContract] = {}
    errors: List[str] = []
    invalid: List[str] = []
    for tenant_id, contract_raw in raw.items():
        if not isinstance(tenant_id, str) or not tenant_id:
            errors.append("tenant id must be a non-empty string")
            continue
        try:
            contracts[tenant_id] = parse_contract(tenant_id, contract_raw)
        except ContractError as exc:
            errors.append(str(exc))
            invalid.append(tenant_id)
    return contracts, errors, invalid
