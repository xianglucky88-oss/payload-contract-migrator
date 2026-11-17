"""Ordered migration rules applied to a working document.

Rule order is semantic: each rule reads and writes a single working document,
so ``default currency`` before ``unit_convert`` means something different from
the reverse.  Every rule appends a trace entry (applied / skipped / failed) —
the rule-hit trajectory attached to each outcome.  A rule that cannot be
applied raises :class:`RuleFailure`; the engine turns it into a quarantine
with the partial trace preserved.
"""
from copy import deepcopy
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, List

from .models import Rule


class RuleFailure(Exception):
    """A rule could not be applied to this event; the event must be quarantined."""

    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


def _trace(trace: List[Dict[str, Any]], rule: Rule, result: str, detail: str) -> None:
    trace.append(
        {
            "seq": rule.seq,
            "rule_id": rule.rule_id,
            "op": rule.op,
            "result": result,
            "detail": detail,
        }
    )


def _convert_number(value: Any, rate: Any, precision: Any):
    """Deterministic decimal conversion (no binary float drift in the report)."""
    result = Decimal(str(value)) * Decimal(str(rate))
    if precision is not None:
        result = result.quantize(Decimal(1).scaleb(-precision), rounding=ROUND_HALF_UP)
    return float(result)


def apply_rule(doc: Dict[str, Any], rule: Rule, trace: List[Dict[str, Any]]) -> None:
    p = rule.params
    op = rule.op

    if op == "default":
        target = p["target"]
        if target not in doc or doc[target] is None:
            doc[target] = deepcopy(p["value"])
            _trace(trace, rule, "applied", "filled missing %r with declared default" % target)
        else:
            _trace(trace, rule, "skipped", "%r already present; default not applied" % target)
        return

    source = p.get("source")
    if op == "merge":
        missing = [s for s in p["sources"] if s not in doc]
        if missing:
            _trace(trace, rule, "failed", "merge sources %s missing" % missing)
            raise RuleFailure(
                "merge_source_missing", "merge sources %s are missing" % missing
            )
        merged = p["delimiter"].join(str(doc[s]) for s in p["sources"])
        for s in p["sources"]:
            doc.pop(s, None)
        doc[p["target"]] = merged
        _trace(trace, rule, "applied", f"merged {p['sources']} into {p['target']!r}")
        return

    if source not in doc:
        _trace(trace, rule, "skipped", "source %r missing (optional field)" % source)
        return

    if op == "copy":
        target = p["target"]
        doc[target] = doc[source]
        if source != target:
            del doc[source]
            _trace(trace, rule, "applied", f"renamed {source!r} to {target!r}")
        else:
            _trace(trace, rule, "applied", "passed %r through unchanged" % source)
        return

    if op == "split":
        value = doc[source]
        if not isinstance(value, str):
            _trace(
                trace,
                rule,
                "failed",
                f"source {source!r} is {type(value).__name__}, not a string",
            )
            raise RuleFailure(
                "not_a_string", "cannot split non-string field %r" % source
            )
        parts = value.split(p["delimiter"])
        targets = p["targets"]
        if len(parts) != len(targets):
            if p["on_arity_mismatch"] == "pad":
                parts = (parts + [""] * len(targets))[: len(targets)]
            else:
                _trace(
                    trace,
                    rule,
                    "failed",
                    "split of %r produced %d parts, expected %d"
                    % (source, len(parts), len(targets)),
                )
                raise RuleFailure(
                    "arity_mismatch",
                    "split of %r produced %d parts, expected %d"
                    % (source, len(parts), len(targets)),
                )
        del doc[source]
        for name, part in zip(targets, parts):
            doc[name] = part
        _trace(trace, rule, "applied", f"split {source!r} into {targets}")
        return

    if op == "enum_map":
        value = doc[source]
        key = value if isinstance(value, str) else str(value)
        target = p["target"]
        if key in p["map"]:
            mapped = p["map"][key]
        elif p["on_unmapped"] == "default":
            mapped = p["default"]
            _trace(
                trace,
                rule,
                "applied",
                f"unmapped value {value!r} replaced by declared default",
            )
            doc[target] = mapped
            if source != target:
                del doc[source]
            return
        else:
            _trace(trace, rule, "failed", f"value {value!r} has no mapping")
            raise RuleFailure(
                "unmapped_enum", f"value {value!r} of field {source!r} has no mapping"
            )
        doc[target] = mapped
        if source != target:
            del doc[source]
        _trace(trace, rule, "applied", f"mapped {source!r}={value!r} to {mapped!r}")
        return

    if op == "unit_convert":
        value = doc[source]
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            _trace(trace, rule, "failed", "source %r is not numeric" % source)
            raise RuleFailure("not_numeric", "field %r is not numeric" % source)
        unit_field = p["unit_field"]
        if unit_field is not None:
            actual = doc.get(unit_field)
            if actual != p["from_unit"]:
                _trace(
                    trace,
                    rule,
                    "failed",
                    f"unit {actual!r} does not match declared from_unit {p['from_unit']!r}",
                )
                raise RuleFailure(
                    "illegal_unit",
                    f"field {unit_field!r} declares unit {actual!r},"
                    f" expected {p['from_unit']!r}",
                )
        converted = _convert_number(value, p["rate"], p["precision"])
        target = p["target"]
        doc[target] = converted
        if source != target:
            del doc[source]
        if unit_field is not None:
            doc[unit_field] = p["to_unit"]
        _trace(
            trace,
            rule,
            "applied",
            f"converted {source!r} from {p['from_unit']} to {p['to_unit']}"
            f" at rate {p['rate']}",
        )
        return

    _trace(trace, rule, "failed", "unknown op %r" % op)
    raise RuleFailure("unknown_op", "unknown op %r" % op)
