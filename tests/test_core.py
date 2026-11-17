"""End-to-end tests for the payload contract migrator.

The suite is organised around the guarantees the migration contract promises:

* static proof of the source -> target transform before any event is touched;
* per-event migration semantics (split / merge / enum rename / unit change /
  defaults) and the rule-hit trace;
* idempotency, deduplication and duplicate delivery handling;
* explicit batch boundaries (isolate vs transactional vs single);
* deterministic, resumable sharding;
* replay from quarantine evidence, including tamper detection and target
  schema tightening;
* the report envelope, which is a migration report and not a generic DLQ.
"""
import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from payload_contract_migrator.digest import tagged
from payload_contract_migrator.engine import shard_of
from payload_contract_migrator.models import Event
from payload_contract_migrator.pipeline import run
from payload_contract_migrator.replay import build_replay_job

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "fixtures"


def contract_a(ruleset="a-r3", rules=None, source_extra=None, target_extra=None):
    """Tenant A: split a full name, rename an enum, convert USD -> EUR."""
    contract = {
        "source_schema": {
            "name": "profile",
            "version": "v1",
            "fields": {
                "id": {"type": "string", "required": True},
                "full_name": {"type": "string", "required": True},
                "status": {"type": "string", "required": False},
                "amount": {"type": "number", "required": True},
                "currency": {"type": "string", "required": False},
            },
        },
        "target_schema": {
            "name": "profile",
            "version": "v2",
            "fields": {
                "id": {"type": "string", "required": True},
                "first_name": {"type": "string", "required": True},
                "last_name": {"type": "string", "required": True},
                "status": {
                    "type": "string",
                    "required": True,
                    "enum": ["active", "inactive", "closed"],
                },
                "amount_eur": {"type": "number", "required": True},
                "currency": {
                    "type": "string",
                    "required": True,
                    "enum": ["USD", "EUR"],
                },
            },
        },
        "ruleset_version": ruleset,
        "rules": [
            {"id": "keep-id", "op": "copy", "source": "id"},
            {"id": "default-currency", "op": "default", "target": "currency", "value": "USD"},
            {
                "id": "split-name",
                "op": "split",
                "source": "full_name",
                "targets": ["first_name", "last_name"],
                "delimiter": " ",
            },
            {
                "id": "rename-status",
                "op": "enum_map",
                "source": "status",
                "map": {"A": "active", "I": "inactive", "C": "closed"},
                "on_unmapped": "quarantine",
            },
            {
                "id": "usd-to-eur",
                "op": "unit_convert",
                "source": "amount",
                "target": "amount_eur",
                "from_unit": "USD",
                "to_unit": "EUR",
                "unit_field": "currency",
                "rate": 0.9,
                "precision": 2,
            },
        ],
    }
    if rules is not None:
        contract["rules"] = rules
    contract["source_schema"]["fields"].update(source_extra or {})
    contract["target_schema"]["fields"].update(target_extra or {})
    return contract


def contract_b():
    """Tenant B: merge two names and convert cents -> EUR."""
    return {
        "source_schema": {
            "name": "txn",
            "version": "v1",
            "fields": {
                "id": {"type": "string", "required": True},
                "given_name": {"type": "string", "required": True},
                "family_name": {"type": "string", "required": True},
                "state": {"type": "string", "required": True},
                "total_cents": {"type": "integer", "required": True},
                "ccy": {"type": "string", "required": True},
            },
        },
        "target_schema": {
            "name": "txn",
            "version": "v2",
            "fields": {
                "id": {"type": "string", "required": True},
                "full_name": {"type": "string", "required": True},
                "state": {
                    "type": "string",
                    "required": True,
                    "enum": ["new", "done", "void"],
                },
                "total_eur": {"type": "number", "required": True},
                "ccy": {"type": "string", "required": True, "enum": ["EUR"]},
            },
        },
        "ruleset_version": "b-r1",
        "rules": [
            {"id": "keep-id", "op": "copy", "source": "id"},
            {
                "id": "merge-name",
                "op": "merge",
                "sources": ["given_name", "family_name"],
                "target": "full_name",
                "delimiter": " ",
            },
            {
                "id": "lower-state",
                "op": "enum_map",
                "source": "state",
                "map": {"NEW": "new", "DONE": "done", "VOID": "void"},
                "on_unmapped": "quarantine",
            },
            {
                "id": "cents-to-eur",
                "op": "unit_convert",
                "source": "total_cents",
                "target": "total_eur",
                "from_unit": "EUR",
                "to_unit": "EUR",
                "unit_field": "ccy",
                "rate": 0.01,
                "precision": 2,
            },
        ],
    }


def default_contracts():
    return {"tenant-a": contract_a(), "tenant-b": contract_b()}


def event(tenant, eid, version, payload, ruleset_version=None):
    ev = {
        "tenant_id": tenant,
        "event_id": eid,
        "schema_version": version,
        "payload": payload,
    }
    if ruleset_version is not None:
        ev["ruleset_version"] = ruleset_version
    return ev


def a1_payload():
    return {
        "id": "a1",
        "full_name": "Ada Lovelace",
        "status": "A",
        "amount": 10,
        "currency": "USD",
    }


def make_job(events, contracts=None, mode="batch", shard_count=None, resume=None, run_id=None):
    job = {
        "mode": mode,
        "tenant_contracts": default_contracts() if contracts is None else contracts,
        "events": events,
    }
    if shard_count is not None:
        job["shard_count"] = shard_count
    if resume is not None:
        job["resume"] = resume
    if run_id is not None:
        job["run_id"] = run_id
    return job


def outcomes_by_id(report, eid):
    return [o for o in report["outcomes"] if o["event_id"] == eid]


def one_outcome(report, eid):
    matches = outcomes_by_id(report, eid)
    assert len(matches) == 1, f"expected exactly one outcome for {eid}, got {len(matches)}"
    return matches[0]


def codes(outcome):
    return [r["code"] for r in outcome["reasons"]]


class TestContractProof(unittest.TestCase):
    def test_valid_contracts_parse(self):
        report = run(make_job([event("tenant-a", "e1", "v1", a1_payload())]))
        self.assertEqual(report["contract_errors"], [])
        self.assertEqual(set(report["tenants"]), {"tenant-a", "tenant-b"})
        self.assertTrue(report["tenants"]["tenant-a"]["rule_digest"].startswith("sha256:"))

    def test_rule_digest_changes_with_rule_order(self):
        rules = copy.deepcopy(contract_a()["rules"])
        reordered = [rules[0], rules[2], rules[3], rules[4], rules[1]]
        c1 = make_job(
            [event("tenant-a", "e1", "v1", a1_payload())],
            {"tenant-a": contract_a("a-r3")},
        )
        c2 = make_job(
            [event("tenant-a", "e1", "v1", a1_payload())],
            {"tenant-a": contract_a("a-r3", rules=reordered)},
        )
        self.assertNotEqual(
            run(c1)["tenants"]["tenant-a"]["rule_digest"],
            run(c2)["tenants"]["tenant-a"]["rule_digest"],
        )

    def test_rule_digest_changes_with_ruleset_version(self):
        j1 = make_job(
            [event("tenant-a", "e1", "v1", a1_payload())],
            {"tenant-a": contract_a("a-r3")},
        )
        j2 = make_job(
            [event("tenant-a", "e1", "v1", a1_payload())],
            {"tenant-a": contract_a("a-r4")},
        )
        self.assertNotEqual(
            run(j1)["tenants"]["tenant-a"]["rule_digest"],
            run(j2)["tenants"]["tenant-a"]["rule_digest"],
        )

    def test_rule_referencing_unavailable_source_is_rejected(self):
        rules = [{"id": "bad", "op": "copy", "source": "ghost", "target": "id"}]
        report = run(
            make_job(
                [event("tenant-a", "e1", "v1", a1_payload())],
                {"tenant-a": contract_a(rules=rules)},
            )
        )
        self.assertEqual(report["status"], "failed")
        self.assertTrue(any("ghost" in e for e in report["contract_errors"]))
        outcome = one_outcome(report, "e1")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["code"], "invalid_contract")

    def test_unconsumed_source_field_is_rejected(self):
        rules = [r for r in contract_a()["rules"] if r["id"] != "split-name"]
        report = run(
            make_job(
                [event("tenant-a", "e1", "v1", a1_payload())],
                {"tenant-a": contract_a(rules=rules)},
            )
        )
        self.assertEqual(report["status"], "failed")
        self.assertTrue(any("full_name" in e for e in report["contract_errors"]))

    def test_required_target_without_producer_is_rejected(self):
        report = run(
            make_job(
                [event("tenant-a", "e1", "v1", a1_payload())],
                {
                    "tenant-a": contract_a(
                        target_extra={"loyalty_tier": {"type": "string", "required": True}}
                    )
                },
            )
        )
        self.assertEqual(report["status"], "failed")
        self.assertTrue(any("loyalty_tier" in e for e in report["contract_errors"]))

    def test_enum_map_value_outside_target_enum_is_rejected(self):
        rules = copy.deepcopy(contract_a()["rules"])
        rules[3]["map"] = {"A": "active", "I": "inactive", "C": "archived"}
        report = run(
            make_job(
                [event("tenant-a", "e1", "v1", a1_payload())],
                {"tenant-a": contract_a(rules=rules)},
            )
        )
        self.assertEqual(report["status"], "failed")
        self.assertTrue(any("archived" in e for e in report["contract_errors"]))

    def test_broken_tenant_does_not_hide_healthy_tenant(self):
        contracts = default_contracts()
        contracts["tenant-a"] = contract_a(
            rules=[{"id": "bad", "op": "copy", "source": "ghost"}]
        )
        report = run(
            make_job(
                [
                    event("tenant-a", "e1", "v1", a1_payload()),
                    event(
                        "tenant-b",
                        "e2",
                        "v1",
                        {
                            "id": "b1",
                            "given_name": "Linus",
                            "family_name": "T",
                            "state": "NEW",
                            "total_cents": 5,
                            "ccy": "EUR",
                        },
                    ),
                ],
                contracts,
            )
        )
        self.assertEqual(report["status"], "failed")
        self.assertEqual(one_outcome(report, "e1")["status"], "quarantined")
        self.assertEqual(one_outcome(report, "e2")["status"], "migrated")


class TestMigrationSemantics(unittest.TestCase):
    def test_happy_path_split_enum_unit_default(self):
        report = run(make_job([event("tenant-a", "e1", "v1", a1_payload())]))
        self.assertEqual(report["status"], "ok")
        outcome = one_outcome(report, "e1")
        self.assertEqual(outcome["status"], "migrated")
        self.assertEqual(
            outcome["output"],
            {
                "id": "a1",
                "first_name": "Ada",
                "last_name": "Lovelace",
                "status": "active",
                "amount_eur": 9.0,
                "currency": "EUR",
            },
        )
        self.assertEqual(
            [t["rule_id"] for t in outcome["rule_trace"]],
            ["keep-id", "default-currency", "split-name", "rename-status", "usd-to-eur"],
        )
        self.assertEqual(
            outcome["compatibility"],
            {"stage": "target", "compatible": True, "issues": []},
        )

    def test_rule_trace_shows_default_skipped_when_present(self):
        report = run(make_job([event("tenant-a", "e1", "v1", a1_payload())]))
        trace = {t["rule_id"]: t for t in one_outcome(report, "e1")["rule_trace"]}
        self.assertEqual(trace["default-currency"]["result"], "skipped")

    def test_default_fills_missing_optional_field(self):
        payload = a1_payload()
        del payload["currency"]
        report = run(make_job([event("tenant-a", "e1", "v1", payload)]))
        outcome = one_outcome(report, "e1")
        self.assertEqual(outcome["status"], "migrated")
        trace = {t["rule_id"]: t for t in outcome["rule_trace"]}
        self.assertEqual(trace["default-currency"]["result"], "applied")
        self.assertEqual(outcome["output"]["amount_eur"], 9.0)

    def test_rule_order_changes_semantics(self):
        # Default currency *after* the conversion means the conversion sees no
        # currency at all and the unit guard fails.
        rules = copy.deepcopy(contract_a()["rules"])
        reordered = [rules[0], rules[2], rules[3], rules[4], rules[1]]
        payload = a1_payload()
        del payload["currency"]
        report = run(
            make_job(
                [event("tenant-a", "e1", "v1", payload)],
                {"tenant-a": contract_a(rules=reordered)},
            )
        )
        outcome = one_outcome(report, "e1")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["failure"], "illegal_unit")

    def test_tenant_b_merge_and_cents_conversion(self):
        report = run(
            make_job(
                [
                    event(
                        "tenant-b",
                        "b1",
                        "v1",
                        {
                            "id": "b1",
                            "given_name": "Linus",
                            "family_name": "Torvalds",
                            "state": "NEW",
                            "total_cents": 12345,
                            "ccy": "EUR",
                        },
                    )
                ]
            )
        )
        outcome = one_outcome(report, "b1")
        self.assertEqual(outcome["status"], "migrated")
        self.assertEqual(
            outcome["output"],
            {
                "id": "b1",
                "full_name": "Linus Torvalds",
                "state": "new",
                "total_eur": 123.45,
                "ccy": "EUR",
            },
        )

    def test_split_arity_mismatch_quarantines(self):
        report = run(
            make_job(
                [
                    event(
                        "tenant-a",
                        "e1",
                        "v1",
                        {
                            "id": "a1",
                            "full_name": "Ada",
                            "status": "A",
                            "amount": 1,
                            "currency": "USD",
                        },
                    )
                ]
            )
        )
        outcome = one_outcome(report, "e1")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["failure"], "arity_mismatch")

    def test_split_arity_pad_policy(self):
        rules = copy.deepcopy(contract_a()["rules"])
        rules[2]["on_arity_mismatch"] = "pad"
        report = run(
            make_job(
                [
                    event(
                        "tenant-a",
                        "e1",
                        "v1",
                        {
                            "id": "a1",
                            "full_name": "Ada",
                            "status": "A",
                            "amount": 1,
                            "currency": "USD",
                        },
                    )
                ],
                {"tenant-a": contract_a(rules=rules)},
            )
        )
        outcome = one_outcome(report, "e1")
        self.assertEqual(outcome["status"], "migrated")
        self.assertEqual(outcome["output"]["last_name"], "")

    def test_merge_missing_optional_source_fails_at_rule_level(self):
        contract = contract_b()
        contract["source_schema"]["fields"]["family_name"] = {
            "type": "string",
            "required": False,
        }
        report = run(
            make_job(
                [
                    event(
                        "tenant-b",
                        "b1",
                        "v1",
                        {
                            "id": "b1",
                            "given_name": "Ken",
                            "state": "DONE",
                            "total_cents": 1,
                            "ccy": "EUR",
                        },
                    )
                ],
                {"tenant-b": contract},
            )
        )
        outcome = one_outcome(report, "b1")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["failure"], "merge_source_missing")

    def test_enum_map_default_policy(self):
        rules = copy.deepcopy(contract_a()["rules"])
        rules[3]["on_unmapped"] = "default"
        rules[3]["default"] = "inactive"
        report = run(
            make_job(
                [
                    event(
                        "tenant-a",
                        "e1",
                        "v1",
                        {
                            "id": "a1",
                            "full_name": "Ada Lovelace",
                            "status": "Z",
                            "amount": 1,
                            "currency": "USD",
                        },
                    )
                ],
                {"tenant-a": contract_a(rules=rules)},
            )
        )
        outcome = one_outcome(report, "e1")
        self.assertEqual(outcome["status"], "migrated")
        self.assertEqual(outcome["output"]["status"], "inactive")

    def test_unknown_field_quarantined_not_silent(self):
        payload = dict(a1_payload(), nickname="ada")
        outcome = one_outcome(run(make_job([event("tenant-a", "e1", "v1", payload)])), "e1")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["code"], "unknown_field")
        self.assertEqual(outcome["reasons"][0]["field"], "nickname")

    def test_missing_required_source_field(self):
        payload = a1_payload()
        del payload["full_name"]
        outcome = one_outcome(run(make_job([event("tenant-a", "e1", "v1", payload)])), "e1")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["code"], "missing_required")

    def test_non_numeric_amount_rejected_at_source(self):
        payload = dict(a1_payload(), amount="bad")
        outcome = one_outcome(run(make_job([event("tenant-a", "e1", "v1", payload)])), "e1")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["code"], "type_mismatch")
        self.assertEqual(outcome["reasons"][0]["field"], "amount")

    def test_illegal_unit_quarantined(self):
        payload = dict(a1_payload(), currency="JPY")
        outcome = one_outcome(run(make_job([event("tenant-a", "e1", "v1", payload)])), "e1")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["failure"], "illegal_unit")

    def test_unmapped_enum_quarantined(self):
        payload = dict(a1_payload(), status="X")
        outcome = one_outcome(run(make_job([event("tenant-a", "e1", "v1", payload)])), "e1")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["failure"], "unmapped_enum")

    def test_missing_optional_field_fails_target_compatibility(self):
        payload = a1_payload()
        del payload["status"]
        outcome = one_outcome(run(make_job([event("tenant-a", "e1", "v1", payload)])), "e1")
        self.assertEqual(outcome["status"], "quarantined")
        seen = codes(outcome)
        self.assertIn("incompatible_target", seen)
        self.assertIn("missing_required", seen)
        self.assertEqual(outcome["compatibility"]["stage"], "target")

    def test_unknown_tenant_quarantined(self):
        outcome = one_outcome(
            run(make_job([event("tenant-z", "e1", "v1", {"id": "x"})])), "e1"
        )
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["code"], "unknown_tenant")

    def test_unsupported_schema_version(self):
        outcome = one_outcome(
            run(make_job([event("tenant-a", "e1", "v9", a1_payload())])), "e1"
        )
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["code"], "unsupported_schema_version")


class TestIdempotencyAndDedup(unittest.TestCase):
    def migrated_output(self):
        report = run(make_job([event("tenant-a", "e1", "v1", a1_payload())]))
        return one_outcome(report, "e1")["output"]

    def test_already_migrated_event_bypasses_without_retransform(self):
        output = self.migrated_output()
        report = run(
            make_job([event("tenant-a", "e2", "v2", output, ruleset_version="a-r3")])
        )
        outcome = one_outcome(report, "e2")
        self.assertEqual(outcome["status"], "bypassed")
        self.assertEqual(outcome["output"], output)
        # A second 0.9 conversion would have produced 8.1, not 9.0.
        self.assertEqual(outcome["output"]["amount_eur"], 9.0)

    def test_rerun_of_same_batch_is_stable(self):
        job = make_job(
            [event("tenant-a", "e1", "v1", a1_payload())], run_id="run-fixed"
        )
        self.assertEqual(run(job), run(job))

    def test_ruleset_conflict_not_silent(self):
        output = self.migrated_output()
        report = run(
            make_job([event("tenant-a", "e2", "v2", output, ruleset_version="a-r2")])
        )
        outcome = one_outcome(report, "e2")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["code"], "ruleset_conflict")

    def test_missing_ruleset_proof_quarantined(self):
        output = self.migrated_output()
        report = run(make_job([event("tenant-a", "e2", "v2", output)]))
        outcome = one_outcome(report, "e2")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["code"], "missing_ruleset_proof")

    def test_bypass_validates_against_current_target_schema(self):
        output = self.migrated_output()
        del output["last_name"]
        report = run(
            make_job([event("tenant-a", "e2", "v2", output, ruleset_version="a-r3")])
        )
        outcome = one_outcome(report, "e2")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["code"], "bypass_validation_failed")

    def test_identical_redelivery_deduplicated(self):
        ev = event("tenant-a", "e1", "v1", a1_payload())
        report = run(make_job([ev, copy.deepcopy(ev)]))
        outcomes = outcomes_by_id(report, "e1")
        self.assertEqual([o["status"] for o in outcomes], ["migrated", "duplicate"])
        self.assertFalse(outcomes[1]["conflicting"])
        self.assertEqual(report["counts"]["duplicate"], 1)
        self.assertEqual(report["status"], "ok")

    def test_conflicting_duplicate_is_visible(self):
        ev1 = event("tenant-a", "e1", "v1", a1_payload())
        ev2 = event("tenant-a", "e1", "v1", dict(a1_payload(), amount=999))
        report = run(make_job([ev1, ev2]))
        dup = [o for o in outcomes_by_id(report, "e1") if o["status"] == "duplicate"][0]
        self.assertTrue(dup["conflicting"])
        self.assertEqual(report["status"], "partial")

    def test_input_order_does_not_change_outcomes(self):
        events = [
            event("tenant-a", "e1", "v1", a1_payload()),
            event("tenant-a", "e2", "v1", dict(a1_payload(), id="a2", amount="bad")),
            event(
                "tenant-b",
                "e3",
                "v1",
                {
                    "id": "b1",
                    "given_name": "L",
                    "family_name": "T",
                    "state": "NEW",
                    "total_cents": 5,
                    "ccy": "EUR",
                },
            ),
            event("tenant-a", "e4", "v1", dict(a1_payload(), id="a4", currency="JPY")),
        ]

        def normalized(report):
            report = copy.deepcopy(report)
            cleaned = []
            for outcome in report["outcomes"]:
                outcome.pop("seq", None)
                outcome.pop("duplicate_of", None)
                cleaned.append(outcome)
            report["outcomes"] = sorted(
                cleaned,
                key=lambda o: (
                    o["tenant_id"],
                    o["event_id"],
                    o["evidence"]["payload_digest"],
                    o["status"],
                ),
            )
            return report

        forward = run(make_job(events, run_id="run-order"))
        backward = run(make_job(list(reversed(events)), run_id="run-order"))
        self.assertEqual(normalized(forward), normalized(backward))


class TestBatchBoundary(unittest.TestCase):
    def test_partial_failure_is_explicit(self):
        report = run(
            make_job(
                [
                    event("tenant-a", "good", "v1", a1_payload()),
                    event("tenant-a", "bad", "v1", dict(a1_payload(), id="a2", amount="bad")),
                ]
            )
        )
        self.assertEqual(report["status"], "partial")
        self.assertEqual(report["counts"]["migrated"], 1)
        self.assertEqual(report["counts"]["quarantined"], 1)
        self.assertEqual(one_outcome(report, "good")["status"], "migrated")
        self.assertEqual(one_outcome(report, "bad")["status"], "quarantined")

    def test_isolate_policy_commits_survivors(self):
        job = make_job(
            [
                event("tenant-a", "good", "v1", a1_payload()),
                event("tenant-a", "bad", "v1", dict(a1_payload(), id="a2", amount="bad")),
            ]
        )
        job["batch_policy"] = "isolate"
        report = run(job)
        self.assertTrue(one_outcome(report, "good")["committed"])
        self.assertTrue(one_outcome(report, "bad")["committed"])

    def test_transactional_policy_aborts_whole_batch(self):
        job = make_job(
            [
                event("tenant-a", "good", "v1", a1_payload()),
                event("tenant-a", "bad", "v1", dict(a1_payload(), id="a2", amount="bad")),
            ]
        )
        job["batch_policy"] = "transactional"
        report = run(job)
        self.assertEqual(report["status"], "aborted")
        self.assertTrue(all(o["committed"] is False for o in report["outcomes"]))

    def test_single_mode_requires_exactly_one_event(self):
        report = run(
            make_job(
                [
                    event("tenant-a", "e1", "v1", a1_payload()),
                    event("tenant-a", "e2", "v1", a1_payload()),
                ],
                mode="single",
            )
        )
        self.assertEqual(report["status"], "failed")
        self.assertTrue(any("single mode" in e for e in report["errors"]))

    def test_single_mode_migrates_one_event(self):
        report = run(make_job([event("tenant-a", "e1", "v1", a1_payload())], mode="single"))
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["mode"], "single")

    def test_structural_job_errors_fail_loudly(self):
        report = run({"tenant_contracts": default_contracts(), "events": "not-a-list"})
        self.assertEqual(report["status"], "failed")
        self.assertTrue(report["errors"])
        report = run(make_job([], shard_count=2))
        self.assertEqual(report["status"], "failed")
        self.assertTrue(any("shard_count" in e for e in report["errors"]))

    def test_unknown_keys_are_warnings_not_silent(self):
        job = make_job([event("tenant-a", "e1", "v1", a1_payload())], run_id="run-warn")
        job["typo_key"] = True
        job["events"][0]["another_typo"] = 1
        report = run(job)
        self.assertEqual(report["status"], "ok")
        self.assertTrue(any("typo_key" in w for w in report["warnings"]))
        self.assertTrue(any("another_typo" in w for w in report["warnings"]))


class TestSharding(unittest.TestCase):
    def shard_events(self):
        return [
            event("tenant-a", "e%d" % i, "v1", dict(a1_payload(), id="a%d" % i))
            for i in range(6)
        ]

    def test_sharding_is_deterministic(self):
        job = make_job(self.shard_events(), mode="sharded", shard_count=2, run_id="run-shard")
        first, second = run(job), run(job)
        self.assertEqual(first, second)
        self.assertEqual(len(first["shards"]), 2)
        for shard in first["shards"]:
            self.assertTrue(shard["checkpoint"].startswith("sha256:"))
            self.assertFalse(shard["resumed"])

    def test_shard_assignment_is_order_independent(self):
        events = self.shard_events()
        j1 = make_job(events, mode="sharded", shard_count=2, run_id="run-shard")
        j2 = make_job(
            list(reversed(events)), mode="sharded", shard_count=2, run_id="run-shard"
        )
        keys1 = {s["shard"]: s["event_keys"] for s in run(j1)["shards"]}
        keys2 = {s["shard"]: s["event_keys"] for s in run(j2)["shards"]}
        self.assertEqual(keys1, keys2)

    def test_resume_skips_completed_shards(self):
        events = self.shard_events()
        full = run(make_job(events, mode="sharded", shard_count=2, run_id="run-shard"))
        shard0 = next(s for s in full["shards"] if s["shard"] == 0)
        resume = {
            "shard_count": 2,
            "completed": [
                {
                    "shard": 0,
                    "checkpoint": shard0["checkpoint"],
                    "event_keys": shard0["event_keys"],
                    "counts": shard0["counts"],
                }
            ],
        }
        resumed = run(
            make_job(
                events,
                mode="sharded",
                shard_count=2,
                resume=resume,
                run_id="run-shard-resume",
            )
        )
        self.assertEqual(resumed["counts"]["skipped_resume"], len(shard0["event_keys"]))
        for outcome in resumed["outcomes"]:
            key = f"{outcome['tenant_id']}:{outcome['event_id']}"
            if key in shard0["event_keys"]:
                self.assertEqual(outcome["status"], "skipped_resume")
            else:
                original = one_outcome(full, outcome["event_id"])
                self.assertEqual(outcome["status"], original["status"])
                self.assertEqual(outcome["output"], original["output"])
        resumed_shard0 = next(s for s in resumed["shards"] if s["shard"] == 0)
        self.assertTrue(resumed_shard0["resumed"])
        self.assertEqual(resumed_shard0["checkpoint"], shard0["checkpoint"])

    def test_late_arrival_to_sealed_shard_quarantines(self):
        ev = event("tenant-a", "late-1", "v1", a1_payload())
        idx = shard_of(
            Event(seq=0, tenant_id="tenant-a", event_id="late-1", schema_version="v1", payload={}),
            2,
        )
        resume = {
            "shard_count": 2,
            "completed": [{"shard": idx, "checkpoint": "sha256:sealed", "event_keys": []}],
        }
        report = run(
            make_job([ev], mode="sharded", shard_count=2, resume=resume)
        )
        outcome = one_outcome(report, "late-1")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["code"], "shard_sealed")

    def test_shard_layout_change_fails_loudly(self):
        resume = {"shard_count": 3, "completed": []}
        report = run(
            make_job(
                self.shard_events(),
                mode="sharded",
                shard_count=2,
                resume=resume,
            )
        )
        self.assertEqual(report["status"], "failed")
        self.assertTrue(any("shard_layout_changed" in e for e in report["errors"]))


class TestReplay(unittest.TestCase):
    def quarantined_report(self):
        return run(
            make_job(
                [
                    event("tenant-a", "e-bad-enum", "v1", dict(a1_payload(), status="X")),
                    event("tenant-a", "e-bad-unit", "v1", dict(a1_payload(), id="a2", currency="JPY")),
                ],
                run_id="run-original",
            )
        )

    def test_failed_events_replay_from_original_payload(self):
        original = self.quarantined_report()
        self.assertEqual(original["status"], "partial")
        fixed = contract_a("a-r4")
        fixed["rules"][3]["map"]["X"] = "closed"
        replay_job = build_replay_job(
            original, {"tenant-a": fixed}, event_ids=["e-bad-enum"]
        )
        replayed = run(replay_job)
        outcome = one_outcome(replayed, "e-bad-enum")
        self.assertEqual(outcome["status"], "migrated")
        self.assertEqual(outcome["output"]["status"], "closed")
        self.assertTrue(outcome["evidence"]["replay_verified"])
        self.assertEqual(outcome["evidence"]["ruleset_version"], "a-r4")

    def test_replay_detects_tampered_payload(self):
        original = self.quarantined_report()
        replay_job = build_replay_job(
            original, {"tenant-a": contract_a()}, event_ids=["e-bad-enum"]
        )
        replay_job["events"][0]["payload"]["amount"] = 999
        outcome = one_outcome(run(replay_job), "e-bad-enum")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["code"], "replay_evidence_mismatch")

    def test_replay_under_same_contract_fails_identically(self):
        original = self.quarantined_report()
        replay_job = build_replay_job(
            original, {"tenant-a": contract_a()}, event_ids=["e-bad-unit"]
        )
        outcome = one_outcome(run(replay_job), "e-bad-unit")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["failure"], "illegal_unit")
        self.assertTrue(outcome["evidence"]["replay_verified"])

    def test_target_tightening_with_default_allows_replay(self):
        original = self.quarantined_report()
        tightened = contract_a(
            "a-r4",
            target_extra={"loyalty_tier": {"type": "string", "required": True}},
        )
        tightened["rules"].append(
            {"id": "default-tier", "op": "default", "target": "loyalty_tier", "value": "standard"}
        )
        replay_job = build_replay_job(
            original, {"tenant-a": tightened}, event_ids=["e-bad-unit"]
        )
        # Still a bad unit: replay must not paper over the original failure.
        bad = one_outcome(run(replay_job), "e-bad-unit")
        self.assertEqual(bad["status"], "quarantined")
        good_report = run(
            make_job(
                [event("tenant-a", "e-good", "v1", a1_payload())],
                {"tenant-a": tightened},
            )
        )
        good = one_outcome(good_report, "e-good")
        self.assertEqual(good["status"], "migrated")
        self.assertEqual(good["output"]["loyalty_tier"], "standard")

    def test_target_tightening_surfaces_missing_required_at_runtime(self):
        tightened = contract_a(
            "a-r4",
            source_extra={"tier": {"type": "string", "required": False}},
            target_extra={"loyalty_tier": {"type": "string", "required": True}},
        )
        tightened["rules"].append(
            {"id": "copy-tier", "op": "copy", "source": "tier", "target": "loyalty_tier"}
        )
        report = run(
            make_job(
                [event("tenant-a", "e1", "v1", a1_payload())],
                {"tenant-a": tightened},
            )
        )
        outcome = one_outcome(report, "e1")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertIn("missing_required", codes(outcome))

    def test_tightened_ruleset_conflicts_with_old_migrated_events(self):
        migrated = one_outcome(
            run(make_job([event("tenant-a", "e1", "v1", a1_payload())])), "e1"
        )["output"]
        tightened = contract_a(
            "a-r4",
            target_extra={"loyalty_tier": {"type": "string", "required": True}},
        )
        tightened["rules"].append(
            {"id": "default-tier", "op": "default", "target": "loyalty_tier", "value": "standard"}
        )
        report = run(
            make_job(
                [event("tenant-a", "e1", "v2", migrated, ruleset_version="a-r3")],
                {"tenant-a": tightened},
            )
        )
        outcome = one_outcome(report, "e1")
        self.assertEqual(outcome["status"], "quarantined")
        self.assertEqual(outcome["reasons"][0]["code"], "ruleset_conflict")


class TestReportContract(unittest.TestCase):
    def test_report_type_and_envelope(self):
        report = run(make_job([event("tenant-a", "e1", "v1", a1_payload())]))
        self.assertEqual(report["report_type"], "contract_migration_report")
        self.assertEqual(report["report_version"], "1")
        self.assertNotIn("dead_letter", report)
        self.assertNotIn("dead_letters", report)

    def test_success_evidence_is_auditable(self):
        report = run(make_job([event("tenant-a", "e1", "v1", a1_payload())]))
        evidence = one_outcome(report, "e1")["evidence"]
        self.assertEqual(evidence["target_schema_version"], "v2")
        self.assertEqual(evidence["ruleset_version"], "a-r3")
        self.assertTrue(evidence["rule_digest"].startswith("sha256:"))
        self.assertEqual(evidence["payload_digest"], tagged(a1_payload()))
        self.assertEqual(evidence["original_payload"], a1_payload())

    def test_quarantine_entries_carry_replayable_evidence(self):
        report = run(
            make_job([event("tenant-a", "e1", "v1", dict(a1_payload(), amount="bad"))])
        )
        outcome = one_outcome(report, "e1")
        self.assertEqual(outcome["status"], "quarantined")
        evidence = outcome["evidence"]
        self.assertEqual(evidence["original_payload"]["amount"], "bad")
        self.assertEqual(evidence["payload_digest"], tagged(evidence["original_payload"]))
        self.assertEqual(evidence["source_schema_version"], "v1")
        self.assertEqual(evidence["ruleset_version"], "a-r3")
        self.assertTrue(outcome["reasons"])

    def test_run_id_defaults_to_content_digest(self):
        job = make_job([event("tenant-a", "e1", "v1", a1_payload())])
        report = run(job)
        self.assertTrue(report["run_id"].startswith("run-"))
        self.assertEqual(report["run_id"], run(job)["run_id"])


class TestFixturesAndCli(unittest.TestCase):
    def test_input_fixture(self):
        payload = json.loads((FIXTURES / "input.json").read_text())
        report = run(payload)
        self.assertEqual(report["status"], "partial")
        self.assertEqual(
            report["counts"],
            {
                "received": 12,
                "migrated": 2,
                "bypassed": 1,
                "quarantined": 8,
                "duplicate": 1,
                "skipped_resume": 0,
            },
        )

    def test_replay_fixture_recovers_quarantined_event(self):
        payload = json.loads((FIXTURES / "replay.json").read_text())
        report = run(payload)
        self.assertEqual(report["status"], "ok")
        outcome = one_outcome(report, "evt-a8")
        self.assertEqual(outcome["status"], "migrated")
        self.assertTrue(outcome["evidence"]["replay_verified"])
        self.assertEqual(outcome["output"]["status"], "inactive")
        self.assertEqual(outcome["output"]["amount_eur"], 29.7)

    def _run_cli(self, path):
        env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
        return subprocess.run(
            [sys.executable, "-m", "payload_contract_migrator.cli", str(path)],
            capture_output=True,
            text=True,
            env=env,
        )

    def test_cli_exit_codes(self):
        with tempfile.TemporaryDirectory() as tmp:
            ok_job = Path(tmp) / "ok.json"
            ok_job.write_text(
                json.dumps(make_job([event("tenant-a", "e1", "v1", a1_payload())]))
            )
            bad_job = Path(tmp) / "bad.json"
            bad_job.write_text(json.dumps({"events": "nope"}))

            ok = self._run_cli(ok_job)
            self.assertEqual(ok.returncode, 0)
            self.assertEqual(json.loads(ok.stdout)["status"], "ok")

            partial = self._run_cli(FIXTURES / "input.json")
            self.assertEqual(partial.returncode, 1)
            self.assertEqual(json.loads(partial.stdout)["status"], "partial")

            failed = self._run_cli(bad_job)
            self.assertEqual(failed.returncode, 2)
            self.assertEqual(json.loads(failed.stdout)["status"], "failed")


if __name__ == "__main__":
    unittest.main()
