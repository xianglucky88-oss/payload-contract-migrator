import json
import unittest
from pathlib import Path
from payload_contract_migrator.pipeline import run
from payload_contract_migrator.parser import load_records
from payload_contract_migrator.engine import calculate

def fixture():
    return json.loads((Path(__file__).parents[1] / "fixtures" / "input.json").read_text())

class TestCore(unittest.TestCase):
    def test_pipeline_and_modules(self):
        payload = fixture()
        rows = load_records(payload)
        self.assertTrue(rows)
        data = calculate(rows, payload)
        self.assertIsInstance(data, dict)
        self.assertTrue(data)
        result = run(payload)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["project"], 'payload-contract-migrator')

        assert len(result["migrated"]) == 1 and len(result["quarantined"]) == 1
