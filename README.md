# Payload Contract Migrator

Migrate versioned tenant payloads with explicit quarantine and replay semantics.

## Modules

- `src/payload_contract_migrator/models.py`: immutable result contract.
- `src/payload_contract_migrator/parser.py`: input validation and record extraction.
- `src/payload_contract_migrator/engine.py`: domain calculation only.
- `src/payload_contract_migrator/report.py`: stable output envelope.
- `src/payload_contract_migrator/pipeline.py`: orchestration boundary.
- `src/payload_contract_migrator/cli.py`: deterministic JSON CLI.

## Run

```bash
PYTHONPATH=src python -m payload_contract_migrator.cli
PYTHONPATH=src python -m unittest discover -s tests -v
```

The fixture is local and deterministic; no network or credentials are required.
