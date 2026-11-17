import argparse
import json
import sys
from pathlib import Path

from .pipeline import run

EXIT_CODES = {"ok": 0, "partial": 1, "aborted": 1, "failed": 2}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate versioned tenant payloads with explicit quarantine and replay semantics."
    )
    parser.add_argument(
        "input",
        nargs="?",
        default=str(Path(__file__).parents[2] / "fixtures" / "input.json"),
    )
    args = parser.parse_args()
    payload = json.loads(Path(args.input).read_text())
    report = run(payload)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    sys.exit(EXIT_CODES.get(report["status"], 2))


if __name__ == "__main__":
    main()
