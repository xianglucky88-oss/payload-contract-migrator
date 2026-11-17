"""Orchestration boundary: parse the job, run the engine, always emit a report."""
from typing import Any, Dict

from .engine import run_job
from .parser import JobError, parse_job
from .report import build_failure_report


def run(payload: Any) -> Dict[str, Any]:
    try:
        job = parse_job(payload)
    except JobError as exc:
        return build_failure_report(payload, str(exc))
    return run_job(job)
