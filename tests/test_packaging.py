import re
from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_runtime_and_test_requirements_are_exactly_pinned() -> None:
    for filename in ("requirements.txt", "requirements-test.txt"):
        lines = [
            line.strip()
            for line in (ROOT / filename).read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]
        assert lines
        assert all(
            re.match(r"^[A-Za-z0-9_.-]+(?:\[[A-Za-z0-9_,.-]+\])?==[^=<>~!]+$", line)
            for line in lines
        )


def test_operator_runbook_documents_secure_connections_and_backfill() -> None:
    text = (ROOT / "README.md").read_text()
    for required in (
        "architron_monitoring_gcp",
        "architron_monitoring_azure_blob",
        "architron_monitoring_postgres",
        "architron_monitoring_smtp",
        "data interval",
        "maximum_bytes_billed",
        "backfill",
        "never",
    ):
        assert required in text
