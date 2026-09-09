from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from architron_monitoring_airflow.azure import (
    AzureBlob,
    ManifestEntry,
    build_period_replacement,
    classify_blob,
)
from architron_monitoring_airflow.dq import CheckResult, CheckSeverity, gate_publication
from architron_monitoring_airflow.hashing import deterministic_key
from architron_monitoring_airflow.normalization import normalize_reporting_currency
from architron_monitoring_airflow.reconciliation import reconcile
from architron_monitoring_airflow.retention import retention_cutoffs


def test_deterministic_key_is_order_independent_and_secret_safe() -> None:
    first = deterministic_key({"project": "p", "cost": Decimal("1.20"), "day": date(2026, 8, 1)})
    second = deterministic_key({"day": date(2026, 8, 1), "cost": Decimal("1.20"), "project": "p"})
    assert first == second
    assert len(first) == 64
    with pytest.raises(ValueError, match="secret"):
        deterministic_key({"client_secret": "must-not-hash"})


def test_republished_azure_blob_is_reprocessed_only_when_identity_changes() -> None:
    blob = AzureBlob(
        path="exports/20260801-20260831/part-0.csv",
        etag='"etag-new"',
        size=500,
        last_modified=datetime(2026, 8, 16, tzinfo=UTC),
        period_start=date(2026, 8, 1),
        period_end=date(2026, 8, 31),
        schema_version="2025-03-01",
    )
    unchanged = ManifestEntry(blob.path, '"etag-new"', 500, "PUBLISHED")
    replaced = ManifestEntry(blob.path, '"etag-old"', 450, "PUBLISHED")

    assert classify_blob(blob, unchanged).action == "SKIP"
    assert classify_blob(blob, replaced).action == "REPLACE_PERIOD"
    assert classify_blob(blob, None).action == "REPLACE_PERIOD"


def test_azure_mtd_publication_replaces_whole_period_transactionally() -> None:
    blob = AzureBlob(
        path="exports/202608/part.csv",
        etag="abc",
        size=10,
        last_modified=datetime(2026, 8, 16, tzinfo=UTC),
        period_start=date(2026, 8, 1),
        period_end=date(2026, 8, 31),
        schema_version="2025-03-01",
    )
    plan = build_period_replacement(blob, expected_schema_version="2025-03-01")
    assert plan.period_start == date(2026, 8, 1)
    assert (
        plan.delete_predicate
        == "provider = %(provider)s AND usage_date >= %(start)s AND usage_date < %(end)s"
    )
    assert plan.parameters["end"] == date(2026, 9, 1)
    with pytest.raises(ValueError, match="schema"):
        build_period_replacement(blob, expected_schema_version="future")


def test_currency_normalization_never_fabricates_idr() -> None:
    idr = normalize_reporting_currency(Decimal("100"), "idr")
    unsupported = normalize_reporting_currency(Decimal("1.5"), "USD")
    supplied = normalize_reporting_currency(
        Decimal("1.5"),
        "USD",
        provider_reporting_cost=Decimal("24000"),
        provider_reporting_currency="IDR",
    )
    assert idr.reporting_cost == Decimal("100")
    assert idr.status == "SUPPORTED"
    assert unsupported.reporting_cost is None
    assert unsupported.status == "UNSUPPORTED_REPORTING_CURRENCY"
    assert supplied.reporting_cost == Decimal("24000")
    assert supplied.method == "PROVIDER_SUPPLIED"


def test_reconciliation_uses_decimal_tolerance_and_records_both_totals() -> None:
    result = reconcile(10, Decimal("100.00"), 10, Decimal("100.01"), Decimal("0.02"))
    assert result.passed is True
    assert result.row_delta == 0
    assert result.amount_delta == Decimal("0.01")
    assert reconcile(10, Decimal("100"), 9, Decimal("99"), Decimal("0.02")).passed is False


def test_critical_dq_failure_blocks_publication_but_warning_does_not() -> None:
    warning = CheckResult("missing_budget", False, CheckSeverity.WARNING, "one missing")
    critical = CheckResult("duplicate_keys", False, CheckSeverity.CRITICAL, "duplicate")
    assert gate_publication([warning]).allowed is True
    assert gate_publication([warning, critical]).allowed is False
    assert gate_publication([warning, critical]).blocking_checks == ("duplicate_keys",)


def test_retention_preserves_current_and_previous_calendar_year() -> None:
    cutoffs = retention_cutoffs(date(2026, 8, 16))
    assert cutoffs.cost_before == date(2025, 1, 1)
    assert cutoffs.temporary_before == date(2026, 7, 17)
    assert cutoffs.quarantine_before == date(2026, 7, 17)
