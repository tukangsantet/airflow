from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from architron_monitoring_airflow.gcp import (
    GCPExportSource,
    build_incremental_query,
    choose_canonical_rows,
)
from architron_monitoring_airflow.models import DataInterval, SourceRow


def test_data_interval_applies_lookback_without_moving_upper_bound() -> None:
    interval = DataInterval(
        start=datetime(2026, 8, 15, tzinfo=UTC),
        end=datetime(2026, 8, 16, tzinfo=UTC),
    )

    extraction = interval.with_lookback(days=3)

    assert extraction.start == datetime(2026, 8, 12, tzinfo=UTC)
    assert extraction.end == interval.end


def test_data_interval_rejects_naive_or_reversed_timestamps() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        DataInterval(datetime(2026, 1, 1), datetime(2026, 1, 2))
    with pytest.raises(ValueError, match="before"):
        DataInterval(
            datetime(2026, 1, 2, tzinfo=UTC),
            datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_gcp_query_has_mandatory_partition_bounds_and_byte_guard_metadata() -> None:
    source = GCPExportSource(
        billing_account_id="BA-1",
        query_project="query-project",
        table="dataset-project.billing.gcp_billing_export_resource_v1_x",
        location="asia-southeast1",
        partition_field="usage_start_time",
        export_type="DETAILED",
        maximum_bytes_billed=10_000_000,
    )

    query = build_incremental_query(source)

    assert "`dataset-project.billing.gcp_billing_export_resource_v1_x`" in query.sql
    assert "usage_start_time >= @window_start" in query.sql
    assert "usage_start_time < @window_end" in query.sql
    assert query.parameters == ("window_start", "window_end")
    assert query.maximum_bytes_billed == 10_000_000
    assert query.location == "asia-southeast1"


def test_gcp_query_rejects_injectable_identifiers() -> None:
    source = GCPExportSource(
        billing_account_id="BA-1",
        query_project="query-project",
        table="project.dataset.table`; DROP TABLE raw.cost; --",
        location="US",
        partition_field="usage_start_time",
        export_type="DETAILED",
        maximum_bytes_billed=1,
    )
    with pytest.raises(ValueError, match="identifier"):
        build_incremental_query(source)


def test_detailed_export_wins_without_double_counting_standard() -> None:
    standard = SourceRow(
        provider="GCP",
        period=date(2026, 8, 1),
        business_key="BA-1:row-1",
        amount=Decimal("125.50"),
        source_export_type="STANDARD",
        lineage="p.d.standard",
    )
    detailed = SourceRow(
        provider="GCP",
        period=date(2026, 8, 1),
        business_key="BA-1:row-1",
        amount=Decimal("125.50"),
        source_export_type="DETAILED",
        lineage="p.d.detailed",
    )

    selected = choose_canonical_rows([standard, detailed])

    assert selected == [detailed]
    assert sum(row.amount for row in selected) == Decimal("125.50")


def test_standard_is_period_fallback_when_detailed_is_absent() -> None:
    standard = SourceRow(
        provider="GCP",
        period=date(2025, 1, 1),
        business_key="BA-1:old-row",
        amount=Decimal("10"),
        source_export_type="STANDARD",
        lineage="p.d.standard",
    )
    assert choose_canonical_rows([standard]) == [standard]
