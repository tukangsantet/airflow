from __future__ import annotations

import csv
import gzip
import io
import json
import zlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import cast

import pytest

from architron_monitoring_airflow.dag_factory import dispatch_step
from architron_monitoring_airflow.models import DataInterval
from architron_monitoring_airflow.production import (
    ALL_DAG_STEPS,
    AzureBlobAdapter,
    BigQueryAdapter,
    ConfigurationError,
    PostgresAdapter,
    ProductionRuntime,
    QueryJobConfig,
    RuntimeConfig,
    SQLCommand,
    _azure_usage_date,
    _canonicalize_azure_row,
    _discover_azure,
    _effective_interval,
    _extract_gcp,
    _sql_for_step,
    _stage_azure,
    _validated_context,
    builtin_handlers,
    load_runtime_config,
    resolve_handler,
)


def _payload() -> str:
    return json.dumps(
        {
            "gcp_sources": [
                {
                    "billing_account_id": "BA-1",
                    "query_project": "query-project",
                    "table": "billing-project.dataset.table",
                    "location": "US",
                    "partition_field": "usage_start_time",
                    "export_type": "DETAILED",
                    "maximum_bytes_billed": 1000,
                }
            ],
            "azure": {
                "account_url": "https://example.blob.core.windows.net",
                "container": "exports",
                "prefix": "actual/",
                "schema_version": "2025-03-01",
                "required_headers": ["Date", "CostInBillingCurrency"],
                "max_blob_bytes": 2048,
                "max_uncompressed_bytes": 8192,
                "csv_chunk_bytes": 128,
            },
            "postgres": {"statement_timeout_ms": 60000, "batch_rows": 100},
            "retention": {"operational_days": 30},
        }
    )


def test_every_dag_task_has_a_builtin_handler() -> None:
    handlers = builtin_handlers()
    assert set(handlers) == set(ALL_DAG_STEPS)
    assert len(handlers) == 33
    assert all(callable(handler) for handler in handlers.values())
    assert not any(
        marker in handlers
        for marker in (
            "architron_monitoring_data_quality.duplicates",
            "architron_monitoring_reconcile_cost.raw_to_core",
            "architron_monitoring_retention_cleanup.cleanup_files",
        )
    )


def test_builtin_is_default_and_override_must_be_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    step = "architron_monitoring_data_quality.run_checks"
    assert resolve_handler(step) is builtin_handlers()[step]
    monkeypatch.setenv(
        "ARCHITRON_MONITORING_STEP_HANDLERS", json.dumps({step: "tests.fake:handler"})
    )
    # Environment mappings are ignored unless the trusted override switch is enabled.
    assert resolve_handler(step) is builtin_handlers()[step]


def test_manual_backfill_period_overrides_airflow_interval() -> None:
    interval, run_id = _validated_context(
        {
            "data_interval_start": datetime(2026, 8, 20, tzinfo=UTC),
            "data_interval_end": datetime(2026, 8, 21, tzinfo=UTC),
            "run_id": "manual__2026-01",
            "params": {
                "period_start": "2026-01-01T00:00:00+00:00",
                "period_end": "2026-02-01T00:00:00+00:00",
                "lookback_days": 0,
            },
        }
    )

    assert run_id == "manual__2026-01"
    assert interval.start == datetime(2026, 1, 1, tzinfo=UTC)
    assert interval.end == datetime(2026, 2, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    "params",
    [
        {"period_start": "2026-01-01T00:00:00+00:00", "period_end": None},
        {"period_start": None, "period_end": "2026-02-01T00:00:00+00:00"},
        {"period_start": "2026-01-01T00:00:00", "period_end": "2026-02-01T00:00:00"},
        {"period_start": "2026-01-01T00:00:00Z", "period_end": "not-a-timestamp"},
        {"period_start": 20260101, "period_end": "2026-02-01T00:00:00Z"},
        {
            "period_start": "2026-01-01T00:00:00Z",
            "period_end": "2026-01-01T00:00:00Z",
        },
        {
            "period_start": "2026-02-01T00:00:00+00:00",
            "period_end": "2026-01-01T00:00:00+00:00",
        },
        {
            "period_start": "2025-01-01T00:00:00+00:00",
            "period_end": "2026-02-01T00:00:00+00:00",
        },
    ],
)
def test_manual_backfill_period_rejects_unsafe_ranges(params: dict[str, object]) -> None:
    with pytest.raises(ConfigurationError, match="period"):
        _validated_context(
            {
                "data_interval_start": datetime(2026, 8, 20, tzinfo=UTC),
                "data_interval_end": datetime(2026, 8, 21, tzinfo=UTC),
                "run_id": "manual__invalid",
                "params": {**params, "lookback_days": 0},
            }
        )


def test_manual_backfill_accepts_exactly_366_days() -> None:
    interval, _ = _validated_context(
        {
            "data_interval_start": datetime(2026, 8, 20, tzinfo=UTC),
            "data_interval_end": datetime(2026, 8, 21, tzinfo=UTC),
            "run_id": "manual__boundary",
            "params": {
                "period_start": "2026-01-01T00:00:00Z",
                "period_end": "2027-01-02T00:00:00Z",
                "lookback_days": 0,
            },
        }
    )

    assert interval.end - interval.start == timedelta(days=366)


def test_missing_or_secret_configuration_fails_closed() -> None:
    with pytest.raises(ConfigurationError, match="architron_monitoring_config"):
        load_runtime_config(lambda _name: None)
    secret_payload = json.loads(_payload())
    secret_payload["password"] = "not-a-real-value"  # noqa: S105
    with pytest.raises(ConfigurationError, match="secret"):
        load_runtime_config(lambda _name: json.dumps(secret_payload))
    unknown_payload = json.loads(_payload())
    unknown_payload["postgres"]["unchecked_option"] = True
    with pytest.raises(ConfigurationError, match="postgres"):
        load_runtime_config(lambda _name: json.dumps(unknown_payload))


def test_runtime_config_validates_named_connections() -> None:
    seen: list[str] = []

    def connection(name: str) -> object:
        seen.append(name)
        return object()

    config = load_runtime_config(
        lambda name: _payload() if name == "architron_monitoring_config" else None, connection
    )
    assert isinstance(config, RuntimeConfig)
    assert seen == [
        "architron_monitoring_gcp",
        "architron_monitoring_azure_blob",
        "architron_monitoring_postgres",
        "architron_monitoring_smtp",
    ]


def test_bigquery_adapter_uses_named_parameters_location_and_byte_limit() -> None:
    calls: list[dict[str, object]] = []
    result_calls = 0

    class Job:
        def result(self) -> list[dict[str, object]]:
            nonlocal result_calls
            result_calls += 1
            return []

    class Client:
        def query(self, query: str, **kwargs: object) -> Job:
            calls.append({"sql": query, **kwargs})
            return Job()

    adapter = BigQueryAdapter(Client(), query_parameter_factory=lambda n, _t, v: (n, v))
    adapter.query(
        "SELECT 1 WHERE ts >= @window_start AND ts < @window_end",
        parameters={"window_start": "start", "window_end": "end"},
        location="US",
        maximum_bytes_billed=1000,
        project_id="query-project",
    )
    call = calls[0]
    assert call["location"] == "US"
    assert call["project"] == "query-project"
    job = cast(QueryJobConfig, call["job_config"])
    assert job.maximum_bytes_billed == 1000
    assert job.query_parameters == [("window_start", "start"), ("window_end", "end")]
    assert result_calls == 1
    with pytest.raises(ValueError, match="positive"):
        adapter.query(
            "SELECT 1", parameters={}, location="US", maximum_bytes_billed=0, project_id="p"
        )


def test_azure_csv_reader_is_streamed_and_size_bounded() -> None:
    adapter = AzureBlobAdapter(max_blob_bytes=20, max_uncompressed_bytes=20, csv_chunk_bytes=4)
    rows = list(adapter.read_csv(io.BytesIO(b"a,b\n1,2\n"), declared_size=8))
    assert rows == [{"a": "1", "b": "2"}]
    with pytest.raises(ValueError, match="size limit"):
        list(adapter.read_csv(io.BytesIO(b"a,b\n"), declared_size=21))


def test_azure_csv_reader_never_reads_past_declared_bytes_with_bom() -> None:
    class TrackingStream(io.BytesIO):
        bytes_read = 0

        def read(self, size: int | None = -1) -> bytes:
            block = super().read(size)
            self.bytes_read += len(block)
            return block

    declared = b"\xef\xbb\xbfa,b\n1,2\n"
    stream = TrackingStream(declared + b"SHOULD_NOT_BE_READ")
    adapter = AzureBlobAdapter(max_blob_bytes=100, max_uncompressed_bytes=100, csv_chunk_bytes=4)

    assert list(adapter.read_csv(stream, declared_size=len(declared))) == [{"a": "1", "b": "2"}]
    assert stream.bytes_read == len(declared)


def test_azure_csv_reader_handles_bom_split_across_chunks() -> None:
    payload = b"\xef\xbb\xbfa,b\n1,2\n"
    adapter = AzureBlobAdapter(max_blob_bytes=100, max_uncompressed_bytes=100, csv_chunk_bytes=1)

    assert list(adapter.read_csv(io.BytesIO(payload), declared_size=len(payload))) == [
        {"a": "1", "b": "2"}
    ]


def test_azure_csv_reader_streams_gzip_and_bounds_uncompressed_bytes() -> None:
    payload = gzip.compress(b"a,b\n1,2\n")
    adapter = AzureBlobAdapter(
        max_blob_bytes=100,
        max_uncompressed_bytes=100,
        csv_chunk_bytes=3,
    )

    assert list(
        adapter.read_csv(
            io.BytesIO(payload),
            declared_size=len(payload),
            gzip_compressed=True,
        )
    ) == [{"a": "1", "b": "2"}]

    compressed_bomb = gzip.compress(b"a,b\n" + b"1,2\n" * 100)
    with pytest.raises(ValueError, match="uncompressed size limit"):
        list(
            AzureBlobAdapter(
                max_blob_bytes=100,
                max_uncompressed_bytes=20,
                csv_chunk_bytes=3,
            ).read_csv(
                io.BytesIO(compressed_bomb),
                declared_size=len(compressed_bomb),
                gzip_compressed=True,
            )
        )


def test_azure_usage_date_accepts_lowercase_mca_date() -> None:
    assert _azure_usage_date({"date": "08/05/2026"}) == datetime(2026, 8, 5).date()


def test_azure_row_canonicalizes_mca_camel_case_without_dropping_source_fields() -> None:
    source = {
        "date": "08/05/2026",
        "costInBillingCurrency": "10.25",
        "billingCurrency": "IDR",
        "subscriptionId": "subscription-1",
        "productName": "Example",
    }

    canonical = _canonicalize_azure_row(source)

    assert canonical == {
        **source,
        "Date": "08/05/2026",
        "CostInBillingCurrency": "10.25",
        "BillingCurrencyCode": "IDR",
        "SubscriptionId": "subscription-1",
    }


def test_azure_stage_streams_with_etag_and_validates_pinned_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    compressed_payload = gzip.compress(b"Date,CostInBillingCurrency\n2026-08-15,10\n")

    class Blob:
        name = "actual/part_0.csv.gz"
        etag = '"etag-1"'
        size = len(compressed_payload)
        last_modified = datetime(2026, 8, 16, tzinfo=UTC)

    class Download:
        def chunks(self) -> list[bytes]:
            return [compressed_payload[:5], compressed_payload[5:]]

    class BlobClient:
        def download_blob(self, **kwargs: object) -> Download:
            assert kwargs == {
                "etag": '"etag-1"',
                "match_condition": "IfNotModified",
            }
            return Download()

    class Container:
        def list_blobs(self, **kwargs: object) -> list[object]:
            assert kwargs == {"name_starts_with": "actual/"}
            return [
                SimpleNamespace(
                    name="actual/manifest.json",
                    etag='"manifest"',
                    size=10,
                    last_modified=datetime(2026, 8, 16, tzinfo=UTC),
                ),
                Blob(),
            ]

        def get_blob_client(self, name: str) -> BlobClient:
            assert name == "actual/part_0.csv.gz"
            return BlobClient()

    class Postgres:
        def __init__(self) -> None:
            self.commands: list[SQLCommand] = []

        def transaction(self, commands: list[SQLCommand] | tuple[SQLCommand, ...]) -> None:
            self.commands.extend(commands)

    config = load_runtime_config(lambda _name: _payload())
    postgres = Postgres()
    runtime = ProductionRuntime(
        config=config,
        postgres=cast(PostgresAdapter, postgres),
        azure_container=Container(),
    )
    monkeypatch.setattr("architron_monitoring_airflow.production._runtime_loader", lambda: runtime)
    monkeypatch.setattr(
        "architron_monitoring_airflow.production._azure_if_not_modified", lambda: "IfNotModified"
    )
    dispatch_step(
        "architron_monitoring_ingest_azure_actual_cost.stage_files",
        data_interval_start=datetime(2026, 8, 15, tzinfo=UTC),
        data_interval_end=datetime(2026, 8, 16, tzinfo=UTC),
        run_id="scheduled__safe",
        params={"lookback_days": 0},
    )
    assert len(postgres.commands) == 1
    assert "source_row" in postgres.commands[0].parameters
    assert postgres.commands[0].parameters["usage_date"] == datetime(2026, 8, 15).date()
    assert "ON CONFLICT" in postgres.commands[0].sql


def test_dispatch_passes_only_nonsecret_context_and_returns_no_xcom_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: dict[str, object] = {}

    def handler(**context: object) -> None:
        received.update(context)

    monkeypatch.setattr(
        "architron_monitoring_airflow.dag_factory.resolve_handler", lambda _step: handler
    )
    dispatch_step(
        "architron_monitoring_data_quality.run_checks",
        data_interval_start=datetime(2026, 8, 15, tzinfo=UTC),
        data_interval_end=datetime(2026, 8, 16, tzinfo=UTC),
        run_id="scheduled__safe",
        params={"lookback_days": 3},
        connection_uri="postgresql://user:password@example/db",
    )

    assert set(received) == {"data_interval_start", "data_interval_end", "run_id", "params"}


def test_azure_period_replace_uses_one_parameterized_database_function() -> None:
    config = load_runtime_config(lambda _name: _payload())
    interval = DataInterval(datetime(2026, 8, 15, tzinfo=UTC), datetime(2026, 8, 16, tzinfo=UTC))
    commands = _sql_for_step(
        "architron_monitoring_ingest_azure_actual_cost.replace_period",
        interval,
        "scheduled__safe",
        config,
    )
    assert len(commands) == 1
    assert "ingestion.replace_azure_period" in commands[0].sql
    assert "2026-08" not in commands[0].sql
    assert commands[0].parameters["start"] == datetime(2026, 8, 1, tzinfo=UTC)
    assert commands[0].parameters["end"] == datetime(2026, 9, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ("step", "database_function"),
    [
        (
            "architron_monitoring_ingest_azure_actual_cost.reconcile",
            "ingestion.reconcile_azure_period",
        ),
        (
            "architron_monitoring_normalize_cloud_cost.reconcile",
            "ingestion.reconcile_normalized_batch",
        ),
        ("architron_monitoring_refresh_cost_marts.reconcile", "mart.reconcile_cost_staging"),
    ],
)
def test_publication_reconciliation_steps_execute_real_database_gates(
    step: str, database_function: str
) -> None:
    command = _sql_for_step(
        step,
        DataInterval(datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 9, 1, tzinfo=UTC)),
        "scheduled__safe",
        load_runtime_config(lambda _name: _payload()),
    )[0]

    assert database_function in command.sql
    assert "run_architron_monitoring_step" not in command.sql


def test_azure_manifest_discovery_keeps_identity_scoped_to_current_run() -> None:
    class Azure:
        def list_blobs(self, *, name_starts_with: str):  # type: ignore[no-untyped-def]
            assert name_starts_with
            return [
                SimpleNamespace(
                    name="month/manifest.json",
                    etag="manifest",
                    size=20,
                    last_modified=datetime(2026, 8, 15, tzinfo=UTC),
                ),
                SimpleNamespace(
                    name="month/file.csv",
                    etag="etag-1",
                    size=100,
                    last_modified=datetime(2026, 8, 15, tzinfo=UTC),
                ),
            ]

    class Postgres:
        def __init__(self) -> None:
            self.commands: list[SQLCommand] = []

        def transaction(self, commands: list[SQLCommand]) -> None:
            self.commands.extend(commands)

    interval = DataInterval(datetime(2026, 8, 15, tzinfo=UTC), datetime(2026, 8, 16, tzinfo=UTC))
    postgres = Postgres()
    runtime = ProductionRuntime(
        config=load_runtime_config(lambda _name: _payload()),
        postgres=cast(PostgresAdapter, postgres),
        azure_container=Azure(),
    )

    _discover_azure(runtime, interval, "run-current")
    assert len(postgres.commands) == 1
    command = postgres.commands[0]
    assert "ON CONFLICT (run_id, path, etag, size_bytes) DO UPDATE" in command.sql
    assert "run_id=EXCLUDED.run_id" not in command.sql
    assert "status='DISCOVERED'" in command.sql


def test_scheduled_azure_and_downstream_steps_use_month_to_date_interval() -> None:
    daily = DataInterval(
        datetime(2026, 8, 24, tzinfo=UTC),
        datetime(2026, 8, 28, tzinfo=UTC),
    )
    expected = DataInterval(
        datetime(2026, 8, 1, tzinfo=UTC),
        datetime(2026, 8, 28, tzinfo=UTC),
    )

    assert (
        _effective_interval(
            "architron_monitoring_ingest_azure_actual_cost.discover_blobs", daily, {"params": {}}
        )
        == expected
    )
    assert (
        _effective_interval(
            "architron_monitoring_normalize_cloud_cost.normalize", daily, {"params": {}}
        )
        == expected
    )
    assert (
        _effective_interval(
            "architron_monitoring_ingest_gcp_billing.extract_detailed", daily, {"params": {}}
        )
        == daily
    )


def test_manual_period_is_not_rewritten_to_month_to_date() -> None:
    manual = DataInterval(
        datetime(2026, 8, 5, tzinfo=UTC),
        datetime(2026, 8, 6, tzinfo=UTC),
    )
    assert (
        _effective_interval(
            "architron_monitoring_ingest_azure_actual_cost.discover_blobs",
            manual,
            {"params": {"period_start": "2026-08-05T00:00:00Z"}},
        )
        == manual
    )


def test_oversized_azure_blob_is_quarantined_before_discovery_fails() -> None:
    class Azure:
        def list_blobs(self, *, name_starts_with: str):  # type: ignore[no-untyped-def]
            return [SimpleNamespace(name="bad.csv", etag="e", size=10**12, last_modified=None)]

    class Postgres:
        def __init__(self) -> None:
            self.commands: list[SQLCommand] = []

        def transaction(self, commands: list[SQLCommand]) -> None:
            self.commands.extend(commands)

    postgres = Postgres()
    runtime = ProductionRuntime(
        config=load_runtime_config(lambda _name: _payload()),
        postgres=cast(PostgresAdapter, postgres),
        azure_container=Azure(),
    )

    with pytest.raises(ValueError, match="size limit"):
        _discover_azure(
            runtime,
            DataInterval(datetime(2026, 8, 15, tzinfo=UTC), datetime(2026, 8, 16, tzinfo=UTC)),
            "run-oversized",
        )

    assert any("ingestion.quarantine" in command.sql for command in postgres.commands)
    assert any("QUARANTINED" in command.sql for command in postgres.commands)


@pytest.mark.parametrize(
    "parse_error",
    [csv.Error("malformed CSV"), zlib.error("corrupted gzip deflate")],
)
def test_azure_parser_error_is_quarantined_before_staging_fails(
    monkeypatch: pytest.MonkeyPatch, parse_error: Exception
) -> None:
    item = SimpleNamespace(name="bad.csv", etag="e", size=4, last_modified=None)

    class Azure:
        def list_blobs(self, *, name_starts_with: str):  # type: ignore[no-untyped-def]
            return [item]

        def get_blob_client(self, _name: str):  # type: ignore[no-untyped-def]
            return SimpleNamespace(
                download_blob=lambda **_kwargs: SimpleNamespace(chunks=lambda: iter((b"bad\n",)))
            )

    class Postgres:
        def __init__(self) -> None:
            self.commands: list[SQLCommand] = []

        def transaction(self, commands):  # type: ignore[no-untyped-def]
            self.commands.extend(commands)

    def reject(*_args: object, **_kwargs: object):
        raise parse_error

    monkeypatch.setattr(AzureBlobAdapter, "read_csv", reject)
    monkeypatch.setattr(
        "architron_monitoring_airflow.production._azure_if_not_modified", lambda: "IfNotModified"
    )
    postgres = Postgres()
    runtime = ProductionRuntime(
        config=load_runtime_config(lambda _name: _payload()),
        postgres=cast(PostgresAdapter, postgres),
        azure_container=Azure(),
    )

    with pytest.raises(type(parse_error)):
        _stage_azure(runtime, "run-parser-error")

    assert any("ingestion.quarantine" in command.sql for command in postgres.commands)


def test_gcp_canonicalization_never_sums_standard_with_available_detailed() -> None:
    config = load_runtime_config(lambda _name: _payload())
    interval = DataInterval(datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 9, 1, tzinfo=UTC))

    command = _sql_for_step(
        "architron_monitoring_ingest_gcp_billing.canonicalize",
        interval,
        "scheduled__safe",
        config,
    )[0]

    assert "export_type = 'DETAILED'" in command.sql
    assert "NOT EXISTS" in command.sql
    assert "billing_account_id" in command.sql


def test_gcp_extraction_flushes_batches_before_consuming_all_rows() -> None:
    consumed = 0

    class BigQuery:
        def query(self, *_args: object, **_kwargs: object):  # type: ignore[no-untyped-def]
            nonlocal consumed
            for index in range(205):
                consumed += 1
                yield {"row": index}

    class Postgres:
        def __init__(self) -> None:
            self.batch_sizes: list[int] = []

        def transaction(self, commands: list[SQLCommand]) -> None:
            if not self.batch_sizes:
                assert consumed == 100
            self.batch_sizes.append(len(commands))

    config = load_runtime_config(lambda _name: _payload())
    postgres = Postgres()
    runtime = ProductionRuntime(
        config=config,
        postgres=cast(PostgresAdapter, postgres),
        bigquery=cast(BigQueryAdapter, BigQuery()),
    )

    _extract_gcp(
        runtime,
        "DETAILED",
        DataInterval(datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 8, 2, tzinfo=UTC)),
        "scheduled__safe",
    )

    assert postgres.batch_sizes == [100, 100, 5]
