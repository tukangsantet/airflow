from __future__ import annotations

import importlib
from datetime import UTC, datetime, timedelta

import pytest

from architron_monitoring_airflow.alerts import AlertGate, AlertPolicy, build_failure_alert
from architron_monitoring_airflow.dag_factory import DAGSpec, configured_catchup
from architron_monitoring_airflow.production import ALL_DAG_STEPS
from architron_monitoring_airflow.publication import PublicationPlan, parameterized_delete

DAG_MODULES = {
    "architron_monitoring_daily_pipeline": (
        "extract_detailed",
        "stage_files",
        "normalize",
        "run_checks",
        "record_results",
    ),
    "architron_monitoring_retention_cleanup": ("cleanup_database",),
}
EXPECTED_SCHEDULES = {
    "architron_monitoring_daily_pipeline": "0 2 * * *",
    "architron_monitoring_retention_cleanup": "0 7 * * 0",
}


def test_all_dags_import_without_airflow_or_provider_packages() -> None:
    for name, tasks in DAG_MODULES.items():
        module = importlib.import_module(f"dags.{name}")
        assert module.DAG_SPEC.dag_id == name
        assert module.DAG_SPEC.schedule == EXPECTED_SCHEDULES[name]
        assert module.DAG_SPEC.catchup is True
        assert module.DAG_SPEC.max_active_runs == 1
        assert set(tasks).issubset(module.DAG_SPEC.tasks)
        assert hasattr(module, "dag")


def test_dag_defaults_are_bounded_and_retry_with_backoff() -> None:
    module = importlib.import_module("dags.architron_monitoring_daily_pipeline")
    defaults = module.DAG_SPEC.default_args
    assert defaults["retries"] >= 2
    assert defaults["retry_exponential_backoff"] is True
    assert defaults["execution_timeout"] <= timedelta(hours=2)
    assert callable(defaults["on_failure_callback"])


def test_daily_only_mode_disables_historical_catchup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = DAGSpec("example", "0 2 * * *", ("step",), catchup=True)
    assert configured_catchup(spec) is True
    monkeypatch.setenv("ARCHITRON_MONITORING_DISABLE_CATCHUP", "1")
    assert configured_catchup(spec) is False
    monkeypatch.setenv("ARCHITRON_MONITORING_DISABLE_CATCHUP", "yes")
    with pytest.raises(ValueError, match="ARCHITRON_MONITORING_DISABLE_CATCHUP"):
        configured_catchup(spec)


def test_daily_pipeline_preserves_provider_order_and_quality_gates() -> None:
    module = importlib.import_module("dags.architron_monitoring_daily_pipeline")
    active_steps = set(ALL_DAG_STEPS) - {
        "architron_monitoring_retention_cleanup.cleanup_database",
        # TEMPORARILY DISABLED in the daily DAG; built-in handler remains registered.
        "architron_monitoring_gcp_security.scan",
    }
    assert set(module._HANDLER_STEPS.values()) == active_steps
    groups = dict(module._GROUPS)
    assert groups["azure_ingestion"].index("stage_files") < groups["azure_ingestion"].index("validate_schema")
    assert {
        ("azure_token_refresh.refresh", "gcp_inventory.scan"),
        ("azure_token_refresh.refresh", "gcp_ingestion.extract_detailed"),
        ("azure_token_refresh.refresh", "azure_ingestion.discover_blobs"),
        ("azure_token_refresh.refresh", "fx_ingestion.fetch_rates"),
    }.issubset(set(module._DEPENDENCIES))
    assert ("azure_ingestion.record_freshness", "normalization.normalize") in module._DEPENDENCIES
    assert ("data_quality.record_results", "reconciliation.provider_sources") in module._DEPENDENCIES


def test_failure_alert_is_redacted_and_deduplicated_with_cooldown() -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    alert = build_failure_alert(
        dag_id="architron_monitoring_daily_pipeline",
        task_id="extract",
        logical_date=now,
        exception=RuntimeError("password=do-not-leak token abc"),
    )
    assert "do-not-leak" not in alert.body
    assert "abc" not in alert.body
    assert "RuntimeError" in alert.body

    gate = AlertGate(AlertPolicy(cooldown=timedelta(hours=1)))
    assert gate.should_send(alert, now) is True
    gate.record_sent(alert, now)
    assert gate.should_send(alert, now + timedelta(minutes=59)) is False
    assert gate.should_send(alert, now + timedelta(hours=1)) is True


def test_publication_sql_uses_parameters_and_transactional_staging() -> None:
    predicate = parameterized_delete(
        table="raw.azure_cost",
        where="provider = %(provider)s AND usage_date >= %(start)s AND usage_date < %(end)s",
    )
    assert "%s" not in predicate.sql
    assert "%(provider)s" in predicate.sql
    plan = PublicationPlan.for_period_replace(
        target_table="raw.azure_cost", staging_table="ingestion.azure_cost_stage", delete=predicate
    )
    assert plan.statements[0] == "BEGIN"
    assert plan.statements[-1] == "COMMIT"
    assert any("DELETE FROM raw.azure_cost" in sql for sql in plan.statements)
    assert any(
        "INSERT INTO raw.azure_cost SELECT * FROM ingestion.azure_cost_stage" in sql
        for sql in plan.statements
    )
