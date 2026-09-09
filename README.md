# Airflow installer repository

Docker Compose deployment for Apache Airflow **2.11.2** with `CeleryExecutor`, PostgreSQL metadata, Redis broker, and parallel workers.

The DAG source is intentionally maintained in a separate repository. This repository contains only the Airflow runtime/installer assets; it does not copy or vendor the Architron DAG source.

## Repository layout

Expected sibling layout on the on-prem host:

```text
/workspace/
├── airflow/      # this repository
└── architron/    # application/DAG repository
    └── airflow/
        ├── dags/
        └── architron_monitoring_airflow/
```

The default paths are configured in `.env.example`:

```env
ARCHITRON_DAGS_SOURCE_PATH=../architron/airflow/dags
ARCHITRON_DAG_PACKAGE_SOURCE_PATH=../architron/airflow/architron_monitoring_airflow
```

Adjust them when the two repositories are not siblings.

## Quick start

```bash
cd /workspace/airflow
bash scripts/bootstrap.sh
docker compose config --quiet
docker compose up -d --build
```

The web UI host port is controlled by `AIRFLOW_PORT` and defaults to `8090`, not the default host port `8080`.

Read [DEPLOYMENT.md](DEPLOYMENT.md) for prerequisites, scaling, health checks, backups, and operational commands.

## DAG contract and provider connections

The source DAG package expects protected Airflow Connections named:

- `architron_monitoring_gcp`
- `architron_monitoring_azure_blob`
- `architron_monitoring_postgres`
- `architron_monitoring_smtp`

The detailed connection and non-secret Variable contract is documented in [ARCHITRON_DAG_OPERATIONS.md](ARCHITRON_DAG_OPERATIONS.md). Never place credentials, connection URIs, source rows, secrets, or tokens in DAG source, Variables, task parameters, logs, XCom, Git, or generated documentation.

The pipeline uses timezone-aware Airflow **data interval** bounds, controlled **backfill**, provider-specific validation, and `maximum_bytes_billed` for bounded BigQuery work. Publication is gated by validation and reconciliation; a failed batch must never replace the last successfully published batch.

## Tests

The tests in `tests/` validate the DAG package and deployment contracts. With both repositories checked out in the default sibling layout:

```bash
PYTHONPATH=../architron/airflow python -m pytest -q
```

The Compose stack itself requires Docker Engine and the Compose v2 plugin; it cannot be exercised by Python tests alone.
