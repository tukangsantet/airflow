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

The web UI host port is controlled by `AIRFLOW_PORT` and defaults to `8090`, not the default host port `8080`. Logs and PostgreSQL/Redis data are persisted in host directories configured by `AIRFLOW_LOGS_PATH`, `POSTGRES_DATA_PATH`, and `REDIS_DATA_PATH`. DAGs are mounted read-only from the separate `architron` repository; optional non-secret config, plugins, and include assets are also host-mounted read-only.

## Azure token refresh

The daily DAG starts with the `azure_token_refresh.refresh` task as a global identity bootstrap. It runs the operator-provided host-mounted script at `/opt/airflow/secrets/python/fetch_azure_token.py`; the script is not copied into or installed in the Airflow image. `AIRFLOW_SECRETS_PATH` controls the host directory mounted at `/opt/airflow/secrets` for every scheduler/worker container. The refresh task gates the GCP inventory, GCP ingestion, Azure ingestion, and FX provider roots, so the refreshed Azure token is available before GCP Workload Identity Federation credentials are initialized.

Before starting Compose, verify the host file exists and that the Airflow container UID can read the script and write any token/cache state it needs:

```bash
test -r /opt/airflow/secrets/python/fetch_azure_token.py
```

The task suppresses the script's stdout/stderr so an accidentally printed token cannot enter Airflow logs or XCom, applies a 300-second timeout by default, and fails on a non-zero exit code. The script must persist the refreshed credential in the shared cache/file or external secret store consumed by both the GCP WIF configuration and the Azure tasks; setting an environment variable inside the subprocess or only printing a token does not pass it to later Airflow tasks. Adjust `ARCHITRON_AZURE_TOKEN_REFRESH_SCRIPT` and `ARCHITRON_AZURE_TOKEN_REFRESH_TIMEOUT_SECONDS` in `.env` when required.

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
