# Architron Monitoring Airflow package

## Docker Compose on-prem deployment

Untuk menjalankan Airflow 2.11.2 dengan `CeleryExecutor`, PostgreSQL, Redis broker, dan worker paralel, ikuti [DEPLOYMENT.md](DEPLOYMENT.md). Setup Compose berada di `docker-compose.yml`, dan port web host diatur melalui `.env` (`AIRFLOW_PORT`, default `8090`).

```bash
bash scripts/bootstrap.sh
docker compose up -d --build
```

Official installation reference: [Installation from PyPI](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html). Always select the constraints file matching the native Airflow and Python versions actually installed on-premises.

Production-oriented, provider-neutral cloud-cost ingestion primitives and two operational DAGs (one grouped daily pipeline plus weekly retention). DAG modules deliberately import when Airflow and cloud providers are absent, so unit-test and static-analysis environments need no credentials. When Airflow is absent, each module exposes `DAG_SPEC` and `dag = None`.

## Installation

Use the Python interpreter owned by the native Airflow service. Provider pins are taken from the official Airflow 2.11.2 / Python 3.11 constraints file. If the existing installation differs, do **not** replace it blindly—regenerate provider pins from that exact Airflow/Python release's official constraints and test them before deployment. Airflow core is intentionally not listed in `requirements.txt`, so installing this package cannot silently replace the native service.

```bash
python -m pip install --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.11.txt" -r requirements.txt
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m pip_audit -r requirements-dev.txt
```

`requirements-test.txt` and `requirements-dev.txt` intentionally carry the same exact safe tool pins; the latter matches the root CI install path.

Copy/symlink `dags/` into the configured DAG folder and make `architron_monitoring_airflow/` importable (for example via the Airflow deployment's Python path). Test imports before scheduler restart.

## Architron database migration

Run `alembic upgrade head` from `backend/` before enabling the scheduler. Migrations `025`-`033` add the raw/staging/quality/FX persistence and versioned GCP inventory and Security Command Center finding snapshots used by these handlers and keep `core.cloud_cost`, `mart.daily_cost`, and the existing `ingestion.step_results` contract. Migration `025` extends `ingestion.step_results` in place; it does not delete rows. If a pre-existing database has duplicate `(run_id, step)` records, resolve them under an operator-approved retention policy before retrying the migration; the unique constraint fails closed rather than silently discarding history.

## Protected Airflow connections

Create these four named Connections in an Airflow secrets backend (preferred) or the Airflow connection store. The JSON below shows the exact field contract. Every angle-bracket value is an operator-supplied placeholder; **do not copy a real credential into this repository or an Airflow Variable**.

```json
{
  "architron_monitoring_gcp": {
    "conn_type": "google_cloud_platform",
    "extra": {
      "project": "<GCP_QUERY_PROJECT>",
      "credential_config_file": "<PROTECTED_WIF_EXTERNAL_ACCOUNT_JSON_PATH>",
      "impersonation_chain": "<TARGET_SERVICE_ACCOUNT_EMAIL>"
    }
  },
  "architron_monitoring_azure_blob": {
    "conn_type": "wasb",
    "host": "<AZURE_STORAGE_ACCOUNT_NAME>",
    "login": "<AZURE_CLIENT_OR_ACCOUNT_ID>",
    "password": "<AZURE_CLIENT_SECRET_OR_ACCOUNT_KEY>",
    "extra": {
      "tenant_id": "<AZURE_TENANT_ID>",
      "account_url": "https://<AZURE_STORAGE_ACCOUNT_NAME>.blob.core.windows.net"
    }
  },
  "architron_monitoring_postgres": {
    "conn_type": "postgres",
    "host": "<POSTGRES_HOST>",
    "port": 5432,
    "schema": "<POSTGRES_DATABASE>",
    "login": "<ARCHITRON_MONITORING_INGESTION_ROLE>",
    "password": "<POSTGRES_PASSWORD>",
    "extra": {"sslmode": "verify-full", "sslrootcert": "<CA_CERT_PATH>"}
  },
  "architron_monitoring_smtp": {
    "conn_type": "smtp",
    "host": "<SMTP_HOST>",
    "port": 587,
    "login": "<SMTP_LOGIN>",
    "password": "<SMTP_PASSWORD>",
    "extra": {
      "sender": "<SENDER_EMAIL>",
      "recipients": ["<ALERT_RECIPIENT_EMAIL>"],
      "tls_mode": "STARTTLS",
      "timeout_seconds": 30,
      "cooldown_seconds": 3600
    }
  }
}
```

Use Workload Identity Federation for `architron_monitoring_gcp` and certificate authentication for `architron_monitoring_azure_blob` where possible; omit unused fallback credential fields rather than setting empty secrets. For on-prem WIF, mount the external-account credential configuration through the Airflow secrets backend or a protected worker path and set only `credential_config_file` plus the optional `impersonation_chain` (the target service-account email) in the GCP Connection extra. Reuse this single Connection for every project in `gcp_monitoring.project_ids`; do not create project-specific key files. Grant the impersonated service account least-privilege read roles on each allow-listed project (Cloud Asset Viewer, IAM/security-review read access, Service Account Viewer, Service Usage Viewer, and Security Center Findings Viewer), or at an approved folder or organization scope, and enable the required APIs. Grant BigQuery Data Viewer only on approved billing datasets and Job User only on query projects. Grant Azure Blob list/read only. The PostgreSQL role must use TLS, must not be a superuser, and must be separate from the Airflow metadata role.

## Non-secret Airflow Variable

Create exactly one JSON Variable named `architron_monitoring_config`. It is strictly validated and may contain inventory and limits only. Unknown top-level fields, missing required fields, empty inventories, invalid identifiers, non-positive limits, retention below 30 days, and secret-like keys fail closed. The optional `gcp_monitoring` section configures the project allow-list and REST bounds for the daily inventory and Security Command Center scans. The SCC scanner uses only the read-only findings list endpoint and the same WIF credential; it does not enable detectors, mutate findings, or use Premium/Enterprise APIs.

```json
{
  "gcp_sources": [
    {
      "billing_account_id": "<GCP_BILLING_ACCOUNT_ID>",
      "query_project": "<GCP_QUERY_PROJECT>",
      "table": "<BILLING_PROJECT>.<BILLING_DATASET>.<DETAILED_EXPORT_TABLE>",
      "location": "<BIGQUERY_LOCATION>",
      "partition_field": "usage_start_time",
      "export_type": "DETAILED",
      "maximum_bytes_billed": 10000000000
    },
    {
      "billing_account_id": "<GCP_BILLING_ACCOUNT_ID>",
      "query_project": "<GCP_QUERY_PROJECT>",
      "table": "<BILLING_PROJECT>.<BILLING_DATASET>.<STANDARD_EXPORT_TABLE>",
      "location": "<BIGQUERY_LOCATION>",
      "partition_field": "usage_start_time",
      "export_type": "STANDARD",
      "maximum_bytes_billed": 10000000000
    }
  ],
  "azure": {
    "account_url": "https://<AZURE_STORAGE_ACCOUNT_NAME>.blob.core.windows.net",
    "container": "<ACTUAL_COST_EXPORT_CONTAINER>",
    "prefix": "<ACTUAL_COST_EXPORT_PREFIX>",
    "schema_version": "<PINNED_EXPORT_SCHEMA_VERSION>",
    "required_headers": [
      "SubscriptionId",
      "Date",
      "CostInBillingCurrency",
      "BillingCurrencyCode"
    ],
    "max_blob_bytes": 536870912,
    "max_uncompressed_bytes": 2147483648,
    "csv_chunk_bytes": 1048576
  },
  "postgres": {
    "statement_timeout_ms": 120000,
    "batch_rows": 1000
  },
  "retention": {"operational_days": 30},
  "gcp_monitoring": {
    "project_ids": ["<PROJECT_ID_1>", "<PROJECT_ID_2>"],
    "asset_types": [],
    "max_assets_per_project": 10000,
    "page_size": 100,
    "request_timeout_seconds": 30
  }
}
```

Never put credentials, connection URIs, source rows, secrets, or tokens in DAG source, Variables, task parameters, logs, XCom, Git, or generated documentation. The built-ins load all provider hooks lazily, validate all four named Connections before external work, persist manifests/watermarks/results in PostgreSQL, and return `None` from every task so Airflow does not XCom-push source or credential material.

## Built-in handler contract

Every task in both DAGs resolves to a built-in handler in `architron_monitoring_airflow.production`; `ARCHITRON_MONITORING_STEP_HANDLERS` is not required. Built-ins receive only:

- `data_interval_start` / `data_interval_end` (the authoritative half-open Airflow data interval),
- `run_id` for lineage,
- validated non-secret `params` (`lookback_days`, optional `period_start`/`period_end`).

An emergency custom handler is accepted only when `ARCHITRON_MONITORING_ENABLE_TRUSTED_HANDLER_OVERRIDES=1` and `ARCHITRON_MONITORING_STEP_HANDLERS` contains a trusted `dag_id.task_id` to `module:function` JSON mapping. Treat this as code execution: restrict both environment variables to deployment administrators and remove the switch after the incident.

The built-ins enforce bounded GCP SQL, Detailed-over-Standard precedence, Azure ETag/size manifest identity, pinned schema quarantine, full-period MTD replacement, deterministic keys, currency normalization, DQ gating, reconciliation, and retention cutoffs. Watermarks never replace mandatory partition bounds. Publication and batch status are committed together so dashboards continue using the last published batch after failure.

## Scheduling, reruns, and backfill

The old provider, normalization, mart, quality, and reconciliation DAG files are intentionally not copied into the merged repository. This avoids duplicate scheduling. Existing Airflow deployments should pause the old DAGs before deploying `dags/architron_monitoring_daily_pipeline.py`; the consolidated tasks still dispatch the original `architron_monitoring_*.step` handler names, so persisted step results and dashboard tables remain compatible. The DAG count is reduced without collapsing the 33 quality-preserving handler steps (including the SCC scan): each step keeps its own retry, timeout, failure callback, and no-XCom policy.


The grouped daily pipeline starts at 02:00 UTC; GCP inventory, SCC findings, GCP cost, Azure, and FX ingestion branches run in parallel, then normalization, marts, data quality, and reconciliation run behind explicit quality gates. Retention runs weekly at 07:00 UTC Sunday. DAG specs have catchup enabled for controlled scheduler backfills, one active run, three exponential-backoff retries, and a two-hour task timeout. After an explicit initial load, set `ARCHITRON_MONITORING_DISABLE_CATCHUP=1` in every scheduler/worker process so only new daily intervals are scheduled; only `0` and `1` are accepted. Extraction starts at `data_interval_start - lookback_days` and ends strictly before `data_interval_end`; watermarks never replace mandatory partition bounds. A GCP query must use named parameters and a `maximum_bytes_billed` job setting.

For a manual backfill, supply timezone-aware ISO-8601 `period_start` and `period_end` together through DAG-run configuration. The half-open period overrides the scheduler interval, is limited to 366 days, and then applies `lookback_days`; use zero lookback for bounded monthly historical loads. Rerunning is safe because deterministic business keys merge into staging, GCP Detailed wins over Standard instead of being summed, and a changed Azure MTD blob replaces the complete billing period transactionally. Source Azure blobs are never deleted. For WSL development, run the package checks from this directory with the Airflow service interpreter and keep provider credentials in the named Connections above.

## Source inventory and schema policy

GCP inventory must record billing account, query project, dataset project/name/table, export type, location, partition field, date coverage, currency, and Detailed history coverage before queries are enabled. Table/partition identifiers are strictly validated; values use query parameters. Azure requires an explicitly pinned Cost Management Actual Cost export schema version; unknown or malformed files are quarantined and block publication.

Only provider-billed IDR or authoritative provider-supplied IDR is included in reporting totals. Non-IDR without provider conversion is marked `UNSUPPORTED_REPORTING_CURRENCY`, excluded, and surfaced by DQ. Credit sign conventions must be confirmed from real exports and reconciliation before handler activation.
