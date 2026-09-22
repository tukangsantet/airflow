# Airflow 2.11 on-premises Docker Compose deployment

This directory is the **separate installer repository** for the Architron monitoring DAGs. The DAG source remains in the sibling application repository and is mounted read-only at runtime.

Expected on-premises layout:

```text
/workspace/
├── airflow/      # this installer repository
└── architron/    # application/DAG repository
    └── airflow/
        ├── dags/
        └── architron_monitoring_airflow/
```

It runs:

- Apache Airflow **2.11.2** on Python 3.11.
- `LocalExecutor` for parallel task execution inside the scheduler container.
- An existing on-prem PostgreSQL server, published on host port `5434`, for Airflow metadata.
- Scheduler, webserver, triggerer, and one-time database/user initializer.

The webserver listens on the container's normal `8080`, but the host port is configurable and defaults to **8090**. This Compose project does not create PostgreSQL or Redis; it connects to the existing PostgreSQL server through `host.docker.internal:5434` by default.

## Prerequisites

On the on-prem Ubuntu host, install:

- Docker Engine with the Compose v2 plugin.
- At least 4 GB RAM available for the Compose project; 8 GB is preferable for provider libraries and parallel LocalExecutor tasks.
- Outbound HTTPS access to pull images and download the official Airflow constraints file during image build.
- A firewall rule allowing the selected `AIRFLOW_PORT` only from the intended admin network.

Do not commit `.env`. The repository `.gitignore` excludes it.

## First start

```bash
cd /workspace/airflow
bash scripts/bootstrap.sh

# Defaults assume /workspace/architron is the sibling DAG repository.
# Adjust these two values in .env if the repositories are elsewhere:
# ARCHITRON_DAGS_SOURCE_PATH
# ARCHITRON_DAG_PACKAGE_SOURCE_PATH
# Set the existing PostgreSQL connection in .env:
# AIRFLOW_DB_HOST=host.docker.internal
# AIRFLOW_DB_PORT=5434
# AIRFLOW_DB_NAME=airflow
# AIRFLOW_DB_USER=airflow
# AIRFLOW_DB_PASSWORD=<existing-postgres-password>

# The daily DAG's first task runs this host-provided file before GCP/Azure provider work.
test -r /opt/airflow/secrets/python/fetch_azure_token.py

# Change AIRFLOW_PORT, bind address, parallelism, or admin settings if needed.
# Validate interpolation without printing rendered secrets:
docker compose config --quiet

docker compose up -d --build
```

The first start creates the configured host directories, builds the custom image, migrates the Airflow metadata database, creates the initial Admin user, and starts all services. The initialization is idempotent: subsequent `docker compose up` runs do not recreate the existing Admin user.

Open:

```text
http://<onprem-host>:<AIRFLOW_PORT>
```

The initial username and password are in the local `.env`. Change the password through Airflow after the first login according to your policy.

The bootstrap script sets `AIRFLOW_UID` to the current non-root host user's numeric UID so Airflow can write to the host-mounted log directory. If `.env` is created manually, set it with `id -u` and ensure the configured log directory is writable by that UID.

## Parallel execution and scaling

`LocalExecutor` runs task processes from the scheduler container. Configure `AIRFLOW_PARALLELISM` and `AIRFLOW_MAX_ACTIVE_TASKS_PER_DAG` according to the available CPU and memory. The DAG itself also controls task dependencies, retries, and `max_active_runs`.

## Operations

```bash
# Status and recent logs
docker compose ps
docker compose logs --tail=100 airflow-init
docker compose logs --tail=100 airflow-scheduler
docker compose logs --tail=100 airflow-triggerer

# Health endpoint and DAG listing
curl --fail "http://127.0.0.1:${AIRFLOW_PORT:-8090}/health"
docker compose exec airflow-scheduler airflow dags list

# Stop Airflow containers; the external PostgreSQL service is not touched.
docker compose down
```

Use `docker compose up -d --build` after changing `Dockerfile` or `requirements.txt`. DAG changes are read from the mounted Architron repository; restart the scheduler if an immediate reload is needed. A rebuild is not required for ordinary DAG source changes.

## Configuration

The supported deployment knobs are in `.env.example`:

- `ARCHITRON_DAGS_SOURCE_PATH`: host path to the `dags/` directory in the separate Architron repo.
- `ARCHITRON_DAG_PACKAGE_SOURCE_PATH`: host path to the `architron_monitoring_airflow/` package in that repo.
- `AIRFLOW_LOGS_PATH`: host directory for scheduler, webserver, triggerer, and task logs.
- `AIRFLOW_DB_HOST`: hostname reachable from the Airflow containers; defaults to `host.docker.internal`.
- `AIRFLOW_DB_PORT`: external PostgreSQL host port; defaults to `5434`.
- `AIRFLOW_DB_NAME`: dedicated Airflow metadata database; defaults to `airflow`.
- `AIRFLOW_DB_USER` / `AIRFLOW_DB_PASSWORD`: Airflow metadata database credentials.
- `AIRFLOW_CONFIG_PATH`: host directory for optional non-secret Airflow config/local settings; mounted read-only.
- `AIRFLOW_PLUGINS_PATH`: host directory for optional custom plugins; mounted read-only.
- `AIRFLOW_INCLUDE_PATH`: host directory for optional SQL/templates/assets; mounted read-only.
- `AIRFLOW_SECRETS_PATH`: host directory containing `python/fetch_azure_token.py` and any restricted Azure token/cache state; mounted at `/opt/airflow/secrets` for all Airflow services.
- `ARCHITRON_AZURE_TOKEN_REFRESH_SCRIPT`: absolute container path to the host-mounted refresh script; default `/opt/airflow/secrets/python/fetch_azure_token.py`.
- `ARCHITRON_AZURE_TOKEN_REFRESH_TIMEOUT_SECONDS`: maximum refresh runtime; default `300` seconds.
- `AIRFLOW_PORT`: host port exposed for the web UI/API; default `8090`, not Airflow's default host port.
- `AIRFLOW_BIND_ADDRESS`: host bind address; use `127.0.0.1` behind a reverse proxy or `0.0.0.0` only when firewall policy permits.
- `AIRFLOW_PARALLELISM`: global Airflow task parallelism.
- `AIRFLOW_MAX_ACTIVE_TASKS_PER_DAG`: per-DAG active task limit.
- External PostgreSQL credentials and Airflow Fernet/webserver keys.

If a manually chosen password contains URL-reserved characters, URL-encode it before using it in the SQLAlchemy connection string, or use the hex values generated by `bootstrap.sh`.

## Persistence and backups

The Compose file uses host bind mounts configured in `.env` for logs and source/config assets. Airflow metadata is stored in the external PostgreSQL deployment:

- `${AIRFLOW_LOGS_PATH}` → `/opt/airflow/logs`: task, scheduler, webserver, and triggerer logs.
- External PostgreSQL database configured by `AIRFLOW_DB_*`: Airflow metadata; **must be backed up**.
- `${AIRFLOW_CONFIG_PATH}` → `/opt/airflow/config`: optional non-secret Airflow configuration/local settings, read-only.
- `${AIRFLOW_PLUGINS_PATH}` → `/opt/airflow/plugins`: optional custom plugins, read-only inside containers.
- `${AIRFLOW_INCLUDE_PATH}` → `/opt/airflow/include`: optional SQL/templates/assets, read-only.
- `${AIRFLOW_SECRETS_PATH}` → `/opt/airflow/secrets`: operator-managed Azure refresh script and restricted token/cache state; mounted for every Airflow service. This host directory is intentionally not created by the bootstrap script and must be prepared outside Git.
- `${ARCHITRON_DAGS_SOURCE_PATH}` → `/opt/airflow/dags`: DAG entrypoints from the separate Architron repo, read-only.
- `${ARCHITRON_DAG_PACKAGE_SOURCE_PATH}` → `/opt/airflow/dags/architron_monitoring_airflow`: reusable DAG package, read-only.

Back up PostgreSQL using a tested `pg_dump` policy and back up the configured host data directories according to the recovery plan. Treat the Fernet key as a required backup secret: losing it makes encrypted Airflow connection values unreadable. Do not put provider credentials in DAG source, Variables, task parameters, logs, or Git.

## Production notes

- This Compose file does not create or expose PostgreSQL or Redis. The existing PostgreSQL service must be reachable at the configured host and port.
- Put TLS/reverse proxy authentication in front of the webserver when it is reachable beyond localhost. Airflow's built-in login is not a substitute for network controls.
- Keep the initial `.env` permissions at `0600` and restrict host access to the Docker operator.
- The Compose stack has no Celery broker, worker fleet, or Flower service.
- `docker compose config --quiet` validates rendered YAML but does not prove image pulls, database migrations, provider credentials, source repository paths, or external cloud API access.
- Test a disposable external PostgreSQL database and a sample DAG run before production cutover. Do not delete the external PostgreSQL database during Airflow maintenance.
