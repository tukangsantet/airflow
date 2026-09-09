#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

env_value() {
  local requested_key="$1"
  local key value
  while IFS='=' read -r key value; do
    if [[ "$key" == "$requested_key" ]]; then
      printf '%s' "$value"
      return 0
    fi
  done < .env
  return 1
}

prepare_host_dirs() {
  # Use the host user's UID for Airflow's writable bind-mounted log directory.
  # When bootstrap is run as root, retain the image default and chown logs below.
  host_uid="$(id -u)"
  if [[ "$host_uid" == "0" ]]; then
    host_uid=50000
  fi

  airflow_logs_path="$(env_value AIRFLOW_LOGS_PATH || printf './logs')"
  postgres_data_path="$(env_value POSTGRES_DATA_PATH || printf './data/postgres')"
  redis_data_path="$(env_value REDIS_DATA_PATH || printf './data/redis')"
  airflow_config_path="$(env_value AIRFLOW_CONFIG_PATH || printf './config')"
  airflow_plugins_path="$(env_value AIRFLOW_PLUGINS_PATH || printf './plugins')"
  airflow_include_path="$(env_value AIRFLOW_INCLUDE_PATH || printf './include')"

  mkdir -p "$airflow_logs_path" "$postgres_data_path" "$redis_data_path" "$airflow_config_path" "$airflow_plugins_path" "$airflow_include_path"
  chmod 750 "$airflow_logs_path" "$airflow_config_path" "$airflow_plugins_path" "$airflow_include_path"
  chmod 700 "$postgres_data_path" "$redis_data_path"
  if [[ "$(id -u)" == "0" ]]; then
    chown "${host_uid}:0" "$airflow_logs_path"
  fi
}

if [[ -f .env ]]; then
  prepare_host_dirs
  echo ".env already exists; secrets were not changed. Host mount directories are ready."
  exit 0
fi

command -v openssl >/dev/null 2>&1 || {
  echo "ERROR: openssl is required to generate local secrets." >&2
  exit 1
}

cp .env.example .env
prepare_host_dirs

random_hex() {
  openssl rand -hex 24
}

# Fernet keys are URL-safe base64 encodings of exactly 32 random bytes.
fernet_key="$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '=')"
airflow_secret="$(random_hex)"
admin_password="$(random_hex)"
postgres_password="$(random_hex)"
redis_password="$(random_hex)"

sed -i \
  -e "s|^AIRFLOW_UID=.*|AIRFLOW_UID=${host_uid}|" \
  -e "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=${fernet_key}|" \
  -e "s|^AIRFLOW_SECRET_KEY=.*|AIRFLOW_SECRET_KEY=${airflow_secret}|" \
  -e "s|^_AIRFLOW_WWW_USER_PASSWORD=.*|_AIRFLOW_WWW_USER_PASSWORD=${admin_password}|" \
  -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${postgres_password}|" \
  -e "s|^REDIS_PASSWORD=.*|REDIS_PASSWORD=${redis_password}|" \
  .env

chmod 600 .env

echo "Created .env with random local secrets (values were not printed)."
echo "Review .env, especially AIRFLOW_PORT and the initial admin username/email."
echo "Next: docker compose up -d --build"
