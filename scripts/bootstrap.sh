#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
  echo ".env already exists; leaving it unchanged."
  exit 0
fi

command -v openssl >/dev/null 2>&1 || {
  echo "ERROR: openssl is required to generate local secrets." >&2
  exit 1
}

cp .env.example .env

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
