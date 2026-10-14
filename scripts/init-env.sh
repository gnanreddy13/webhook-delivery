#!/usr/bin/env bash
# Creates .env from .env.example with a random API key and database password.
# Refuses to overwrite an existing .env unless run with --force.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f .env && "${1:-}" != "--force" ]]; then
  echo ".env already exists; not overwriting (use --force to replace it)" >&2
  exit 1
fi

rand_hex() { od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'; }

DB_PASSWORD=$(rand_hex 24)
API_KEY=$(rand_hex 32)
ENC_KEY=$(rand_hex 32)

sed -e "s/REPLACE_WITH_DB_PASSWORD/$DB_PASSWORD/g" \
    -e "s/^API_KEY=.*/API_KEY=$API_KEY/" \
    -e "s/^SECRET_ENCRYPTION_KEY=$/SECRET_ENCRYPTION_KEY=$ENC_KEY/" \
    .env.example > .env.tmp
chmod 600 .env.tmp
mv .env.tmp .env
echo "wrote .env with a generated API_KEY, SECRET_ENCRYPTION_KEY and POSTGRES_PASSWORD (mode 600)"
echo "note: if a Postgres volume from an older .env exists, its password won't change; reset it with: docker compose down -v"
