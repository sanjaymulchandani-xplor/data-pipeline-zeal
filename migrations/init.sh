#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    $(cat /docker-entrypoint-initdb.d/001_initial_schema.sql)
EOSQL

