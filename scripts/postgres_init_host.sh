#!/bin/sh
set -eu

forest_host_password=$(cat /run/forest-secrets/host_postgres_password)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v forest_host_password="$forest_host_password" <<'SQL'
CREATE ROLE forest_host LOGIN PASSWORD :'forest_host_password'
    NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE DATABASE forest_host OWNER forest_host;
REVOKE CONNECT ON DATABASE forest_host FROM PUBLIC;
REVOKE CONNECT ON DATABASE forest_deployment FROM PUBLIC;
SQL
unset forest_host_password
