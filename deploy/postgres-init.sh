#!/bin/sh
set -eu
export NDS_MIGRATOR_PASSWORD="$(cat /run/secrets/postgres_migrator_password)"
export NDS_RUNTIME_PASSWORD="$(cat /run/secrets/postgres_runtime_password)"
psql --no-psqlrc --set ON_ERROR_STOP=1 --username postgres --dbname nds_observability <<'SQL'
\getenv migrator_password NDS_MIGRATOR_PASSWORD
\getenv runtime_password NDS_RUNTIME_PASSWORD
CREATE ROLE nds_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'migrator_password';
CREATE ROLE nds_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'runtime_password';
REVOKE ALL ON DATABASE nds_observability FROM PUBLIC;
GRANT CONNECT ON DATABASE nds_observability TO nds_migrator,nds_runtime;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
ALTER SCHEMA public OWNER TO nds_migrator;
GRANT USAGE ON SCHEMA public TO nds_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE nds_migrator IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO nds_runtime;
ALTER ROLE nds_runtime SET statement_timeout='3s';
ALTER ROLE nds_runtime SET lock_timeout='1s';
ALTER ROLE nds_runtime SET idle_in_transaction_session_timeout='5s';
SQL
unset NDS_MIGRATOR_PASSWORD NDS_RUNTIME_PASSWORD
