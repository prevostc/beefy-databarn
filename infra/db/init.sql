CREATE EXTENSION IF NOT EXISTS pg_stat_statements;


-- this will revoke default database privileges (CREATE, CONNECT ...) from roles in 'PUBLIC' (all roles).
REVOKE ALL ON DATABASE beefy FROM public;
REVOKE ALL ON SCHEMA public FROM public;
REVOKE ALL ON DATABASE postgres FROM public;
-- drop owned by grafana_ro cascade;
-- drop user grafana_ro;


DO $$
BEGIN
    CREATE USER grafana_ro WITH PASSWORD 'grafana_ro' NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, moving to next statement', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

GRANT CONNECT ON DATABASE beefy TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;

DO $$
BEGIN
    CREATE USER api_ro WITH PASSWORD 'api_ro' NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, moving to next statement', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

GRANT CONNECT ON DATABASE beefy TO api_ro;
GRANT USAGE ON SCHEMA public TO api_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO api_ro;

DO $$
BEGIN
    CREATE USER meltano WITH PASSWORD 'meltano' NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, moving to next statement', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;
CREATE SCHEMA IF NOT EXISTS meltano;
GRANT USAGE ON SCHEMA public TO meltano;
GRANT USAGE ON SCHEMA pg_catalog TO meltano;
GRANT USAGE ON SCHEMA information_schema TO meltano;
GRANT CONNECT ON DATABASE beefy TO meltano;
GRANT ALL ON SCHEMA meltano TO meltano;
GRANT ALL ON ALL TABLES IN SCHEMA meltano TO meltano;
ALTER ROLE meltano SET search_path TO meltano,public;
