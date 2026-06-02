#!/bin/sh

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<'EOF'
DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'btree_gist') THEN
		CREATE EXTENSION IF NOT EXISTS btree_gist;
	END IF;
END
$$;
EOF