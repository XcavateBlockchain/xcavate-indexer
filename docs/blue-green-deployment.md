# Blue-Green Deployment Strategy for Xcavate Indexer

## Overview

This document describes the blue-green deployment strategy for running the Xcavate SubQuery indexer with zero downtime during upgrades. The core idea is straightforward:

- **Instance A (Blue)**: Currently serving queries — the live production indexer.
- **Instance B (Green)**: Starts fresh, syncs from block 1 using updated indexer code, and only begins serving queries once fully caught up.

Once Instance B has synced to the current chain tip and is verified healthy, it becomes the live instance. Instance A is then either decommissioned or kept as the warm standby for the next upgrade cycle.

This approach guarantees that **query clients never see an outage** — they are always talking to at least one fully functional GraphQL endpoint.

## Why This Pattern

SubQuery indexers are data-intensive and slow to sync — a full resync from block 1 can take many hours or days depending on chain history and hardware. Traditional "stop old, deploy new" deployments cause downtime during this sync period. Blue-green deployment decouples the sync process from the live query path:

1. The live instance stays running and serving queries uninterrupted.
2. The new instance syncs in the background on its own resources.
3. When ready, traffic switches atomically (DNS or load balancer update).
4. The old instance becomes the warm standby.

## Architecture

```
                    ┌───────────────────────────────┐
                    │         Load Balancer /        │
                    │         DNS Router             │
                    │         (Caddy/Nginx)           │
                    └──────┬──────────────────┬─────┘
                           │                  │
                    ┌──────▼─────┐    ┌───────▼──────┐
                    │  Instance A │    │  Instance B  │
                    │  (Blue)     │    │  (Green)     │
                    │             │    │              │
                    │  Running    │    │  Syncing     │
                    │  GraphQL    │    │  GraphQL     │
                    │  API        │    │  API         │
                    │  :3000      │    │  :3001       │
                    └──────┬─────┘    └───────┬──────┘
                           │                  │
                    ┌──────▼──────────────────▼──────┐
                    │         Shared PostgreSQL        │
                    │         (or separate DBs)        │
                    │                                  │
                    │  DB Schema: app_blue / app_green │
                    └──────────────────────────────────┘
```

### Key Design Decisions

- **Separate database schemas per instance** — each instance uses its own
  `db-schema` value (e.g., `app_blue` / `app_green`). This lets both run
  simultaneously without schema conflicts, since each version of the indexer
  code may have different entity definitions.
- **Separate network configurations** — each instance points to the same
  chain RPC endpoint but is configured independently in the docker-compose
  file.
- **External load balancer** — Caddy or Nginx routes `/` to the active
  instance's GraphQL port. Switching is a config change + reload (sub-second).
- **Optional: separate PostgreSQL instances** — for very large chains or
  when both instances need full parallel performance, run separate PG
  containers. For most cases, a single PG with separate schemas works fine.

## Deployment Guide

### Prerequisites

- Docker Compose v2
- Access to the Xcavate chain RPC endpoint(s) (archive node recommended)
- At least 2x the disk/RAM required for a single instance (or separate PG
  containers)
- A load balancer (Caddy recommended for auto TLS, Nginx works too)

### Step 1 — Initial Setup (Instance A)

Run the first instance using the existing `docker-compose.yml`. No changes needed.

```bash
cd /path/to/xcavate-indexer

# Start instance A (blue) — the initial live instance
docker compose up -d postgres subquery-node graphql-engine
```

At this point, Instance A is syncing. Once it reaches the chain tip, it
becomes the production service.

### Step 2 — Prepare for an Upgrade

When a code change needs to be deployed (new handler logic, schema
migration, dependency update):

**a. Create and merge the code change:**

```bash
# On main branch (or deploy target)
git pull origin main
# Make your changes, commit, create PR, merge to main
```

**b. Create the second instance configuration:**

In the same project directory, create a second docker-compose configuration
for the new instance:

```bash
# Create a copy for instance B (green) with different ports and schema
cp docker-compose.yml docker-compose-v2.yml
```

Then edit `docker-compose-v2.yml`:

1. **Rename service identifiers** to avoid conflicts:
   - `postgres` → `postgres_green`
   - `subquery-node` → `subquery-node_green`
   - `graphql-engine` → `graphql-engine_green`

2. **Change database schema** to avoid conflicts:
   - In `subquery-node` section: change `--db-schema=app` to `--db-schema=app_green`
   - In `postgres_green` environment: keep POSTGRES_PASSWORD and DB envs the same

3. **Change GraphQL port**:
   - In `graphql-engine_green` section: change port mapping from `3000:3000` to `3001:3000`
   - In `graphql-engine_green` command: change `--name=app` to `--name=app_green`

4. **Change Prometheus/Grafana ports** (optional — or remove if you only
   need one set of monitoring):
   - Prometheus: `9091:9090` (instead of 9090:9090)
   - Grafana: `3003:3000` (instead of 3002:3000)

5. **Use the latest image tag** to pick up code changes. If building from
   source, add a build step.

Example `docker-compose-v2.yml` excerpt:

```yaml
services:
  postgres_green:
    build:
      context: .
      dockerfile: ./docker/pg-Dockerfile
    restart: unless-stopped
    ports:
      - 5434:5432
    volumes:
      - .data/postgres_green:/var/lib/postgresql/data
    environment:
      POSTGRES_PASSWORD: postgres

  subquery-node_green:
    image: subquerynetwork/subql-node-substrate:latest
    depends_on:
      "postgres_green":
        condition: service_healthy
    restart: unless-stopped
    environment:
      DB_USER: postgres
      DB_PASS: postgres
      DB_DATABASE: postgres
      DB_HOST: postgres_green
      DB_PORT: 5432
    volumes:
      - ./:/app
    command:
      - -f=/app/project.ts
      - --db-schema=app_green    # DIFFERENT SCHEMA
      - --workers=2
      - --batch-size=15
      - --unfinalized-blocks=false
      - --unsafe
      - --skip-transactions

  graphql-engine_green:
    image: subquerynetwork/subql-query:latest
    ports:
      - 3001:3000               # DIFFERENT PORT
    depends_on:
      "postgres_green":
        condition: service_healthy
      "subquery-node_green":
        condition: service_healthy
    restart: always
    environment:
      DB_USER: postgres
      DB_PASS: postgres
      DB_DATABASE: postgres
      DB_HOST: postgres_green
      DB_PORT: 5432
      GRAPHQL_CORS_ORIGINS: https://realxmessage.xcavate.io,https://indexer.realxmarket.io
    command:
      - --name=app_green        # DIFFERENT NAME
      - --playground
      - --indexer=http://subquery-node_green:3000
      - --cors
```

### Step 3 — Start the Green Instance (Syncing)

```bash
# Start instance B (green) in the background — it will sync from block 1
docker compose -f docker-compose-v2.yml up -d
```

Monitor the sync progress:

```bash
# Check subquery-node logs
docker compose -f docker-compose-v2.yml logs -f subquery-node_green

# Check GraphQL endpoint readiness (returns 200 when ready)
curl -s http://localhost:3001/ready

# Check the synced block number via GraphQL
curl -s -X POST http://localhost:3001/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ _metadata { block { number } } }"}' \
  | python3 -m json.tool
```

### Step 4 — Monitor Sync Progress

While green is syncing, continue running blue as production. Monitor both:

```bash
# Compare blocks
# Blue (current production)
curl -s -X POST http://localhost:3000/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ _metadata { block { number } } }"}'

# Green (catching up)
curl -s -X POST http://localhost:3001/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ _metadata { block { number } } }"}'
```

When green's block number matches blue's, green is fully caught up.

### Step 5 — Cutover (Atomic Switch)

Once green is fully synced, switch traffic to it.

**Option A: Docker Compose only (no load balancer)**

If you don't have an external load balancer, you can stop blue and restart
green on port 3000:

```bash
# Stop blue instance
docker compose stop postgres subquery-node graphql-engine

# Update docker-compose-v2.yml to use port 3000 (the production port)
# Edit the graphql-engine section: port 3000:3000

# Start green on port 3000
docker compose -f docker-compose-v2.yml up -d
```

**Option B: Nginx load balancer (recommended)**

With Nginx, update the upstream config to point to green:

```nginx
# /etc/nginx/conf.d/indexer.conf
upstream graphql_backend {
    server 127.0.0.1:3001;  # Switched from 3000 (blue) to 3001 (green)
}

server {
    listen 80;
    server_name indexer.realxmarket.io;

    location /graphql {
        proxy_pass http://graphql_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

```bash
sudo nginx -t && sudo nginx -s reload
```

**Option C: Caddy (simpler, auto TLS)**

```caddyfile
# /etc/caddy/Caddyfile
indexer.realxmarket.io {
    reverse_proxy 127.0.0.1:3001  # Switched from :3000 (blue) to :3001 (green)
}
```

```bash
sudo caddy reload
```

### Step 6 — Post-Cutover Verification

After switching, verify that all queries work correctly:

```bash
# Check GraphQL playground is accessible
curl -s http://localhost:3001/graphql

# Run a health query
curl -s -X POST http://localhost:3001/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ _metadata { network { network } } }"}'

# Spot-check a known entity
curl -s -X POST http://localhost:3001/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ buckets(limit: 1) { id name bucketId } }"}'
```

### Step 7 — Promote Blue to Standby (Optional)

After cutover, the old blue instance (Instance A) can be repurposed as the
warm standby for the next upgrade:

1. Stop the old blue GraphQL engine but keep it running (it's still at the
   latest block).
2. On the next upgrade, start a new green instance (now blue becomes green).
3. This way, at any time, one instance is always live and the other is at
   the latest state.

```bash
# Keep blue running but stop the GraphQL engine
docker compose stop graphql-engine

# Restart it on port 3000 after green has been promoted (blue becomes green)
# by updating docker-compose-v2.yml and running it again
```

### Step 8 — Cleanup Old Resources

After confirming the new instance is stable for 24-48 hours:

```bash
# Stop and remove the old instance
docker compose -f docker-compose-v2.yml down

# Optionally remove old database data to free space
docker compose -f docker-compose-v2.yml down -v

# Remove the old compose file
rm docker-compose-v2.yml

# Rename the new compose file back to the standard name
mv docker-compose.yml docker-compose-v2.yml
# (or keep both, just swap roles)
```

## Rollback Procedure

If something goes wrong with the green instance after cutover:

```bash
# Quick rollback: switch the load balancer back to blue
# Nginx:
upstream graphql_backend {
    server 127.0.0.1:3000;  # Switch back to blue
}
sudo nginx -s reload

# Caddy:
reverse_proxy 127.0.0.1:3000  # Back to blue

# If no load balancer:
# Stop green, restart blue on port 3000
docker compose -f docker-compose-v2.yml stop
docker compose start
```

Since blue was never stopped, its data is fully consistent and queries will
resume immediately.

## Schema Migration Considerations

When the GraphQL schema (`schema.graphql`) changes between deployments, you
must handle PostgreSQL schema migration carefully:

### Safe Schema Migration Pattern

1. **Add new entities/fields** — safe to add without downtime. New tables
   are created automatically by SubQuery. Old queries continue to work.

2. **Rename or remove entities/fields** — these are breaking changes.
   Plan carefully:
   - Old queries referencing removed fields will fail.
   - Old instances may not have the new tables.
   - **Solution:** Deploy green with the new schema. Green will create
     the new tables and begin writing data. After cutover, old clients
     must be updated to use new fields, or you can keep old entity names
     as aliases.

3. **Change entity types** — if an entity's primary key structure changes,
   you cannot migrate data from the old schema to the new automatically.
   You may need:
   - A migration script that runs before deployment (export/import)
   - Or run the indexer from genesis on both schemas, accepting the sync
     time as downtime

4. **Additive changes only during active deployment** — the safest
   approach for zero-downtime deployments is to only add new entities,
   fields, and indexes in a deployment. Breaking changes should be
   deployed during a planned maintenance window.

### Schema Migration Checklist

Before deploying with schema changes:

- [ ] All new entities are backward compatible (no data loss from old queries)
- [ ] No existing entity/field is removed in this deployment
- [ ] All query clients are notified of new fields
- [ ] Database migration script is tested on a staging chain
- [ ] Rollback plan accounts for schema differences

## Monitoring and Health Checks

### Health Check Endpoints

Each instance exposes a readiness endpoint:

```bash
# Blue instance (port 3000)
curl -s -X GET http://localhost:3000/ready

# Green instance (port 3001)
curl -s -X GET http://localhost:3001/ready
```

Return `200 OK` when the GraphQL engine is ready, `503` when not.

### Sync Progress Monitoring

Monitor block progression to detect sync stalls:

```bash
# Check blue's synced block
curl -s -X POST http://localhost:3000/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ _metadata { block { number } } }"}'

# Check green's synced block
curl -s -X POST http://localhost:3001/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ _metadata { block { number } } }"}'
```

### Prometheus Metrics

Both instances expose Prometheus metrics at `/metrics`. Key metrics:

| Metric | Description |
|--------|-------------|
| `subql_runtime_block_processing` | Current block being processed |
| `subql_runtime_blocks_indexed_total` | Total blocks indexed |
| `subql_runtime_error_total` | Total processing errors |
| `postgres_connection_active` | Active DB connections |

Access at `http://localhost:9090` (or `:9091` for green).

### Grafana Dashboards

Import the SubQuery dashboard template at `http://localhost:3002` (blue)
or `http://localhost:3003` (green) to visualize sync progress, query
latency, and error rates.

## Resource Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| RAM | 8 GB (single instance) | 16 GB (both instances) |
| CPU | 4 cores | 8 cores |
| Disk (PostgreSQL) | 200 GB | 500 GB |
| Disk (Docker volumes) | 100 GB | 200 GB |
| Network | 100 Mbps | 1 Gbps |

**For blue-green deployments:** double the resources (or use separate
PostgreSQL instances with shared disk).

## Full Example: Complete docker-compose Files

### Production docker-compose.yml (Blue)

```yaml
services:
  postgres:
    image: postgres:14-alpine
    restart: unless-stopped
    ports:
      - 5432:5432
    volumes:
      - pg_blue_data:/var/lib/postgresql/data
    environment:
      POSTGRES_PASSWORD: postgres

  subquery-node:
    image: subquerynetwork/subql-node-substrate:latest
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped
    environment:
      DB_USER: postgres
      DB_PASS: postgres
      DB_DATABASE: postgres
      DB_HOST: postgres
      DB_PORT: 5432
    volumes:
      - ./:/app
    command:
      - -f=/app/project.ts
      - --db-schema=app_blue
      - --workers=2
      - --batch-size=15
      - --unfinalized-blocks=false
      - --unsafe
      - --skip-transactions

  graphql-engine:
    image: subquerynetwork/subql-query:latest
    ports:
      - 3000:3000
    depends_on:
      postgres:
        condition: service_healthy
      subquery-node:
        condition: service_healthy
    restart: always
    environment:
      DB_USER: postgres
      DB_PASS: postgres
      DB_DATABASE: postgres
      DB_HOST: postgres
      DB_PORT: 5432
      GRAPHQL_CORS_ORIGINS: https://realxmessage.xcavate.io,https://indexer.realxmarket.io
    command:
      - --name=app_blue
      - --playground
      - --indexer=http://subquery-node:3000
      - --cors

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    command:
      - --config.file=/etc/prometheus/prometheus.yml
    ports:
      - 9090:9090
    restart: always

  grafana:
    image: grafana/grafana:latest
    ports:
      - 3002:3000
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    restart: always

volumes:
  pg_blue_data:
```

### Staging docker-compose-v2.yml (Green)

```yaml
services:
  postgres:
    image: postgres:14-alpine
    restart: unless-stopped
    ports:
      - 5433:5432
    volumes:
      - pg_green_data:/var/lib/postgresql/data
    environment:
      POSTGRES_PASSWORD: postgres

  subquery-node:
    image: subquerynetwork/subql-node-substrate:latest
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped
    environment:
      DB_USER: postgres
      DB_PASS: postgres
      DB_DATABASE: postgres
      DB_HOST: postgres
      DB_PORT: 5432
    volumes:
      - ./:/app
    command:
      - -f=/app/project.ts
      - --db-schema=app_green
      - --workers=2
      - --batch-size=15
      - --unfinalized-blocks=false
      - --unsafe
      - --skip-transactions

  graphql-engine:
    image: subquerynetwork/subql-query:latest
    ports:
      - 3001:3000
    depends_on:
      postgres:
        condition: service_healthy
      subquery-node:
        condition: service_healthy
    restart: always
    environment:
      DB_USER: postgres
      DB_PASS: postgres
      DB_DATABASE: postgres
      DB_HOST: postgres
      DB_PORT: 5432
      GRAPHQL_CORS_ORIGINS: https://realxmessage.xcavate.io,https://indexer.realxmarket.io
    command:
      - --name=app_green
      - --playground
      - --indexer=http://subquery-node:3000
      - --cors

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus-v2.yml:/etc/prometheus/prometheus.yml
    command:
      - --config.file=/etc/prometheus/prometheus.yml
    ports:
      - 9091:9090
    restart: always

  grafana:
    image: grafana/grafana:latest
    ports:
      - 3003:3000
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    restart: always

volumes:
  pg_green_data:
```

## Maintenance Checklist

### Pre-Deployment

- [ ] Code review and test the new indexer version locally
- [ ] Verify schema.graphql changes are backward compatible
- [ ] Test sync on a staging chain or local fork
- [ ] Prepare the green docker-compose configuration
- [ ] Update load balancer config

### During Deployment

- [ ] Start green instance: `docker compose -f docker-compose-v2.yml up -d`
- [ ] Monitor sync progress until green reaches chain tip
- [ ] Verify green's queries work correctly
- [ ] Switch load balancer to green
- [ ] Monitor error rates and latency for 30 minutes

### Post-Deployment

- [ ] Verify all queries return expected results
- [ ] Check Prometheus/Grafana for any anomalies
- [ ] Update documentation if schema changed
- [ ] Promote blue to warm standby
- [ ] Delete old compose file and volumes after 24-48 hours

### Weekly Review

- [ ] Check disk usage for both instances
- [ ] Review logs for errors
- [ ] Verify sync gap between blue and green (if both running)
- [ ] Update dependency versions if applicable

## Troubleshooting

### Green instance never catches up

- Check that the chain RPC endpoint is healthy and responsive
- Increase `--workers` and `--batch-size` in the green config
- Check disk I/O speed — PostgreSQL on slow disks syncs slowly
- Verify no resource constraints (RAM/CPU limits on the host)

### Query errors after cutover

- Check that the new schema matches what clients expect
- Verify the GraphQL engine is healthy: `curl http://localhost:3001/ready`
- Check logs: `docker compose -f docker-compose-v2.yml logs graphql-engine_green`
- Roll back immediately if errors persist

### Database disk full

- Both instances store data in PostgreSQL data directories (`.data/postgres*`)
- Monitor disk usage: `docker volume ls` + `df -h`
- Prune unused data: `docker system prune`
- Expand the volume before the next sync cycle

### Port conflicts

- Ensure 3000, 3001, 5432, 5433, 9090, 9091, 3002, 3003 are not used by other services
- Change ports in docker-compose if conflicts exist
- Update load balancer config accordingly

## Appendix: Quick Commands Reference

```bash
# Start blue (production)
docker compose up -d

# Start green (syncing)
docker compose -f docker-compose-v2.yml up -d

# Check sync status
curl -s -X POST http://localhost:3000/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ _metadata { block { number } } }"}'

# Check health
curl -s http://localhost:3000/ready
curl -s http://localhost:3001/ready

# View logs
docker compose logs -f subquery-node
docker compose -f docker-compose-v2.yml logs -f subquery-node

# Stop blue
docker compose stop

# Stop green
docker compose -f docker-compose-v2.yml stop

# Full cleanup
docker compose down -v
docker compose -f docker-compose-v2.yml down -v
```