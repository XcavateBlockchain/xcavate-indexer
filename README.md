# Xcavate Indexer

SubQuery indexer for the Xcavate parachain (Paseo testnet). Exposes a GraphQL API consumed by the Xcavate mobile app.

Indexed pallets:

| Pallet           | Entities                                                |
| ---------------- | ------------------------------------------------------- |
| `buckets`        | `Bucket`, `BucketContributor`, `BucketAdmin`, `Message` |
| `did`            | `Did`                                                   |
| `realEstateNfts` | `Property` *(handler disabled in `project.ts`)*         |

## Run locally

```bash
npm install
cp .env.example .env   # set ENDPOINT + CHAIN_ID
npm run dev
```

GraphQL playground: <http://localhost:3000>.

Requires Docker. Local config uses `--unsafe` (needed for IPFS fetches) and `--unfinalized-blocks=false` (matches OnFinality).

## Example query

```graphql
{
  buckets(orderBy: BUCKET_ID_ASC) {
    nodes {
      id name creator isWritable
      messages(orderBy: CREATED_BLOCK_DESC, first: 10) {
        nodes { id contributor contentType ipfsContent }
      }
    }
  }
}
```

## Deploy to OnFinality

```bash
npm run build
subql publish
```

Paste the resulting IPFS CID into the OnFinality dashboard. Required settings:

- **Enable Unsafe Flag** — ON (needed for IPFS fetch)
- **Enable Skip Transactions** — ON (3× speed-up)
- **Enable Historical Data** — OFF
- **Unfinalized Blocks** — OFF
- **Batch Size** — 100
- **Query Limit** — 500

Schema change → pick *Reindex*. Code-only change → *Continue*.

## Notes

- `startBlock` only applies on a fresh DB. On restart the indexer resumes from `_metadata.lastProcessedHeight`. To re-index from scratch: `sudo rm -rf .data && docker compose down -v`.
- `ensureBucket` (in `src/mappings/buckets.ts`) backfills missing parent Bucket rows from chain storage. Without it, child FK inserts would crash the worker when starting after a `BucketCreated` event. Backfilled rows have `creator = null` and an approximate `createdBlock`.
- IPFS content is fetched only for `contentType` starting with `text/plain`. Failures are silent and never retried — re-index to recover.
- Gateway hardcoded in `src/mappings/common.ts`.

## Layout

```
project.ts         SubQuery config (handlers, startBlock)
schema.graphql     entity definitions
docker-compose.yml local stack
src/mappings/
  buckets.ts       bucket/membership/message handlers + ensureBucket
  did.ts           DID handlers + key resolution
  realEstate.ts    Property handler (disabled)
  common.ts        utf8 / hex / IPFS helpers
```

## License

MIT.
