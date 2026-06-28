import path from "path";

import {
  SubstrateDatasourceKind,
  SubstrateHandlerKind,
  type SubstrateProject,
} from "@subql/types";
import * as dotenv from "dotenv";

const mode = process.env.NODE_ENV ?? "production";

// Load the appropriate .env file
const dotenvPath = path.resolve(
  __dirname,
  `.env${mode !== "production" ? `.${mode}` : ""}`,
);
dotenv.config({ path: dotenvPath, quiet: true });

// Can expand the Datasource processor types via the generic param
const project: SubstrateProject = {
  specVersion: "1.0.0",
  version: "0.0.2",
  name: "xcavate-indexer",
  description:
    "SubQuery indexer for the Xcavate parachain: real-estate NFTs, buckets, messages, and marketplace.",
  runner: {
    node: {
      name: "@subql/node",
      version: ">=3.0.1",
    },
    query: {
      name: "@subql/query",
      version: "*",
    },
  },
  schema: {
    file: "./schema.graphql",
  },
  network: {
    /* The genesis hash of the network (hash of block 0) */
    chainId: process.env.CHAIN_ID!,
    /**
     * These endpoint(s) should be public non-pruned archive node
     * We recommend providing more than one endpoint for improved reliability, performance, and uptime
     * Public nodes may be rate limited, which can affect indexing speed
     * When developing your project we suggest getting a private API key
     * If you use a rate limited endpoint, adjust the --batch-size and --workers parameters
     * These settings can be found in your docker-compose.yaml, they will slow indexing but prevent your project being rate limited
     */
    endpoint: process.env.ENDPOINT!.split(","),
  },
  dataSources: [
    {
      kind: SubstrateDatasourceKind.Runtime,
      startBlock: 1,
      mapping: {
        file: "./dist/index.js",
        handlers: [
          {
            kind: SubstrateHandlerKind.Block,
            handler: "handleStartupSyncBlock",
          },
          {
            kind: SubstrateHandlerKind.Block,
            handler: "handleRealEstateNftsSyncBlock",
          },
          {
            kind: SubstrateHandlerKind.Block,
            handler: "handleMarketplaceSyncBlock",
          },
          {
            kind: SubstrateHandlerKind.Block,
            handler: "handleRealWorldAssetsSyncBlock",
          },
          {
            kind: SubstrateHandlerKind.Event,
            handler: "handleRealEstateNftsEvent",
            filter: {
              module: "realEstateNfts",
            },
          },
          {
            kind: SubstrateHandlerKind.Event,
            handler: "handleMarketplaceEvent",
            filter: {
              module: "marketplace",
            },
          },
          {
            kind: SubstrateHandlerKind.Event,
            handler: "handleRealWorldAssetsEvent",
            filter: {
              module: "realWorldAsset",
            },
          },
          {
            kind: SubstrateHandlerKind.Event,
            handler: "handleBucketsEvent",
            filter: {
              module: "buckets",
            },
          },
        ],
      },
    },
  ],
};

// Must set default to the project instance
export default project;
