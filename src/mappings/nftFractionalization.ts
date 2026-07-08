import type { SubstrateBlock, SubstrateEvent } from "@subql/types";

import { NftFractionalized, NftUnified } from "../types";

import {
  getNumber,
  formatError,
} from "./common";

// ---------------------------------------------------------------------------
// Schema: NftFractionalization
// type NftFractionalized @entity { id: ID! collectionId: Int itemId: Int }
// type NftUnified @entity { id: ID! collectionId: Int itemId: Int }
// ---------------------------------------------------------------------------

let nftFractionalizationSyncInFlight: Promise<void> | null = null;
let nftFractionalizationSynced = false;

export async function handleNftFractionalizationEvent(
  event: SubstrateEvent,
): Promise<void> {
  const blockNumber = event.block.block.header.number.toNumber();
  const method = event.event.method;

  logger.info(`Block ${blockNumber}: nftFractionalization.${method}`);

  const args = event.event.data as unknown[];

  switch (method) {
    case "NftFractionalized":
      return handleNftFractionalized(args, blockNumber);
    case "NftUnified":
      return handleNftUnified(args, blockNumber);
    default:
      return;
  }
}

export async function handleNftFractionalizationSyncBlock(
  block: SubstrateBlock,
): Promise<void> {
  const blockNumber = block.block.header.number.toNumber();
  await ensureNftFractionalizationSynced(blockNumber);
}

export async function ensureNftFractionalizationSynced(
  blockNumber: number,
): Promise<void> {
  if (nftFractionalizationSynced) return;
  if (blockNumber == 0) return;
  nftFractionalizationSyncInFlight ??= syncNftFractionalizationFromStorage(
    blockNumber,
  )
    .then(() => {
      nftFractionalizationSynced = true;
    })
    .catch((e) => {
      logger.error(
        `Block ${blockNumber}: nftFractionalization storage sync failed — ${formatError(e)}`,
      );
    })
    .finally(() => {
      nftFractionalizationSyncInFlight = null;
    });
  await nftFractionalizationSyncInFlight;
}

async function handleNftFractionalized(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event fields: collection_id, item_id
  const collectionId = getNumber(args[0]);
  const itemId = getNumber(args[1]);
  if (collectionId == null || itemId == null) return;

  const id = `${collectionId}-${itemId}`;
  const existing = await NftFractionalized.get(id);

  const row = NftFractionalized.create({
    id,
    collectionId,
    itemId,
    createdBlock: existing?.createdBlock ?? blockNumber,
  });
  await row.save();
}

async function handleNftUnified(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event fields: collection_id, item_id
  const collectionId = getNumber(args[0]);
  const itemId = getNumber(args[1]);
  if (collectionId == null || itemId == null) return;

  const id = `${collectionId}-${itemId}`;
  const existing = await NftUnified.get(id);

  const row = NftUnified.create({
    id,
    collectionId,
    itemId,
    createdBlock: existing?.createdBlock ?? blockNumber,
  });
  await row.save();
}

// Storage sync helpers
async function syncNftFractionalizationFromStorage(
  blockNumber: number,
): Promise<void> {
  logger.info(`Block ${blockNumber}: syncing nftFractionalization storage`);
  // Storage items would go here if there are relevant storage maps
  logger.info(`Block ${blockNumber}: nftFractionalization storage sync complete`);
}