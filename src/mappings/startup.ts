import type { SubstrateBlock } from "@subql/types";

import { formatError } from "./common";
import { ensureBucketsSynced } from "./buckets";
import { ensureDidsSynced } from "./did";
import { ensureMarketplaceSynced } from "./marketplace";
import { ensureRealEstateNftsSynced } from "./realEstateNfts";

let startupSyncInFlight: Promise<void> | null = null;
let startupSynced = false;

export async function handleStartupSyncBlock(
  block: SubstrateBlock,
): Promise<void> {
  const blockNumber = block.block.header.number.toNumber();
  if (startupSynced) return;

  startupSyncInFlight ??= (async () => {
    try {
      logger.info(`Block ${blockNumber}: startup storage sync begin`);
      await ensureDidsSynced(blockNumber);
      await ensureRealEstateNftsSynced(blockNumber);
      await ensureBucketsSynced(blockNumber);
      await ensureMarketplaceSynced(blockNumber);
      startupSynced = true;
      logger.info(`Block ${blockNumber}: startup storage sync complete`);
    } catch (e) {
      logger.error(
        `Block ${blockNumber}: startup storage sync failed — ${formatError(e)}`,
      );
    } finally {
      startupSyncInFlight = null;
    }
  })();

  await startupSyncInFlight;
}
