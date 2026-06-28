import type { SubstrateBlock, SubstrateEvent } from "@subql/types";

import {
  RealEstateNft,
  RealWorldAsset,
  RealWorldAssetOwner,
} from "../types";

import {
  type OptionLike,
  asOption,
  asRecord,
  asStorageValue,
  formatError,
  getBoolean,
  getNumber,
  getStorageKeyArgs,
  toJsonValue,
  toStringValue,
  toUtf8String,
} from "./common";

let realWorldAssetsSyncInFlight: Promise<void> | null = null;
let realWorldAssetsSynced = false;

export async function handleRealWorldAssetsEvent(
  event: SubstrateEvent,
): Promise<void> {
  const blockNumber = event.block.block.header.number.toNumber();
  const method = event.event.method;

  await ensureRealWorldAssetsSynced(blockNumber);

  logger.info(`Block ${blockNumber}: realWorldAsset.${method}`);

  const args = event.event.data as unknown[];

  switch (method) {
    case "PropertySharesCreated":
      return syncAssetFromEvent(args[0], blockNumber);
    case "PropertyNftBurned":
      return syncAssetFromEvent(args[2], blockNumber);
    default:
      return;
  }
}

export async function handleRealWorldAssetsSyncBlock(
  block: SubstrateBlock,
): Promise<void> {
  const blockNumber = block.block.header.number.toNumber();
  await ensureRealWorldAssetsSynced(blockNumber);
}

export async function ensureRealWorldAssetsSynced(
  blockNumber: number,
): Promise<void> {
  if (realWorldAssetsSynced) return;
  if (blockNumber == 0) {
    return;
  }
  realWorldAssetsSyncInFlight ??= syncRealWorldAssetsFromStorage(blockNumber)
    .then(() => {
      realWorldAssetsSynced = true;
    })
    .catch((e) => {
      logger.error(
        `Block ${blockNumber}: realWorldAsset storage sync failed — ${formatError(e)}`,
      );
    })
    .finally(() => {
      realWorldAssetsSyncInFlight = null;
    });
  await realWorldAssetsSyncInFlight;
}

function getField(
  record: Record<string, unknown>,
  snakeName: string,
  camelName: string,
): unknown {
  return record[snakeName] ?? record[camelName];
}

function stringifyValue(value: unknown): string | undefined {
  const stringValue = toStringValue(value);
  if (stringValue != null) return stringValue;

  try {
    return JSON.stringify(toJsonValue(value));
  } catch {
    return undefined;
  }
}

function getLocation(value: unknown): string | undefined {
  if (value == null) return undefined;
  try {
    return toUtf8String(value);
  } catch {
    return stringifyValue(value);
  }
}

function getAccountArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(String);

  const record = asRecord(value);
  if (typeof record?.toArray === "function") {
    const result = (record.toArray as () => unknown).call(value);
    if (Array.isArray(result)) return result.map(String);
  }

  const jsonValue = toJsonValue(value);
  if (Array.isArray(jsonValue)) return jsonValue.map(String);

  return [];
}

function getAssetId(value: unknown): number | undefined {
  return getNumber(value);
}

async function syncRealWorldAssetsFromStorage(
  blockNumber: number,
): Promise<void> {
  logger.info(`Block ${blockNumber}: syncing realWorldAsset storage`);

  const pallet = asRecord(api.query?.realWorldAsset);
  if (!pallet) {
    logger.error(`Block ${blockNumber}: realWorldAsset pallet unavailable`);
    return;
  }

  await syncEntries(
    pallet.PropertyAssetInfo ?? pallet.propertyAssetInfo,
    "realWorldAsset.propertyAssetInfo",
    blockNumber,
    async (args, opt) => {
      const assetId = getAssetId(args[0]);
      if (assetId == null) return;
      await upsertRealWorldAsset(assetId, opt, blockNumber);
    },
  );

  await syncEntries(
    pallet.PropertyOwner ?? pallet.propertyOwner,
    "realWorldAsset.propertyOwner",
    blockNumber,
    async (args, opt) => {
      const assetId = getAssetId(args[0]);
      if (assetId == null) return;
      await upsertOwners(assetId, opt, blockNumber);
    },
  );

  await syncEntries(
    pallet.PropertyOwnerShares ?? pallet.propertyOwnerShares,
    "realWorldAsset.propertyOwnerShares",
    blockNumber,
    async (args, opt) => {
      const assetId = getAssetId(args[0]);
      const account = toStringValue(args[1]);
      if (assetId == null || !account) return;
      await upsertOwnerShare(assetId, account, opt, blockNumber);
    },
  );

  logger.info(`Block ${blockNumber}: realWorldAsset storage sync complete`);
}

async function syncEntries(
  storage: unknown,
  storageName: string,
  blockNumber: number,
  handle: (args: unknown[], opt: OptionLike) => Promise<void>,
): Promise<void> {
  const record = asRecord(storage);
  const target = (typeof storage === "function" ? storage : record) as
    | { entries?: () => Promise<unknown> }
    | undefined;
  const entriesFn = target?.entries;
  if (typeof entriesFn !== "function") {
    logger.warn(`Block ${blockNumber}: ${storageName}.entries unavailable`);
    return;
  }

  const entries = await entriesFn.call(target);
  if (!Array.isArray(entries) || entries.length === 0) {
    logger.info(`Block ${blockNumber}: ${storageName} storage entries=0`);
    return;
  }

  let synced = 0;
  for (const [storageKey, value] of entries) {
    const args = getStorageKeyArgs(storageKey);
    if (!args) continue;
    await handle(args, asStorageValue(value));
    synced += 1;
  }

  logger.info(
    `Block ${blockNumber}: ${storageName} storage entries=${entries.length}, handled=${synced}`,
  );
}

async function syncAssetFromEvent(
  assetArg: unknown,
  blockNumber: number,
): Promise<void> {
  const assetId = getAssetId(assetArg);
  if (assetId == null) return;

  try {
    const opt = asOption(
      await api.query.realWorldAsset.propertyAssetInfo(assetId),
    );
    await upsertRealWorldAsset(assetId, opt, blockNumber);

    const ownersOpt = asStorageValue(
      await api.query.realWorldAsset.propertyOwner(assetId),
    );
    await upsertOwners(assetId, ownersOpt, blockNumber);
  } catch (e) {
    logger.warn(
      `Block ${blockNumber}: realWorldAsset(${assetId}) sync failed: ${formatError(e)}`,
    );
  }
}

async function upsertRealWorldAsset(
  assetId: number,
  opt: OptionLike | undefined,
  blockNumber: number,
): Promise<void> {
  const id = assetId.toString();
  if (!opt?.isSome) {
    const existing = await RealWorldAsset.get(id);
    if (existing) await RealWorldAsset.remove(id);
    return;
  }

  const record = asRecord(toJsonValue(opt.unwrap()));
  if (!record) return;

  const collectionId = getNumber(getField(record, "collection_id", "collectionId"));
  const itemId = getNumber(getField(record, "item_id", "itemId"));
  const realEstateNftId = await resolveRealEstateNftId(collectionId, itemId);
  const existing = await RealWorldAsset.get(id);

  const row = RealWorldAsset.create({
    id,
    assetId,
    collectionId: collectionId ?? undefined,
    itemId: itemId ?? undefined,
    realEstateNftId,
    region: getNumber(record.region),
    location: getLocation(record.location),
    price: stringifyValue(record.price),
    shareAmount: getNumber(getField(record, "share_amount", "shareAmount")),
    spvCreated: getBoolean(getField(record, "spv_created", "spvCreated")),
    finalized: getBoolean(record.finalized),
    ownerAccounts: existing?.ownerAccounts,
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertOwners(
  assetId: number,
  opt: OptionLike | undefined,
  blockNumber: number,
): Promise<void> {
  const id = assetId.toString();
  const asset = await RealWorldAsset.get(id);
  if (!asset) {
    logger.warn(
      `Block ${blockNumber}: realWorldAsset owners skipped; asset ${assetId} missing`,
    );
    return;
  }

  const accounts = opt?.isSome ? getAccountArray(opt.unwrap()) : [];
  asset.ownerAccounts = accounts.length > 0 ? accounts : undefined;
  asset.updatedBlock = blockNumber;
  await asset.save();

  for (const account of accounts) {
    const ownerId = `${assetId}-${account}`;
    const existing = await RealWorldAssetOwner.get(ownerId);
    const row = RealWorldAssetOwner.create({
      id: ownerId,
      assetId: id,
      assetIdNumber: assetId,
      account,
      shareAmount: existing?.shareAmount,
      updatedBlock: blockNumber,
    });
    await row.save();
  }
}

async function upsertOwnerShare(
  assetId: number,
  account: string,
  opt: OptionLike | undefined,
  blockNumber: number,
): Promise<void> {
  const assetRowId = assetId.toString();
  const asset = await RealWorldAsset.get(assetRowId);
  if (!asset) {
    logger.warn(
      `Block ${blockNumber}: realWorldAsset owner share skipped; asset ${assetId} missing`,
    );
    return;
  }

  const ownerId = `${assetId}-${account}`;
  const shareAmount = opt?.isSome ? getNumber(opt.unwrap()) : undefined;
  const row = RealWorldAssetOwner.create({
    id: ownerId,
    assetId: assetRowId,
    assetIdNumber: assetId,
    account,
    shareAmount,
    updatedBlock: blockNumber,
  });
  await row.save();
}

async function resolveRealEstateNftId(
  collectionId: number | undefined,
  itemId: number | undefined,
): Promise<string | undefined> {
  if (collectionId == null || itemId == null) return undefined;

  const rows = await RealEstateNft.getByFields(
    [
      ["collection", "=", collectionId],
      ["item", "=", itemId],
    ],
    { limit: 1 },
  );

  return rows[0]?.id;
}
