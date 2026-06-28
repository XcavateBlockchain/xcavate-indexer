import type { SubstrateBlock, SubstrateEvent } from "@subql/types";

import { RealEstateNft } from "../types";

import {
  asRecord,
  asStorageValue,
  formatError,
  getBoolean,
  getNumber,
  getStorageKeyArgs,
  getString,
  getStringArray,
  parseJson,
  toUtf8String,
} from "./common";

let realEstateNftsSyncInFlight: Promise<void> | null = null;
let realEstateNftsSynced = false;

export async function handleRealEstateNftsEvent(
  event: SubstrateEvent,
): Promise<void> {
  const blockNumber = event.block.block.header.number.toNumber();
  const method = event.event.method;

  await ensureRealEstateNftsSynced(blockNumber);

  logger.info(`Block ${blockNumber}: realEstateNfts.${method}`);

  switch (method) {
    case "ItemMetadataSet":
      return handleItemMetadataSet(event, blockNumber);
    case "Burned":
      return handleBurned(event, blockNumber);
  }
}

export async function handleRealEstateNftsSyncBlock(
  block: SubstrateBlock,
): Promise<void> {
  const blockNumber = block.block.header.number.toNumber();
  await ensureRealEstateNftsSynced(blockNumber);
}

async function handleItemMetadataSet(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [collectionArg, itemArg, dataArg] = event.event.data as unknown[];
  const collection = String(collectionArg);
  const item = String(itemArg);

  logger.info(
    `Block ${blockNumber}: ItemMetadataSet — collection=${collection}, item=${item}`,
  );

  await upsertPropertyFromMetadata(
    blockNumber,
    collection,
    item,
    dataArg,
    "event",
  );
}

async function handleBurned(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [collectionArg, itemArg] = event.event.data as unknown[];
  const collection = Number(String(collectionArg));
  const item = Number(String(itemArg));

  logger.info(
    `Block ${blockNumber}: Burned — collection=${collection}, item=${item}`,
  );

  const rows = await RealEstateNft.getByFields(
    [
      ["collection", "=", collection],
      ["item", "=", item],
    ],
    { limit: 10 },
  );

  if (rows.length === 0) {
    logger.warn(
      `Block ${blockNumber}: Burned for (${collection}, ${item}) but no Property row found`,
    );
    return;
  }

  for (const row of rows) {
    await RealEstateNft.remove(row.id);
    logger.info(
      `Block ${blockNumber}: removed property ${row.id} (burned NFT ${collection}-${item})`,
    );
  }
}

export async function ensureRealEstateNftsSynced(
  blockNumber: number,
): Promise<void> {
  if (realEstateNftsSynced) return;
  if (blockNumber == 0) {
    return;
  }
  realEstateNftsSyncInFlight ??= syncRealEstateNftsFromStorage(blockNumber)
    .then(() => {
      realEstateNftsSynced = true;
    })
    .catch((e) => {
      logger.error(
        `Block ${blockNumber}: realEstateNfts storage sync failed — ${formatError(e)}`,
      );
    })
    .finally(() => {
      realEstateNftsSyncInFlight = null;
    });
  await realEstateNftsSyncInFlight;
}

async function syncRealEstateNftsFromStorage(blockNumber: number): Promise<void> {
  logger.info(
    `Block ${blockNumber}: syncing realEstateNfts item metadata from storage`,
  );

  const pallet = asRecord(api.query?.realEstateNfts);
  const itemMetadataOf = (pallet?.itemMetadataOf ??
    pallet?.ItemMetadataOf) as
    | { entries?: () => Promise<unknown> }
    | undefined;
  const entriesFn = itemMetadataOf?.entries;
  if (typeof entriesFn !== "function") {
    logger.error(
      `Block ${blockNumber}: realEstateNfts.itemMetadataOf.entries is unavailable`,
    );
    return;
  }

  const entries = await entriesFn.call(itemMetadataOf);
  if (!Array.isArray(entries) || entries.length === 0) {
    logger.info(`Block ${blockNumber}: no realEstateNfts metadata in storage`);
    return;
  }

  let synced = 0;
  for (const [storageKey, metadataOpt] of entries) {
    const args = getStorageKeyArgs(storageKey);
    if (!args || args.length < 2) {
      logger.warn(
        `Block ${blockNumber}: realEstateNfts storage key missing args`,
      );
      continue;
    }

    const collection = String(args[0]);
    const item = String(args[1]);

    const opt = asStorageValue(metadataOpt);
    if (!opt?.isSome) continue;

    const metadata = asRecord(opt.unwrap());
    const dataArg = metadata?.data;
    if (dataArg == null) {
      logger.warn(
        `Block ${blockNumber}: ItemMetadataOf ${collection}-${item} missing data`,
      );
      continue;
    }

    await upsertPropertyFromMetadata(
      blockNumber,
      collection,
      item,
      dataArg,
      "storage",
    );
    synced += 1;
  }

  logger.info(
    `Block ${blockNumber}: synced ${synced} realEstateNfts items from storage`,
  );
}

async function upsertPropertyFromMetadata(
  blockNumber: number,
  collection: string,
  item: string,
  dataArg: unknown,
  source: "event" | "storage",
): Promise<void> {
  try {
    const jsonStr = toUtf8String(dataArg);
    const data = parseJson(jsonStr);
    const record = asRecord(data);
    if (!record) {
      logger.warn(
        `Block ${blockNumber}: ${source} metadata for ${collection}-${item} returned non-object payload`,
      );
      return;
    }

    const id = getString(record.id) ?? `${collection}-${item}`;
    const company = asRecord(record.company);
    const address = asRecord(record.address);
    const financials = asRecord(record.financials);
    const attributes = asRecord(record.attributes);
    const propertyName = getString(record.propertyName);

    const property = RealEstateNft.create({
      id,
      collection: getNumber(collection) ?? undefined,
      item: getNumber(item) ?? undefined,
      propertyId: getString(record.propertyId),
      propertyName,
      propertyType: getString(record.propertyType),
      status: getString(record.status),
      propertyDescription: getString(record.propertyDescription),
      developerAddress: getString(record.developerAddress),
      accountAddress: getString(record.accountAddress),
      legalRepresentative: getString(record.legalRepresentative),
      planningCode: getString(record.planningCode),
      buildingControlCode: getString(record.buildingControlCode),
      map: getString(record.map),
      createdAt: getString(record.createdAt),
      updatedAt: getString(record.updatedAt),

      companyName: getString(company?.name),
      companyLogo: getString(company?.logo),

      street: getString(address?.street),
      townCity: getString(address?.townCity),
      postCode: getString(address?.postCode),
      flatOrUnit: getString(address?.flatOrUnit),
      localAuthority: getString(address?.localAuthority),

      propertyPrice: getNumber(financials?.propertyPrice),
      pricePerToken: getNumber(financials?.pricePerToken),
      numberOfTokens: getNumber(financials?.numberOfTokens),
      estimatedRentalIncome: getNumber(financials?.estimatedRentalIncome),
      stampDutyTax: getNumber(financials?.stampDutyTax),
      annualServiceCharge: getNumber(financials?.annualServiceCharge),
      isStampDutyPaid: getBoolean(financials?.isStampDutyPaid),
      isAnnualServiceChargePaid: getBoolean(
        financials?.isAnnualServiceChargePaid,
      ),

      numberOfBedrooms: getNumber(attributes?.numberOfBedrooms),
      numberOfBathrooms: getNumber(attributes?.numberOfBathrooms),
      area: getString(attributes?.area),
      outdoorSpace: getString(attributes?.outdoorSpace),
      offStreetParking: getString(attributes?.offStreetParking),
      quality: getString(attributes?.quality),
      constructionDate: getString(attributes?.constructionDate),

      files: getStringArray(record.files),
    });

    await property.save();
    logger.info(
      `Block ${blockNumber}: saved property ${id} — ${propertyName ?? "unknown"} (${source})`,
    );
  } catch (e) {
    logger.error(
      `Block ${blockNumber}: failed to decode property (${source}) — ${formatError(e)}`,
    );
  }
}
