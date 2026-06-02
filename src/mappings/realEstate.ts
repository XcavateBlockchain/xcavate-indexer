import type { SubstrateEvent } from "@subql/types";

import { Property } from "../types";

import {
  asRecord,
  formatError,
  getBoolean,
  getNumber,
  getString,
  getStringArray,
  parseJson,
  toUtf8String,
} from "./common";

export async function handleRealEstateEvent(
  event: SubstrateEvent,
): Promise<void> {
  const blockNumber = event.block.block.header.number.toNumber();
  const method = event.event.method;

  logger.info(`Block ${blockNumber}: realEstateNfts.${method}`);

  switch (method) {
    case "ItemMetadataSet":
      return handleItemMetadataSet(event, blockNumber);
    case "Burned":
      return handleBurned(event, blockNumber);
  }
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

  try {
    const jsonStr = toUtf8String(dataArg);
    const data = parseJson(jsonStr);
    const record = asRecord(data);
    if (!record) {
      logger.warn(
        `Block ${blockNumber}: ItemMetadataSet for ${collection}-${item} returned non-object metadata`,
      );
      return;
    }

    const id = getString(record.id) ?? `${collection}-${item}`;
    const company = asRecord(record.company);
    const address = asRecord(record.address);
    const financials = asRecord(record.financials);
    const attributes = asRecord(record.attributes);
    const propertyName = getString(record.propertyName);

    const property = Property.create({
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
      `Block ${blockNumber}: saved property ${id} — ${propertyName ?? "unknown"}`,
    );
  } catch (e) {
    logger.error(
      `Block ${blockNumber}: failed to decode property — ${formatError(e)}`,
    );
  }
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

  const rows = await Property.getByFields(
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
    await Property.remove(row.id);
    logger.info(
      `Block ${blockNumber}: removed property ${row.id} (burned NFT ${collection}-${item})`,
    );
  }
}
