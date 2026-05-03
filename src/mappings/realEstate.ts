import { SubstrateEvent } from "@subql/types";
import { Property } from "../types";
import { bytesToUtf8 } from "./common";

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
  const [collectionArg, itemArg, dataArg] = event.event.data;
  const collection = collectionArg.toString();
  const item = itemArg.toString();

  logger.info(
    `Block ${blockNumber}: ItemMetadataSet — collection=${collection}, item=${item}`,
  );

  try {
    const jsonStr = bytesToUtf8(dataArg);
    const data = JSON.parse(jsonStr);
    const id: string = data.id ?? `${collection}-${item}`;

    const property = Property.create({
      id,
      collection: Number(collection),
      item: Number(item),
      propertyId: data.propertyId ?? undefined,
      propertyName: data.propertyName ?? undefined,
      propertyType: data.propertyType ?? undefined,
      status: data.status ?? undefined,
      propertyDescription: data.propertyDescription ?? undefined,
      developerAddress: data.developerAddress ?? undefined,
      accountAddress: data.accountAddress ?? undefined,
      legalRepresentative: data.legalRepresentative ?? undefined,
      planningCode: data.planningCode ?? undefined,
      buildingControlCode: data.buildingControlCode ?? undefined,
      map: data.map ?? undefined,
      createdAt: data.createdAt ?? undefined,
      updatedAt: data.updatedAt ?? undefined,

      companyName: data.company?.name ?? undefined,
      companyLogo: data.company?.logo ?? undefined,

      street: data.address?.street ?? undefined,
      townCity: data.address?.townCity ?? undefined,
      postCode: data.address?.postCode ?? undefined,
      flatOrUnit: data.address?.flatOrUnit ?? undefined,
      localAuthority: data.address?.localAuthority ?? undefined,

      propertyPrice: data.financials?.propertyPrice ?? undefined,
      pricePerToken: data.financials?.pricePerToken ?? undefined,
      numberOfTokens: data.financials?.numberOfTokens ?? undefined,
      estimatedRentalIncome:
        data.financials?.estimatedRentalIncome ?? undefined,
      stampDutyTax: data.financials?.stampDutyTax ?? undefined,
      annualServiceCharge: data.financials?.annualServiceCharge ?? undefined,
      isStampDutyPaid: data.financials?.isStampDutyPaid ?? undefined,
      isAnnualServiceChargePaid:
        data.financials?.isAnnualServiceChargePaid ?? undefined,

      numberOfBedrooms: data.attributes?.numberOfBedrooms ?? undefined,
      numberOfBathrooms: data.attributes?.numberOfBathrooms ?? undefined,
      area: data.attributes?.area ?? undefined,
      outdoorSpace: data.attributes?.outdoorSpace ?? undefined,
      offStreetParking: data.attributes?.offStreetParking ?? undefined,
      quality: data.attributes?.quality ?? undefined,
      constructionDate: data.attributes?.constructionDate ?? undefined,

      files: Array.isArray(data.files) ? data.files : undefined,
    });

    await property.save();
    logger.info(
      `Block ${blockNumber}: saved property ${id} — ${data.propertyName}`,
    );
  } catch (e) {
    logger.error(`Block ${blockNumber}: failed to decode property — ${e}`);
  }
}

async function handleBurned(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [collectionArg, itemArg] = event.event.data;
  const collection = Number(collectionArg.toString());
  const item = Number(itemArg.toString());

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
