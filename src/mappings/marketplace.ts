import type { SubstrateBlock, SubstrateEvent } from "@subql/types";

import {
  MarketplaceListingSpvProposals,
  MarketplaceOngoingLawyerVotings,
  MarketplaceOngoingObjectListings,
  MarketplaceOngoingOffers,
  MarketplacePropertyLawyers,
  MarketplaceShareListings,
  MarketplaceShareOwners,
  MarketplaceUserLawyerVotes,
  RealEstateNft,
  RealWorldAsset,
} from "../types";

import {
  type OptionLike,
  asOption,
  asRecord,
  asStorageValue,
  formatError,
  getBigInt,
  getBoolean,
  getNumber,
  getStorageKeyArgs,
  getString,
  toJsonValue,
  toStringValue,
} from "./common";
let marketplaceSyncInFlight: Promise<void> | null = null;
let marketplaceSynced = false;

export async function handleMarketplaceEvent(
  event: SubstrateEvent,
): Promise<void> {
  const blockNumber = event.block.block.header.number.toNumber();
  const method = event.event.method;

  await ensureMarketplaceSynced(blockNumber);

  logger.info(`Block ${blockNumber}: marketplace.${method}`);

  const args = event.event.data as unknown[];

  switch (method) {
    case "ObjectListed":
    case "ObjectUpdated":
    case "ListingDelisted":
    case "PrimarySaleCompleted":
    case "PrimarySaleSoldOut":
    case "AllPropertySharesClaimed":
    case "UnclaimedRelisted":
    case "UnclaimedSharesWithdrawn":
    case "InvestmentCancelled":
    case "DeveloperDepositReturned":
      return syncListingFromEvent(method, args, blockNumber);
    case "PropertySharesBought":
      await syncListingFromEvent(method, args, blockNumber);
      return syncShareOwnerFromEvent(args, 0, 2, blockNumber);
    case "PropertySharesClaimed":
      await syncListingFromEvent(method, args, blockNumber);
      return syncShareOwnerFromEvent(args, 0, 2, blockNumber);
    case "SharesRelisted":
    case "RelistedSharesBought":
      return syncShareListingFromEvent(args, blockNumber);
    case "OfferCreated":
    case "OfferCancelled":
    case "OfferAccepted":
    case "OfferRejected":
      return syncOfferFromEvent(args, blockNumber);
    case "DeveloperLawyerProposed":
    case "SpvLawyerProposed":
    case "LawyerRemovedFromCase":
    case "DocumentsConfirmed":
    case "LawyerCostsAllocated":
    case "RealEstateLawyerProposalFinalized":
    case "SpvLawyerVoteFinalized":
      return syncPropertyLawyerFromEvent(method, args, blockNumber);
    case "VotedOnLawyer":
      return syncLawyerVotingFromEvent(args, blockNumber);
    // Events from issues #8 and #15 - these were silently dropped
    case "SpvCreated":
      return syncListingFromEvent(method, args, blockNumber);
    case "PropertySharesSent":
      return syncShareOwnerFromEvent(args, 1, 3, blockNumber);
    case "SaleCancelledUnclaimed":
      return syncListingFromEvent(method, args, blockNumber);
    case "RejectedFundsWithdrawn":
      return syncListingFromEvent(method, args, blockNumber);
    case "ExpiredFundsWithdrawn":
      return syncListingFromEvent(method, args, blockNumber);
    case "SharesUnfrozen":
      return syncListingFromEvent(method, args, blockNumber);
    default:
      return;
  }
}

export async function handleMarketplaceSyncBlock(
  block: SubstrateBlock,
): Promise<void> {
  const blockNumber = block.block.header.number.toNumber();
  await ensureMarketplaceSynced(blockNumber);
}

export async function ensureMarketplaceSynced(
  blockNumber: number,
): Promise<void> {
  if (marketplaceSynced) return;
  if (blockNumber == 0) {
    return;
  }
  marketplaceSyncInFlight ??= syncMarketplaceFromStorage(blockNumber)
    .then(() => {
      marketplaceSynced = true;
    })
    .catch((e) => {
      logger.error(
        `Block ${blockNumber}: marketplace storage sync failed — ${formatError(e)}`,
      );
    })
    .finally(() => {
      marketplaceSyncInFlight = null;
    });
  await marketplaceSyncInFlight;
}

function stringifyJson(value: unknown): string | undefined {
  if (value == null) return undefined;
  try {
    return JSON.stringify(value);
  } catch {
    return undefined;
  }
}

function getListingId(value: unknown): bigint | undefined {
  const num = getNumber(value);
  return num != null ? BigInt(num) : undefined;
}

function getField(
  record: Record<string, unknown>,
  snakeName: string,
  camelName: string,
): unknown {
  return record[snakeName] ?? record[camelName];
}

function getListingIdFromEvent(
  method: string,
  args: unknown[],
): bigint | undefined {
  switch (method) {
    case "LawyerRemovedFromCase":
    case "DocumentsConfirmed":
      return getListingId(args[1]);
    default:
      return getListingId(args[0]);
  }
}

async function syncListingFromEvent(
  method: string,
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  const listingId = getListingIdFromEvent(method, args);
  if (listingId == null) return;

  await syncListingSnapshot(listingId, blockNumber);
}

async function syncShareListingFromEvent(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  const listingId = getListingId(args[0]);
  if (listingId == null) return;

  await syncShareListing(listingId, blockNumber);
}

async function syncOfferFromEvent(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  const listingId = getListingId(args[0]);
  if (listingId == null) return;

  const offeror = toStringValue(args[1]);
  if (!offeror) return;

  await syncOngoingOffer(listingId, offeror, blockNumber);
}

async function syncShareOwnerFromEvent(
  args: unknown[],
  listingIndex: number,
  accountIndex: number,
  blockNumber: number,
): Promise<void> {
  const listingId = getListingId(args[listingIndex]);
  const account = toStringValue(args[accountIndex]);
  if (listingId == null || !account) return;

  await syncShareOwner(listingId, account, blockNumber);
}

async function syncPropertyLawyerFromEvent(
  method: string,
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  const listingId = getListingIdFromEvent(method, args);
  if (listingId == null) return;

  await syncPropertyLawyer(listingId, blockNumber);
  await syncListingSpvProposal(listingId, blockNumber);
}

async function syncLawyerVotingFromEvent(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  const listingId = getListingId(args[0]);
  const voter = toStringValue(args[1]);
  const proposalId = toStringValue(args[7]);

  if (listingId != null) {
    await syncListingSpvProposal(listingId, blockNumber);
  }

  if (proposalId) {
    await syncOngoingLawyerVoting(proposalId, blockNumber, listingId);
    if (voter) {
      await syncUserLawyerVote(proposalId, voter, blockNumber, listingId);
    }
  }
}

async function syncListingSnapshot(
  listingId: bigint,
  blockNumber: number,
): Promise<void> {
  await syncOngoingObjectListing(listingId, blockNumber);
  await syncShareListing(listingId, blockNumber);
  await syncPropertyLawyer(listingId, blockNumber);
  await syncListingSpvProposal(listingId, blockNumber);
}

async function syncMarketplaceFromStorage(blockNumber: number): Promise<void> {
  logger.info(`Block ${blockNumber}: syncing marketplace storage`);

  const pallet = asRecord(api.query?.marketplace);
  if (!pallet) {
    logger.error(`Block ${blockNumber}: marketplace pallet unavailable`);
    return;
  }

  const listingProposalMap = new Map<string, bigint>();

  await syncEntries(
    pallet.OngoingObjectListing ?? pallet.ongoingObjectListing,
    "marketplace.ongoingObjectListing",
    blockNumber,
    async (args, opt) => {
      const listingId = getListingId(args[0]);
      if (listingId == null) return;
      await upsertOngoingObjectListing(listingId, opt, blockNumber);
    },
  );

  await syncEntries(
    pallet.ShareListings ?? pallet.shareListings,
    "marketplace.shareListings",
    blockNumber,
    async (args, opt) => {
      const listingId = getListingId(args[0]);
      if (listingId == null) return;
      await upsertShareListing(listingId, opt, blockNumber);
    },
  );

  await syncEntries(
    pallet.PropertyLawyer ?? pallet.propertyLawyer,
    "marketplace.propertyLawyer",
    blockNumber,
    async (args, opt) => {
      const listingId = getListingId(args[0]);
      if (listingId == null) return;
      await upsertPropertyLawyer(listingId, opt, blockNumber);
    },
  );

  await syncEntries(
    pallet.ListingSpvProposal ?? pallet.listingSpvProposal,
    "marketplace.listingSpvProposal",
    blockNumber,
    async (args, opt) => {
      const listingId = getListingId(args[0]);
      if (listingId == null) return;
      const proposalId = opt?.isSome ? String(opt.unwrap()) : undefined;
      if (proposalId) {
        listingProposalMap.set(proposalId, listingId);
      }
      await upsertListingSpvProposal(listingId, proposalId, blockNumber);
    },
  );

  await syncEntries(
    pallet.OngoingLawyerVoting ?? pallet.ongoingLawyerVoting,
    "marketplace.ongoingLawyerVoting",
    blockNumber,
    async (args, opt) => {
      const proposalId = toStringValue(args[0]);
      if (!proposalId) return;
      const listingIdFromMap = listingProposalMap.get(proposalId);
      await upsertOngoingLawyerVoting(
        proposalId,
        blockNumber,
        listingIdFromMap,
      );
    },
  );

  await syncEntries(
    pallet.UserLawyerVote ?? pallet.userLawyerVote,
    "marketplace.userLawyerVote",
    blockNumber,
    async (args, opt) => {
      const proposalId = toStringValue(args[0]);
      const voter = toStringValue(args[1]);
      if (!proposalId || !voter) return;
      const listingIdFromMap = listingProposalMap.get(proposalId);
      await upsertUserLawyerVote(
        proposalId,
        voter,
        blockNumber,
        listingIdFromMap,
      );
    },
  );

  await syncEntries(
    pallet.ShareOwner ?? pallet.shareOwner,
    "marketplace.shareOwner",
    blockNumber,
    async (args, opt) => {
      const account = toStringValue(args[0]);
      const listingId = getListingId(args[1]);
      if (!account || listingId == null) return;
      await upsertShareOwner(listingId, account, opt, blockNumber);
    },
  );

  await syncEntries(
    pallet.OngoingOffers ?? pallet.ongoingOffers,
    "marketplace.ongoingOffers",
    blockNumber,
    async (args, opt) => {
      const listingId = getListingId(args[0]);
      const offeror = toStringValue(args[1]);
      if (listingId == null || !offeror) return;
      await upsertOngoingOffer(listingId, offeror, opt, blockNumber);
    },
  );

  logger.info(`Block ${blockNumber}: marketplace storage sync complete`);
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

async function upsertOngoingObjectListing(
  listingId: bigint,
  opt: ReturnType<typeof asOption> | undefined,
  blockNumber: number,
): Promise<void> {
  const id = listingId.toString();
  if (!opt?.isSome) {
    const existing = await MarketplaceOngoingObjectListings.get(id);
    if (existing) await MarketplaceOngoingObjectListings.remove(id);
    return;
  }

  const record = asRecord(toJsonValue(opt.unwrap()));
  if (!record) return;

  const assetIdNum = getNumber(getField(record, "asset_id", "assetId"));
  const collectionIdNum = getNumber(
    getField(record, "collection_id", "collectionId"),
  );
  const itemIdNum = getNumber(getField(record, "item_id", "itemId"));

  const realEstateNftId = await resolveRealEstateNftId(
    collectionIdNum,
    itemIdNum,
  );
  const realWorldAssetId = await resolveRealWorldAssetId(assetIdNum);

  const row = MarketplaceOngoingObjectListings.create({
    id,
    listingId: listingId,
    assetId: assetIdNum !== undefined ? BigInt(assetIdNum) : undefined,
    realWorldAssetId,
    collectionId: collectionIdNum !== undefined ? BigInt(collectionIdNum) : undefined,
    itemId: itemIdNum !== undefined ? BigInt(itemIdNum) : undefined,
    realEstateNftId,
    realEstateDeveloper: getString(
      getField(record, "real_estate_developer", "realEstateDeveloper"),
    ),
    sharePrice: getField(record, "share_price", "sharePrice") != null
      ? String(getField(record, "share_price", "sharePrice"))
      : undefined,
    shareAmount: getBigInt(getField(record, "share_amount", "shareAmount")),
    listedShareAmount: getBigInt(
      getField(record, "listed_share_amount", "listedShareAmount"),
    ),
    taxPaidByDeveloper: getBoolean(
      getField(record, "tax_paid_by_developer", "taxPaidByDeveloper"),
    ),
    tax: getBigInt(record.tax),
    listingExpiry: getBigInt(
      getField(record, "listing_expiry", "listingExpiry"),
    ),
    claimExpiry: getBigInt(getField(record, "claim_expiry", "claimExpiry")),
    relistCount: getBigInt(getField(record, "relist_count", "relistCount")),
    unclaimedShareAmount: getBigInt(
      getField(record, "unclaimed_share_amount", "unclaimedShareAmount"),
    ),
    collectedFunds: stringifyJson(
      toJsonValue(getField(record, "collected_funds", "collectedFunds")),
    ),
    collectedTax: stringifyJson(
      toJsonValue(getField(record, "collected_tax", "collectedTax")),
    ),
    collectedFees: stringifyJson(
      toJsonValue(getField(record, "collected_fees", "collectedFees")),
    ),
    investorFunds: stringifyJson(
      toJsonValue(getField(record, "investor_funds", "investorFunds")),
    ),
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertShareListing(
  listingId: bigint,
  opt: ReturnType<typeof asOption> | undefined,
  blockNumber: number,
): Promise<void> {
  const id = listingId.toString();
  if (!opt?.isSome) {
    const existing = await MarketplaceShareListings.get(id);
    if (existing) await MarketplaceShareListings.remove(id);
    return;
  }

  const record = asRecord(toJsonValue(opt.unwrap()));
  if (!record) return;

  const assetIdNum = getNumber(getField(record, "asset_id", "assetId"));
  const assetId = assetIdNum !== undefined ? BigInt(assetIdNum) : undefined;
  const collectionIdNum = getNumber(
    getField(record, "collection_id", "collectionId"),
  );
  const itemIdNum = getNumber(getField(record, "item_id", "itemId"));

  const realEstateNftId = await resolveRealEstateNftId(
    collectionIdNum,
    itemIdNum,
  );
  const realWorldAssetId = await resolveRealWorldAssetId(assetIdNum);

  const row = MarketplaceShareListings.create({
    id,
    listingId: listingId,
    ongoingObjectListingId: id,
    seller: getString(record.seller),
    sharePrice: getField(record, "share_price", "sharePrice") != null
      ? String(getField(record, "share_price", "sharePrice"))
      : undefined,
    assetId: assetId,
    realWorldAssetId,
    collectionId: collectionIdNum !== undefined ? BigInt(collectionIdNum) : undefined,
    itemId: itemIdNum !== undefined ? BigInt(itemIdNum) : undefined,
    realEstateNftId,
    amount: getBigInt(record.amount),
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertPropertyLawyer(
  listingId: bigint,
  opt: ReturnType<typeof asOption> | undefined,
  blockNumber: number,
): Promise<void> {
  const id = listingId.toString();
  if (!opt?.isSome) {
    const existing = await MarketplacePropertyLawyers.get(id);
    if (existing) await MarketplacePropertyLawyers.remove(id);
    return;
  }

  const record = asRecord(toJsonValue(opt.unwrap()));
  if (!record) return;

  const row = MarketplacePropertyLawyers.create({
    id,
    listingId: listingId,
    ongoingObjectListingId: id,
    realEstateDeveloperLawyer: getString(
      getField(record, "real_estate_developer_lawyer", "realEstateDeveloperLawyer"),
    ),
    spvLawyer: getString(getField(record, "spv_lawyer", "spvLawyer")),
    realEstateDeveloperStatus: getString(
      getField(record, "real_estate_developer_status", "realEstateDeveloperStatus"),
    ),
    spvStatus: getString(getField(record, "spv_status", "spvStatus")),
    realEstateDeveloperLawyerCosts: getString(
      getField(record, "real_estate_developer_lawyer_costs", "realEstateDeveloperLawyerCosts"),
    ),
    spvLawyerCosts: getString(getField(record, "spv_lawyer_costs", "spvLawyerCosts")),
    legalProcessExpiry: getBigInt(
      getField(record, "legal_process_expiry", "legalProcessExpiry"),
    ),
    secondAttempt: getBoolean(record.second_attempt ?? record.secondAttempt),
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertListingSpvProposal(
  listingId: bigint,
  proposalId: string | undefined,
  blockNumber: number,
): Promise<void> {
  const id = listingId.toString();

  const row = MarketplaceListingSpvProposals.create({
    id,
    listingId: listingId,
    proposalId,
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertOngoingLawyerVoting(
  proposalId: string,
  blockNumber: number,
  listingId?: bigint,
): Promise<void> {
  const id = proposalId;

  const row = MarketplaceOngoingLawyerVotings.create({
    id,
    listingId: listingId,
    proposalId,
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertUserLawyerVote(
  proposalId: string,
  voter: string,
  blockNumber: number,
  listingId?: bigint,
): Promise<void> {
  const id = `${proposalId}-${voter}`;

  const row = MarketplaceUserLawyerVotes.create({
    id,
    listingId,
    proposalId,
    voter,
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertShareOwner(
  listingId: bigint,
  account: string,
  opt: ReturnType<typeof asOption> | undefined,
  blockNumber: number,
): Promise<void> {
  const id = `${listingId.toString()}-${account}`;

  if (opt?.isSome) {
    const record = asRecord(toJsonValue(opt.unwrap()));
    if (!record) return;

    const row = MarketplaceShareOwners.create({
      id,
      listingId: listingId,
      ongoingObjectListingId: listingId.toString(),
      account: account,
      shareAmount: getBigInt(getField(record, "share_amount", "shareAmount")),
      paidFunds: getField(record, "paid_funds", "paidFunds") != null
        ? String(getField(record, "paid_funds", "paidFunds"))
        : undefined,
      paidTax: getField(record, "paid_tax", "paidTax") != null
        ? String(getField(record, "paid_tax", "paidTax"))
        : undefined,
      relistCount: getBigInt(getField(record, "relist_count", "relistCount")),
      updatedBlock: blockNumber,
    });

    await row.save();
  } else {
    const existing = await MarketplaceShareOwners.get(id);
    if (existing) await MarketplaceShareOwners.remove(id);
  }
}

async function upsertOngoingOffer(
  listingId: bigint,
  offeror: string,
  opt: ReturnType<typeof asOption> | undefined,
  blockNumber: number,
): Promise<void> {
  const id = `${listingId.toString()}-${offeror}`;

  if (opt?.isSome) {
    const record = asRecord(toJsonValue(opt.unwrap()));
    if (!record) return;

    const assetId = getNumber(getField(record, "asset_id", "assetId"));
    const collectionId = getNumber(
      getField(record, "collection_id", "collectionId"),
    );
    const itemId = getNumber(getField(record, "item_id", "itemId"));

    const realEstateNftId = await resolveRealEstateNftId(
      collectionId,
      itemId,
    );
    const realWorldAssetId = await resolveRealWorldAssetId(assetId);

    const row = MarketplaceOngoingOffers.create({
      id,
      listingId: listingId,
      ongoingObjectListingId: listingId.toString(),
      offeror,
      sharePrice: getField(record, "share_price", "sharePrice") != null
        ? String(getField(record, "share_price", "sharePrice"))
        : undefined,
      amount: getBigInt(record.amount),
      paymentAssets: getBigInt(
        getField(record, "payment_assets", "paymentAssets"),
      ),
      paymentAssetId: realWorldAssetId,
      nonce: getString(record.nonce),
      updatedBlock: blockNumber,
    });

    await row.save();
  } else {
    const existing = await MarketplaceOngoingOffers.get(id);
    if (existing) await MarketplaceOngoingOffers.remove(id);
  }
}

// ---------------------------------------------------------------------------
// Storage sync helpers — bridge from event-triggered queries to upserts.
// These use bigint to match the metadata U64 types for listing/asset IDs.
// ---------------------------------------------------------------------------

async function syncOngoingObjectListing(
  listingId: bigint,
  blockNumber: number,
): Promise<void> {
  try {
    const opt = asOption(await api.query.marketplace.ongoingObjectListing(listingId));
    await upsertOngoingObjectListing(listingId, opt, blockNumber);
  } catch (e) {
    logger.warn(
      `Block ${blockNumber}: ongoingObjectListing(${listingId}) failed: ${formatError(e)}`,
    );
  }
}

async function syncShareListing(
  listingId: bigint,
  blockNumber: number,
): Promise<void> {
  try {
    const opt = asOption(await api.query.marketplace.shareListings(listingId));
    await upsertShareListing(listingId, opt, blockNumber);
  } catch (e) {
    logger.warn(
      `Block ${blockNumber}: shareListings(${listingId}) failed: ${formatError(e)}`,
    );
  }
}

async function syncPropertyLawyer(
  listingId: bigint,
  blockNumber: number,
): Promise<void> {
  try {
    const opt = asOption(await api.query.marketplace.propertyLawyer(listingId));
    await upsertPropertyLawyer(listingId, opt, blockNumber);
  } catch (e) {
    logger.warn(
      `Block ${blockNumber}: propertyLawyer(${listingId}) failed: ${formatError(e)}`,
    );
  }
}

async function syncListingSpvProposal(
  listingId: bigint,
  blockNumber: number,
): Promise<void> {
  try {
    const opt = asOption(
      await api.query.marketplace.listingSpvProposal(listingId),
    );
    const proposalId = opt?.isSome ? String(opt.unwrap()) : undefined;
    await upsertListingSpvProposal(listingId, proposalId, blockNumber);
  } catch (e) {
    logger.warn(
      `Block ${blockNumber}: listingSpvProposal(${listingId}) failed: ${formatError(e)}`,
    );
  }
}

async function syncOngoingOffer(
  listingId: bigint,
  offeror: string,
  blockNumber: number,
): Promise<void> {
  try {
    const opt = asOption(
      await api.query.marketplace.ongoingOffers(listingId, offeror),
    );
    await upsertOngoingOffer(listingId, offeror, opt, blockNumber);
  } catch (e) {
    logger.warn(
      `Block ${blockNumber}: ongoingOffers(${listingId}, ${offeror}) failed: ${formatError(e)}`,
    );
  }
}

async function syncShareOwner(
  listingId: bigint,
  account: string,
  blockNumber: number,
): Promise<void> {
  try {
    const opt = asOption(
      await api.query.marketplace.shareOwner(account, listingId),
    );
    await upsertShareOwner(listingId, account, opt, blockNumber);
  } catch (e) {
    logger.warn(
      `Block ${blockNumber}: shareOwner(${account}, ${listingId}) failed: ${formatError(e)}`,
    );
  }
}

async function syncOngoingLawyerVoting(
  proposalId: string,
  blockNumber: number,
  listingId?: bigint,
): Promise<void> {
  try {
    const opt = asOption(
      await api.query.marketplace.ongoingLawyerVoting(proposalId),
    );
    await upsertOngoingLawyerVoting(proposalId, blockNumber, listingId);
  } catch (e) {
    logger.warn(
      `Block ${blockNumber}: ongoingLawyerVoting(${proposalId}) failed: ${formatError(e)}`,
    );
  }
}

async function syncUserLawyerVote(
  proposalId: string,
  voter: string,
  blockNumber: number,
  listingId?: bigint,
): Promise<void> {
  try {
    const opt = asOption(
      await api.query.marketplace.userLawyerVote(proposalId, voter),
    );
    await upsertUserLawyerVote(proposalId, voter, blockNumber, listingId);
  } catch (e) {
    logger.warn(
      `Block ${blockNumber}: userLawyerVote(${proposalId}, ${voter}) failed: ${formatError(e)}`,
    );
  }
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

async function resolveRealWorldAssetId(
  assetId: number | undefined,
): Promise<string | undefined> {
  if (assetId == null) return undefined;

  const id = assetId.toString();
  const row = await RealWorldAsset.get(id);
  return row?.id;
}
