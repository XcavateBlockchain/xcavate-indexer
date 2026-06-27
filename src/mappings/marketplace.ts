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
      return syncTokenOwnerFromEvent(args, 0, 2, blockNumber);
    case "PropertySharesClaimed":
      await syncListingFromEvent(method, args, blockNumber);
      return syncTokenOwnerFromEvent(args, 0, 2, blockNumber);
    case "SharesRelisted":
    case "RelistedSharesBought":
      return syncTokenListingFromEvent(args, blockNumber);
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

function getListingId(value: unknown): number | undefined {
  return getNumber(value) ?? undefined;
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
): number | undefined {
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

async function syncTokenListingFromEvent(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  const listingId = getListingId(args[0]);
  if (listingId == null) return;

  await syncTokenListing(listingId, blockNumber);
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

async function syncTokenOwnerFromEvent(
  args: unknown[],
  listingIndex: number,
  accountIndex: number,
  blockNumber: number,
): Promise<void> {
  const listingId = getListingId(args[listingIndex]);
  const account = toStringValue(args[accountIndex]);
  if (listingId == null || !account) return;

  await syncTokenOwner(listingId, account, blockNumber);
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
  listingId: number,
  blockNumber: number,
): Promise<void> {
  await syncOngoingObjectListing(listingId, blockNumber);
  await syncTokenListing(listingId, blockNumber);
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

  const listingProposalMap = new Map<string, number>();

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
      await upsertTokenListing(listingId, opt, blockNumber);
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
      const listingId = listingProposalMap.get(proposalId);
      await upsertOngoingLawyerVoting(
        proposalId,
        listingId,
        opt,
        blockNumber,
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
      const listingId = listingProposalMap.get(proposalId);
      await upsertUserLawyerVote(
        proposalId,
        voter,
        listingId,
        opt,
        blockNumber,
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
      await upsertTokenOwner(listingId, account, opt, blockNumber);
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
  listingId: number,
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

  const row = MarketplaceOngoingObjectListings.create({
    id,
    listingId,
    assetId: assetId ?? undefined,
    realWorldAssetId,
    collectionId: collectionId ?? undefined,
    itemId: itemId ?? undefined,
    realEstateNftId,
    realEstateDeveloper: getString(
      getField(record, "real_estate_developer", "realEstateDeveloper"),
    ),
    tokenPrice: getField(record, "token_price", "tokenPrice") != null
      ? String(getField(record, "token_price", "tokenPrice"))
      : undefined,
    tokenAmount: getNumber(getField(record, "token_amount", "tokenAmount")),
    listedTokenAmount: getNumber(
      getField(record, "listed_token_amount", "listedTokenAmount"),
    ),
    taxPaidByDeveloper: getBoolean(
      getField(record, "tax_paid_by_developer", "taxPaidByDeveloper"),
    ),
    tax: getNumber(record.tax),
    listingExpiry: getNumber(
      getField(record, "listing_expiry", "listingExpiry"),
    ),
    claimExpiry: getNumber(getField(record, "claim_expiry", "claimExpiry")),
    relistCount: getNumber(getField(record, "relist_count", "relistCount")),
    unclaimedTokenAmount: getNumber(
      getField(record, "unclaimed_token_amount", "unclaimedTokenAmount"),
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

async function upsertTokenListing(
  listingId: number,
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

  const row = MarketplaceShareListings.create({
    id,
    listingId,
    ongoingObjectListingId: id,
    seller: getString(record.seller),
    tokenPrice: getField(record, "token_price", "tokenPrice") != null
      ? String(getField(record, "token_price", "tokenPrice"))
      : undefined,
    assetId: assetId ?? undefined,
    realWorldAssetId,
    collectionId: collectionId ?? undefined,
    itemId: itemId ?? undefined,
    realEstateNftId,
    amount: getNumber(record.amount),
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertPropertyLawyer(
  listingId: number,
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
    listingId,
    ongoingObjectListingId: id,
    realEstateDeveloperLawyer: getString(
      getField(
        record,
        "real_estate_developer_lawyer",
        "realEstateDeveloperLawyer",
      ),
    ),
    spvLawyer: getString(getField(record, "spv_lawyer", "spvLawyer")),
    realEstateDeveloperStatus: getString(
      getField(
        record,
        "real_estate_developer_status",
        "realEstateDeveloperStatus",
      ),
    ),
    spvStatus: getString(getField(record, "spv_status", "spvStatus")),
    realEstateDeveloperLawyerCosts: stringifyJson(
      toJsonValue(
        getField(
          record,
          "real_estate_developer_lawyer_costs",
          "realEstateDeveloperLawyerCosts",
        ),
      ),
    ),
    spvLawyerCosts: stringifyJson(
      toJsonValue(getField(record, "spv_lawyer_costs", "spvLawyerCosts")),
    ),
    legalProcessExpiry: getNumber(
      getField(record, "legal_process_expiry", "legalProcessExpiry"),
    ),
    secondAttempt: getBoolean(getField(record, "second_attempt", "secondAttempt")),
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertListingSpvProposal(
  listingId: number,
  proposalId: string | undefined,
  blockNumber: number,
): Promise<void> {
  const id = listingId.toString();
  if (!proposalId) {
    const existing = await MarketplaceListingSpvProposals.get(id);
    if (existing) await MarketplaceListingSpvProposals.remove(id);
    return;
  }
  const row = MarketplaceListingSpvProposals.create({
    id,
    listingId,
    ongoingObjectListingId: id,
    proposalId,
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertOngoingLawyerVoting(
  proposalId: string,
  listingId: number | undefined,
  opt: ReturnType<typeof asOption> | undefined,
  blockNumber: number,
): Promise<void> {
  const id = proposalId;
  if (!opt?.isSome) {
    const existing = await MarketplaceOngoingLawyerVotings.get(id);
    if (existing) await MarketplaceOngoingLawyerVotings.remove(id);
    return;
  }

  const record = asRecord(toJsonValue(opt.unwrap()));
  if (!record) return;

  const row = MarketplaceOngoingLawyerVotings.create({
    id,
    proposalId,
    listingId,
    ongoingObjectListingId: listingId != null ? listingId.toString() : undefined,
    yesVotingPower: getNumber(
      getField(record, "yes_voting_power", "yesVotingPower"),
    ),
    noVotingPower: getNumber(
      getField(record, "no_voting_power", "noVotingPower"),
    ),
    abstainVotingPower: getNumber(
      getField(record, "abstain_voting_power", "abstainVotingPower"),
    ),
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertUserLawyerVote(
  proposalId: string,
  voter: string,
  listingId: number | undefined,
  opt: ReturnType<typeof asOption> | undefined,
  blockNumber: number,
): Promise<void> {
  const id = `${proposalId}-${voter}`;
  if (!opt?.isSome) {
    const existing = await MarketplaceUserLawyerVotes.get(id);
    if (existing) await MarketplaceUserLawyerVotes.remove(id);
    return;
  }

  const record = asRecord(toJsonValue(opt.unwrap()));
  if (!record) return;

  const voteRecord = asRecord(record.vote);
  const assetId = getNumber(getField(record, "asset_id", "assetId"));
  const realWorldAssetId = await resolveRealWorldAssetId(assetId);

  const row = MarketplaceUserLawyerVotes.create({
    id,
    proposalId,
    listingId,
    ongoingObjectListingId: listingId != null ? listingId.toString() : undefined,
    voter,
    vote: voteRecord ? Object.keys(voteRecord)[0] : getString(record.vote),
    assetId,
    realWorldAssetId,
    power: getNumber(record.power),
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertTokenOwner(
  listingId: number,
  account: string,
  opt: ReturnType<typeof asOption> | undefined,
  blockNumber: number,
): Promise<void> {
  const id = `${listingId}-${account}`;
  if (!opt?.isSome) {
    const existing = await MarketplaceShareOwners.get(id);
    if (existing) await MarketplaceShareOwners.remove(id);
    return;
  }

  const record = asRecord(toJsonValue(opt.unwrap()));
  if (!record) return;

  const row = MarketplaceShareOwners.create({
    id,
    listingId,
    ongoingObjectListingId: listingId.toString(),
    account,
    tokenAmount: getNumber(getField(record, "token_amount", "tokenAmount")),
    paidFunds: stringifyJson(
      toJsonValue(getField(record, "paid_funds", "paidFunds")),
    ),
    paidTax: stringifyJson(toJsonValue(getField(record, "paid_tax", "paidTax"))),
    relistCount: getNumber(getField(record, "relist_count", "relistCount")),
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function upsertOngoingOffer(
  listingId: number,
  offeror: string,
  opt: ReturnType<typeof asOption> | undefined,
  blockNumber: number,
): Promise<void> {
  const id = `${listingId}-${offeror}`;
  if (!opt?.isSome) {
    const existing = await MarketplaceOngoingOffers.get(id);
    if (existing) await MarketplaceOngoingOffers.remove(id);
    return;
  }

  const record = asRecord(toJsonValue(opt.unwrap()));
  if (!record) return;

  const paymentAssets = getNumber(
    getField(record, "payment_assets", "paymentAssets"),
  );
  const paymentAssetId = await resolveRealWorldAssetId(paymentAssets);

  const row = MarketplaceOngoingOffers.create({
    id,
    listingId,
    ongoingObjectListingId: listingId.toString(),
    offeror,
    tokenPrice: getField(record, "token_price", "tokenPrice") != null
      ? String(getField(record, "token_price", "tokenPrice"))
      : undefined,
    amount: getNumber(record.amount),
    paymentAssets,
    paymentAssetId,
    nonce: toStringValue(record.nonce),
    updatedBlock: blockNumber,
  });

  await row.save();
}

async function syncOngoingObjectListing(
  listingId: number,
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

async function syncTokenListing(
  listingId: number,
  blockNumber: number,
): Promise<void> {
  try {
    const opt = asOption(await api.query.marketplace.shareListings(listingId));
    await upsertTokenListing(listingId, opt, blockNumber);
  } catch (e) {
    logger.warn(
      `Block ${blockNumber}: shareListings(${listingId}) failed: ${formatError(e)}`,
    );
  }
}

async function syncPropertyLawyer(
  listingId: number,
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
  listingId: number,
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
  listingId: number,
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

async function syncTokenOwner(
  listingId: number,
  account: string,
  blockNumber: number,
): Promise<void> {
  try {
    const opt = asOption(
      await api.query.marketplace.shareOwner(account, listingId),
    );
    await upsertTokenOwner(listingId, account, opt, blockNumber);
  } catch (e) {
    logger.warn(
      `Block ${blockNumber}: shareOwner(${account}, ${listingId}) failed: ${formatError(e)}`,
    );
  }
}

async function syncOngoingLawyerVoting(
  proposalId: string,
  blockNumber: number,
  listingId?: number,
): Promise<void> {
  try {
    const opt = asOption(
      await api.query.marketplace.ongoingLawyerVoting(proposalId),
    );
    await upsertOngoingLawyerVoting(proposalId, listingId, opt, blockNumber);
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
  listingId?: number,
): Promise<void> {
  try {
    const opt = asOption(
      await api.query.marketplace.userLawyerVote(proposalId, voter),
    );
    await upsertUserLawyerVote(proposalId, voter, listingId, opt, blockNumber);
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
