import type { SubstrateBlock, SubstrateEvent } from "@subql/types";

import {
  asOption,
  asStorageValue,
  formatError,
  getBigInt,
  getNumber,
  toUtf8String,
  toStringValue,
} from "./common";

// ---------------------------------------------------------------------------
// RealEstateAssets: 26 events (NFT-style pallet events)
// Created, Issued, Transferred, Burned, TeamChanged, OwnerChanged
// Frozen, Thawed, AssetFrozen, AssetThawed, AccountsDestroyed
// ApprovalsDestroyed, DestructionStarted, Destroyed, ForceCreated
// MetadataSet, MetadataCleared, ApprovedTransfer, ApprovalCancelled
// TransferredApproved, AssetStatusChanged, AssetMinBalanceChanged
// Touched, Blocked, Deposited, Withdrawn
// ---------------------------------------------------------------------------

let realEstateAssetsSyncInFlight: Promise<void> | null = null;
let realEstateAssetsSynced = false;

export async function handleRealEstateAssetsEvent(
  event: SubstrateEvent,
): Promise<void> {
  const blockNumber = event.block.block.header.number.toNumber();
  const method = event.event.method;

  logger.info(`Block ${blockNumber}: realEstateAssets.${method}`);

  const args = event.event.data as unknown[];

  switch (method) {
    case "Created":
      return handleCreated(args, blockNumber);
    case "Issued":
      return handleIssued(args, blockNumber);
    case "Transferred":
      return handleTransferred(args, blockNumber);
    case "Burned":
      return handleBurned(args, blockNumber);
    case "TeamChanged":
      return handleTeamChanged(args, blockNumber);
    case "OwnerChanged":
      return handleOwnerChanged(args, blockNumber);
    case "Frozen":
      return handleFrozen(args, blockNumber);
    case "Thawed":
      return handleThawed(args, blockNumber);
    case "AssetFrozen":
      return handleAssetFrozen(args, blockNumber);
    case "AssetThawed":
      return handleAssetThawed(args, blockNumber);
    case "AccountsDestroyed":
      return handleAccountsDestroyed(args, blockNumber);
    case "ApprovalsDestroyed":
      return handleApprovalsDestroyed(args, blockNumber);
    case "DestructionStarted":
      return handleDestructionStarted(args, blockNumber);
    case "Destroyed":
      return handleDestroyed(args, blockNumber);
    case "ForceCreated":
      return handleForceCreated(args, blockNumber);
    case "MetadataSet":
      return handleMetadataSet(args, blockNumber);
    case "MetadataCleared":
      return handleMetadataCleared(args, blockNumber);
    case "ApprovedTransfer":
      return handleApprovedTransfer(args, blockNumber);
    case "ApprovalCancelled":
      return handleApprovalCancelled(args, blockNumber);
    case "TransferredApproved":
      return handleTransferredApproved(args, blockNumber);
    case "AssetStatusChanged":
      return handleAssetStatusChanged(args, blockNumber);
    case "AssetMinBalanceChanged":
      return handleAssetMinBalanceChanged(args, blockNumber);
    case "Touched":
      return handleTouched(args, blockNumber);
    case "Blocked":
      return handleBlocked(args, blockNumber);
    case "Deposited":
      return handleDeposited(args, blockNumber);
    case "Withdrawn":
      return handleWithdrawn(args, blockNumber);
    default:
      return;
  }
}

export async function handleRealEstateAssetsSyncBlock(
  block: SubstrateBlock,
): Promise<void> {
  const blockNumber = block.block.header.number.toNumber();
  await ensureRealEstateAssetsSynced(blockNumber);
}

export async function ensureRealEstateAssetsSynced(
  blockNumber: number,
): Promise<void> {
  if (realEstateAssetsSynced) return;
  if (blockNumber == 0) return;
  realEstateAssetsSyncInFlight ??= syncRealEstateAssetsFromStorage(
    blockNumber,
  )
    .then(() => {
      realEstateAssetsSynced = true;
    })
    .catch((e) => {
      logger.error(
        `Block ${blockNumber}: realEstateAssets storage sync failed — ${formatError(e)}`,
      );
    })
    .finally(() => {
      realEstateAssetsSyncInFlight = null;
    });
  await realEstateAssetsSyncInFlight;
}

// Generic NFT event handlers - log for now, add entity logic later
async function handleCreated(args: unknown[], blockNumber: number): Promise<void> {
  // Event: owner, total_supply
  const owner = toStringValue(args[0]);
  const totalSupply = getBigInt(args[1]);
  logger.info(
    `Block ${blockNumber}: Created owner=${owner} totalSupply=${totalSupply}`,
  );
}

async function handleIssued(args: unknown[], blockNumber: number): Promise<void> {
  // Event: owner, to, amount
  const owner = toStringValue(args[0]);
  const to = toStringValue(args[1]);
  const amount = getBigInt(args[2]);
  logger.info(
    `Block ${blockNumber}: Issued owner=${owner} to=${to} amount=${amount}`,
  );
}

async function handleTransferred(args: unknown[], blockNumber: number): Promise<void> {
  // Event: from, to, value
  const from = toStringValue(args[0]);
  const to = toStringValue(args[1]);
  const value = getBigInt(args[2]);
  logger.info(
    `Block ${blockNumber}: Transferred from=${from} to=${to} value=${value}`,
  );
}

async function handleBurned(args: unknown[], blockNumber: number): Promise<void> {
  // Event: owner, value
  const owner = toStringValue(args[0]);
  const value = getBigInt(args[1]);
  logger.info(
    `Block ${blockNumber}: Burned owner=${owner} value=${value}`,
  );
}

async function handleTeamChanged(args: unknown[], blockNumber: number): Promise<void> {
  // Event: team
  const team = toStringValue(args[0]);
  logger.info(`Block ${blockNumber}: TeamChanged team=${team}`);
}

async function handleOwnerChanged(args: unknown[], blockNumber: number): Promise<void> {
  // Event: from, to
  const from = toStringValue(args[0]);
  const to = toStringValue(args[1]);
  logger.info(
    `Block ${blockNumber}: OwnerChanged from=${from} to=${to}`,
  );
}

async function handleFrozen(args: unknown[], blockNumber: number): Promise<void> {
  // Event: id
  const id = getBigInt(args[0]);
  logger.info(`Block ${blockNumber}: Frozen id=${id}`);
}

async function handleThawed(args: unknown[], blockNumber: number): Promise<void> {
  // Event: id
  const id = getBigInt(args[0]);
  logger.info(`Block ${blockNumber}: Thawed id=${id}`);
}

async function handleAssetFrozen(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id
  const assetId = getBigInt(args[0]);
  logger.info(`Block ${blockNumber}: AssetFrozen assetId=${assetId}`);
}

async function handleAssetThawed(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id
  const assetId = getBigInt(args[0]);
  logger.info(`Block ${blockNumber}: AssetThawed assetId=${assetId}`);
}

async function handleAccountsDestroyed(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id, nonces
  const assetId = getBigInt(args[0]);
  const nonces = getNumber(args[1]);
  logger.info(
    `Block ${blockNumber}: AccountsDestroyed assetId=${assetId} nonces=${nonces}`,
  );
}

async function handleApprovalsDestroyed(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id, approvals
  const assetId = getBigInt(args[0]);
  const approvals = getNumber(args[1]);
  logger.info(
    `Block ${blockNumber}: ApprovalsDestroyed assetId=${assetId} approvals=${approvals}`,
  );
}

async function handleDestructionStarted(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id
  const assetId = getBigInt(args[0]);
  logger.info(`Block ${blockNumber}: DestructionStarted assetId=${assetId}`);
}

async function handleDestroyed(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id
  const assetId = getBigInt(args[0]);
  logger.info(`Block ${blockNumber}: Destroyed assetId=${assetId}`);
}

async function handleForceCreated(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id, owner
  const assetId = getBigInt(args[0]);
  const owner = toStringValue(args[1]);
  logger.info(
    `Block ${blockNumber}: ForceCreated assetId=${assetId} owner=${owner}`,
  );
}

async function handleMetadataSet(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id, data
  const assetId = getBigInt(args[0]);
  const data = toStringValue(args[1]);
  logger.info(
    `Block ${blockNumber}: MetadataSet assetId=${assetId} data=${data}`,
  );
}

async function handleMetadataCleared(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id
  const assetId = getBigInt(args[0]);
  logger.info(`Block ${blockNumber}: MetadataCleared assetId=${assetId}`);
}

async function handleApprovedTransfer(args: unknown[], blockNumber: number): Promise<void> {
  // Event: from, to, value
  const from = toStringValue(args[0]);
  const to = toStringValue(args[1]);
  const value = getBigInt(args[2]);
  logger.info(
    `Block ${blockNumber}: ApprovedTransfer from=${from} to=${to} value=${value}`,
  );
}

async function handleApprovalCancelled(args: unknown[], blockNumber: number): Promise<void> {
  // Event: from, to
  const from = toStringValue(args[0]);
  const to = toStringValue(args[1]);
  logger.info(
    `Block ${blockNumber}: ApprovalCancelled from=${from} to=${to}`,
  );
}

async function handleTransferredApproved(args: unknown[], blockNumber: number): Promise<void> {
  // Event: from, to, value
  const from = toStringValue(args[0]);
  const to = toStringValue(args[1]);
  const value = getBigInt(args[2]);
  logger.info(
    `Block ${blockNumber}: TransferredApproved from=${from} to=${to} value=${value}`,
  );
}

async function handleAssetStatusChanged(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id, status
  const assetId = getBigInt(args[0]);
  const status = toStringValue(args[1]);
  logger.info(
    `Block ${blockNumber}: AssetStatusChanged assetId=${assetId} status=${status}`,
  );
}

async function handleAssetMinBalanceChanged(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id, new_min_balance
  const assetId = getBigInt(args[0]);
  const newMinBalance = getBigInt(args[1]);
  logger.info(
    `Block ${blockNumber}: AssetMinBalanceChanged assetId=${assetId} new=${newMinBalance}`,
  );
}

async function handleTouched(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id
  const assetId = getBigInt(args[0]);
  logger.info(`Block ${blockNumber}: Touched assetId=${assetId}`);
}

async function handleBlocked(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id, to
  const assetId = getBigInt(args[0]);
  const to = toStringValue(args[1]);
  logger.info(
    `Block ${blockNumber}: Blocked assetId=${assetId} to=${to}`,
  );
}

async function handleDeposited(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id, who, value
  const assetId = getBigInt(args[0]);
  const who = toStringValue(args[1]);
  const value = getBigInt(args[2]);
  logger.info(
    `Block ${blockNumber}: Deposited assetId=${assetId} who=${who} value=${value}`,
  );
}

async function handleWithdrawn(args: unknown[], blockNumber: number): Promise<void> {
  // Event: asset_id, who, value
  const assetId = getBigInt(args[0]);
  const who = toStringValue(args[1]);
  const value = getBigInt(args[2]);
  logger.info(
    `Block ${blockNumber}: Withdrawn assetId=${assetId} who=${who} value=${value}`,
  );
}

// Storage sync
async function syncRealEstateAssetsFromStorage(
  blockNumber: number,
): Promise<void> {
  logger.info(`Block ${blockNumber}: syncing realEstateAssets storage`);
  // Storage sync can be added later if there are relevant storage maps
  logger.info(`Block ${blockNumber}: realEstateAssets storage sync complete`);
}