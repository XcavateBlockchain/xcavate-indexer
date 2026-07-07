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
// PropertyManagement: 12 events
// LettingAgentAdded, LettingAgentRemoved, LettingAgentSet
// IncomeDistributed, WithdrawFunds, LettingAgentProposed
// VotedOnLettingAgent, LettingAgentRejected
// LettingAgentResignationInitiated, LettingAgentResignationFinalized
// ResignationProcessingFailed, SharesUnfrozen
// ---------------------------------------------------------------------------

export async function handlePropertyManagementEvent(
  event: SubstrateEvent,
): Promise<void> {
  const blockNumber = event.block.block.header.number.toNumber();
  const method = event.event.method;

  logger.info(`Block ${blockNumber}: propertyManagement.${method}`);

  const args = event.event.data as unknown[];

  switch (method) {
    case "LettingAgentAdded":
      return handleLettingAgentAdded(args, blockNumber);
    case "LettingAgentRemoved":
      return handleLettingAgentRemoved(args, blockNumber);
    case "LettingAgentSet":
      return handleLettingAgentSet(args, blockNumber);
    case "IncomeDistributed":
      return handleIncomeDistributed(args, blockNumber);
    case "WithdrawFunds":
      return handleWithdrawFunds(args, blockNumber);
    case "LettingAgentProposed":
      return handleLettingAgentProposed(args, blockNumber);
    case "VotedOnLettingAgent":
      return handleVotedOnLettingAgent(args, blockNumber);
    case "LettingAgentRejected":
      return handleLettingAgentRejected(args, blockNumber);
    case "LettingAgentResignationInitiated":
      return handleLettingAgentResignationInitiated(args, blockNumber);
    case "LettingAgentResignationFinalized":
      return handleLettingAgentResignationFinalized(args, blockNumber);
    case "ResignationProcessingFailed":
      return handleResignationProcessingFailed(args, blockNumber);
    case "SharesUnfrozen":
      return handleSharesUnfrozen(args, blockNumber);
    default:
      return;
  }
}

export async function handlePropertyManagementSyncBlock(
  block: SubstrateBlock,
): Promise<void> {
  const blockNumber = block.block.header.number.toNumber();
  // Storage sync can be added later if needed
}

async function handleLettingAgentAdded(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: asset_id, agent_id
  const assetId = getBigInt(args[0]);
  const agentId = toStringValue(args[1]);
  if (assetId == null || !agentId) return;

  logger.info(
    `Block ${blockNumber}: LettingAgentAdded asset=${assetId} agent=${agentId}`,
  );
}

async function handleLettingAgentRemoved(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: asset_id, agent_id
  const assetId = getBigInt(args[0]);
  const agentId = toStringValue(args[1]);
  if (assetId == null || !agentId) return;

  logger.info(
    `Block ${blockNumber}: LettingAgentRemoved asset=${assetId} agent=${agentId}`,
  );
}

async function handleLettingAgentSet(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: asset_id, agent_id
  const assetId = getBigInt(args[0]);
  const agentId = toStringValue(args[1]);
  if (assetId == null || !agentId) return;

  logger.info(
    `Block ${blockNumber}: LettingAgentSet asset=${assetId} agent=${agentId}`,
  );
}

async function handleIncomeDistributed(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: asset_id, amount, recipient
  const assetId = getBigInt(args[0]);
  const amount = getBigInt(args[1]);
  const recipient = toStringValue(args[2]);
  if (assetId == null || recipient) {
    logger.info(
      `Block ${blockNumber}: IncomeDistributed asset=${assetId} amount=${amount} recipient=${recipient}`,
    );
  }
}

async function handleWithdrawFunds(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: asset_id, amount, recipient
  const assetId = getBigInt(args[0]);
  const amount = getBigInt(args[1]);
  const recipient = toStringValue(args[2]);
  if (assetId != null) {
    logger.info(
      `Block ${blockNumber}: WithdrawFunds asset=${assetId} amount=${amount} recipient=${recipient}`,
    );
  }
}

async function handleLettingAgentProposed(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: asset_id, agent_id, proposal_id
  const assetId = getBigInt(args[0]);
  const agentId = toStringValue(args[1]);
  const proposalId = getBigInt(args[2]);
  if (assetId != null) {
    logger.info(
      `Block ${blockNumber}: LettingAgentProposed asset=${assetId} agent=${agentId} proposal=${proposalId}`,
    );
  }
}

async function handleVotedOnLettingAgent(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: proposal_id, voter, vote
  const proposalId = getBigInt(args[0]);
  const voter = toStringValue(args[1]);
  const vote = toStringValue(args[2]);
  if (proposalId != null) {
    logger.info(
      `Block ${blockNumber}: VotedOnLettingAgent proposal=${proposalId} voter=${voter} vote=${vote}`,
    );
  }
}

async function handleLettingAgentRejected(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: asset_id, agent_id
  const assetId = getBigInt(args[0]);
  const agentId = toStringValue(args[1]);
  if (assetId != null) {
    logger.info(
      `Block ${blockNumber}: LettingAgentRejected asset=${assetId} agent=${agentId}`,
    );
  }
}

async function handleLettingAgentResignationInitiated(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: asset_id, agent_id, timestamp
  const assetId = getBigInt(args[0]);
  const agentId = toStringValue(args[1]);
  const timestamp = getBigInt(args[2]);
  if (assetId != null) {
    logger.info(
      `Block ${blockNumber}: LettingAgentResignationInitiated asset=${assetId} agent=${agentId} ts=${timestamp}`,
    );
  }
}

async function handleLettingAgentResignationFinalized(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: asset_id, agent_id
  const assetId = getBigInt(args[0]);
  const agentId = toStringValue(args[1]);
  if (assetId != null) {
    logger.info(
      `Block ${blockNumber}: LettingAgentResignationFinalized asset=${assetId} agent=${agentId}`,
    );
  }
}

async function handleResignationProcessingFailed(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: asset_id, agent_id
  const assetId = getBigInt(args[0]);
  const agentId = toStringValue(args[1]);
  if (assetId != null) {
    logger.info(
      `Block ${blockNumber}: ResignationProcessingFailed asset=${assetId} agent=${agentId}`,
    );
  }
}

async function handleSharesUnfrozen(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: proposal_id, asset_id, voter, amount
  const proposalId = getBigInt(args[0]);
  const assetId = getBigInt(args[1]);
  const voter = toStringValue(args[2]);
  const amount = getBigInt(args[3]);
  if (proposalId != null) {
    logger.info(
      `Block ${blockNumber}: SharesUnfrozen proposal=${proposalId} asset=${assetId} voter=${voter} amount=${amount}`,
    );
  }
}