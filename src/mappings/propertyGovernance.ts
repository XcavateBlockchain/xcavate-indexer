import type { SubstrateBlock, SubstrateEvent } from "@subql/types";

import {
  formatError,
  getBigInt,
  toStringValue,
} from "./common";

// ---------------------------------------------------------------------------
// PropertyGovernance: 13 events
// Proposed, Challenge, VotedOnProposal, VotedOnChallenge
// ProposalExecuted, AgentSlashed, AgentChanged
// ProposalRejected, ChallengeRejected, ProposalThresHoldNotReached
// ProposalProcessingFailed, ChallengeProcessingFailed, SharesUnfrozen
// ---------------------------------------------------------------------------

export async function handlePropertyGovernanceEvent(
  event: SubstrateEvent,
): Promise<void> {
  const blockNumber = event.block.block.header.number.toNumber();
  const method = event.event.method;

  logger.info(`Block ${blockNumber}: propertyGovernance.${method}`);

  const args = event.event.data as unknown[];

  switch (method) {
    case "Proposed":
      return handleProposed(args, blockNumber);
    case "Challenge":
      return handleChallenge(args, blockNumber);
    case "VotedOnProposal":
      return handleVotedOnProposal(args, blockNumber);
    case "VotedOnChallenge":
      return handleVotedOnChallenge(args, blockNumber);
    case "ProposalExecuted":
      return handleProposalExecuted(args, blockNumber);
    case "AgentSlashed":
      return handleAgentSlashed(args, blockNumber);
    case "AgentChanged":
      return handleAgentChanged(args, blockNumber);
    case "ProposalRejected":
      return handleProposalRejected(args, blockNumber);
    case "ChallengeRejected":
      return handleChallengeRejected(args, blockNumber);
    case "ProposalThresHoldNotReached":
      return handleProposalThresHoldNotReached(args, blockNumber);
    case "ProposalProcessingFailed":
      return handleProposalProcessingFailed(args, blockNumber);
    case "ChallengeProcessingFailed":
      return handleChallengeProcessingFailed(args, blockNumber);
    case "SharesUnfrozen":
      return handleSharesUnfrozen(args, blockNumber);
    default:
      return;
  }
}

export async function handlePropertyGovernanceSyncBlock(
  block: SubstrateBlock,
): Promise<void> {
  const blockNumber = block.block.header.number.toNumber();
  // Storage sync can be added later if needed
}

async function handleProposed(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: proposal_id, proposer, description, threshold
  const proposalId = getBigInt(args[0]);
  const proposer = toStringValue(args[1]);
  const description = toStringValue(args[2]);
  const threshold = getBigInt(args[3]);
  if (proposalId != null) {
    logger.info(
      `Block ${blockNumber}: Proposed id=${proposalId} proposer=${proposer} threshold=${threshold}`,
    );
  }
}

async function handleChallenge(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: proposal_id, challenger, reason
  const proposalId = getBigInt(args[0]);
  const challenger = toStringValue(args[1]);
  const reason = toStringValue(args[2]);
  if (proposalId != null) {
    logger.info(
      `Block ${blockNumber}: Challenge id=${proposalId} challenger=${challenger}`,
    );
  }
}

async function handleVotedOnProposal(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: proposal_id, voter, vote
  const proposalId = getBigInt(args[0]);
  const voter = toStringValue(args[1]);
  const vote = toStringValue(args[2]);
  if (proposalId != null) {
    logger.info(
      `Block ${blockNumber}: VotedOnProposal id=${proposalId} voter=${voter}`,
    );
  }
}

async function handleVotedOnChallenge(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: challenge_id, voter, vote
  const challengeId = getBigInt(args[0]);
  const voter = toStringValue(args[1]);
  const vote = toStringValue(args[2]);
  if (challengeId != null) {
    logger.info(
      `Block ${blockNumber}: VotedOnChallenge id=${challengeId} voter=${voter}`,
    );
  }
}

async function handleProposalExecuted(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: proposal_id, success
  const proposalId = getBigInt(args[0]);
  const success = args[1] as boolean | undefined;
  if (proposalId != null) {
    logger.info(
      `Block ${blockNumber}: ProposalExecuted id=${proposalId} success=${success}`,
    );
  }
}

async function handleAgentSlashed(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: agent_id, amount
  const agentId = toStringValue(args[0]);
  const amount = getBigInt(args[1]);
  if (agentId) {
    logger.info(
      `Block ${blockNumber}: AgentSlashed agent=${agentId} amount=${amount}`,
    );
  }
}

async function handleAgentChanged(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: old_agent, new_agent
  const oldAgent = toStringValue(args[0]);
  const newAgent = toStringValue(args[1]);
  if (oldAgent) {
    logger.info(
      `Block ${blockNumber}: AgentChanged old=${oldAgent} new=${newAgent}`,
    );
  }
}

async function handleProposalRejected(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: proposal_id, reason
  const proposalId = getBigInt(args[0]);
  const reason = toStringValue(args[1]);
  if (proposalId != null) {
    logger.info(
      `Block ${blockNumber}: ProposalRejected id=${proposalId}`,
    );
  }
}

async function handleChallengeRejected(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: challenge_id, reason
  const challengeId = getBigInt(args[0]);
  const reason = toStringValue(args[1]);
  if (challengeId != null) {
    logger.info(
      `Block ${blockNumber}: ChallengeRejected id=${challengeId}`,
    );
  }
}

async function handleProposalThresHoldNotReached(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: proposal_id
  const proposalId = getBigInt(args[0]);
  if (proposalId != null) {
    logger.info(
      `Block ${blockNumber}: ProposalThresHoldNotReached id=${proposalId}`,
    );
  }
}

async function handleProposalProcessingFailed(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: proposal_id
  const proposalId = getBigInt(args[0]);
  if (proposalId != null) {
    logger.info(
      `Block ${blockNumber}: ProposalProcessingFailed id=${proposalId}`,
    );
  }
}

async function handleChallengeProcessingFailed(
  args: unknown[],
  blockNumber: number,
): Promise<void> {
  // Event: challenge_id
  const challengeId = getBigInt(args[0]);
  if (challengeId != null) {
    logger.info(
      `Block ${blockNumber}: ChallengeProcessingFailed id=${challengeId}`,
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