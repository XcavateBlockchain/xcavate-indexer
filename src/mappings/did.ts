import type { SubstrateBlock, SubstrateEvent } from "@subql/types";

import { Did } from "../types";

import {
  asOption,
  asRecord,
  asStorageValue,
  formatError,
  getStorageKeyArgs,
  getString,
  getStringArray,
  toNumber,
  toJsonValue,
  toUtf8String,
} from "./common";

let didSyncInFlight: Promise<void> | null = null;
let didSynced = false;

export async function handleDidSyncBlock(
  block: SubstrateBlock,
): Promise<void> {
  const blockNumber = block.block.header.number.toNumber();
  await ensureDidsSynced(blockNumber);
}

export async function handleDidEvent(event: SubstrateEvent): Promise<void> {
  const blockNumber = event.block.block.header.number.toNumber();
  const method = event.event.method;

  await ensureDidsSynced(blockNumber);

  logger.info(`Block ${blockNumber}: did.${method}`);

  switch (method) {
    case "DidCreated":
      return handleDidCreated(event, blockNumber);
    case "DidUpdated":
      return handleDidUpdated(event, blockNumber);
    case "DidDeleted":
      return handleDidDeleted(event, blockNumber);
    case "DepositOwnerChanged":
      return handleDepositOwnerChanged(event, blockNumber);
  }
}

export async function ensureDidsSynced(blockNumber: number): Promise<void> {
  if (didSynced) return;
  didSyncInFlight ??= syncDidsFromStorage(blockNumber)
    .then(() => {
      didSynced = true;
    })
    .catch((e) => {
      logger.error(
        `Block ${blockNumber}: DID storage sync failed — ${formatError(e)}`,
      );
    })
    .finally(() => {
      didSyncInFlight = null;
    });
  await didSyncInFlight;
}

async function syncDidsFromStorage(blockNumber: number): Promise<void> {
  logger.info(`Block ${blockNumber}: syncing DID storage`);

  const didStorage = api.query?.did?.did as
    | { entries?: () => Promise<unknown> }
    | undefined;
  const entriesFn = didStorage?.entries;
  if (typeof entriesFn !== "function") {
    logger.warn(`Block ${blockNumber}: did.did.entries unavailable`);
    return;
  }

  const entries = await entriesFn.call(didStorage);
  if (!Array.isArray(entries) || entries.length === 0) {
    logger.info(`Block ${blockNumber}: did.did storage entries=0`);
    return;
  }

  let synced = 0;
  for (const [storageKey, value] of entries) {
    const args = getStorageKeyArgs(storageKey);
    const didId = args?.[0] != null ? toUtf8String(args[0]) : undefined;
    if (!didId) continue;

    const storedOpt = asStorageValue(value);
    if (!storedOpt.isSome) continue;

    await upsertDidFromStorageValue(
      didId,
      storedOpt.unwrap(),
      blockNumber,
      false,
    );
    synced += 1;
  }

  logger.info(
    `Block ${blockNumber}: did.did storage entries=${entries.length}, handled=${synced}`,
  );
}

// New DID — read full state from storage and save.
async function handleDidCreated(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const args = event.event.data as unknown[];
  const didId = String(args[1]);
  await syncDidFromStorage(didId, blockNumber, true);
}

// DID keys/endpoints changed — re-read full state from storage.
async function handleDidUpdated(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const args = event.event.data as unknown[];
  const didId = String(args[0]);
  await syncDidFromStorage(didId, blockNumber, false);
}

// DID was deleted on-chain — remove the row.
async function handleDidDeleted(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const args = event.event.data as unknown[];
  const didId = String(args[0]);

  const existing = await Did.get(didId);
  if (!existing) {
    logger.warn(
      `Block ${blockNumber}: DidDeleted for ${didId} but no row found`,
    );
    return;
  }

  await Did.remove(didId);
  logger.info(`Block ${blockNumber}: removed DID ${didId}`);
}

// Updates the depositOwner field directly from the event; both old and new
// owners are in the args, so no storage read is needed.
async function handleDepositOwnerChanged(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const args = event.event.data as unknown[];
  const didId = String(args[0]);
  const fromOwner = String(args[1]);
  const toOwner = String(args[2]);

  const existing = await Did.get(didId);
  if (!existing) {
    logger.warn(
      `Block ${blockNumber}: DepositOwnerChanged for ${didId} but no row — backfilling`,
    );
    await syncDidFromStorage(didId, blockNumber, false);
    return;
  }

  existing.depositOwner = toOwner;
  existing.updatedBlock = blockNumber;
  await existing.save();

  logger.info(
    `Block ${blockNumber}: DID ${didId} deposit owner ${fromOwner} → ${toOwner}`,
  );
}

// Reads did.did(id) from storage, resolves key hashes via publicKeys map,
// and upserts a Did row.
async function syncDidFromStorage(
  didId: string,
  blockNumber: number,
  isCreation: boolean,
): Promise<void> {
  try {
    const stored = await api.query.did.did(didId);
    const storedOpt = asOption(stored);
    if (!storedOpt?.isSome) {
      logger.warn(
        `Block ${blockNumber}: DID ${didId} not in storage — skipping`,
      );
      return;
    }

    await upsertDidFromStorageValue(
      didId,
      storedOpt.unwrap(),
      blockNumber,
      isCreation,
    );
  } catch (e) {
    logger.error(
      `Block ${blockNumber}: failed to sync DID ${didId} — ${formatError(e)}`,
    );
  }
}

async function upsertDidFromStorageValue(
  didId: string,
  storedValue: unknown,
  blockNumber: number,
  isCreation: boolean,
): Promise<void> {
  const jsonValue = toJsonValue(storedValue);
  const d = asRecord(jsonValue);
  if (!d) {
    logger.warn(
      `Block ${blockNumber}: DID ${didId} storage JSON invalid — skipping`,
    );
    return;
  }

  const publicKeys = d.publicKeys;
  const authKey = resolvePublicKey(publicKeys, getString(d.authenticationKey));
  const kaHashes = getStringArray(d.keyAgreementKeys) ?? [];
  const kaKeys = kaHashes
    .map((h) => resolvePublicKey(publicKeys, h))
    .filter((k): k is string => !!k);
  const delegationKey = resolvePublicKey(publicKeys, getString(d.delegationKey));
  const attestationKey = resolvePublicKey(publicKeys, getString(d.attestationKey));
  const lastTxCounter = toNumber(d.lastTxCounter);

  const deposit = asRecord(d.deposit);
  const depositOwner = getString(deposit?.owner);
  const depositAmount = toNumber(deposit?.amount);

  const existing = await Did.get(didId);

  const did = Did.create({
    id: didId,
    authenticationKey: authKey ?? undefined,
    keyAgreementKeys: kaKeys.length > 0 ? kaKeys : undefined,
    delegationKey: delegationKey ?? undefined,
    attestationKey: attestationKey ?? undefined,
    lastTxCounter,
    depositOwner,
    depositAmount,
    createdBlock:
      existing?.createdBlock ?? (isCreation ? blockNumber : undefined),
    updatedBlock: blockNumber,
  });

  await did.save();
  logger.info(
    `Block ${blockNumber}: ${isCreation ? "created" : "updated"} DID ${didId} (auth=${authKey?.slice(0, 16) ?? "?"}…, ${kaKeys.length} ka keys, nonce=${lastTxCounter ?? "?"})`,
  );
}

// Looks up a key hash in the BTreeMap and returns the raw hex bytes of the
// underlying Sr25519 / Ed25519 / Ecdsa / X25519 / Account variant.
function resolvePublicKey(
  publicKeys: unknown,
  hash: string | undefined,
): string | undefined {
  if (!publicKeys || !hash) return undefined;

  let entryValue: unknown;
  if (Array.isArray(publicKeys)) {
    for (const entry of publicKeys) {
      if (Array.isArray(entry) && entry[0] === hash) {
        entryValue = entry[1];
        break;
      }
    }
  } else {
    const record = asRecord(publicKeys);
    entryValue = record?.[hash];
  }
  if (!entryValue) return undefined;

  const entry = asRecord(entryValue);
  const key = asRecord(entry?.key);
  if (!key) return undefined;

  const ver = asRecord(key.publicVerificationKey) ??
    asRecord(key.PublicVerificationKey);
  if (ver) {
    return (
      getString(ver.sr25519) ??
      getString(ver.Sr25519) ??
      getString(ver.ed25519) ??
      getString(ver.Ed25519) ??
      getString(ver.ecdsa) ??
      getString(ver.Ecdsa) ??
      getString(ver.account) ??
      getString(ver.Account)
    );
  }
  const enc = asRecord(key.publicEncryptionKey) ??
    asRecord(key.PublicEncryptionKey);
  if (enc) {
    return getString(enc.x25519) ?? getString(enc.X25519);
  }
  return undefined;
}
