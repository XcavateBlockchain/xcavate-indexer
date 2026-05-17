import { SubstrateEvent } from "@subql/types";
import { Did } from "../types";

export async function handleDidEvent(event: SubstrateEvent): Promise<void> {
  const blockNumber = event.block.block.header.number.toNumber();
  const method = event.event.method;

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

// New DID — read full state from storage and save.
async function handleDidCreated(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const didId = event.event.data[1].toString();
  await syncDidFromStorage(didId, blockNumber, true);
}

// DID keys/endpoints changed — re-read full state from storage.
async function handleDidUpdated(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const didId = event.event.data[0].toString();
  await syncDidFromStorage(didId, blockNumber, false);
}

// DID was deleted on-chain — remove the row.
async function handleDidDeleted(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const didId = event.event.data[0].toString();

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
  const args = event.event.data;
  const didId = args[0].toString();
  const fromOwner = args[1].toString();
  const toOwner = args[2].toString();

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
    const stored = (await api.query.did.did(didId)) as any;
    if (!stored?.isSome) {
      logger.warn(
        `Block ${blockNumber}: DID ${didId} not in storage — skipping`,
      );
      return;
    }

    const d = stored.unwrap().toJSON() as any;

    const authKey = resolvePublicKey(d.publicKeys, d.authenticationKey);
    const kaHashes: string[] = Array.isArray(d.keyAgreementKeys)
      ? d.keyAgreementKeys
      : [];
    const kaKeys = kaHashes
      .map((h) => resolvePublicKey(d.publicKeys, h))
      .filter((k): k is string => !!k);
    const delegationKey = resolvePublicKey(d.publicKeys, d.delegationKey);
    const attestationKey = resolvePublicKey(d.publicKeys, d.attestationKey);
    const lastTxCounter =
      d.lastTxCounter != null ? Number(d.lastTxCounter) : undefined;

    const depositOwner = d.deposit?.owner ?? undefined;
    const depositAmount =
      d.deposit?.amount != null ? Number(d.deposit.amount) : undefined;

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
  } catch (e) {
    logger.error(
      `Block ${blockNumber}: failed to sync DID ${didId} — ${e}`,
    );
  }
}

// Looks up a key hash in the BTreeMap and returns the raw hex bytes of the
// underlying Sr25519 / Ed25519 / Ecdsa / X25519 / Account variant.
function resolvePublicKey(
  publicKeys: any,
  hash: string | undefined,
): string | undefined {
  if (!publicKeys || !hash) return undefined;

  let entry: any;
  if (Array.isArray(publicKeys)) {
    const found = publicKeys.find((p: any) => p?.[0] === hash);
    entry = found ? found[1] : undefined;
  } else {
    entry = publicKeys[hash];
  }
  if (!entry) return undefined;

  const key = entry.key;
  if (!key) return undefined;

  const ver = key.publicVerificationKey ?? key.PublicVerificationKey;
  if (ver) {
    return (
      ver.sr25519 ??
      ver.Sr25519 ??
      ver.ed25519 ??
      ver.Ed25519 ??
      ver.ecdsa ??
      ver.Ecdsa ??
      ver.account ??
      ver.Account
    );
  }
  const enc = key.publicEncryptionKey ?? key.PublicEncryptionKey;
  if (enc) {
    return enc.x25519 ?? enc.X25519;
  }
  return undefined;
}
