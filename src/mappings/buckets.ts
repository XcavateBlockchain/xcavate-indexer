import type { SubstrateEvent } from "@subql/types";

import { Bucket, BucketAdmin, BucketContributor, Message } from "../types";

import {
  asOption,
  asRecord,
  formatError,
  fetchIpfsText,
  toHexString,
  toNumber,
  toUtf8String,
} from "./common";

export async function handleBucketsEvent(
  event: SubstrateEvent,
): Promise<void> {
  const blockNumber = event.block.block.header.number.toNumber();
  const method = event.event.method;

  logger.info(`Block ${blockNumber}: buckets.${method}`);

  switch (method) {
    case "BucketCreated":
      return handleBucketCreated(event, blockNumber);
    case "BucketDeleted":
      return handleBucketDeleted(event, blockNumber);
    case "PausedBucket":
      return handlePausedBucket(event, blockNumber);
    case "BucketWritableWithKey":
      return handleBucketWritableWithKey(event, blockNumber);
    case "ContributorAdded":
      return handleContributorAdded(event, blockNumber);
    case "ContributorRemoved":
      return handleContributorRemoved(event, blockNumber);
    case "AdminAdded":
      return handleAdminAdded(event, blockNumber);
    case "AdminRemoved":
      return handleAdminRemoved(event, blockNumber);
    case "NewMessage":
      return handleNewMessage(event, blockNumber);
    case "MessageDeleted":
      return handleMessageDeleted(event, blockNumber);
  }
}

// Saves a new Bucket row. Handles both the OLD 3-arg and NEW 4-arg event shapes.
async function handleBucketCreated(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const args = event.event.data as unknown[];
  const namespaceId = Number(String(args[0]));
  const bucketId = Number(String(args[1]));

  let name: string | undefined;
  let category: string | undefined;
  let creatorArg: unknown;

  const maybeStruct = asRecord(args[2]);
  const metadata = asRecord(maybeStruct?.metadata);
  const isNewShape = metadata != null;

  if (isNewShape) {
    name = metadata ? toUtf8String(metadata.name) : undefined;
    category = metadata ? toUtf8String(metadata.category) : undefined;
    creatorArg = args[3];
  } else {
    creatorArg = args[2];
    try {
      const stored = await api.query.buckets.buckets(namespaceId, bucketId);
      const storedOpt = asOption(stored);
      if (storedOpt?.isSome) {
        const storedValue = asRecord(storedOpt.unwrap());
        const storedMeta = asRecord(storedValue?.metadata);
        if (storedMeta) {
          name = toUtf8String(storedMeta.name);
          category = toUtf8String(storedMeta.category);
        }
      }
    } catch (e) {
      logger.warn(
        `Block ${blockNumber}: BucketCreated (${namespaceId}, ${bucketId}) — storage fallback failed: ${formatError(e)}`,
      );
    }
  }

  const creatorOpt = asOption(creatorArg);
  const creator = creatorOpt?.isSome
    ? String(creatorOpt.unwrap())
    : undefined;

  const bucket = Bucket.create({
    id: bucketId.toString(),
    namespaceId,
    bucketId,
    creator,
    name,
    category,
    isWritable: false,
    encryptionKey: undefined,
    createdBlock: blockNumber,
  });

  await bucket.save();
  logger.info(
    `Block ${blockNumber}: saved bucket ${bucketId} — ${name} (${category})`,
  );
}

// Backfills a Bucket row from storage when the parent event was missed
// (e.g. created before startBlock). Without this, FK inserts on Message /
// BucketContributor / BucketAdmin crash the worker.
async function ensureBucket(
  namespaceId: number,
  bucketId: number,
  blockNumber: number,
): Promise<void> {
  const existing = await Bucket.get(bucketId.toString());
  if (existing) return;

  logger.warn(
    `Block ${blockNumber}: bucket ${bucketId} not in DB — backfilling from storage (ns=${namespaceId})`,
  );

  try {
    const stored = await api.query.buckets.buckets(namespaceId, bucketId);
    const storedOpt = asOption(stored);
    if (!storedOpt?.isSome) {
      logger.warn(
        `Block ${blockNumber}: bucket ${bucketId} (ns=${namespaceId}) not in storage either — child rows will FK-fail`,
      );
      return;
    }
    const storedValue = asRecord(storedOpt.unwrap());
    const storedMeta = asRecord(storedValue?.metadata);
    if (!storedMeta) {
      logger.warn(
        `Block ${blockNumber}: bucket ${bucketId} (ns=${namespaceId}) missing metadata in storage`,
      );
      return;
    }
    const name = toUtf8String(storedMeta.name);
    const category = toUtf8String(storedMeta.category);

    let isWritable = false;
    let encryptionKey: string | undefined;
    try {
      const status = asRecord(storedValue?.status);
      if (status?.isWritable === true) {
        isWritable = true;
        encryptionKey = toHexString(status.asWritable);
      }
    } catch {
      /* leave defaults */
    }

    const bucket = Bucket.create({
      id: bucketId.toString(),
      namespaceId,
      bucketId,
      creator: undefined,
      name,
      category,
      isWritable,
      encryptionKey,
      createdBlock: blockNumber,
    });
    await bucket.save();
    logger.info(
      `Block ${blockNumber}: backfilled bucket ${bucketId} (${name})`,
    );
  } catch (e) {
    logger.error(
      `Block ${blockNumber}: ensureBucket(${bucketId}) failed: ${formatError(e)}`,
    );
  }
}

// Removes a Bucket row (cascades to its children via FK).
async function handleBucketDeleted(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [namespaceArg, bucketArg] = event.event.data as unknown[];
  const namespaceId = Number(String(namespaceArg));
  const bucketId = Number(String(bucketArg));

  logger.info(
    `Block ${blockNumber}: BucketDeleted — namespace=${namespaceId}, bucket=${bucketId}`,
  );

  const existing = await Bucket.get(bucketId.toString());
  if (!existing) {
    logger.warn(
      `Block ${blockNumber}: BucketDeleted for bucket ${bucketId} but no row found`,
    );
    return;
  }

  await Bucket.remove(bucketId.toString());
  logger.info(`Block ${blockNumber}: removed bucket ${bucketId}`);
}

// Marks a Bucket as locked (no writes allowed) and clears the encryption key.
async function handlePausedBucket(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [namespaceArg, bucketArg] = event.event.data as unknown[];
  const namespaceId = Number(String(namespaceArg));
  const bucketId = Number(String(bucketArg));

  logger.info(
    `Block ${blockNumber}: PausedBucket — namespace=${namespaceId}, bucket=${bucketId}`,
  );

  await ensureBucket(namespaceId, bucketId, blockNumber);

  const bucket = await Bucket.get(bucketId.toString());
  if (!bucket) {
    logger.warn(
      `Block ${blockNumber}: PausedBucket for bucket ${bucketId} but ensureBucket failed`,
    );
    return;
  }

  bucket.isWritable = false;
  bucket.encryptionKey = undefined;
  await bucket.save();
  logger.info(`Block ${blockNumber}: paused bucket ${bucketId}`);
}

// Marks a Bucket as writable and stores the new symmetric encryption key.
async function handleBucketWritableWithKey(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [namespaceArg, bucketArg, keyArg] = event.event.data as unknown[];
  const namespaceId = Number(String(namespaceArg));
  const bucketId = Number(String(bucketArg));
  const encryptionKey = toHexString(keyArg);

  logger.info(
    `Block ${blockNumber}: BucketWritableWithKey — namespace=${namespaceId}, bucket=${bucketId}`,
  );

  if (!encryptionKey) {
    logger.warn(
      `Block ${blockNumber}: BucketWritableWithKey for bucket ${bucketId} missing encryption key`,
    );
    return;
  }

  await ensureBucket(namespaceId, bucketId, blockNumber);

  const bucket = await Bucket.get(bucketId.toString());
  if (!bucket) {
    logger.warn(
      `Block ${blockNumber}: BucketWritableWithKey for bucket ${bucketId} but ensureBucket failed`,
    );
    return;
  }

  bucket.isWritable = true;
  bucket.encryptionKey = encryptionKey;
  await bucket.save();
  logger.info(
    `Block ${blockNumber}: bucket ${bucketId} writable with key ${encryptionKey.slice(0, 16)}...`,
  );
}

// Adds a contributor (write permission) to a bucket.
async function handleContributorAdded(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [namespaceArg, bucketArg, contributorArg] =
    event.event.data as unknown[];
  const namespaceId = Number(String(namespaceArg));
  const bucketId = Number(String(bucketArg));
  const subjectId = String(contributorArg);
  const id = `${bucketId}-${subjectId}`;

  await ensureBucket(namespaceId, bucketId, blockNumber);

  const row = BucketContributor.create({
    id,
    bucketId: bucketId.toString(),
    subjectId,
    addedBlock: blockNumber,
  });
  await row.save();
  logger.info(
    `Block ${blockNumber}: added contributor ${subjectId} to bucket ${bucketId}`,
  );
}

// Removes a contributor from a bucket.
async function handleContributorRemoved(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [, bucketArg, contributorArg] = event.event.data as unknown[];
  const bucketId = Number(String(bucketArg));
  const subjectId = String(contributorArg);
  const id = `${bucketId}-${subjectId}`;

  const existing = await BucketContributor.get(id);
  if (!existing) {
    logger.warn(
      `Block ${blockNumber}: ContributorRemoved for ${id} but no row found`,
    );
    return;
  }

  await BucketContributor.remove(id);
  logger.info(
    `Block ${blockNumber}: removed contributor ${subjectId} from bucket ${bucketId}`,
  );
}

// Adds an admin (manage-membership permission) to a bucket.
async function handleAdminAdded(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [namespaceArg, bucketArg, adminArg] = event.event.data as unknown[];
  const namespaceId = Number(String(namespaceArg));
  const bucketId = Number(String(bucketArg));
  const subjectId = String(adminArg);
  const id = `${bucketId}-${subjectId}`;

  await ensureBucket(namespaceId, bucketId, blockNumber);

  const row = BucketAdmin.create({
    id,
    bucketId: bucketId.toString(),
    subjectId,
    addedBlock: blockNumber,
  });
  await row.save();
  logger.info(
    `Block ${blockNumber}: added admin ${subjectId} to bucket ${bucketId}`,
  );
}

// Removes an admin from a bucket.
async function handleAdminRemoved(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [, bucketArg, adminArg] = event.event.data as unknown[];
  const bucketId = Number(String(bucketArg));
  const subjectId = String(adminArg);
  const id = `${bucketId}-${subjectId}`;

  const existing = await BucketAdmin.get(id);
  if (!existing) {
    logger.warn(
      `Block ${blockNumber}: AdminRemoved for ${id} but no row found`,
    );
    return;
  }

  await BucketAdmin.remove(id);
  logger.info(
    `Block ${blockNumber}: removed admin ${subjectId} from bucket ${bucketId}`,
  );
}

// Saves a new Message and fetches its IPFS body if it's text/plain.
// Handles both OLD 3-arg and NEW 5-arg event shapes.
async function handleNewMessage(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const args = event.event.data as unknown[];
  const isNewShape = args.length >= 5;

  let bucketId: number;
  let messageId: number;
  let contributor: string;
  let messageStruct: Record<string, unknown> | undefined;

  let namespaceId: number;
  if (isNewShape) {
    namespaceId = Number(String(args[0]));
    bucketId = Number(String(args[1]));
    messageId = Number(String(args[2]));
    messageStruct = asRecord(args[3]);
    contributor = String(args[4]);
  } else {
    // OLD event shape has no namespace_id; Xcavate has only ever used 0.
    namespaceId = 0;
    bucketId = Number(String(args[0]));
    messageId = Number(String(args[1]));
    contributor = String(args[2]);
  }

  const id = `${bucketId}-${messageId}`;

  await ensureBucket(namespaceId, bucketId, blockNumber);

  try {
    let m = messageStruct;

    if (!m) {
      const stored = await api.query.buckets.messages(bucketId, messageId);
      const storedOpt = asOption(stored);
      if (!storedOpt?.isSome) {
        logger.warn(
          `Block ${blockNumber}: NewMessage ${id} — storage entry missing`,
        );
        return;
      }
      m = asRecord(storedOpt.unwrap());
    }

    if (!m) {
      logger.warn(`Block ${blockNumber}: NewMessage ${id} — invalid payload`);
      return;
    }

    const metadata = asRecord(m.metadata);
    if (!metadata) {
      logger.warn(`Block ${blockNumber}: NewMessage ${id} — missing metadata`);
      return;
    }

    const reference = toUtf8String(m.reference);
    const tagOpt = asOption(m.tag);
    const tag = tagOpt?.isSome ? toUtf8String(tagOpt.unwrap()) : undefined;
    const description = toUtf8String(metadata.description);
    const contentType = toUtf8String(metadata.contentType);
    const contentHash = toHexString(metadata.contentHash);
    if (!contentHash) {
      logger.warn(
        `Block ${blockNumber}: NewMessage ${id} — missing content hash`,
      );
      return;
    }
    const createdBlock = toNumber(metadata.createdAt) ?? blockNumber;

    let ipfsContent: string | undefined;
    if (contentType.startsWith("text/plain")) {
      ipfsContent = await fetchIpfsText(reference);
    }

    const message = Message.create({
      id,
      bucketId: bucketId.toString(),
      messageId,
      contributor,
      reference,
      tag,
      description,
      contentType,
      contentHash,
      createdBlock,
      ipfsContent,
    });

    await message.save();
    logger.info(
      `Block ${blockNumber}: saved message ${id} by ${contributor} (${contentType})`,
    );
  } catch (e) {
    logger.error(
      `Block ${blockNumber}: failed to decode NewMessage ${id} — ${formatError(e)}`,
    );
  }
}

// Removes a message row.
async function handleMessageDeleted(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [bucketArg, messageIdArg] = event.event.data as unknown[];
  const bucketId = Number(String(bucketArg));
  const messageId = Number(String(messageIdArg));
  const id = `${bucketId}-${messageId}`;

  const existing = await Message.get(id);
  if (!existing) {
    logger.warn(
      `Block ${blockNumber}: MessageDeleted for ${id} but no row found`,
    );
    return;
  }

  await Message.remove(id);
  logger.info(`Block ${blockNumber}: removed message ${id}`);
}