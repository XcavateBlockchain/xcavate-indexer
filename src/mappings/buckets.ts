import type { SubstrateBlock, SubstrateEvent } from "@subql/types";

import {
  Bucket,
  BucketAdmin,
  BucketContributor,
  Message,
  Namespace,
  NamespaceManager,
  Tag,
  TagMessageCount,
} from "../types";

import {
  asOption,
  asRecord,
  asStorageValue,
  formatError,
  fetchIpfsText,
  getStorageKeyArgs,
  toHexString,
  toJsonValue,
  toNumber,
  toUtf8String,
  toSs58,
} from "./common";

let bucketsSyncInFlight: Promise<void> | null = null;
let bucketsSynced = false;

export async function handleBucketsSyncBlock(
  block: SubstrateBlock,
): Promise<void> {
  const blockNumber = block.block.header.number.toNumber();
  await ensureBucketsSynced(blockNumber);
}

export async function handleBucketsEvent(
  event: SubstrateEvent,
): Promise<void> {
  const blockNumber = event.block.block.header.number.toNumber();
  const method = event.event.method;

  await ensureBucketsSynced(blockNumber);

  logger.info(`Block ${blockNumber}: buckets.${method}`);

  switch (method) {
    // Namespace lifecycle
    case "NamespaceCreated":
      return handleNamespaceCreated(event, blockNumber);
    case "NamespaceDeleted":
      return handleNamespaceDeleted(event, blockNumber);
    // Manager lifecycle
    case "ManagerAdded":
      return handleManagerAdded(event, blockNumber);
    case "ManagerRemoved":
      return handleManagerRemoved(event, blockNumber);
    // Bucket lifecycle
    case "BucketCreated":
      return handleBucketCreated(event, blockNumber);
    case "BucketDeleted":
      return handleBucketDeleted(event, blockNumber);
    case "PausedBucket":
      return handlePausedBucket(event, blockNumber);
    case "BucketWritableWithKey":
      return handleBucketWritableWithKey(event, blockNumber);
    // Contributor lifecycle
    case "ContributorAdded":
      return handleContributorAdded(event, blockNumber);
    case "ContributorRemoved":
      return handleContributorRemoved(event, blockNumber);
    // Admin lifecycle
    case "AdminAdded":
      return handleAdminAdded(event, blockNumber);
    case "AdminRemoved":
      return handleAdminRemoved(event, blockNumber);
    // Tag lifecycle
    case "NewTag":
      return handleNewTag(event, blockNumber);
    case "TagDeleted":
      return handleTagDeleted(event, blockNumber);
    // Message lifecycle
    case "NewMessage":
      return handleNewMessage(event, blockNumber);
    case "MessageDeleted":
      return handleMessageDeleted(event, blockNumber);
  }
}

// ---------------------------------------------------------------------------
// Sync gate — ensures the initial storage snapshot is taken before processing
// event-driven writes.  Follows the same pattern used by the other pendants.
// ---------------------------------------------------------------------------

export async function ensureBucketsSynced(blockNumber: number): Promise<void> {
  if (bucketsSynced) return;
  if (blockNumber == 0) return;

  bucketsSyncInFlight ??= syncBucketsFromStorage(blockNumber)
    .then(() => {
      bucketsSynced = true;
    })
    .catch((e) => {
      logger.error(
        `Block ${blockNumber}: buckets storage sync failed — ${formatError(e)}`,
      );
    })
    .finally(() => {
      bucketsSyncInFlight = null;
    });
  await bucketsSyncInFlight;
}

// ---------------------------------------------------------------------------
// Full storage sync (one-shot at start block)
// ---------------------------------------------------------------------------

async function syncBucketsFromStorage(blockNumber: number): Promise<void> {
  logger.info(`Block ${blockNumber}: syncing buckets storage`);

  const pallet = asRecord(api.query?.buckets);
  if (!pallet) {
    logger.error(`Block ${blockNumber}: buckets pallet unavailable`);
    return;
  }

  // 1. Namespaces — new
  await syncBucketStorageEntries(
    pallet.namespaces,
    "buckets.namespaces",
    blockNumber,
    async (args, value) => {
      const namespaceId = toNumber(args[0]);
      if (namespaceId == null) return;
      await upsertNamespaceFromStorage(namespaceId, value, blockNumber);
    },
  );

  // 2. Buckets (existing)
  await syncBucketStorageEntries(
    pallet.buckets,
    "buckets.buckets",
    blockNumber,
    async (args, value) => {
      const namespaceId = toNumber(args[0]);
      const bucketId = toNumber(args[1]);
      if (namespaceId == null || bucketId == null) return;
      await upsertBucketFromStorage(namespaceId, bucketId, value, blockNumber);
    },
  );

  // 3. Contributors (existing)
  await syncBucketStorageEntries(
    pallet.contributors,
    "buckets.contributors",
    blockNumber,
    async (args) => {
      const namespaceId = toNumber(args[0]);
      const bucketId = toNumber(args[1]);
      const subjectRaw = args[2];
      if (namespaceId == null || bucketId == null || subjectRaw == null) return;
      await upsertBucketContributor(namespaceId, bucketId, subjectRaw, blockNumber);
    },
  );

  // 4. Admins (existing)
  await syncBucketStorageEntries(
    pallet.admins,
    "buckets.admins",
    blockNumber,
    async (args) => {
      const namespaceId = toNumber(args[0]);
      const bucketId = toNumber(args[1]);
      const subjectRaw = args[2];
      if (namespaceId == null || bucketId == null || subjectRaw == null) return;
      await upsertBucketAdmin(namespaceId, bucketId, subjectRaw, blockNumber);
    },
  );

  // 5. Managers — new
  await syncBucketStorageEntries(
    pallet.managers,
    "buckets.managers",
    blockNumber,
    async (args) => {
      const namespaceId = toNumber(args[0]);
      const subjectRaw = args[1];
      if (namespaceId == null || subjectRaw == null) return;
      await upsertNamespaceManager(namespaceId, subjectRaw, blockNumber);
    },
  );

  // 6. Messages (existing)
  await syncBucketStorageEntries(
    pallet.messages,
    "buckets.messages",
    blockNumber,
    async (args, value) => {
      const namespaceId = toNumber(args[0]);
      const bucketId = toNumber(args[1]);
      const messageId = toNumber(args[2]);
      if (namespaceId == null || bucketId == null || messageId == null) return;
      await upsertMessageFromStorage(namespaceId, bucketId, messageId, value, blockNumber);
    },
  );

  // 7. Tags — new
  await syncBucketStorageEntries(
    pallet.tags,
    "buckets.tags",
    blockNumber,
    async (args) => {
      const bucketId = toNumber(args[0]);
      const tagRaw = args[1];
      if (bucketId == null || tagRaw == null) return;
      await upsertTagFromStorage(bucketId, tagRaw, blockNumber);
    },
  );

  // 8. TagMessages — new (message counts per tag)
  await syncBucketStorageEntries(
    pallet.tagMessages,
    "buckets.tagMessages",
    blockNumber,
    async (args, value) => {
      const bucketId = toNumber(args[0]);
      const tagRaw = args[1];
      const count = toNumber(value);
      if (bucketId == null || tagRaw == null || count == null) return;
      await upsertTagMessageCount(bucketId, tagRaw, count, blockNumber);
    },
  );

  logger.info(`Block ${blockNumber}: buckets storage sync complete`);
}

// ---------------------------------------------------------------------------
// Generic storage-entry iterator (reused by all pendants)
// ---------------------------------------------------------------------------

async function syncBucketStorageEntries(
  storage: unknown,
  storageName: string,
  blockNumber: number,
  handle: (args: unknown[], value: unknown) => Promise<void>,
): Promise<void> {
  const target = (typeof storage === "function" ? storage : asRecord(storage)) as
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
  for (const [storageKey, rawValue] of entries) {
    const args = getStorageKeyArgs(storageKey);
    if (!args) continue;

    const value = asStorageValue(rawValue);
    if (!value.isSome) continue;

    await handle(args, value.unwrap());
    synced += 1;
  }

  logger.info(
    `Block ${blockNumber}: ${storageName} storage entries=${entries.length}, handled=${synced}`,
  );
}

// ---------------------------------------------------------------------------
// Namespace
// ---------------------------------------------------------------------------

// Upserts a Namespace row from storage.
async function upsertNamespaceFromStorage(
  namespaceId: number,
  storedValue: unknown,
  blockNumber: number,
): Promise<void> {
  const raw = asRecord(storedValue);
  const json = asRecord(toJsonValue(storedValue));
  const metadata = asRecord(raw?.metadata) ?? asRecord(json?.metadata);
  if (!metadata) return;

  const existing = await Namespace.get(namespaceId.toString());

  const properties = metadata.properties;
  const propertiesStr = stringifyJson(properties);

  const name = metadata.name != null ? toUtf8String(metadata.name) : existing?.name;
  const schemaUri = metadata.schemaUri != null ? toUtf8String(metadata.schemaUri) :
    existing?.schemaUri ?? toUtf8String(metadata.schema_uri);
  const createdAt = toNumber(metadata.createdAt);

  const ns = Namespace.create({
    id: namespaceId.toString(),
    namespaceId,
    name,
    schemaUri,
    properties: propertiesStr,
    createdAt: createdAt ?? blockNumber,
    creator: existing?.creator,
  });

  await ns.save();
  logger.info(
    `Block ${blockNumber}: saved namespace ${namespaceId} — ${name ?? "unknown"}`,
  );
}

// ---------------------------------------------------------------------------
// NamespaceManager
// ---------------------------------------------------------------------------

// Upserts a NamespaceManager row (called from both event handler and storage sync).
async function upsertNamespaceManager(
  namespaceId: number,
  subjectRaw: unknown,
  blockNumber: number,
): Promise<void> {
  // Ensure the namespace row exists (FK guard).
  await ensureNamespace(namespaceId, blockNumber);

  const managerId = (await toSs58(subjectRaw, 0)) ?? toUtf8String(subjectRaw) ?? String(subjectRaw);
  const id = `${namespaceId}-${managerId}`;
  const existing = await NamespaceManager.get(id);

  const row = NamespaceManager.create({
    id,
    namespaceId: namespaceId.toString(),
    manager: managerId,
    addedBlock: existing?.addedBlock ?? blockNumber,
  });
  await row.save();
}

// Backfills a Namespace row from storage when the parent event was missed.
async function ensureNamespace(
  namespaceId: number,
  blockNumber: number,
): Promise<void> {
  const existing = await Namespace.get(namespaceId.toString());
  if (existing) return;

  logger.warn(
    `Block ${blockNumber}: namespace ${namespaceId} not in DB — backfilling from storage`,
  );

  try {
    const stored = await api.query.buckets.namespaces(namespaceId);
    const storedOpt = asOption(stored);
    if (!storedOpt?.isSome) {
      logger.warn(
        `Block ${blockNumber}: namespace ${namespaceId} not in storage either — child rows will FK-fail`,
      );
      return;
    }

    await upsertNamespaceFromStorage(namespaceId, storedOpt.unwrap(), blockNumber);
    logger.info(`Block ${blockNumber}: backfilled namespace ${namespaceId}`);
  } catch (e) {
    logger.error(
      `Block ${blockNumber}: ensureNamespace(${namespaceId}) failed: ${formatError(e)}`,
    );
  }
}

// ---------------------------------------------------------------------------
// Bucket helpers (existing, with namespace-aware storage lookups)
// ---------------------------------------------------------------------------

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

async function upsertBucketFromStorage(
  namespaceId: number,
  bucketId: number,
  storedValue: unknown,
  blockNumber: number,
): Promise<void> {
  const raw = asRecord(storedValue);
  const json = asRecord(toJsonValue(storedValue));
  const metadata = asRecord(raw?.metadata) ?? asRecord(json?.metadata);
  const status = asRecord(raw?.status) ?? asRecord(json?.status);
  const existing = await Bucket.get(bucketId.toString());

  const isWritable = status?.isWritable === true || status?.writable != null ||
    status?.Writable != null;
  const writableValue = isWritable
    ? status?.asWritable ?? status?.writable ?? status?.Writable
    : undefined;
  const encryptionKey = writableValue != null
    ? toHexString(writableValue) ?? toUtf8String(writableValue)
    : undefined;

  const bucket = Bucket.create({
    id: bucketId.toString(),
    namespaceId,
    bucketId,
    creator: existing?.creator,
    name: metadata?.name != null ? toUtf8String(metadata.name) : existing?.name,
    category: metadata?.category != null
      ? toUtf8String(metadata.category)
      : existing?.category,
    isWritable,
    encryptionKey,
    createdBlock:
      existing?.createdBlock ?? toNumber(metadata?.createdAt) ?? blockNumber,
  });

  await bucket.save();
}

async function upsertBucketContributor(
  namespaceId: number,
  bucketId: number,
  subjectRaw: unknown,
  blockNumber: number,
): Promise<void> {
  await ensureBucket(namespaceId, bucketId, blockNumber);
  const bucket = await Bucket.get(bucketId.toString());
  if (!bucket) {
    logger.warn(
      `Block ${blockNumber}: contributor ${String(subjectRaw)} skipped; bucket ${bucketId} missing`,
    );
    return;
  }
  const subjectId = (await toSs58(subjectRaw, 0)) ?? toUtf8String(subjectRaw) ?? String(subjectRaw);

  const id = `${bucketId}-${subjectId}`;
  const existing = await BucketContributor.get(id);
  const row = BucketContributor.create({
    id,
    bucketId: bucketId.toString(),
    subjectId,
    addedBlock: existing?.addedBlock ?? blockNumber,
  });
  await row.save();
}

async function upsertBucketAdmin(
  namespaceId: number,
  bucketId: number,
  subjectRaw: unknown,
  blockNumber: number,
): Promise<void> {
  await ensureBucket(namespaceId, bucketId, blockNumber);
  const bucket = await Bucket.get(bucketId.toString());
  if (!bucket) {
    logger.warn(
      `Block ${blockNumber}: admin ${String(subjectRaw)} skipped; bucket ${bucketId} missing`,
    );
    return;
  }
  const subjectId = (await toSs58(subjectRaw, 0)) ?? toUtf8String(subjectRaw) ?? String(subjectRaw);

  const id = `${bucketId}-${subjectId}`;
  const existing = await BucketAdmin.get(id);
  const row = BucketAdmin.create({
    id,
    bucketId: bucketId.toString(),
    subjectId,
    addedBlock: existing?.addedBlock ?? blockNumber,
  });
  await row.save();
}

// ---------------------------------------------------------------------------
// Message
// ---------------------------------------------------------------------------

async function upsertMessageFromStorage(
  namespaceId: number,
  bucketId: number,
  messageId: number,
  storedValue: unknown,
  blockNumber: number,
): Promise<void> {
  await ensureBucket(namespaceId, bucketId, blockNumber);
  const bucket = await Bucket.get(bucketId.toString());
  if (!bucket) {
    logger.warn(
      `Block ${blockNumber}: message ${bucketId}-${messageId} skipped; bucket missing`,
    );
    return;
  }

  const raw = asRecord(storedValue);
  const json = asRecord(toJsonValue(storedValue));
  const message = raw ?? json;
  const metadata = asRecord(raw?.metadata) ?? asRecord(json?.metadata);
  if (!message || !metadata) return;

  const id = `${bucketId}-${messageId}`;
  const existing = await Message.get(id);
  const tagOpt = asOption(message.tag);
  const tag = tagOpt?.isSome ? toUtf8String(tagOpt.unwrap()) : undefined;
  const contentType = toUtf8String(metadata.contentType);
  const reference = toUtf8String(message.reference);
  const contentHash = toHexString(metadata.contentHash);
  if (!contentHash) return;

  let ipfsContent = existing?.ipfsContent;
  if (!ipfsContent && contentType.startsWith("text/plain")) {
    ipfsContent = await fetchIpfsText(reference);
  }

  const row = Message.create({
    id,
    bucketId: bucketId.toString(),
    messageId,
    contributor: existing?.contributor ?? "unknown",
    reference,
    tag,
    description: toUtf8String(metadata.description),
    contentType,
    contentHash,
    createdBlock: toNumber(metadata.createdAt) ?? existing?.createdBlock ?? blockNumber,
    ipfsContent,
  });

  await row.save();
}

// ---------------------------------------------------------------------------
// Tag helpers
// ---------------------------------------------------------------------------

// Upserts a Tag row from storage.
async function upsertTagFromStorage(
  bucketId: number,
  tagRaw: unknown,
  blockNumber: number,
): Promise<void> {
  const tagStr = toUtf8String(tagRaw);
  if (!tagStr) return;

  await ensureBucket(0, bucketId, blockNumber);
  const bucket = await Bucket.get(bucketId.toString());
  if (!bucket) return;

  const id = `${bucketId}-${tagStr}`;
  const existing = await Tag.get(id);

  const row = Tag.create({
    id,
    bucketId: bucketId.toString(),
    tagName: tagStr,
    createdBlock: existing?.createdBlock ?? blockNumber,
    creator: existing?.creator,
    messageCount: 0,
  });
  await row.save();
}

// Upserts / updates a TagMessageCount row (synced from TagMessages storage).
async function upsertTagMessageCount(
  bucketId: number,
  tagRaw: unknown,
  count: number,
  blockNumber: number,
): Promise<void> {
  const tagStr = toUtf8String(tagRaw);
  if (!tagStr) return;

  const id = `${bucketId}-${tagStr}`;

  // Also update the Tag row's messageCount if it exists.
  const existingTag = await Tag.get(id);
  if (existingTag) {
    existingTag.messageCount = count;
    await existingTag.save();
  }

  const existingTmc = await TagMessageCount.get(id);
  const row = TagMessageCount.create({
    id,
    bucketId: bucketId.toString(),
    tagName: tagStr,
    count,
    updatedBlock: existingTmc?.updatedBlock ?? blockNumber,
  });
  await row.save();
}

// ---------------------------------------------------------------------------
// String helpers
// ---------------------------------------------------------------------------

function stringifyJson(value: unknown): string | undefined {
  if (value == null) return undefined;
  try {
    return JSON.stringify(value);
  } catch {
    return undefined;
  }
}

// ---------------------------------------------------------------------------
// Event handlers — Namespace
// ---------------------------------------------------------------------------

// Handles a NamespaceCreated event.  The event payload includes the full
// NamespaceMetadata struct so no storage read is required.
async function handleNamespaceCreated(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  // Event fields per metadata: namespace_id (u32), metadata (NamespaceMetadata), creator (Option<SubjectId>)
  const args = event.event.data as unknown[];
  const namespaceId = Number(String(args[0]));

  const metadata = asRecord(args[1]);
  if (!metadata) {
    // Fall back to storage read.
    await ensureNamespace(namespaceId, blockNumber);
    return;
  }

  const creatorArg = args[2];
  const creatorOpt = asOption(creatorArg);
  const creator = creatorOpt?.isSome
    ? (await toSs58(creatorOpt.unwrap(), 0)) ?? String(creatorOpt.unwrap())
    : undefined;

  const existing = await Namespace.get(namespaceId.toString());
  const properties = metadata.properties;
  const propertiesStr = stringifyJson(properties);

  const name = metadata.name != null ? toUtf8String(metadata.name) : existing?.name;
  const schemaUri = metadata.schemaUri != null ? toUtf8String(metadata.schemaUri) :
    existing?.schemaUri ?? toUtf8String(metadata.schema_uri);

  const ns = Namespace.create({
    id: namespaceId.toString(),
    namespaceId,
    name,
    schemaUri,
    properties: propertiesStr ?? existing?.properties,
    createdAt: existing?.createdAt ?? toNumber(metadata.createdAt) ?? blockNumber,
    creator: existing?.creator ?? creator,
  });

  await ns.save();
  logger.info(
    `Block ${blockNumber}: created namespace ${namespaceId} — ${name ?? "unknown"}`,
  );
}

// Removes a Namespace row (cascades to managers via FK).
async function handleNamespaceDeleted(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const args = event.event.data as unknown[];
  const namespaceId = Number(String(args[0]));

  logger.info(`Block ${blockNumber}: NamespaceDeleted — namespace=${namespaceId}`);

  const existing = await Namespace.get(namespaceId.toString());
  if (!existing) {
    logger.warn(
      `Block ${blockNumber}: NamespaceDeleted for ${namespaceId} but no row found`,
    );
    return;
  }

  await Namespace.remove(namespaceId.toString());
  logger.info(`Block ${blockNumber}: removed namespace ${namespaceId}`);
}

// ---------------------------------------------------------------------------
// Event handlers — Manager
// ---------------------------------------------------------------------------

// Adds a manager to a namespace.
async function handleManagerAdded(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  // Fields: namespace_id, manager, caller
  const args = event.event.data as unknown[];
  const namespaceId = Number(String(args[0]));
  const subjectRaw = args[1];

  await ensureNamespace(namespaceId, blockNumber);
  await upsertNamespaceManager(namespaceId, subjectRaw, blockNumber);

  const managerId = (await toSs58(subjectRaw, 0)) ?? String(subjectRaw);
  logger.info(
    `Block ${blockNumber}: added manager ${managerId} to namespace ${namespaceId}`,
  );
}

// Removes a manager from a namespace.
async function handleManagerRemoved(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const args = event.event.data as unknown[];
  const namespaceId = Number(String(args[0]));
  const subjectRaw = args[1];

  const managerId = (await toSs58(subjectRaw, 0)) ?? String(subjectRaw);
  const id = `${namespaceId}-${managerId}`;

  const existing = await NamespaceManager.get(id);
  if (!existing) {
    logger.warn(
      `Block ${blockNumber}: ManagerRemoved for ${id} but no row found`,
    );
    return;
  }

  await NamespaceManager.remove(id);
  logger.info(
    `Block ${blockNumber}: removed manager ${managerId} from namespace ${namespaceId}`,
  );
}

// ---------------------------------------------------------------------------
// Event handlers — Bucket (existing, slightly adapted)
// ---------------------------------------------------------------------------

// Saves a new Bucket row.  The event payload includes the full BucketDetails
// struct, so we read name / category / encryption directly from the event.
async function handleBucketCreated(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  // Event fields per metadata: namespace_id, bucket_id, BucketDetails (struct), creator (Option<SubjectId>)
  const args = event.event.data as unknown[];
  const namespaceId = Number(String(args[0]));
  const bucketId = Number(String(args[1]));

  let name: string | undefined;
  let category: string | undefined;
  let isWritable = false;
  let encryptionKey: string | undefined;
  let creatorArg: unknown;

  const bucketDetails = asRecord(args[2]);
  if (bucketDetails) {
    const metadata = asRecord(bucketDetails.metadata);
    name = metadata?.name != null ? toUtf8String(metadata.name) : undefined;
    category = metadata?.category != null ? toUtf8String(metadata.category) : undefined;
    creatorArg = args[3];

    // Extract writable status from BucketDetails if present.
    const status = asRecord(bucketDetails.status);
    if (status?.isWritable === true || status?.writable != null) {
      isWritable = true;
      encryptionKey = toHexString(status.asWritable ?? status.writable) ?? undefined;
    }
  } else {
    // Legacy event shape: namespace_id, bucket_id, creator, metadata
    creatorArg = args[2];
  }

  const creatorOpt = asOption(creatorArg);
  const creator = creatorOpt?.isSome
    ? (await toSs58(creatorOpt.unwrap(), 0)) ?? String(creatorOpt.unwrap())
    : undefined;

  const bucket = Bucket.create({
    id: bucketId.toString(),
    namespaceId,
    bucketId,
    creator,
    name,
    category,
    isWritable,
    encryptionKey,
    createdBlock: blockNumber,
  });

  await bucket.save();
  logger.info(
    `Block ${blockNumber}: saved bucket ${bucketId} — ${name} (${category})`,
  );
}

// Removes a Bucket row (cascades to its children via FK).
async function handleBucketDeleted(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  // Event: namespace_id, bucket_id
  const args = event.event.data as unknown[];
  const namespaceId = Number(String(args[0]));
  const bucketId = Number(String(args[1]));

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
  const args = event.event.data as unknown[];
  const namespaceId = Number(String(args[0]));
  const bucketId = Number(String(args[1]));

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
  const args = event.event.data as unknown[];
  const namespaceId = Number(String(args[0]));
  const bucketId = Number(String(args[1]));
  const keyArg = args[2];
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

// ---------------------------------------------------------------------------
// Event handlers — Contributor (existing, with namespace_id arg)
// ---------------------------------------------------------------------------

// Adds a contributor (write permission) to a bucket.
async function handleContributorAdded(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const args = event.event.data as unknown[];
  const namespaceId = Number(String(args[0]));
  const bucketId = Number(String(args[1]));
  const subjectRaw = args[2];
  const subjectId = (await toSs58(subjectRaw, 0)) ?? String(subjectRaw);
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
  const args = event.event.data as unknown[];
  const bucketId = Number(String(args[1]));
  const subjectRaw = args[2];
  const subjectId = (await toSs58(subjectRaw, 0)) ?? String(subjectRaw);
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

// ---------------------------------------------------------------------------
// Event handlers — Admin (existing, with namespace_id arg)
// ---------------------------------------------------------------------------

// Adds an admin (manage-membership permission) to a bucket.
async function handleAdminAdded(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const args = event.event.data as unknown[];
  const namespaceId = Number(String(args[0]));
  const bucketId = Number(String(args[1]));
  const subjectRaw = args[2];
  const subjectId = (await toSs58(subjectRaw, 0)) ?? String(subjectRaw);
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
  const args = event.event.data as unknown[];
  const bucketId = Number(String(args[1]));
  const subjectRaw = args[2];
  const subjectId = (await toSs58(subjectRaw, 0)) ?? String(subjectRaw);
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

// ---------------------------------------------------------------------------
// Event handlers — Tag
// ---------------------------------------------------------------------------

// Adds a new tag to a bucket.
async function handleNewTag(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  // Event fields per metadata: bucket_id, tag, creator (Option<SubjectId>)
  const args = event.event.data as unknown[];
  const bucketId = Number(String(args[0]));
  const tagRaw = args[1];
  const tagStr = toUtf8String(tagRaw);
  if (!tagStr) return;

  const creatorArg = args[2];
  const creatorOpt = asOption(creatorArg);
  const creator = creatorOpt?.isSome
    ? (await toSs58(creatorOpt.unwrap(), 0)) ?? String(creatorOpt.unwrap())
    : undefined;

  const id = `${bucketId}-${tagStr}`;
  const existing = await Tag.get(id);

  const row = Tag.create({
    id,
    bucketId: bucketId.toString(),
    tagName: tagStr,
    createdBlock: existing?.createdBlock ?? blockNumber,
    creator: existing?.creator ?? creator,
    messageCount: existing?.messageCount ?? 0,
  });

  await row.save();
  logger.info(`Block ${blockNumber}: created tag ${tagStr} in bucket ${bucketId}`);
}

// Removes a tag from a bucket.
async function handleTagDeleted(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  // Event fields: bucket_id, tag
  const args = event.event.data as unknown[];
  const bucketId = Number(String(args[0]));
  const tagRaw = args[1];
  const tagStr = toUtf8String(tagRaw);
  if (!tagStr) return;

  const id = `${bucketId}-${tagStr}`;

  const existing = await Tag.get(id);
  if (!existing) {
    logger.warn(
      `Block ${blockNumber}: TagDeleted for ${id} but no row found`,
    );
    return;
  }

  await Tag.remove(id);

  // Also remove the tag message count.
  const existingTmc = await TagMessageCount.get(id);
  if (existingTmc) {
    await TagMessageCount.remove(id);
  }

  logger.info(`Block ${blockNumber}: removed tag ${tagStr} from bucket ${bucketId}`);
}

// ---------------------------------------------------------------------------
// Event handlers — Message (existing, with namespace_id arg)
// ---------------------------------------------------------------------------

// Saves a new Message and fetches its IPFS body if it's text/plain.
async function handleNewMessage(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  // Event fields per metadata: namespace_id, bucket_id, message_id, MessageDetails, contributor (SubjectId)
  const args = event.event.data as unknown[];
  const namespaceId = Number(String(args[0]));
  const bucketId = Number(String(args[1]));
  const messageId = Number(String(args[2]));
  const messageStruct = asRecord(args[3]);
  const contributor = (await toSs58(args[4], 0)) ?? String(args[4]);

  const id = `${bucketId}-${messageId}`;

  await ensureBucket(namespaceId, bucketId, blockNumber);

  try {
    let m = messageStruct;

    if (!m) {
      const stored = await api.query.buckets.messages(namespaceId, bucketId, messageId);
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
  // Event fields: bucket_id, message_id
  const args = event.event.data as unknown[];
  const bucketId = Number(String(args[0]));
  const messageId = Number(String(args[1]));
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
