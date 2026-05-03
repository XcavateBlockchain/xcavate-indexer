import { SubstrateEvent } from "@subql/types";
import { Bucket, BucketAdmin, BucketContributor, Message } from "../types";
import { bytesToUtf8, fetchIpfsText } from "./common";

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

async function handleBucketCreated(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  // NEW event shape: (namespace_id, bucket_id, bucket: BucketDetails, creator)
  const [namespaceArg, bucketArg, bucketStructArg, creatorArg] =
    event.event.data;
  const namespaceId = Number(namespaceArg.toString());
  const bucketId = Number(bucketArg.toString());

  const bucketStruct = bucketStructArg as any;
  const name = bytesToUtf8(bucketStruct.metadata.name);
  const category = bytesToUtf8(bucketStruct.metadata.category);

  const creatorOpt = creatorArg as any;
  const creator = creatorOpt?.isSome
    ? creatorOpt.unwrap().toString()
    : undefined;

  const bucket = Bucket.create({
    id: bucketId.toString(),
    namespaceId,
    bucketId,
    creator,
    name,
    category,
    // status defaults to Locked at creation; admin must call resume_writing
    isWritable: false,
    encryptionKey: undefined,
    createdBlock: blockNumber,
  });

  await bucket.save();
  logger.info(
    `Block ${blockNumber}: saved bucket ${bucketId} — ${name} (${category})`,
  );
}

async function handleBucketDeleted(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [namespaceArg, bucketArg] = event.event.data;
  const namespaceId = Number(namespaceArg.toString());
  const bucketId = Number(bucketArg.toString());

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

async function handlePausedBucket(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [namespaceArg, bucketArg] = event.event.data;
  const namespaceId = Number(namespaceArg.toString());
  const bucketId = Number(bucketArg.toString());

  logger.info(
    `Block ${blockNumber}: PausedBucket — namespace=${namespaceId}, bucket=${bucketId}`,
  );

  const bucket = await Bucket.get(bucketId.toString());
  if (!bucket) {
    logger.warn(
      `Block ${blockNumber}: PausedBucket for bucket ${bucketId} but no row found`,
    );
    return;
  }

  bucket.isWritable = false;
  bucket.encryptionKey = undefined;
  await bucket.save();
  logger.info(`Block ${blockNumber}: paused bucket ${bucketId}`);
}

async function handleBucketWritableWithKey(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [namespaceArg, bucketArg, keyArg] = event.event.data;
  const namespaceId = Number(namespaceArg.toString());
  const bucketId = Number(bucketArg.toString());
  const encryptionKey = keyArg.toHex();

  logger.info(
    `Block ${blockNumber}: BucketWritableWithKey — namespace=${namespaceId}, bucket=${bucketId}`,
  );

  const bucket = await Bucket.get(bucketId.toString());
  if (!bucket) {
    logger.warn(
      `Block ${blockNumber}: BucketWritableWithKey for bucket ${bucketId} but no row found`,
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

async function handleContributorAdded(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [, bucketArg, contributorArg] = event.event.data;
  const bucketId = Number(bucketArg.toString());
  const subjectId = contributorArg.toString();
  const id = `${bucketId}-${subjectId}`;

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

async function handleContributorRemoved(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [, bucketArg, contributorArg] = event.event.data;
  const bucketId = Number(bucketArg.toString());
  const subjectId = contributorArg.toString();
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

async function handleAdminAdded(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [, bucketArg, adminArg] = event.event.data;
  const bucketId = Number(bucketArg.toString());
  const subjectId = adminArg.toString();
  const id = `${bucketId}-${subjectId}`;

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

async function handleAdminRemoved(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [, bucketArg, adminArg] = event.event.data;
  const bucketId = Number(bucketArg.toString());
  const subjectId = adminArg.toString();
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

async function handleNewMessage(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  // NEW event shape: (namespace_id, bucket_id, message_id, message: MessageDetails, contributor)
  const [, bucketArg, messageIdArg, messageStructArg, contributorArg] =
    event.event.data;
  const bucketId = Number(bucketArg.toString());
  const messageId = Number(messageIdArg.toString());
  const contributor = contributorArg.toString();
  const id = `${bucketId}-${messageId}`;

  try {
    const m = messageStructArg as any;

    const reference = bytesToUtf8(m.reference);
    const tag =
      m.tag && m.tag.isSome ? bytesToUtf8(m.tag.unwrap()) : undefined;
    const description = bytesToUtf8(m.metadata.description);
    const contentType = bytesToUtf8(m.metadata.contentType);
    const contentHash = m.metadata.contentHash.toHex();
    const createdBlock = Number(m.metadata.createdAt.toString());

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
      `Block ${blockNumber}: failed to decode NewMessage ${id} — ${e}`,
    );
  }
}

async function handleMessageDeleted(
  event: SubstrateEvent,
  blockNumber: number,
): Promise<void> {
  const [bucketArg, messageIdArg] = event.event.data;
  const bucketId = Number(bucketArg.toString());
  const messageId = Number(messageIdArg.toString());
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