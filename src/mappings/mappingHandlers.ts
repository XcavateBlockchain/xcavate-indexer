import { SubstrateBlock } from "@subql/types";
import { Property, Did, Message } from "../types";

export async function handleBlock(block: SubstrateBlock): Promise<void> {
  const blockNumber = (block as any).block.header.number.toNumber();

  if (!(api.query as any).realEstateNfts?.itemMetadataOf) {
    return;
  }

  let entries: [any, any][];
  try {
    entries = await api.query.realEstateNfts.itemMetadataOf.entries();
  } catch (e) {
    logger.error(
      `Block ${blockNumber}: failed to query RealEstateNfts.ItemMetadataOf — ${e}`,
    );
    return;
  }

  for (const [_key, value] of entries) {
    if (!value || (value as any).isEmpty) continue;

    try {
      const raw = (value as any).toJSON() as { data?: string } | null;
      const hex = raw?.data ?? "";
      if (!hex) continue;

      const jsonStart = hex.indexOf("7b22");
      if (jsonStart === -1) continue;

      const jsonStr = Buffer.from(hex.slice(jsonStart), "hex").toString("utf8");
      const data = JSON.parse(jsonStr);

      const id: string = data.id ?? _key.toString();

      const property = Property.create({
        id,
        propertyName: data.propertyName ?? undefined,
        propertyType: data.propertyType ?? undefined,
        status: data.status ?? undefined,
        propertyPrice: data.financials?.propertyPrice ?? undefined,
        pricePerToken: data.financials?.pricePerToken ?? undefined,
        numberOfTokens: data.financials?.numberOfTokens ?? undefined,
        estimatedRentalIncome:
          data.financials?.estimatedRentalIncome ?? undefined,
        numberOfBedrooms: data.attributes?.numberOfBedrooms ?? undefined,
        numberOfBathrooms: data.attributes?.numberOfBathrooms ?? undefined,
        area: data.attributes?.area ?? undefined,
        street: data.address?.street ?? undefined,
        townCity: data.address?.townCity ?? undefined,
        postCode: data.address?.postCode ?? undefined,
        developerAddress: data.developerAddress ?? undefined,
        createdAt: data.createdAt ?? undefined,
      });

      await property.save();
      logger.info(`Block ${blockNumber}: saved property ${id}`);
    } catch (e) {
      logger.error(`Block ${blockNumber}: failed to decode property — ${e}`);
    }
  }

  // Index Messages
  if ((api.query as any).buckets?.messages) {
    let msgEntries: [any, any][];
    try {
      msgEntries = await (api.query as any).buckets.messages.entries();
    } catch (e) {
      logger.warn(`Block ${blockNumber}: failed to query buckets.messages — ${e}`);
      msgEntries = [];
    }

    for (const [key, value] of msgEntries) {
      if (!value || (value as any).isEmpty) continue;

      try {
        const bucketId = key.args[0].toNumber();
        const messageId = key.args[1].toNumber();
        const id = `${bucketId}-${messageId}`;

        // TODO: Optimize — currently re-reads all storage every 100 blocks.
        // Consider skipping already-indexed messages to avoid redundant IPFS fetches.
        // Could use: const existing = await Message.get(id); if (existing) continue;

        const data = (value as any).toJSON();

        // Decode hex strings to UTF-8
        const decodeHex = (hex: string | null): string | null => {
          if (!hex || hex.length < 4) return null;
          const clean = hex.startsWith("0x") ? hex.slice(2) : hex;
          return Buffer.from(clean, "hex").toString("utf8");
        };

        const reference = decodeHex(data?.reference);
        const tag = decodeHex(data?.tag);
        const description = decodeHex(data?.metadata?.description);
        const contentType = decodeHex(data?.metadata?.contentType);

        // Fetch IPFS content if reference is a CID and content is plain text
        let ipfsContent: string | null = null;
        if (reference?.startsWith("baf") && contentType?.startsWith("text/plain")) {
          try {
            const url = `https://aquamarine-legal-boa-846.mypinata.cloud/ipfs/${reference}`;
            const response = await fetch(url);
            if (response.ok) {
              ipfsContent = await response.text();
            }
          } catch (e) {
            logger.warn(`Block ${blockNumber}: failed to fetch IPFS content for message ${id} — ${e}`);
          }
        }

        const msg = Message.create({
          id,
          bucketId,
          messageId,
          reference: reference ?? undefined,
          tag: tag ?? undefined,
          description: description ?? undefined,
          createdAt: data?.metadata?.createdAt ?? undefined,
          contentType: contentType ?? undefined,
          contentHash: data?.metadata?.contentHash ?? undefined,
          ipfsContent: ipfsContent ?? undefined,
        });

        await msg.save();
        logger.info(`Block ${blockNumber}: saved message ${id}`);
      } catch (e) {
        logger.error(`Block ${blockNumber}: failed to decode message — ${e}`);
      }
    }
  }

  // Index DIDs
  if (!(api.query as any).did?.did) {
    return;
  }

  let didEntries: [any, any][];
  try {
    didEntries = await (api.query as any).did.did.entries();
  } catch (e) {
    logger.warn(`Block ${blockNumber}: failed to query did.did — ${e}`);
    return;
  }

  for (const [key, value] of didEntries) {
    if (!value || (value as any).isEmpty) continue;

    try {
      const id: string = key.args[0].toString(); // SS58 adresa
      const data = (value as any).toJSON();

      // Resolve a publicKeys hash to the actual key value (Sr25519 or X25519)
      const resolveKey = (hash: any): string | null => {
        if (!hash || !data?.publicKeys) return null;
        const entry = data.publicKeys[hash];
        return (
          entry?.key?.publicVerificationKey?.sr25519 ??
          entry?.key?.PublicVerificationKey?.Sr25519 ??
          entry?.key?.publicEncryptionKey?.x25519 ??
          entry?.key?.PublicEncryptionKey?.X25519 ??
          null
        );
      };

      const authenticationKey = resolveKey(data?.authenticationKey);

      // keyAgreementKeys is an array of hashes — resolve each to its X25519 value
      const resolvedKeyAgreementKeys: string[] = [];
      if (Array.isArray(data?.keyAgreementKeys)) {
        for (const keyHash of data.keyAgreementKeys) {
          const resolved = resolveKey(keyHash);
          if (resolved) resolvedKeyAgreementKeys.push(resolved);
        }
      }

      const did = Did.create({
        id,
        authenticationKey: authenticationKey ?? undefined,
        keyAgreementKeys:
          resolvedKeyAgreementKeys.length > 0
            ? JSON.stringify(resolvedKeyAgreementKeys)
            : undefined,
        depositOwner: data?.deposit?.owner ?? undefined,
        depositAmount: data?.deposit?.amount ?? undefined,
        blockNumber: data?.blockNumber ?? undefined,
      });

      await did.save();
      logger.info(`Block ${blockNumber}: saved DID ${id}`);
    } catch (e) {
      logger.error(`Block ${blockNumber}: failed to decode DID — ${e}`);
    }
  }
}
