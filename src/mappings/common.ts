import { request } from "https";
import { hexToU8a, isHex } from "@polkadot/util";
import { encodeAddress, decodeAddress, cryptoWaitReady } from "@polkadot/util-crypto";
import { URL } from "url";

type RecordLike = Record<string, unknown>;
export interface OptionLike {
  isSome: boolean;
  unwrap: () => unknown;
}

function isRecord(value: unknown): value is RecordLike {
  return typeof value === "object" && value !== null;
}

function hasToUtf8(value: unknown): value is RecordLike & {
  toUtf8: () => unknown;
} {
  return isRecord(value) && typeof value.toUtf8 === "function";
}

function hasToHex(value: unknown): value is RecordLike & {
  toHex: () => unknown;
} {
  return isRecord(value) && typeof value.toHex === "function";
}

function hasToNumber(value: unknown): value is RecordLike & {
  toNumber: () => unknown;
} {
  return isRecord(value) && typeof value.toNumber === "function";
}

function isCustomStringifiable(value: unknown): boolean {
  if (!isRecord(value) || typeof value.toString !== "function") return false;
  return value.toString !== Object.prototype.toString;
}

function hexToUtf8(value: string): string | undefined {
  if (!/^0x[0-9a-f]*$/i.test(value)) return undefined;
  return Buffer.from(value.slice(2), "hex").toString("utf8");
}

export function getNumber(value: unknown): number | undefined {
  return toNumber(value);
}

export function asRecord(value: unknown): RecordLike | undefined {
  return isRecord(value) ? value : undefined;
}

export function asOption(value: unknown): OptionLike | undefined {
  if (!isRecord(value)) return undefined;
  const isSome = value.isSome;
  const unwrap = value.unwrap;
  if (typeof isSome === "boolean" && typeof unwrap === "function") {
    return { isSome, unwrap: (unwrap as () => unknown).bind(value) };
  }
  return undefined;
}

export function asStorageValue(value: unknown): OptionLike {
  return asOption(value) ?? { isSome: true, unwrap: () => value };
}

export function getStorageKeyArgs(storageKey: unknown): unknown[] | undefined {
  const record = asRecord(storageKey);
  const args = record?.args;
  return Array.isArray(args) ? args : undefined;
}

export function toUtf8String(value: unknown): string {
  if (typeof value === "string") return hexToUtf8(value) ?? value;
  if (hasToUtf8(value)) {
    const toUtf8 = value.toUtf8;
    try {
      return String(toUtf8.call(value));
    } catch {
      return toHexString(value) ?? toStringValue(value) ?? "";
    }
  }
  return String(value);
}

export function toHexString(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (hasToHex(value)) {
    const toHex = value.toHex;
    return String(toHex.call(value));
  }
  return undefined;
}

export function toJsonValue(value: unknown): unknown {
  const record = asRecord(value);
  const toJson = record?.toJSON;
  if (typeof toJson === "function") {
    return (toJson as () => unknown).call(record);
  }
  return value;
}

export function toStringValue(value: unknown): string | undefined {
  if (value == null) return undefined;
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return String(value);
  }

  const record = asRecord(value);
  if (
    typeof record?.toString === "function" &&
    record.toString !== Object.prototype.toString
  ) {
    return String(record.toString.call(value));
  }

  return undefined;
}

export function toNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (hasToNumber(value)) {
    const parsed = value.toNumber.call(value);
    return typeof parsed === "number" && Number.isFinite(parsed)
      ? parsed
      : undefined;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed.length === 0) return undefined;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  if (isCustomStringifiable(value)) return toNumber(String(value));
  return undefined;
}

export function getString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

export function getBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

export function getStringArray(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) return undefined;
  const strings = value.filter((entry): entry is string => typeof entry === "string");
  return strings.length > 0 ? strings : undefined;
}

export function parseJson(value: string): unknown {
  return JSON.parse(value) as unknown;
}

export function formatError(value: unknown): string {
  return value instanceof Error ? value.message : String(value);
}

// Decodes a Bytes codec value to a UTF-8 string.
export function bytesToUtf8(codec: unknown): string {
  return toUtf8String(codec);
}

// Returns the hex representation of any codec value.
export function bytesToHex(codec: unknown): string {
  return toHexString(codec) ?? String(codec);
}

const IPFS_GATEWAY = "https://aquamarine-legal-boa-846.mypinata.cloud/ipfs/";

// Plain https GET — fetch/axios don't work in the SubQuery VM sandbox.
// Requires the indexer to be started with --unsafe.
function httpsGetText(url: string, timeoutMs = 10_000): Promise<string> {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = request(
      {
        hostname: u.hostname,
        path: u.pathname + u.search,
        method: "GET",
      },
      (res) => {
        if (
          !res.statusCode ||
          res.statusCode < 200 ||
          res.statusCode >= 300
        ) {
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => resolve(data));
      },
    );
    req.on("error", reject);
    req.setTimeout(timeoutMs, () => req.destroy(new Error("timeout")));
    req.end();
  });
}

// Fetches the IPFS content at the given CID and returns it as text.
// Returns undefined on any failure (no retry).
export async function fetchIpfsText(cid: string): Promise<string | undefined> {
  if (!cid?.startsWith("baf")) return undefined;
  try {
    const text = await httpsGetText(`${IPFS_GATEWAY}${cid}`);
    logger.info(`IPFS fetch ${cid} -> ${text.length} bytes`);
    return text;
  } catch (e) {
    logger.warn(`IPFS fetch error ${cid}: ${formatError(e)}`);
    return undefined;
  }
}

let _cryptoReady: Promise<boolean> | null = null;
async function ensureCryptoReady(): Promise<boolean> {
  _cryptoReady ??= cryptoWaitReady();
  return _cryptoReady;
}

// Convert various codec representations (hex, bytes, SS58) to an SS58 address string.
// Returns undefined when conversion fails.
export async function toSs58(value: unknown, prefix = 0): Promise<string | undefined> {
  if (value == null) return undefined;

  // Prefer direct hex representations first.
  const hex = toHexString(value);
  try {
    await ensureCryptoReady();
    if (typeof hex === "string" && hex.length > 0) {
      try {
        return encodeAddress(hexToU8a(hex), prefix);
      } catch {
        // fallthrough to other strategies
      }
    }

    // Try treating the value as a string (might already be SS58 or a plain hex string)
    const s = toStringValue(value) ?? toUtf8String(value);
    if (typeof s === "string") {
      try {
        if (isHex(s)) return encodeAddress(hexToU8a(s), prefix);
        // If it's already an SS58 address, decode and re-encode with requested prefix
        const pub = decodeAddress(s);
        return encodeAddress(pub, prefix);
      } catch {
        // ignore and fallthrough
      }
    }
  } catch {
    // crypto not available or failed — return undefined
  }
  return undefined;
}
