import { request } from "https";
import { URL } from "url";

type RecordLike = Record<string, unknown>;
interface OptionLike {
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

export function toUtf8String(value: unknown): string {
  if (typeof value === "string") return value;
  if (hasToUtf8(value)) {
    const toUtf8 = value.toUtf8;
    return String(toUtf8.call(value));
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

export function toNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed.length === 0) return undefined;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
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
