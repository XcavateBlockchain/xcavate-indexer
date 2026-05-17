import { Bytes } from "@polkadot/types";
import { request } from "https";
import { URL } from "url";

// Decodes a Bytes codec value to a UTF-8 string.
export function bytesToUtf8(codec: any): string {
  return (codec as unknown as Bytes).toUtf8();
}

// Returns the hex representation of any codec value.
export function bytesToHex(codec: any): string {
  return codec.toHex();
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
    logger.info(`IPFS fetch ${cid} → ${text.length} bytes`);
    return text;
  } catch (e: any) {
    logger.warn(`IPFS fetch error ${cid}: ${e?.message ?? e}`);
    return undefined;
  }
}
