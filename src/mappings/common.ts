import { Bytes } from "@polkadot/types";
import { request } from "https";
import { URL } from "url";

// We accept `any` here to side-step the duplicate-Codec-type issue caused by
// having two @polkadot/util versions in the dep tree. At runtime each event arg
// IS a Bytes-compatible instance — the cast is safe.
export function bytesToUtf8(codec: any): string {
  return (codec as unknown as Bytes).toUtf8();
}

export function bytesToHex(codec: any): string {
  return codec.toHex();
}

const IPFS_GATEWAY = "https://aquamarine-legal-boa-846.mypinata.cloud/ipfs/";

// Tiny https GET wrapper — avoids both `fetch` (not in VM sandbox) and
// `axios` (no adapter detected in sandbox). Node's `https` module works.
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
