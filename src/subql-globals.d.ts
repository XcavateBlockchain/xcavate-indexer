export type SubqlQueryFn = (...args: unknown[]) => Promise<unknown>;

export interface SubqlApi {
  query: Record<string, Record<string, SubqlQueryFn>>;
}

declare global {
  const api: SubqlApi;
  const logger: {
    info: (message: string) => void;
    warn: (message: string) => void;
    error: (message: string) => void;
  };
}
