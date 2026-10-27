import { Context, Data, Effect, Schedule } from "effect";

export type SyncMessage =
  | { kind: "list"; from: string; cursor?: string; force?: boolean }
  | { kind: "ingest"; ids: string[] };

export interface Env {
  DB: D1Database;
  VECTORS: VectorizeIndex;
  AI: Ai;
  SYNC: Queue<SyncMessage>;
  GONG_API_BASE_URL: string;
  GONG_API_ACCESS_KEY: string;
  GONG_API_ACCESS_KEY_SECRET: string;
  SYNC_KEY: string;
  OAUTH_ISSUER: string;
  OAUTH_AUTH_SERVER: string;
  OAUTH_VERIFIER_KEY: string;
  ALLOWED_ORG_ID: string;
}

export class Bindings extends Context.Service<Bindings, Env>()("Bindings") {}

export class DbError extends Data.TaggedError("DbError")<{ message: string }> {}
export class AiError extends Data.TaggedError("AiError")<{ message: string }> {}

const describe = (cause: unknown) => (cause instanceof Error ? cause.message : String(cause));
export class BadInput extends Data.TaggedError("BadInput")<{ message: string }> {}

export const useDb = <A>(f: (db: D1Database) => Promise<A>): Effect.Effect<A, DbError, Bindings> =>
  Effect.gen(function* () {
    const { DB } = yield* Bindings;
    return yield* Effect.tryPromise({ try: () => f(DB), catch: (cause) => new DbError({ message: describe(cause) }) });
  });

export const useAi = <A>(f: (env: Env) => Promise<A>): Effect.Effect<A, AiError, Bindings> =>
  Effect.gen(function* () {
    const env = yield* Bindings;
    return yield* Effect.tryPromise({ try: () => f(env), catch: (cause) => new AiError({ message: describe(cause) }) }).pipe(
      Effect.retry({ schedule: Schedule.exponential("2 seconds"), times: 3 }),
    );
  });

export const EMBED_MODEL = "@cf/baai/bge-m3";

export const embed = (texts: string[]) =>
  useAi(async ({ AI }) => {
    const out = (await AI.run(EMBED_MODEL, { text: texts })) as { data?: number[][] };
    if (!out.data || out.data.length !== texts.length) throw new Error("embedding count mismatch");
    return out.data;
  });
