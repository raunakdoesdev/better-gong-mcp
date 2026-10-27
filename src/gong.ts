import { Data, Effect, Schedule, Schema } from "effect";
import { Bindings } from "./env.js";

export class GongError extends Data.TaggedError("GongError")<{ path: string; status: number; detail: string }> {}
export class GongRateLimited extends Data.TaggedError("GongRateLimited")<{ seconds: number }> {}

const opt = <S extends Schema.Top>(s: S) => Schema.optional(Schema.NullOr(s));

const Records = Schema.Struct({ cursor: opt(Schema.String) });

export const Party = Schema.Struct({
  speakerId: opt(Schema.String),
  name: opt(Schema.String),
  emailAddress: opt(Schema.String),
  title: opt(Schema.String),
  affiliation: opt(Schema.String),
  userId: opt(Schema.String),
});

const CrmObject = Schema.Struct({
  objectType: Schema.String,
  objectId: opt(Schema.String),
  fields: opt(Schema.Array(Schema.Struct({ name: Schema.String, value: Schema.Unknown }))),
});

export const Call = Schema.Struct({
  metaData: Schema.Struct({
    id: Schema.String,
    url: opt(Schema.String),
    title: opt(Schema.String),
    started: opt(Schema.String),
    duration: opt(Schema.Number),
    scope: opt(Schema.String),
    direction: opt(Schema.String),
    system: opt(Schema.String),
    media: opt(Schema.String),
    language: opt(Schema.String),
    isPrivate: opt(Schema.Boolean),
  }),
  parties: opt(Schema.Array(Party)),
  content: opt(
    Schema.Struct({
      brief: opt(Schema.String),
      keyPoints: opt(Schema.Array(Schema.Struct({ text: Schema.String }))),
      highlights: opt(Schema.Unknown),
      outline: opt(Schema.Unknown),
      topics: opt(Schema.Unknown),
      trackers: opt(Schema.Unknown),
      callOutcome: opt(Schema.Unknown),
    }),
  ),
  context: opt(Schema.Array(Schema.Struct({ system: opt(Schema.String), objects: opt(Schema.Array(CrmObject)) }))),
});
export type Call = typeof Call.Type;

export const Monologue = Schema.Struct({
  speakerId: opt(Schema.String),
  sentences: Schema.Array(Schema.Struct({ start: Schema.Number, end: Schema.Number, text: Schema.String })),
});
export type Monologue = typeof Monologue.Type;

const ListPage = Schema.Struct({
  calls: Schema.Array(Schema.Struct({ id: Schema.String, isPrivate: opt(Schema.Boolean) })),
  records: Records,
});
const ExtensivePage = Schema.Struct({ calls: Schema.Array(Call), records: Records });
const TranscriptPage = Schema.Struct({
  callTranscripts: Schema.Array(Schema.Struct({ callId: Schema.String, transcript: Schema.Array(Monologue) })),
  records: Records,
});

/** Calls Gong; `null` when Gong answers 404, which it does for a filter that matches no calls. */
const gong = (path: string, body?: unknown) =>
  Effect.gen(function* () {
    const env = yield* Bindings;
    const res = yield* Effect.tryPromise({
      try: () =>
        fetch(`${env.GONG_API_BASE_URL.replace(/\/+$/, "")}${path}`, {
          method: body === undefined ? "GET" : "POST",
          headers: {
            Authorization: `Basic ${btoa(`${env.GONG_API_ACCESS_KEY}:${env.GONG_API_ACCESS_KEY_SECRET}`)}`,
            "Content-Type": "application/json",
          },
          body: body === undefined ? undefined : JSON.stringify(body),
        }),
      catch: (cause) => new GongError({ path, status: 0, detail: String(cause) }),
    });
    if (res.status === 429) {
      const seconds = Number(res.headers.get("retry-after")) || 30;
      if (seconds > 10) return yield* Effect.fail(new GongRateLimited({ seconds }));
      yield* Effect.sleep(`${seconds} seconds`);
      return yield* Effect.fail(new GongError({ path, status: 429, detail: "rate limited" }));
    }
    if (res.status === 404) return null;
    if (!res.ok) {
      const detail = yield* Effect.promise(() => res.text());
      return yield* Effect.fail(new GongError({ path, status: res.status, detail: detail.slice(0, 300) }));
    }
    return yield* Effect.tryPromise({
      try: () => res.json() as Promise<unknown>,
      catch: (cause) => new GongError({ path, status: res.status, detail: `bad json: ${cause}` }),
    });
  }).pipe(
    Effect.retry({
      times: 6,
      schedule: Schedule.exponential("1 second"),
      while: (e) => e._tag === "GongError" && (e.status === 0 || e.status === 429 || e.status >= 500),
    }),
  );

const decode =
  <A>(schema: Schema.Codec<A, unknown>, path: string) =>
  (u: unknown) =>
    Schema.decodeUnknownEffect(schema)(u).pipe(
      Effect.mapError((e) => new GongError({ path, status: 200, detail: `unexpected shape: ${e.message}` })),
    );

export const listCalls = Effect.fn("gong.listCalls")(function* (from: string, cursor?: string) {
  const q = new URLSearchParams({ fromDateTime: from });
  if (cursor) q.set("cursor", cursor);
  const raw = yield* gong(`/v2/calls?${q}`);
  if (raw === null) return { ids: [] as string[], cursor: undefined };
  const page = yield* decode(ListPage, "/v2/calls")(raw);
  return { ids: page.calls.filter((c) => !c.isPrivate).map((c) => c.id), cursor: page.records.cursor ?? undefined };
});

const paged = <A>(path: string, body: object, schema: Schema.Codec<{ records: { cursor?: string | null } } & A, unknown>) =>
  Effect.gen(function* () {
    const pages: A[] = [];
    let cursor: string | undefined;
    do {
      const raw = yield* gong(path, { ...body, cursor });
      if (raw === null) break;
      const page = yield* decode(schema, path)(raw);
      pages.push(page);
      cursor = page.records.cursor ?? undefined;
    } while (cursor);
    return pages;
  });

export const extensive = Effect.fn("gong.extensive")(function* (ids: readonly string[]) {
  const pages = yield* paged("/v2/calls/extensive", {
    filter: { callIds: ids },
    contentSelector: {
      context: "Extended",
      exposedFields: {
        parties: true,
        content: { brief: true, keyPoints: true, highlights: true, outline: true, topics: true, trackers: true, callOutcome: true },
      },
    },
  }, ExtensivePage);
  return pages.flatMap((p) => p.calls);
});

export const transcripts = Effect.fn("gong.transcripts")(function* (ids: readonly string[]) {
  const pages = yield* paged("/v2/calls/transcript", { filter: { callIds: ids } }, TranscriptPage);
  return new Map(pages.flatMap((p) => p.callTranscripts).map((t) => [t.callId, t.transcript] as const));
});
