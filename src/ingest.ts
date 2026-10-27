import { Effect } from "effect";
import { chunkTranscript, type Chunk } from "./chunk.js";
import { AiError, Bindings, DbError, embed, useAi, useDb, type SyncMessage } from "./env.js";
import * as Gong from "./gong.js";

const EMBED_BATCH = 50;
const INGEST_BATCH = 20;

type Account = { id: string; name: string | null; fields: Record<string, unknown> };

export function accountOf(call: Gong.Call): Account | undefined {
  for (const ctx of call.context ?? [])
    for (const o of ctx.objects ?? [])
      if (o.objectType === "Account" && o.objectId) {
        const fields = Object.fromEntries((o.fields ?? []).map((f) => [f.name, f.value]));
        return { id: o.objectId, name: typeof fields.Name === "string" ? fields.Name : null, fields };
      }
  return undefined;
}

const json = (v: unknown) => (v === undefined || v === null ? null : JSON.stringify(v));
const str = (v: unknown) => (typeof v === "string" ? v : null);
const int = (v: unknown) => (typeof v === "number" ? Math.round(v) : typeof v === "string" && v ? Number.parseInt(v, 10) || null : null);

const writeCall = (call: Gong.Call, chunks: Chunk[]) =>
  useDb(async (db) => {
    const m = call.metaData;
    const account = accountOf(call);
    const c = call.content;
    const prior = await db.prepare("SELECT count(*) AS n FROM chunks WHERE call_id = ?").bind(m.id).first<{ n: number }>();
    const stmts: D1PreparedStatement[] = [
      db.prepare("DELETE FROM chunks WHERE call_id = ?").bind(m.id),
      db.prepare("DELETE FROM parties WHERE call_id = ?").bind(m.id),
      db
        .prepare(
          `INSERT OR REPLACE INTO calls (id, title, url, started, started_ms, duration_s, scope, direction, system, media, language,
             account_id, account_name, brief, key_points, highlights, outline, topics, trackers, call_outcome, synced_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .bind(
          m.id, m.title ?? null, m.url ?? null, m.started ?? null, m.started ? Date.parse(m.started) : null,
          m.duration ?? null, m.scope ?? null, m.direction ?? null, m.system ?? null, m.media ?? null, m.language ?? null,
          account?.id ?? null, account?.name ?? null, c?.brief ?? null, json(c?.keyPoints?.map((k) => k.text)),
          json(c?.highlights), json(c?.outline), json(c?.topics), json(c?.trackers), json(c?.callOutcome), null,
        ),
      ...(call.parties ?? []).map((p) =>
        db
          .prepare("INSERT INTO parties (call_id, speaker_id, name, email, title, affiliation, user_id) VALUES (?, ?, ?, ?, ?, ?, ?)")
          .bind(m.id, p.speakerId ?? null, p.name ?? null, p.emailAddress?.toLowerCase() ?? null, p.title ?? null, p.affiliation ?? null, p.userId ?? null),
      ),
      ...chunks.map((ch) =>
        db.prepare("INSERT INTO chunks (call_id, idx, start_s, end_s, text) VALUES (?, ?, ?, ?, ?)").bind(m.id, ch.idx, ch.start_s, ch.end_s, ch.text),
      ),
    ];
    if (account) {
      const f = account.fields;
      stmts.push(
        db
          .prepare(
            `INSERT OR REPLACE INTO accounts (id, name, website, industry, segment, employees, description, fields) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          )
          .bind(account.id, account.name, str(f.Website), str(f.Industry), str(f.Segment__c), int(f.NumberOfEmployees), str(f.Description), JSON.stringify(f)),
      );
    }
    await db.batch(stmts);
    return prior?.n ?? 0;
  });

const indexVectors = (call: Gong.Call, chunks: Chunk[], prior: number) =>
  Effect.gen(function* () {
    const id = call.metaData.id;
    const header = [call.metaData.title, accountOf(call)?.name].filter(Boolean).join(" | ");
    for (let i = 0; i < chunks.length; i += EMBED_BATCH) {
      const batch = chunks.slice(i, i + EMBED_BATCH);
      const values = yield* embed(batch.map((c) => `${header}\n${c.text}`));
      yield* useAi(({ VECTORS }) =>
        VECTORS.upsert(batch.map((c, j) => ({ id: `${id}:${c.idx}`, values: values[j]!, metadata: { call_id: id } }))),
      );
    }
    const stale = Array.from({ length: Math.max(0, prior - chunks.length) }, (_, k) => `${id}:${chunks.length + k}`);
    if (stale.length) yield* useAi(({ VECTORS }) => VECTORS.deleteByIds(stale));
  });

export const ingest = Effect.fn("ingest")(function* (ids: readonly string[]) {
  const [calls, transcripts] = yield* Effect.all([Gong.extensive(ids), Gong.transcripts(ids)], { concurrency: 2 });
  yield* Effect.forEach(
    calls.filter((c) => !c.metaData.isPrivate),
    (call) =>
      Effect.gen(function* () {
        const names = new Map((call.parties ?? []).flatMap((p) => (p.speakerId && p.name ? [[p.speakerId, p.name] as const] : [])));
        const chunks = chunkTranscript(transcripts.get(call.metaData.id) ?? [], names);
        const prior = yield* writeCall(call, chunks);
        yield* indexVectors(call, chunks, prior);
        yield* useDb((db) => db.prepare("UPDATE calls SET synced_at = ? WHERE id = ?").bind(Date.now(), call.metaData.id).run());
      }),
    { concurrency: 2, discard: true },
  );
  yield* Effect.log(`ingested ${calls.length} calls`);
});

/** One page of Gong's call list: queue the calls not yet indexed, then the next page. */
export const listPage = Effect.fn("listPage")(function* (msg: Extract<SyncMessage, { kind: "list" }>) {
  const { SYNC } = yield* Bindings;
  const page = yield* Gong.listCalls(msg.from, msg.cursor);
  let ids = page.ids;
  if (!msg.force && ids.length) {
    const known = yield* useDb((db) =>
      db
        .prepare(`SELECT id FROM calls WHERE synced_at IS NOT NULL AND id IN (${ids.map(() => "?").join(",")})`)
        .bind(...ids)
        .all<{ id: string }>(),
    );
    const done = new Set(known.results.map((r) => r.id));
    ids = ids.filter((id) => !done.has(id));
  }
  const batches: SyncMessage[] = [];
  for (let i = 0; i < ids.length; i += INGEST_BATCH) batches.push({ kind: "ingest", ids: ids.slice(i, i + INGEST_BATCH) });
  if (page.cursor) batches.push({ ...msg, cursor: page.cursor });
  if (batches.length) yield* Effect.tryPromise({ try: () => SYNC.sendBatch(batches.map((body) => ({ body }))), catch: (cause) => new DbError({ message: String(cause) }) });
  yield* Effect.log(`list page: ${page.ids.length} calls, ${ids.length} queued, more=${Boolean(page.cursor)}`);
});

export const handle = (
  msg: SyncMessage,
): Effect.Effect<void, Gong.GongError | Gong.GongRateLimited | DbError | AiError, Bindings> =>
  msg.kind === "list" ? listPage(msg) : ingest(msg.ids);

/** Daily safety net for missed webhooks: re-list the last week. */
export const reconcile = Effect.gen(function* () {
  const { SYNC } = yield* Bindings;
  const from = new Date(Date.now() - 7 * 86_400_000).toISOString();
  yield* Effect.tryPromise({ try: () => SYNC.send({ kind: "list", from }), catch: (cause) => new DbError({ message: String(cause) }) });
});
