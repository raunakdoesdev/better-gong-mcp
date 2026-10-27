import { Effect } from "effect";
import { clock } from "./chunk.js";
import { BadInput, embed, useAi, useDb } from "./env.js";

export type Filter = { account?: string; person?: string; from?: string; to?: string };

const day = (s: string, field: string) => {
  const t = Date.parse(s);
  return Number.isNaN(t) ? Effect.fail(new BadInput({ message: `${field} must be an ISO date, got ${s}` })) : Effect.succeed(t);
};

/** SQL condition over `calls c` for a filter. */
export const where = (f: Filter) =>
  Effect.gen(function* () {
    const sql: string[] = [];
    const params: unknown[] = [];
    if (f.account) {
      sql.push("c.account_name LIKE ?");
      params.push(`%${f.account}%`);
    }
    if (f.person) {
      sql.push("EXISTS (SELECT 1 FROM parties p WHERE p.call_id = c.id AND (p.name LIKE ? OR p.email LIKE ?))");
      params.push(`%${f.person}%`, `%${f.person}%`);
    }
    if (f.from) {
      sql.push("c.started_ms >= ?");
      params.push(yield* day(f.from, "from"));
    }
    if (f.to) {
      sql.push("c.started_ms < ?");
      params.push(yield* day(f.to, "to"));
    }
    return { sql: sql.length ? sql.join(" AND ") : "1", params };
  });

/** Words as quoted FTS5 terms joined by OR, so user punctuation can't break the MATCH syntax. */
export const ftsQuery = (q: string) =>
  (q.match(/[\p{L}\p{N}][\p{L}\p{N}'_-]*/gu) ?? []).map((w) => `"${w.replace(/"/g, "")}"`).join(" OR ");

const CALL_COLUMNS = "c.id AS call_id, c.title, c.started, c.account_name AS account, c.url";
const POOL = 50;
// Vectorize filters must stay under 2 KB of JSON; wider scopes post-filter the global top-k.
const MAX_SCOPED = 80;
const RRF_K = 60;

type Hit = { call_id: string; idx: number; start_s: number; text: string };

export const searchCalls = Effect.fn("searchCalls")(function* (query: string, f: Filter, limit: number) {
  const w = yield* where(f);
  const fts = ftsQuery(query);
  const keyword = fts
    ? useDb((db) =>
        db
          .prepare(
            `SELECT ch.call_id, ch.idx, ch.start_s, snippet(chunks_fts, 0, '**', '**', ' … ', 40) AS text
             FROM chunks_fts JOIN chunks ch ON ch.rowid = chunks_fts.rowid JOIN calls c ON c.id = ch.call_id
             WHERE chunks_fts MATCH ? AND ${w.sql} ORDER BY bm25(chunks_fts) LIMIT ${POOL}`,
          )
          .bind(fts, ...w.params)
          .all<Hit>(),
      ).pipe(Effect.map((r) => r.results))
    : Effect.succeed([] as Hit[]);
  const semantic = Effect.gen(function* () {
    const scoped = w.params.length
      ? yield* useDb((db) =>
          db.prepare(`SELECT c.id FROM calls c WHERE ${w.sql} LIMIT ${MAX_SCOPED + 1}`).bind(...w.params).all<{ id: string }>(),
        ).pipe(Effect.map((r) => r.results.map((x) => x.id)))
      : [];
    if (w.params.length && !scoped.length) return [] as Hit[];
    const filter = scoped.length && scoped.length <= MAX_SCOPED ? { call_id: { $in: scoped } } : undefined;
    const [vector] = yield* embed([query]);
    const matches = yield* useAi(({ VECTORS }) => VECTORS.query(vector!, { topK: POOL, ...(filter && { filter }) }));
    const keys = matches.matches.map((m) => m.id);
    if (!keys.length) return [] as Hit[];
    const rows = yield* useDb((db) =>
      db
        .prepare(
          `SELECT ch.call_id, ch.idx, ch.start_s, substr(ch.text, 1, 400) AS text
           FROM chunks ch JOIN calls c ON c.id = ch.call_id
           WHERE ch.call_id || ':' || ch.idx IN (${keys.map(() => "?").join(",")}) AND ${w.sql}`,
        )
        .bind(...keys, ...w.params)
        .all<Hit>(),
    );
    const byKey = new Map(rows.results.map((r) => [`${r.call_id}:${r.idx}`, r]));
    return keys.flatMap((k) => byKey.get(k) ?? []);
  });
  const [kw, sem] = yield* Effect.all([keyword, semantic], { concurrency: 2 });

  const fused = new Map<string, { hit: Hit; score: number }>();
  for (const list of [kw, sem])
    list.forEach((hit, rank) => {
      const key = `${hit.call_id}:${hit.idx}`;
      const prev = fused.get(key);
      fused.set(key, { hit: prev && list === sem ? prev.hit : hit, score: (prev?.score ?? 0) + 1 / (RRF_K + rank + 1) });
    });
  const top = [...fused.values()].sort((a, b) => b.score - a.score).slice(0, limit);
  if (!top.length) return [];

  const ids = [...new Set(top.map((t) => t.hit.call_id))];
  const calls = yield* useDb((db) =>
    db
      .prepare(`SELECT ${CALL_COLUMNS} FROM calls c WHERE c.id IN (${ids.map(() => "?").join(",")})`)
      .bind(...ids)
      .all<{ call_id: string }>(),
  );
  const byId = new Map(calls.results.map((c) => [c.call_id, c]));
  return top.map(({ hit }) => ({ ...byId.get(hit.call_id), at: clock(hit.start_s), start_s: hit.start_s, snippet: hit.text }));
});

export const listCalls = Effect.fn("listCalls")(function* (f: Filter, limit: number, offset: number) {
  const w = yield* where(f);
  const rows = yield* useDb((db) =>
    db
      .prepare(
        `SELECT ${CALL_COLUMNS}, c.duration_s, c.scope,
           (SELECT group_concat(coalesce(p.name, p.email), ', ') FROM parties p WHERE p.call_id = c.id) AS participants
         FROM calls c WHERE ${w.sql} ORDER BY c.started_ms DESC LIMIT ? OFFSET ?`,
      )
      .bind(...w.params, limit, offset)
      .all(),
  );
  return rows.results;
});

const JSON_COLUMNS = ["key_points", "highlights", "outline", "topics", "trackers", "call_outcome"] as const;

export const getCall = Effect.fn("getCall")(function* (id: string) {
  const [call, parties, account] = yield* useDb((db) =>
    db.batch([
      db.prepare("SELECT * FROM calls WHERE id = ?").bind(id),
      db.prepare("SELECT name, email, title, affiliation FROM parties WHERE call_id = ?").bind(id),
      db.prepare("SELECT a.* FROM accounts a JOIN calls c ON c.account_id = a.id WHERE c.id = ?").bind(id),
    ]),
  );
  const row = call?.results[0] as Record<string, unknown> | undefined;
  if (!row) return yield* Effect.fail(new BadInput({ message: `no call ${id}` }));
  for (const k of JSON_COLUMNS) row[k] = typeof row[k] === "string" ? JSON.parse(row[k]) : row[k];
  const acct = account?.results[0] as Record<string, unknown> | undefined;
  if (acct && typeof acct.fields === "string") acct.fields = JSON.parse(acct.fields);
  return { ...row, parties: parties?.results ?? [], account: acct ?? null };
});

export const getTranscript = Effect.fn("getTranscript")(function* (id: string, fromS?: number, toS?: number) {
  const rows = yield* useDb((db) =>
    db
      .prepare(
        `SELECT c.title, c.started, c.url, ch.text FROM calls c LEFT JOIN chunks ch ON ch.call_id = c.id AND ch.end_s >= ? AND ch.start_s <= ?
         WHERE c.id = ? ORDER BY ch.idx`,
      )
      .bind(fromS ?? 0, toS ?? 1e9, id)
      .all<{ title: string; started: string; url: string; text: string | null }>(),
  );
  const first = rows.results[0];
  if (!first) return yield* Effect.fail(new BadInput({ message: `no call ${id}` }));
  const text = rows.results.flatMap((r) => (r.text ? [r.text] : [])).join("\n");
  return { call_id: id, title: first.title, started: first.started, url: first.url, transcript: text || "(no transcript)" };
});

const WRITE = /\b(insert|update|delete|replace|drop|alter|create|attach|detach|pragma|vacuum|reindex)\b/i;

export const readOnlySql = (sql: string) =>
  /^\s*(select|with)\b/i.test(sql) && !WRITE.test(sql) && !/;\s*\S/.test(sql);

export const runSql = Effect.fn("runSql")(function* (sql: string) {
  if (!readOnlySql(sql)) return yield* Effect.fail(new BadInput({ message: "only a single SELECT/WITH statement is allowed" }));
  const rows = yield* useDb((db) => db.prepare(sql).all()).pipe(
    Effect.mapError((e) => new BadInput({ message: e.cause instanceof Error ? e.cause.message : String(e.cause) })),
  );
  return { rows: rows.results.slice(0, 500), truncated: rows.results.length > 500 };
});
