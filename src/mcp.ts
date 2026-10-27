import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { Effect, Schema } from "effect";
import type { Person } from "./auth.js";
import { AiError, BadInput, Bindings, DbError, type Env } from "./env.js";
import { getCall, getTranscript, listCalls, runSql, searchCalls } from "./search.js";

type Failure = BadInput | DbError | AiError;
type Tool = {
  name: string;
  description: string;
  inputSchema: { type: "object"; [k: string]: unknown };
  call: (args: unknown) => Effect.Effect<unknown, Failure, Bindings>;
};

const tool = <A>(
  name: string,
  description: string,
  params: Schema.Codec<A, unknown>,
  run: (a: A) => Effect.Effect<unknown, Failure, Bindings>,
): Tool => {
  const doc = Schema.toJsonSchemaDocument(params);
  const defs = Object.keys(doc.definitions).length ? { $defs: doc.definitions } : {};
  return {
    name,
    description,
    inputSchema: { ...(doc.schema as object), ...defs, type: "object" },
    call: (args) =>
      Schema.decodeUnknownEffect(params)(args ?? {}).pipe(
        Effect.mapError((e) => new BadInput({ message: e.message })),
        Effect.flatMap(run),
      ),
  };
};

const text = (description: string) => Schema.String.annotate({ description });
const num = (description: string) => Schema.Number.annotate({ description });
const filter = {
  account: Schema.optional(text("Customer/company name, substring match (e.g. 'Acme')")),
  person: Schema.optional(text("Participant name or email, substring match; Reducto employees or customers")),
  from: Schema.optional(text("Only calls that started at or after this ISO date, e.g. 2026-01-01")),
  to: Schema.optional(text("Only calls that started before this ISO date")),
};
const clamp = (n: number | undefined, dflt: number, max: number) => Math.max(1, Math.min(max, Math.floor(n ?? dflt)));

const SCHEMA = `Tables (SQLite/D1):
calls(id, title, url, started ISO, started_ms, duration_s, scope 'External'|'Internal', direction, system, media, language,
  account_id, account_name, brief, key_points JSON, highlights JSON, outline JSON, topics JSON, trackers JSON, call_outcome JSON, synced_at)
parties(call_id, speaker_id, name, email, title, affiliation 'Internal'|'External'|'Unknown', user_id)
accounts(id Salesforce Account id, name, website, industry, segment, employees, description, fields JSON of all Salesforce fields)
chunks(call_id, idx, start_s, end_s, text) -- transcript, ~2000 chars each, lines "[mm:ss] Speaker: words"
chunks_fts(text) -- FTS5 over chunks.text; join with chunks.rowid = chunks_fts.rowid`;

const TOOLS: Tool[] = [
  tool(
    "search_calls",
    "Hybrid keyword + semantic search over every Gong call transcript. Returns the best matching transcript passages with call id, title, account, date and timestamp. Use get_transcript to read around a hit.",
    Schema.Struct({ query: text("What to look for: words, names, or a question"), ...filter, limit: Schema.optional(num("Max hits, default 10, max 50")) }),
    ({ query, limit, ...f }) => searchCalls(query, f, clamp(limit, 10, 50)),
  ),
  tool(
    "list_calls",
    "List Gong calls, newest first, optionally filtered by account, participant and date. Returns id, title, start time, duration, account and participants.",
    Schema.Struct({ ...filter, limit: Schema.optional(num("Default 50, max 500")), offset: Schema.optional(num("Rows to skip")) }),
    ({ limit, offset, ...f }) => listCalls(f, clamp(limit, 50, 500), Math.max(0, Math.floor(offset ?? 0))),
  ),
  tool(
    "get_call",
    "One call's metadata, participants, Salesforce account context and Gong's own summary (brief, key points, highlights, outline, topics, trackers).",
    Schema.Struct({ call_id: text("Gong call id") }),
    ({ call_id }) => getCall(call_id),
  ),
  tool(
    "get_transcript",
    "The full speaker-labelled transcript of a call, one '[mm:ss] Speaker: words' line per turn. Pass from_s/to_s (seconds) to read only part of a long call.",
    Schema.Struct({
      call_id: text("Gong call id"),
      from_s: Schema.optional(num("Start of the window in seconds")),
      to_s: Schema.optional(num("End of the window in seconds")),
    }),
    ({ call_id, from_s, to_s }) =>
      getTranscript(call_id, from_s, to_s).pipe(
        Effect.map((t) => `# ${t.title}\nStarted: ${t.started}\nGong: ${t.url}\n\n${t.transcript}`),
      ),
  ),
  tool(
    "sql",
    `Read-only SQL (one SELECT or WITH statement) over the call index, for counts, joins and anything the other tools don't cover. At most 500 rows.\n${SCHEMA}`,
    Schema.Struct({ query: text("SQLite SELECT statement") }),
    ({ query }) => runSql(query),
  ),
];
const BY_NAME = new Map(TOOLS.map((t) => [t.name, t]));

const INSTRUCTIONS =
  "Reducto's Gong call archive: every recorded customer and internal call, with full transcripts and Salesforce account context. " +
  "Start with search_calls or list_calls, then get_call for the summary and get_transcript for the full conversation.";

const result = (out: unknown) => ({
  content: [{ type: "text" as const, text: typeof out === "string" ? out : JSON.stringify(out, null, 1) }],
});
const failure = (message: string) => ({ isError: true, content: [{ type: "text" as const, text: message }] });

export async function handleMcp(request: Request, env: Env, person: Person): Promise<Response> {
  const server = new Server({ name: "gong-calls", version: "0.1.0" }, { capabilities: { tools: {} }, instructions: INSTRUCTIONS });
  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: TOOLS.map(({ name, description, inputSchema }) => ({ name, description, inputSchema })),
  }));
  server.setRequestHandler(CallToolRequestSchema, async (req) => {
    const t = BY_NAME.get(req.params.name);
    if (!t) return failure(`unknown tool ${req.params.name}`);
    return Effect.runPromise(
      t.call(req.params.arguments).pipe(
        Effect.map(result),
        Effect.tap(() => Effect.log(`${person.email} ${t.name}`)),
        Effect.catch((e) =>
          e._tag === "BadInput"
            ? Effect.succeed(failure(e.message))
            : Effect.logError(`${t.name} failed`, e.cause).pipe(Effect.as(failure(`${t.name} failed (${e._tag}); try again`))),
        ),
        Effect.provideService(Bindings, env),
      ),
    );
  });
  const transport = new WebStandardStreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
  await server.connect(transport);
  return transport.handleRequest(request);
}
