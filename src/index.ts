import { Effect } from "effect";
import { AUTH_SERVER_PATH, authenticate, denied, MCP_PATH, PROTECTED_RESOURCE_PATH, resourceMetadata } from "./auth.js";
import { Bindings, type Env, type SyncMessage } from "./env.js";
import { handle, reconcile } from "./ingest.js";
import { handleMcp } from "./mcp.js";

const provide = <A, E>(env: Env) => (eff: Effect.Effect<A, E, Bindings>) => eff.pipe(Effect.provideService(Bindings, env));

const keyOk = async (given: string | null, expected: string) => {
  if (!given || !expected) return false;
  const a = new TextEncoder().encode(given);
  const b = new TextEncoder().encode(expected);
  return a.byteLength === b.byteLength && crypto.subtle.timingSafeEqual(a, b);
};

/** Gong "Fire webhook" automation rule body: the extensive call record; `isTest` for the rule's test button. */
const webhookCallId = (body: unknown) => {
  const b = body as { callData?: { metaData?: { id?: string } } } | null;
  return b?.callData?.metaData?.id;
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";

    if (path === PROTECTED_RESOURCE_PATH || path === `${PROTECTED_RESOURCE_PATH}${MCP_PATH}`)
      return Response.json(resourceMetadata(url.origin, env.OAUTH_AUTH_SERVER));

    if (path.startsWith(AUTH_SERVER_PATH))
      return Response.redirect(`${env.OAUTH_AUTH_SERVER.replace(/\/+$/, "")}${AUTH_SERVER_PATH}`, 302);

    if (path === MCP_PATH) {
      return Effect.runPromise(
        authenticate(request).pipe(
          Effect.matchEffect({
            onFailure: (e) => Effect.succeed(denied(url.origin, e)),
            onSuccess: (person) => Effect.promise(() => handleMcp(request, env, person)),
          }),
          provide(env),
        ),
      );
    }

    if (path.startsWith("/gong/") && request.method === "POST") {
      if (!(await keyOk(url.searchParams.get("key"), env.SYNC_KEY))) return new Response("forbidden", { status: 403 });
      if (path === "/gong/webhook") {
        const id = webhookCallId(await request.json().catch(() => null));
        if (id) await env.SYNC.send({ kind: "ingest", ids: [id] });
        return Response.json({ queued: id ?? null });
      }
      if (path === "/gong/sync") {
        const from = url.searchParams.get("from") ?? "2015-01-01T00:00:00Z";
        await env.SYNC.send({ kind: "list", from, force: url.searchParams.get("force") === "1" });
        return Response.json({ queued: { from } });
      }
    }

    if (path === "/")
      return new Response(`Reducto Gong calls MCP. Connect your MCP client to ${url.origin}${MCP_PATH} and sign in with your Reducto account.\n`);
    return new Response("not found", { status: 404 });
  },

  async queue(batch: MessageBatch<SyncMessage>, env: Env): Promise<void> {
    for (const msg of batch.messages) {
      await Effect.runPromise(
        handle(msg.body).pipe(
          Effect.matchEffect({
            onSuccess: () => Effect.sync(() => msg.ack()),
            onFailure: (e) =>
              Effect.logError(`sync ${msg.body.kind} failed`, e).pipe(
                Effect.andThen(Effect.sync(() => msg.retry({ delaySeconds: e._tag === "GongRateLimited" ? e.seconds : 60 }))),
              ),
          }),
          provide(env),
        ),
      );
    }
  },

  async scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(Effect.runPromise(reconcile.pipe(provide(env))));
  },
} satisfies ExportedHandler<Env, SyncMessage>;
