/**
 * MCP OAuth (RFC 9728). OAUTH_AUTH_SERVER is the authorization server; it issues
 * PropelAuth-style RS256 JWTs for the org picked at sign-in, and only tokens whose
 * org_member_info.org_id equals ALLOWED_ORG_ID get in.
 */
import { Data, Effect } from "effect";
import { importSPKI, jwtVerify } from "jose";
import { Bindings } from "./env.js";

export const MCP_PATH = "/mcp";
export const PROTECTED_RESOURCE_PATH = "/.well-known/oauth-protected-resource";
export const AUTH_SERVER_PATH = "/.well-known/oauth-authorization-server";
const SCOPE = "mcp";

export class Unauthorized extends Data.TaggedError("Unauthorized")<{ reason: "missing" | "invalid" }> {}
export class Forbidden extends Data.TaggedError("Forbidden")<{ email: string }> {}

export type Person = { email: string; userId?: string };

let cached: { pem: string; key: Promise<CryptoKey> } | undefined;
const verifier = (pem: string) => {
  if (cached?.pem !== pem) cached = { pem, key: importSPKI(pem.replace(/\\n/g, "\n"), "RS256") };
  return cached.key;
};

export const authenticate = (request: Request) =>
  Effect.gen(function* () {
    const env = yield* Bindings;
    const token = request.headers.get("authorization")?.match(/^Bearer\s+(.+)$/i)?.[1];
    if (!token) return yield* Effect.fail(new Unauthorized({ reason: "missing" }));
    const { payload } = yield* Effect.tryPromise({
      try: async () =>
        jwtVerify(token, await verifier(env.OAUTH_VERIFIER_KEY), {
          issuer: env.OAUTH_ISSUER.replace(/\/+$/, ""),
          algorithms: ["RS256"],
        }),
      catch: () => new Unauthorized({ reason: "invalid" }),
    });
    const email = String(payload.email ?? "").toLowerCase();
    if (!email) return yield* Effect.fail(new Unauthorized({ reason: "invalid" }));
    const org = (payload.org_member_info as { org_id?: string } | undefined)?.org_id;
    if (org !== env.ALLOWED_ORG_ID) return yield* Effect.fail(new Forbidden({ email }));
    return { email, userId: payload.sub } satisfies Person;
  });

export const resourceMetadata = (origin: string, authServer: string) => ({
  resource: `${origin}${MCP_PATH}`,
  authorization_servers: [authServer.replace(/\/+$/, "")],
  scopes_supported: [SCOPE],
  bearer_methods_supported: ["header"],
  resource_name: "Reducto Gong calls",
});

export const denied = (origin: string, e: Unauthorized | Forbidden) => {
  if (e._tag === "Forbidden")
    return Response.json(
      { error: `${e.email} is signed in to another organization. Reconnect and choose Reducto AI.` },
      { status: 403 },
    );
  const params = [`scope="${SCOPE}"`, `resource_metadata="${origin}${PROTECTED_RESOURCE_PATH}${MCP_PATH}"`];
  if (e.reason === "invalid") params.unshift('error="invalid_token"');
  return Response.json(
    { error: e.reason === "missing" ? "sign in with your Reducto account" : "invalid or expired token" },
    { status: 401, headers: { "WWW-Authenticate": `Bearer ${params.join(", ")}` } },
  );
};
