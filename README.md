# better-gong-mcp

MCP server over a company's Gong call archive, on Cloudflare Workers (TypeScript + Effect). It calls the Gong REST API directly, so the LLM gets full speaker-labelled transcripts, Gong's call summaries and the Salesforce account context.

Sign-in is MCP OAuth (RFC 9728, dynamic client registration) against `OAUTH_AUTH_SERVER`. Only tokens whose `org_member_info.org_id` equals `ALLOWED_ORG_ID` get access; other orgs get 403.

## Tools

- `search_calls`: hybrid keyword (D1 FTS5) and semantic (Vectorize, bge-m3) search over transcripts, with account/person/date filters.
- `list_calls`: calls by account, person or date.
- `get_call`: metadata, parties, Gong brief/key points/outline, Salesforce account.
- `get_transcript`: full speaker-labelled transcript, optionally a time range.
- `sql`: one read-only SELECT against the D1 schema in `migrations/`.

## Sync

- Queue `gong-calls-sync` runs `list` (one Gong page, fan out) and `ingest` (20 calls: extensive + transcripts -> D1 -> embeddings -> Vectorize) messages.
- Gong webhook: `POST /gong/webhook?key=$SYNC_KEY` with Gong's call payload queues that call.
- Daily cron (09:17 UTC) re-lists the last 7 days in case a webhook was missed.
- Manual: `POST /gong/sync?key=$SYNC_KEY[&from=ISO][&force=1]`. Without `force`, calls already indexed are skipped.

## Develop

```
cp wrangler.example.jsonc wrangler.jsonc   # fill in account, D1, domain, OAuth values
npm i
npm run typecheck && npm test
npm run migrate   # D1 migrations, remote
npm run deploy
```

Secrets (`wrangler secret put`): `GONG_API_BASE_URL`, `GONG_API_ACCESS_KEY`, `GONG_API_ACCESS_KEY_SECRET`, `SYNC_KEY`.

Vectorize index `gong-calls` has a metadata index on `call_id`; filtered semantic search depends on it.
