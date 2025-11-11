# Contributing / House Rules

## Editing style
 - Always provide full-file replacements for any file you modify.
 - Batch multi-file edits into a single change whenever practical.
 - Keep changes minimal and focused on the requested scope (no speculative refactors).

## Logging conventions
 - Use SwiftLog (`import Logging`) with stable labels per subsystem:
   - AppMain: process startup/shutdown
   - Config: configuration loading
   - KVStore: database access and migrations
   - Routes: HTTP routing and middleware
 - Use structured metadata for key fields where relevant:
   - host, port
   - db_path
   - max_body_bytes
   - auth (enabled/disabled)
   - request_id
   - path (for routing/auth events)
   - type, key, prefix, limit (for KV operations)
 - Log levels:
   - info: lifecycle milestones
   - warning: recoverable issues
   - error: failures

## Startup milestones (log these)
 - Config loaded (host, port)
 - Database opened (path)
 - Migrations started and completed
 - Routes registered (auth enabled/disabled, max body size)
 - Service initialized and event loop started

## Error responses
 - Standard error JSON schema:
   { "error": "string", "message": "string", "details": { ... } }
 - Include `request_id` in `details` when available.
 - Include contextual keys in `details` where relevant (e.g., `type`, `key`, `prefix`, `limit`).

## Request ID propagation
 - Accept `X-Request-ID` case-insensitively; generate a UUID if missing.
 - Set `X-Request-ID` on responses.
 - Include `request_id` in error `details`.

## Authentication
 - If a bearer token is configured, enforce `Authorization: Bearer <token>`.
 - On failure, return `401 Unauthorized` with `WWW-Authenticate: Bearer` and a standard JSON error.
 - Never log token values; log only `enabled/disabled` state.

## Parameter validation
 - Validate route parameters (e.g., no slashes, newlines, or control characters).
 - Return `400 Bad Request` with a standard JSON error on invalid input.

## Query parsing
 - For HB2 `Request`, parse query items by constructing a dummy absolute URL from `req.uri.path` and using `URLComponents`.

## Response headers
 - JSON: `Content-Type: application/json; charset=utf-8`.
 - CSV downloads: `Content-Type: text/csv; charset=utf-8` and `Content-Disposition: attachment; filename="<key>.csv"`.

## Status codes
 - PUT: `204 No Content` on success.
 - GET: `200 OK` with content or `404 Not Found` with a standard error.
 - HEAD: `200 OK` if exists, else `404 Not Found`.
 - DELETE: `204 No Content` on success; `404 Not Found` if missing.
 - Errors always use the standard JSON schema.

## List limits
 - Clamp list `limit` to a safe range (e.g., 0…1000). Default to a reasonable value (e.g., 100).

## CORS policy
 - Enable CORS with origin-based allow-list.
 - Allow headers: `Content-Type`, `Authorization`.
 - Allow methods: `GET`, `PUT`, `DELETE`, `HEAD`, `OPTIONS`.

## Environment-driven configuration
 - Logging:
   - `LOG_PATH` (default: `./Logs/app.log`, tilde `~` is expanded)
   - `LOG_MAX_SIZE_BYTES` (default: `5242880`)
   - `LOG_MAX_FILES` (default: `5`)
   - `LOG_LEVEL` (default: `info`; accepts aliases: verbose→trace, warn→warning, err→error, crit→critical)
 - Service:
   - `KV_HOST` (default: `0.0.0.0`)
   - `KV_PORT` (default: `8080`)
   - `KV_DB_PATH` (default: `$HOME/wvhdb.sqlite`)
   - `KV_MAX_BODY` (default: `5242880`)
   - `KV_TOKEN` (optional; if set and non-empty, auth is enabled)

## Testing & verification
 - After changes, run the service and verify startup milestone logs contain expected metadata.
 - Confirm route behavior (health checks, CRUD) and that error responses include `request_id` when available.

## Quality Bar (internal notes)
 - Match protocol requirements exactly (signatures and mutability). Implement the full-arity `LogHandler.log` method; avoid deprecated forwarding.
 - Prefer robust patterns over shortcuts: use class-based handlers when stateful; avoid recursive logging from inside handlers.
 - Harden filesystem IO: expand `~`, create directories, handle permission errors, and provide fallbacks.
 - Assume unknown environments: fail gracefully and emit clear diagnostics.
 - Deliver full-file replacements with a brief rationale for risky areas (protocol conformance, concurrency, IO).

## Pull request expectations
 - Describe the intent and list updated files.
 - Call out any behavior changes explicitly.
 - Keep PRs focused. For large changes, split into reviewable steps.
