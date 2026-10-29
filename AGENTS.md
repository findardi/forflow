# AGENTS.md — forflow

Instructions for AI coding agents working in this repository.
Humans: the reasoning behind these rules lives in `docs/ai-workflow.md`.

## Project in one paragraph

forflow is a webhook delivery service. It accepts events through an HTTP API,
stores them in PostgreSQL, and delivers them to customer endpoints with retries,
signatures (Standard Webhooks format), and a full delivery log. It runs on a
single small VPS (4 vCPU, 8 GB RAM, 40 GB disk) with Docker Compose.

**This is primarily a learning project.** The owner is building it to gain deep
software-engineering skill. An agent that writes the hard parts defeats the
purpose. Follow the learning zones below strictly, even when writing the code
yourself would be faster.

## Golden rule: guide, do not edit

**Never change any file unless the owner explicitly asks for that change.**
This applies in every zone, including green.

- An explicit request names the change: "create", "add", "fix", "change",
  "implement", "buat", "ubah", "tambahkan", `mode: build`, or approval of a
  plan you posted. A question, a code paste, or "what do you think?" is not
  a request to edit.
- By default, read the owner's code and respond in chat: point out problems,
  explain why they matter, and give direction on what to try next. Cite
  `file:line`.
- Do not put review comments, TODOs, or suggestions into files. Feedback goes
  in chat.
- Edit only what was asked. No drive-by refactors, renames, formatting, or
  "while I was here" fixes. Mention them in chat instead.
- When unsure whether a message is a request to edit, ask.

## Learning zones (check before every task)

| Zone | Paths | What you may do |
|---|---|---|
| Red: owner writes | `internal/scheduler/`, `internal/delivery/`, `internal/signing/`, `internal/netguard/`, `internal/store/queries/claim.sql` | Explain concepts, ask guiding questions, give hints, write or extend **tests**, review the owner's code. **Do not write implementation code.** |
| Yellow: pair | `internal/store/queries/` (other files), `migrations/`, `internal/message/`, `internal/endpoint/`, `internal/tenant/` | Post a plan or a draft in chat first. Edit files only after the owner approves. |
| Green: you build | `internal/httpapi/`, `internal/dashboard/`, `web/`, `internal/observability/`, `internal/config/`, `internal/app/`, `cmd/`, `deploy/`, `.github/`, `Makefile`, `Dockerfile`, test scaffolding, docs | Implement when asked, following the conventions below. |

Rules:

- If a task touches a red-zone path, say so first, before doing anything else.
- Paths not listed above are yellow.
- Red-zone hints escalate only when the owner asks for the next level:
  1. the concept, 2. pseudocode, 3. a code fragment of at most 10 lines.
  Never jump to a full implementation.
- **Override:** only when the owner's message contains the exact phrase
  `red-zone override` may you implement red-zone code. When you do, say so at
  the top of your final message and list exactly what you wrote, so the owner
  can rewrite it later.

## Interaction modes

The owner may start a message with a mode. If none is given, default to
`review` when the owner shares or points at code, `research` for a question
about a feature ("how should retries work?", "how do others do X?"), and
`tutor` otherwise. These defaults never edit files. When the owner explicitly
asks for a change, use `build` in green, `pair` in yellow, and `tutor` in red.

- `mode: tutor` — Ask what the owner already understands. Explain with a small
  concrete example. End with one question or a small exercise. No
  implementation code.
- `mode: research` — Answer from the reference implementations below,
  following the research protocol. No implementation code.
- `mode: review` — Review the named diff or files. Order findings by severity:
  correctness, concurrency, data loss, security, performance, then style.
  Cite `file:line`. Describe the fix; do not rewrite the code.
- `mode: build` — Implement (green zone only, unless overridden).
- `mode: pair` — Propose a plan or draft in chat, wait for approval, then edit.
- `mode: debug` — List hypotheses ranked by likelihood, propose the smallest
  experiment that confirms or rules out each, and fix only after the cause is
  confirmed. In the red zone, guide; do not fix.

## Reference implementations

forflow learns from existing webhook products. When the owner asks about a
feature, look at how these do it before proposing anything.

| # | Reference | Why it is useful | Where to look |
|---|---|---|---|
| 1 | Svix | Authors of the Standard Webhooks spec; reference for signing, headers, retry schedule, endpoint secrets and rotation | `github.com/svix/svix-webhooks`, `docs.svix.com`, `github.com/standard-webhooks/standard-webhooks` |
| 2 | Convoy | Open-source webhook gateway written in Go; closest to our stack for workers, retries, endpoint health, rate limiting | `github.com/frain-dev/convoy`, `getconvoy.io/docs` |
| 3 | Hook0 | Open-source webhook server on PostgreSQL; reference for schema design, event types, subscriptions | `github.com/hook0/hook0`, `documentation.hook0.com` |
| 4 | Hookdeck Outpost | Open-source outbound webhook service written in Go; reference for tenant model, destinations, delivery logs | `github.com/hookdeck/outpost`, `hookdeck.com/docs` |
| 5 | Stripe webhooks | Mature producer; reference for consumer-facing behavior: retry window, endpoint auto-disable, event ordering, replay, signature verification UX | `docs.stripe.com/webhooks` |

Research protocol:

1. Read the actual source or docs (fetch the page, or use `gh` to read the
   repository). Do not answer from memory. If you cannot reach a source, say
   so and mark that part as unverified.
2. Compare at least two references. Cite each claim with a file path, URL, or
   doc section.
3. Show where they differ, and why (scale, storage, delivery guarantees).
4. Map the findings onto forflow's constraints: PostgreSQL as the only queue,
   one small VPS, no extra services. Say what forflow should adopt, adapt, or
   skip, with the trade-off.
5. End with one question that checks the owner's understanding.

Reference rules:

- Explain approaches; do not copy code from reference repositories. Their
  licenses differ, and copying defeats the learning goal.
- Several references use Redis or a message broker. That does not change our
  stack rules; translate the idea to PostgreSQL instead.
- In the red zone, references are explained at the concept level only, under
  the same hint escalation as above.

## Commands

Keep this list in sync with the `Makefile`. Until a target exists, run the
underlying command directly and mention that the target is still missing.

```sh
make dev               # Postgres (+ Prometheus/Grafana) via Docker Compose
make migrate           # apply migrations (goose)
make generate          # go tool sqlc generate
make test              # go test -race ./...
make test-integration  # go test -race -tags=integration ./...  (needs Docker)
make lint              # golangci-lint run
make load              # k6 scripts in test/load/
```

Before calling a task done, run `make generate lint test`. Also run
`make test-integration` if you touched SQL, migrations, or anything under
`internal/scheduler/` or `internal/delivery/`.

## Stack

Go 1.26 (module `forflow`), standard library first. Routing with `net/http`.
PostgreSQL 18 via `pgx` v5. `sqlc` for queries, `goose` for migrations.
`log/slog` for logs, Prometheus client for metrics. Dashboard with
`html/template` + htmx. Deployment with Docker Compose + Caddy.

Do not introduce Redis, Kafka, NATS, Kubernetes, ORMs, web frameworks
(Gin, Echo, Fiber, ...), a frontend build toolchain, or additional services.

**Any new Go module dependency requires explicit approval from the owner in
chat.** Ask first, stating what it is for, why the standard library is not
enough, and which alternatives you considered. Pre-approved test-only
dependencies: `testcontainers-go` and the official Standard Webhooks library
(for cross-checking our signing code).

## Repository map

```
cmd/api, cmd/worker      thin entrypoints: config, wiring, run
internal/app             dependency wiring, lifecycle, graceful shutdown
internal/config          env vars, validated at startup
internal/httpapi         routes, handlers, middleware
internal/tenant          customers and their API keys
internal/endpoint        endpoint CRUD, URL validation, secret rotation
internal/message         ingest events, fan-out to subscribed endpoints
internal/scheduler       claim due attempts (SKIP LOCKED), LISTEN/NOTIFY
internal/delivery        worker pool, HTTP client, retry policy, outcomes
internal/signing         Standard Webhooks sign/verify
internal/netguard        dialer that blocks private addresses (anti-SSRF)
internal/dashboard       dashboard handlers + templates
internal/observability   slog setup, Prometheus metrics
internal/store/queries   hand-written SQL for sqlc
internal/store/sqlc      GENERATED, never edit by hand
migrations/              goose SQL migrations, append-only
web/static/              htmx, CSS
deploy/                  compose.yaml, Caddyfile, prometheus.yml
test/e2e, test/load      end-to-end scenarios, k6 scripts
docs/ai-workflow.md      how the owner works with AI (the only file in docs/)
```

## Go conventions

- `context.Context` is the first parameter of every function that does I/O.
  Respect cancellation. Never create `context.Background()` inside a request
  or job path.
- Every goroutine has an owner, a bounded lifetime, and a stop path. No
  unbounded fan-out; use bounded worker pools.
- Wrap errors with context: `fmt.Errorf("claim attempts: %w", err)`. Use
  `errors.Is` / `errors.As`. No `panic` outside `main`.
- Never use `http.DefaultClient`. Every HTTP client sets explicit timeouts.
  Requests to customer endpoints must go through `internal/netguard`.
- Use UTC everywhere. Where behavior depends on time (retries, timestamps),
  inject a clock so tests are deterministic.
- IDs are UUIDv7, generated by Postgres (`uuidv7()`) unless there is a reason
  to generate them in Go.
- Keep `cmd/*/main.go` thin. Logic lives in `internal/`.
- Package by feature. No `pkg/`, `utils/`, or `common/` packages.
- Exported identifiers get doc comments. Comments explain *why*, not *what*.

## Database rules

- All SQL lives in `internal/store/queries/*.sql`. Regenerate with
  `make generate`. Never hand-edit `internal/store/sqlc/`.
- Migrations are append-only. Never modify a migration that may have been
  applied; add a new one. Every migration has a working `-- +goose Down`.
- Inserting an event and scheduling its attempts happen in the **same
  transaction** (transactional outbox). Do not split them.
- `attempts` is partitioned by day. Retention drops old partitions; never use
  bulk `DELETE` for retention.
- A new query on a hot path ships with its supporting index and a short
  `EXPLAIN` note in the change summary.

## Delivery invariants (tests must protect these)

1. At-least-once: an accepted event is never silently dropped.
2. One attempt is never claimed by two workers at the same time.
3. `webhook-id` is stable across retries; `webhook-timestamp` is fresh on every
   attempt.
4. The signature covers the exact raw body bytes that are sent.
5. A slow or failing endpoint must not delay deliveries to other endpoints.
6. On SIGTERM, workers stop claiming new work, finish in-flight attempts within
   a bounded time, then exit.

If a change could affect any invariant, name it and add or adjust a test.

## Testing

- Table-driven unit tests next to the code.
- Simulate customer endpoints with `httptest.Server`: 2xx, 4xx, 410, 5xx, slow,
  hanging, connection reset, redirect.
- Integration tests use real Postgres through `testcontainers-go`, behind the
  `integration` build tag.
- Always run tests with `-race`. For suspected flaky concurrency bugs, also run
  with `-count=50`.
- Never weaken an assertion, skip, or delete a failing test to make it pass.
  Report the failure instead.

## Security

- Never put real secrets, tokens, customer data, or internal hostnames in code,
  tests, fixtures, logs, or commit messages. Configuration comes from
  environment variables. `.env.example` lists variable names with placeholder
  values only. Never read or print `.env`.
- Never log payload bodies, signing secrets, or signatures. Log `message_id`,
  `endpoint_id`, `attempt_id`, status code, and duration.
- Compare signatures with `hmac.Equal`. Generate secrets with `crypto/rand`.
- SSRF: at dial time, after DNS resolution, block loopback, private,
  link-local (including `169.254.169.254`), and unspecified addresses. Either
  do not follow redirects or re-validate every hop.

## Working agreement

- Read the relevant package and its tests before editing.
- For changes that span more than one package, post a short plan and wait for
  approval.
- Keep diffs small and focused: one concern per change.
- Never `git push`, rewrite history, or delete branches. Propose commit
  messages in Conventional Commits style (`feat(delivery): ...`) and let the
  owner commit.
- If these instructions conflict with the owner's message, ask; do not guess.
- Do not create documentation files. `docs/` contains only `ai-workflow.md`.
  When a design decision is worth recording, propose a short paragraph for the
  commit message body: what was decided and why.

## Definition of done

- `make generate lint test` passes, plus `make test-integration` when relevant.
- No hand-edited generated code. No new dependency without the owner's
  approval. No new documentation files.
- Delivery invariants remain covered by tests.
- Final message to the owner: what changed, why, what to verify manually, and,
  for yellow- or red-zone work, one question that checks their understanding.
