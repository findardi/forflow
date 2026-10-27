# How I work with AI on forflow

forflow is a learning project. The goal is not a finished webhook service as
fast as possible; it is to understand, deeply, how one is built. AI is a tutor,
a reviewer, and a builder of the boring parts. It is not the author of the
parts I am here to learn. `AGENTS.md` turns that into rules; this file explains
why they exist.

## Why agents do not edit unless asked

I write the code; the agent reads it and tells me what is wrong and where to
go next. Unrequested edits, even good ones, take away the moment where I find
and fix the problem myself, and they make it hard to tell which code I
actually understand. So the default answer is feedback in chat, and a file
changes only when I ask for that change.

## Why learning zones

Different parts of the system teach different amounts.

- **Red** (scheduler, delivery, signing, netguard, the claim query) holds the
  hard problems: concurrency, at-least-once delivery, retries, cryptographic
  correctness, SSRF. If an agent writes these, I end up with working code and
  no skill. I write them; the agent explains, hints, tests, and reviews.
- **Yellow** (schema, migrations, domain packages) teaches design judgment.
  Reviewing a plan before code is written is where that judgment grows, so
  the agent proposes and I approve.
- **Green** (HTTP wiring, dashboard, config, deploy, CI) is plumbing I already
  know. Letting the agent build it keeps momentum without costing learning.

Hints escalate in steps (concept, then pseudocode, then a fragment of at most
10 lines) because struggling a little before seeing the answer is what makes
it stick. `red-zone override` exists for when I am stuck or short on time; the
agent then lists what it wrote so I can rewrite it later.

## Why reference implementations

Webhook delivery is a solved problem in production, and the fastest way to
learn good design is to study how mature systems made their choices:

- **Svix**: wrote the Standard Webhooks spec, so it is the authority on
  signatures, headers, and secret rotation.
- **Convoy**: written in Go, the closest to my stack.
- **Hook0**: built on PostgreSQL, useful for schema design.
- **Hookdeck Outpost**: written in Go, useful for the tenant and destination
  model.
- **Stripe**: the consumer's view, meaning what a good webhook producer feels
  like to integrate with.

The agent must read real sources and compare at least two, because a single
reference, or the model's memory, hides the trade-offs. Most references use
Redis or a broker; translating their ideas to PostgreSQL-only is itself part
of the exercise.

## Why the stack constraints

One small VPS and PostgreSQL as the only queue are deliberate limits.
`SKIP LOCKED`, `LISTEN/NOTIFY`, partitioning, and the transactional outbox are
the things I want to master, and an external broker would hide them. New
dependencies need my approval for the same reason: every library replaces
something I could have learned.

## Why invariants and tests come first

The six delivery invariants in `AGENTS.md` define "correct" for this system.
Agents may write tests even in the red zone, because a good test tells me what
to build without building it for me.

## How a typical session goes

1. Ask a feature question. The agent researches the references and maps them
   to forflow's constraints.
2. Decide the design together (`mode: pair` for yellow).
3. Write red-zone code myself, using `mode: tutor` when stuck.
4. Ask for `mode: review` on my diff.
5. Commit with a Conventional Commits message; record the decision in the
   commit body.
