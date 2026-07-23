# The Cara Product Doctrine

**Version 1.0 — 2026-07-23.** This document is LAW for every product built on
Cara. It travels with the framework: cloning `cara` into a product delivers the
doctrine with it. A product's `CLAUDE.md` is its atlas (ports, quirks, domain
registry); *this* file is the invariant architecture. Where the two disagree,
this file wins and the atlas is a bug.

**Reading contract — humans and AI agents.** Every rule here is written as
MUST / NEVER and carries a one-line *why*. Agents: you do not get to weigh
these rules against your preferences; a change that violates a rule is wrong
even if it works. Most rules are enforced by guard tests (§11) — if you find a
rule without a guard, adding the guard is part of any work that touches the
rule's area. Deviations require a doctrine amendment (§13), never a local
exception.

---

## 1. Topology — what a product IS

A product is **two deployables + a dev-only kernel + surfaces**:

```
<product>/code/            ← workspace root (NOT a git repo)
  api/                     ← deployable 1: HTTP + WebSocket intake (own repo)
  services/                ← deployable 2: workers, scheduler, queue control-plane (own repo)
  commons/                 ← DEV-ONLY shared kernel (own repo) — never ships (§2)
  <frontend>/              ← storefront / dashboard / admin / extension (own repos)
  docs/                    ← product documentation (own repo, machine-verified §10)
  infrastructure/          ← compose + terraform + monitoring (own repo)
```

- Each child is an **independent nested git repository**. Stage only files that
  belong to the repo you are committing to. `git add -A` at a workspace root is
  forbidden. *(Why: cross-repo bleed is unrecoverable at review time.)*
- The two deployables share **one PostgreSQL database** and speak to each other
  **only through the queue** (RabbitMQ) and the shared tables. Direct imports
  between `api/` and `services/` are physically impossible and must stay so.
  *(Why: this is a modular monolith with CQRS-ish separation — the cheapest
  architecture that scales to this product class. Microservices are refused.)*
- `api` owns intake: validation, authorization, synchronous reads, dispatching
  work. `services` owns execution: pulls, pushes, syncs, sweeps, schedules.
  A queue consumer MUST be able to import the class of every job dispatched to
  it; queue ownership lives in one topology SSOT (§8).
- Cara itself is a dependency, cloned once per product under `commons/cara`
  and exposed to each deployable by symlink. Framework fixes are made in ONE
  canonical clone, pushed, and fast-forwarded everywhere else. Products never
  fork the framework.

## 2. The Kernel — `commons/` is dev-only, and `app.*` is the only runtime name

The kernel exists for one reason: **shared truth must have one source in
development**. It has exactly four packages plus the framework clone:

```
commons/
  models/       every shared table, and NOTHING else. The heart.
  contracts/    what both processes must PARSE THE SAME WAY: queue envelope
                jobs, capability catalog, queue topology, cross-process
                vocabularies. ZERO business logic. ZERO database access.
                ZERO marketplace/vendor names (§4).
  gates/        single-door write logic + invariant keepers (entitlements,
                permissions, capability floors, quantity/fulfillment policy)
                and THEIR persistence (gates/persistence/). If two processes
                could each corrupt an invariant, its keeper lives here.
  shared/       business helpers with PROVEN ≥2-tree consumers, organized in
                themes (domain/ catalog/ events/ runtime/ infra/). Membership
                is counted by a guard; a module that falls to one consumer
                tree is evicted to that tree.
  cara/         the framework clone (a dependency, not kernel content).
```

**Membership tests (apply in order):** is it a table? → `models`. Must both
processes parse it identically? → `contracts`. Does it guard an invariant both
processes could violate? → `gates`. Do ≥2 trees provably import it? →
`shared`. Otherwise it does NOT belong in the kernel — put it in the app tree
that owns the use-case. *(Why: "commons" without membership criteria grows
into a junk drawer; these four names ARE the criteria.)*

**Direction rules (guard-enforced):** `models` imports nothing from the
kernel. `contracts` may import `models` for typing only. `gates` and `shared`
may import `models` and `contracts`. The kernel NEVER imports an app tree.
App trees import the kernel only through the barrels below.

**The single runtime namespace.** Application code — dev and prod alike —
imports kernel code as **`app.*`**: `from app.models import Listing`,
`from app.gates import ListingWriter`, `from app.contracts import CAP_ADS`,
`from app.shared.catalog import ...`. In development, `app/models/`,
`app/contracts/`, `app/gates/`, `app/shared/` are thin **barrels** that
re-export the commons package. *(Why: one import name forever; the file's
origin is a build detail, not a code concern.)*

**Production: the kernel does not ship.** The production build runs
`craft build:vendor-commons`, which for EVERY kernel package copies the
sources into the deployable's `app/<package>/`, rewrites the barrels to
relative imports, rewrites every residual `commons.*` reference to `app.*`,
materializes the cara clone as the image's framework, and deletes `commons/`
from the image. The runtime image contains `app.*`, `cara.*`, `packages.*` —
nothing else. The vendor step auto-discovers kernel subpackages (a hardcoded
list once shipped a broken image) and is covered by framework regression
tests plus a per-product **sandbox dry-run proof**: copy the tree, vendor it,
compile it, import it. A release without a green dry-run is not a release.

## 3. Deployable anatomy — layers, the mirror rule, the domain registry

Every deployable uses the SAME layer names:

```
api/app/                                services/app/
  controllers/<domain>/                   jobs/<domain>/          ← the worker's "controllers"
  requests/<domain>/                      services/<domain>/
  resources/<domain>/                     repositories/<domain>/
  services/<domain>/                      contracts/ (local DI contracts)
  repositories/<domain>/                  commands/<area>/
  policies/  events/  listeners/          providers/
  jobs/  providers/                       support/<theme>/
  support/<theme>/                        models|contracts|gates|shared  (kernel barrels)
  models|contracts|gates|shared (barrels)
services/packages/<plugin>/             ← vertical slices (§4)
services/config/providers.py            ← the composition root
```

- **The mirror rule.** Within a deployable, every layer is partitioned by the
  SAME domain names. If `controllers/orders/` exists, order jobs live in
  `jobs/orders/`, order queries in `repositories/orders/`. No loose files at
  a layer root (only `__init__.py` and at most a documented base class).
  *(Why: a domain's full footprint must be findable by name alone.)*
- **The domain registry.** Each deployable declares its domains in ONE file,
  `app/domains.py`: a dict of `name -> one-line charter`. A guard fails any
  layer folder not in the registry and any registered domain with no members.
  Adding a domain = writing its charter — a deliberate act.
- **Universal domains** exist in every product under the same names: `user`
  (identity/account), `platform` (admin/ops), `billing` (if monetized),
  `shared` (cross-domain plumbing — business logic forbidden there, guarded).
  Product domains beyond these are free but must pass the domain tests:
  nameable aggregates, self-explanatory folder name, members in ≥2 layers,
  6–14 domains per deployable. `misc`, `utils`, `helpers` are forbidden names
  forever.
- Layer barrels re-export their domains; intra-layer imports use the direct
  submodule path, never the barrel *(why: barrel-mid-load self-import is a
  boot-order footgun)*.

## 4. Vertical slices — plug-ins and the Four Legal Seams

Marketplace/vendor/provider integrations are **plug-ins**: everything about
one integration — connector, jobs, normalizers, schedules, manifest, its
tests — lives in `services/packages/<plugin>/`. Core code NEVER imports a
package; packages plug in at the composition root and are resolved through a
**registry** (connector lanes, schedule registration, effect gates).

A plugin's name may appear outside its package in exactly FOUR places:

1. **Data vocabulary** — the slug constant on the owning model
   (`Channel.MARKETPLACE_EBAY`). *(Why: rows outlive code.)*
2. **Composition root** — the one mount line in `config/providers.py`.
3. **Generic ingress** — parameterized routes only:
   `/webhooks/{marketplace}`, `/channels/connect/{marketplace}/callback`.
   Plugin-specific routes are forbidden; the payload is queued and the worker
   resolves the handler through the registry.
4. **Manifest data** — a package publishes its metadata (regions,
   capabilities, display names, schedules) through the registry into
   DB/config; the API renders **data**, never imports plugin code.

Everything else is a violation and a guard greps for it. Kernel `contracts/`
are **brand-blind**: envelopes are generic (`ApplyAdRateJob(channel_id=…)`)
and resolve to a package lane by the channel's marketplace at runtime.

**The rm-rf acceptance test** (a real guard, simulated by unmounting the
plugin from the composition root): with `packages/<plugin>` gone, the product
still compiles, boots, generates its routes, serves existing data, and shows
the plugin's channels as "not installed". Nothing crashes. If deleting a
plugin folder breaks anything outside these expectations, the slice is
leaking and the leak is the bug.

## 5. Code law

- **The flow, one direction only:**
  `controller | job → FormRequest → service (use-case) → repository → model`.
  Controllers and jobs are thin: validate, call one use-case, shape the
  response — zero business branching. Services orchestrate. Repositories own
  ALL SQL/ORM queries. Models carry only their own intrinsic state
  transitions and never import repositories or support code. *(Why: every
  inversion of this arrow produced a real bug in the field — including a
  model→support import that made the model package unimportable.)*
- **Raw SQL lives only in repositories** (app-tree `repositories/` and
  `gates/persistence/`). A deliberate query-compiler is the single exception
  class and must say so in its docstring.
- **Cross-domain access:** a service may call another domain's SERVICE; it
  may NEVER touch another domain's repository. *(Why: data ownership has one
  door.)*
- **Size budgets:** file soft 400 / hard 700 lines — crossing hard triggers a
  split review (adapters split into mixins §6; business logic splits by
  use-case). Controller/job methods ≤ ~40 lines.
- **Naming grammar:** files PascalCase, one public class per file, named for
  the class. Laravel suffixes (`XController`, `XService`, `XRepository`,
  `XResource`, `VerbNounJob`, events past-tense with no suffix). Functions
  snake_case, constants UPPER_SNAKE, routes kebab-case, domain folders
  lowercase English.
- **Imports** at file top, four tiers in order: stdlib → third-party →
  framework/kernel (`cara`, `app.models`, `app.contracts`, …) → app locals.
  Function-local imports only for documented cycle-breaking and inside
  envelope bodies (§8).
- **No backward-compat shims, ever.** Movers migrate every caller in the
  same change. **Fix root causes** — a workaround that "works" is debt with
  interest.

## 6. Patterns — mandated and banned

**Mandated (each earns its keep):**

| Pattern | Where | Why |
|---|---|---|
| Repository | all persistence | one door per table |
| FormRequest + response envelope | every input/output | validation & shape in one place; errors are always 422 |
| Envelope/Body | every cross-process job | serialization shell ≠ work; shells stay importable app-free |
| Outbox + Relay | queue publishing | dispatch survives broker loss; the relay is the only publisher |
| State machine + CAS lease | long-running sync (plan→push→verify) | crash-safe progress, no double work |
| Registry + composition root | plug-ins | core stays plugin-blind; rm-rf test passes |
| Policy object | authorization | one catalog → gate → wrapper path |
| Value object | Money (Decimal), identifiers | floats corrupt money; strings corrupt identity |

**Banned:** domain facades *(hidden coupling, monkeypatch hell)* · service
locator · business queries on models · God classes · backward-compat shims ·
inheritance for code-sharing in domain logic (composition; **mixins only in
adapters**, split with byte-identical public surfaces) · singletons outside
framework facades.

## 7. Data law

Money is `Decimal` end-to-end. Unknown is `NULL`, never 0 or "" *(a fake zero
is a lie that averages into reports)*. Every timestamp is `TIMESTAMPTZ` in
UTC. Multi-tenant tables carry `tenant_id` with a fail-closed scope; raw SQL
binds `tenant_id` explicitly, every time. **Migrations are generated from
models** — one file per table, no incremental `add_/alter_/fix_` files; the
flow is model → dev-DB ALTER → regenerate → check. SQL a model cannot express
is marked `MODEL_LESS = True` with the reason in its docstring.

## 8. Async law

Every job declares `idempotency_params`; state-machine jobs set
`idempotency_cache_results = False` and re-enter through their durable lease.
Every user-triggered server operation exposes **durable operation state**
(`{queued, coalesced, op}`) — fire-and-forget is forbidden; a bare "queued"
toast is a bug. Queue ownership is a single topology SSOT
(`contracts`): API-owned queues are consumed by API-image workers, period.
The relay publishes; workers never replace it; the health probe for the
relay runs in the scheduler *(the observer must not be the observed)*.
Envelope bodies import app code function-locally so shells parse without the
app installed.

## 9. Errors and events

Exceptions form one domain taxonomy rooted at `ServiceException`; controllers
never hand-roll error JSON — the envelope system responds. Validation always
answers 422. Events are past-tense nouns with no suffix (`ProductCollected`).
Decision table: same-process side effect → in-process event/listener;
cross-process or durable → queue job. Fail-closed everywhere: an unconfigured
gate denies; unconfigured production mail refuses to boot; an unknown SLA
means NO deadline rather than an invented one.

## 10. Documentation law

Docs are written in English and **may not lie**: a claim verifier parses
backticked paths/commands against the codebase and fails on broken claims; a
freshness checker flags docs older than their sources. Every product carries
`docs/internal/00-map.md` → architecture pages → generated references, and a
`CLAUDE.md` atlas whose FIRST section binds the product to this doctrine:

> *This product is governed by the Cara Product Doctrine
> (`commons/cara/docs/DOCTRINE.md`). Its rules override local habit. Agents:
> read it before structural work; deviations are rejected, amendments happen
> in the doctrine repo — never as local exceptions.*

## 11. Enforcement — the Guard Pack

Rules live in tests, not prose. The pack every product MUST carry (names may
gain product prefixes, semantics may not):

| Guard | Pins |
|---|---|
| `test_layering_guards` | flow direction, raw-SQL home, model-import ban, purity allowlist, kernel-membership counts (single-consumer eviction), app↛gates/persistence |
| `test_import_wiring` | core↛packages, composition-root mounting, barrel-mid-load ban |
| `test_import_ordering_convention` | the four import tiers |
| `test_domain_registry` | mirror rule + registry membership (§3) |
| `test_vertical_slice_seams` | plugin names only at the Four Seams; brand-blind contracts; rm-rf simulation |
| `test_queue_topology` / deploy topology | queue ownership, worker/image pairing, relay/scheduler separation |
| `test_migration_convention` | model-first migrations, one file per table |
| docs claim/freshness checks | §10 |
| framework: vendor scan regression + dry-run harness | §2 prod story |

Weakening a guard IS a doctrine amendment. An agent that makes a red guard
green by deleting its assertion has failed the task.

## 12. New product bootstrap

1. Create the workspace: `api/ services/ commons/ frontend/ docs/
   infrastructure/` as independent repos; clone cara under `commons/cara`.
2. Lay the kernel: `commons/{models,contracts,gates,shared}` + the four
   barrels in each deployable.
3. Copy the Guard Pack; write `app/domains.py` starting from the universal
   domains; wire the composition root.
4. Write `CLAUDE.md` with the §10 binding paragraph and the product atlas.
5. Set up: model-first migrations, routes generated from controller
   docstrings, the queue topology SSOT, docs verifier, Dockerfiles with the
   vendor step + dry-run proof in CI.

## 13. Amendment

The doctrine changes by PR to the framework repo (version bump + dated
changelog entry at the bottom), then products fast-forward their cara clones.
A product may EXTEND the doctrine in its atlas (stricter, never looser).
Local exceptions do not exist; if reality resists a rule, the rule is amended
for everyone or the code is wrong.

---

*Changelog — 1.0 (2026-07-23): initial codification, extracted from the
synkronus/cheapa architecture program: kernel membership + dev-only vendor
story, single runtime namespace, mirror rule + domain registry, Four Legal
Seams + rm-rf test, size budgets, mandated/banned patterns, guard pack.*
