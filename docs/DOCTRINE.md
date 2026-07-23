# The Cara Product Doctrine

**Version 1.3 — 2026-07-23.** This document is LAW for every product built on
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
- **Scope.** This doctrine governs the backend: the two deployables and the
  kernel. Frontend surfaces follow their own conventions (React Server
  Components, React Query for server state, Zustand UI-only) and are bound
  only by the API contract; a surface doctrine may join later.
- The two deployables share **one PostgreSQL database** and speak to each
  other through the queue (RabbitMQ), the shared tables, and — for payloads
  too large for either — **durable object storage** whose keys are a
  ``contracts`` vocabulary — carried as a typed reference (an ``ObjectRef``
  value object: tenant, key, checksum, size, content type; absolute paths and
  ``..`` rejected at construction), never ad-hoc strings. Direct imports
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

**Ports have a membership rule — `app/ports` must not become the next junk
drawer.** A port exists only when it is (a) a real boundary the consumer owns,
(b) an implementation that can plausibly be swapped or an external-system
edge, or (c) a stable capability used by more than one use-case. Auto-minting
an `XDataContract` per repository is forbidden. Ports prefer typed DTOs and
value objects over `Any`, bare `dict` and shapeless `list`.

**Local DI interfaces are NOT kernel contracts.** A deployable's own
dependency-inversion interfaces (connector contracts, data contracts,
service seams) live in **`app/ports/<domain>/`** — never in `app/contracts`,
which is exclusively the kernel-contracts barrel. This kills the name
collision by construction: the vendor step never merges barrels, it fails
fast if `app/<kernel-pkg>` carries local members.

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
  services/<domain>/                      ports/<domain>/  (local DI interfaces)
  repositories/<domain>/                  commands/<area>/
  policies/  events/  listeners/          providers/
  jobs/  ports/<domain>/  providers/      support/<theme>/
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
  nameable aggregates, a self-explanatory folder name, members in ≥2 layers
  (a single-layer domain triggers review, not auto-rejection). Domain COUNT
  is a REVIEW THRESHOLD, not a hard law — beyond ~14 per deployable review
  for fragmentation; merging names to satisfy a number makes the
  architecture worse. `misc`, `utils`, `helpers` are forbidden names
  forever. Two sanctioned NON-domain groupings exist, declared as such in
  the registries: **transport namespaces** (`ws/`) and **flow stages** (a
  worker's `jobs/pipeline/` stage tree, declared in `app/flows.py` beside
  `app/domains.py`) — they group by mechanics, not business capability.
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
  `controller → FormRequest → service (use-case) → repository → model`;
  `job → validated envelope payload → service (use-case) → repository → model`.
  (Jobs never touch FormRequest — their input contract is the envelope.)
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
- **Imports** follow the Import Law (§5.1).
- **No backward-compat shims, ever.** Movers migrate every caller in the
  same change. **Fix root causes** — a workaround that "works" is debt with
  interest.

### 5.1 The Import Law

- **Placement.** Every import lives at the top of the file, in four tiers:
  stdlib → third-party → framework/kernel (`cara`, `app.models`,
  `app.contracts`, …) → app locals. A function-local import is legal in
  exactly three cases, and MUST carry a `# local:` reason tag naming which:
  1. **envelope bodies** (§8 — shells must parse without the app installed),
  2. **proven cycle-breakers** (`# local: cycle with <module>`),
  3. **heavy/optional dependencies** (browser engines, connector SDKs —
     boot speed and optionality).
  An untagged function-local import is a guard failure. *(Why: lazy imports
  hide dependency direction and rot into superstition.)*
- **Hoisting is decided by the import graph, not by feel.** A local import
  may move to the top iff adding that edge to the module-level import graph
  (barrels included as nodes) creates no cycle. Tooling computes this; a
  cold-import sweep of every module verifies it. Nobody argues about
  circularity from memory.
- **Form.** Consumers import through the barrel: `from app.services import
  ProductService`, `from app.gates import ListingWriter` — never a deep path
  from outside the owning layer. **Siblings inside a layer/package import by
  direct submodule path** (`from app.services.catalog.ProductService import
  ProductService`) — during barrel `__init__` execution the package is only
  partially initialized, and a sibling reaching through the barrel is a
  boot-order crash. The kernel is consumed only through the `app.*` barrels
  (§2).
- **Barrels are generated and verified, never hand-curated.** Every public
  name is re-exported (domain `__init__` AND layer barrel), `__all__`
  alphabetical, completeness guard-enforced. A public name missing from its
  barrel is a bug even before anyone imports it — name/submodule shadowing
  taught us this the hard way.

## 6. Patterns — mandated and banned

**Mandated (each earns its keep):**

| Pattern | Where | Why |
|---|---|---|
| Repository | all persistence | one door per table |
| FormRequest + response envelope | every input/output | validation & shape in one place; validation errors are always 422 |
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
flow is model → dev-DB ALTER → regenerate → check. This regenerate-freely
mode is **pre-launch only**: once a migration has been APPLIED to an
environment you cannot reset, it is immutable (the executor's checksum guard
enforces this) — schema changes become forward migrations, and a squashed
baseline may periodically replace history for fresh installs. SQL a model
cannot express is marked `MODEL_LESS = True` with the reason in its
docstring. **Write ownership:** a shared table is not a shared pen — each
table is declared `api-owned`, `services-owned` or `shared-gate-owned` in a
write-ownership manifest, and a guard checks repository writes against it.

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
app installed. **Transaction ownership:** the use-case service owns the
business transaction; repositories JOIN the ambient transaction and never
commit/rollback on their own — the sole exception is a single atomic
persistence primitive (CAS/lease) fully contained in a repository method.
The outbox record is written in the SAME transaction as the business write.

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
| `test_import_form` | barrel form for consumers, direct path for siblings, no deep paths from outside a layer (§5.1) |
| `test_inline_imports` | every function-local import carries a legal `# local:` reason tag (§5.1) |
| barrel completeness | every public name re-exported, `__all__` alphabetical (§5.1) |
| `test_domain_registry` | mirror rule + registry membership (§3) |
| `test_vertical_slice_seams` | plugin names only at the Four Seams; brand-blind contracts; rm-rf simulation |
| `test_queue_topology` / deploy topology | queue ownership, worker/image pairing, relay/scheduler separation |
| `test_migration_convention` | model-first migrations, one file per table |
| docs claim/freshness checks | §10 |
| framework: vendor scan regression + dry-run harness | §2 prod story |

Weakening a guard IS a doctrine amendment. An agent that makes a red guard
green by deleting its assertion has failed the task.

**The pack converges on ONE implementation.** Guard logic (the AST scanners)
belongs in the framework; products supply only their manifests (domains,
flows, ownership, sunset lists). Until that extraction lands, per-product
guard copies must stay semantically identical — divergence is drift, and
drift in a guard is a doctrine bug.

**Allowlists are sunset debts, not exceptions.** A guard may carry a pinned
allowlist ONLY for violations that predate the rule; every allowlist is
shrink-only (adding an entry is a doctrine amendment), and its size is part
of the guard's output. "Local exceptions do not exist" (§13) refers to
UNTRACKED deviations — a dated, counted, shrink-only allowlist is the
tracked path to zero.

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

*Changelog — 1.1 (2026-07-23): §5.1 The Import Law — top-placement with the
three tagged exceptions, graph-decided hoisting, barrel form for consumers /
direct path for siblings, generated barrels; three new guards in §11.*

*Changelog — 1.2 (2026-07-23): §1 backend scope + durable object-storage
seam; §2 `app/ports` for local DI interfaces (kernel `app/contracts` is
collision-free by construction, vendor fails fast instead of merging); §3
domain count as review threshold + sanctioned transport/flow-stage
groupings; §5 job flow via validated envelope payload (never FormRequest);
§6 422 wording; §7 pre-launch vs applied-immutable migration modes; §11
allowlists as shrink-only sunset debts. Review credit: external 5.6 audit.*

*Changelog — 1.3 (2026-07-23): §2 ports membership rule + typed DTOs; §3
flows.py beside domains.py; §7 write-ownership manifest; §8 transaction
ownership; §1 typed ObjectRef storage references; §11 single-implementation
guard pack intent. Review credit: external GPT audit.*

*Errata — 1.3.1 (2026-07-23): three 1.2/1.3 body amendments had silently
failed to apply (unasserted replacements; only their changelog entries
landed): §3 review-threshold + transport/flow groupings and §7 migration
modes. Applied now; a §8 duplication introduced while fixing was removed in
the same pass. Body and changelog are consistent — every normative topic
appears exactly once. Found by an external audit; the lesson is §11's own
rule: verify the BODY (whitespace-collapsed), not the changelog.*
