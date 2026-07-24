# The Cara Guard Pack

**Reference — 2026-07-24.** This is the operator manual for the architecture
enforcement described by the [Cara Product Doctrine](DOCTRINE.md). The Doctrine
is law; this file explains the mechanism. If prose and scanner behaviour
disagree, fix the framework or amend the Doctrine. Never weaken a product guard
to make a finding disappear.

---

## 1. What the pack is

The Guard Pack is a boot-free architecture checker. A product supplies one
typed `Manifest` per deployable at `app/architecture_manifest.py`; Cara loads
that file directly and runs filesystem/AST scanners. It does not boot the app,
resolve the container, connect to the database or read secrets.

The execution model is deliberately small:

```
app/architecture_manifest.py
        │
        ▼
Manifest.load()
        │
        ├── fourteen pure scanners ──► list[Finding]
        │
        └── BarrelGenerator ─────► check or rewrite __init__.py barrels
```

A `Finding` has a deployable-relative path, a one-based line number when the
violation belongs to a statement, and a message. There are no warning levels:
one finding fails the command and therefore the build.

The pack enforces only the scanners listed below. Doctrine guards for
read-only raw SQL, queue/deploy topology, migrations and documentation remain
product responsibilities until a framework scanner explicitly owns them.

## 2. Manifest reference

The manifest module MUST bind exactly one module-level value:

```python
from cara.architecture.Manifest import Manifest, ManifestRoots, SeamLocations

MANIFEST = Manifest(...)
```

`Manifest.load()` executes this file by location. Keep it boot-free: stdlib,
`cara.architecture`, constants and literal data only. Importing `app`,
configuration, providers or models turns an architecture check into an
application boot and violates the contract.

### `ManifestRoots`

| Field | Meaning |
|---|---|
| `deployable` | Absolute or resolved root of the API/worker repo. Findings are relative to this path. |
| `app` | The deployable's `app/` directory. Layers, domains, ports and jobs are resolved from here. |
| `config` | Optional configuration root used by product scanner scopes and plugin-seam checks. |
| `routes` | Optional route root. Omit it for deployables with no route tree. |
| `packages` | Optional plug-in root such as `services/packages`; package jobs are included in idempotency checks. |
| `scanner_roots` | Scanner id → exact tuple of product-owned trees that scanner walks. Every scanner that calls `scan_dirs()` MUST have an entry; omission raises instead of silently skipping code. |
| `kernel` | Kernel package name → development source directory. Normally the checked-out `models`, `contracts`, `gates` and `shared` directories. An empty map is valid in a vendored image. |
| `consumer_roots` | Process group → roots that prove `commons/shared` consumption. Put API and services in separate groups; a group may contain multiple roots such as `services/app` and `services/packages`. |
| `framework_root_name` | Import root treated as Cara/framework. Default: `cara`. |
| `kernel_dev_root_name` | Development-only kernel import root. Default: `commons`. |
| `local_root_names` | Import roots classified as app-local by `import_tiers`. Default: `app`, `config`, `routes`, `packages`. |

Scanner roots are intentionally explicit. Import form, port implementors and
vertical-slice seams do not have the same honest scope.

### `SeamLocations`

| Field | Meaning |
|---|---|
| `composition_roots` | Deployable-relative files where plug-ins are mounted. A plug-in token is legal there. |
| `manifest_files` | Deployable-relative data-manifest files where plug-in metadata is legal. |
| `data_vocabulary_prefixes` | Deployable-relative directory prefixes containing durable UPPER_SNAKE plug-in constants. |
| `owned_integration_prefixes` | Deployable-relative non-marketplace capability lane → exact provider tokens owned by that lane. Only those tokens are legal inside it; core imports remain findings. |

Generic parameterized ingress needs no field: `/webhooks/{marketplace}` does
not contain a concrete plug-in token.

### `Manifest`

| Field | Meaning |
|---|---|
| `product` | Stable product name used for identity and diagnostics. |
| `deployable` | Stable deployable name, normally `api` or `services`. |
| `roots` | The `ManifestRoots` value above. |
| `layers` | Every barrel/import-governed app layer. Include `ports` only when the deployable has that layer. |
| `domain_layers` | The subset of layers partitioned by domain/flow folders. Cross-cutting trees such as `support` usually stay out. |
| `domains` | Domain name → non-empty one-line charter, normally loaded as literal data from the product's domain registry. |
| `scan_plugin_string_literals` | Must be `True` whenever `plugin_tokens` is non-empty; vertical-slice scanning inspects concrete plug-in strings in comparisons, defaults, dict keys and call arguments. |
| `kernel_barrel_packages` | Kernel packages whose barrels are generated and checked in this deployable's run. Split ownership between twin manifests deliberately to avoid duplicate findings. |
| `seam_kernel_packages` | Kernel packages included in vertical-slice seam scanning. |
| `flows` | Non-domain flow-stage name → charter, for sanctioned mechanics such as `jobs/pipeline`. Default: empty. |
| `universal_domains` | Domain names this product requires everywhere. Every entry must exist in `domains`. |
| `kernel_packages` | The kernel membership vocabulary. Default and doctrine value: `models`, `contracts`, `gates`, `shared`. |
| `plugin_tokens` | Concrete vendor/marketplace slugs policed outside their package homes. |
| `seam_allowlists` | Scanner id → `{relative_path: allowed_hit_count}`. These are counted, shrink-only sunset debts; growth is forbidden. |
| `inline_import_exemptions` | `(relative_path, first_imported_name)` pairs temporarily exempt from a `# local:` tag. Shrink-only. |
| `pure_modules` | Kernel module stems that may not import configured side-effect facades. |
| `single_consumer_allowlist` | `commons/shared` module stems temporarily pinned despite having only one provable process consumer. Shrink-only. |
| `port_membership_tags` | Required comment prefix for a genuine one-implementor external edge. Default: `# port:`. |
| `forbidden_domain_names` | Junk-drawer domain names rejected by the registry scanner. Defaults: `misc`, `utils`, `helpers`. |
| `seam_locations` | The `SeamLocations` value above. |
| `domain_layer_root_allowlist` | Documented loose files permitted at a domain-layer root, normally a base class. Missing/stale entries fail. |
| `job_idempotency_exemptions` | Dated `<relative path>::<ClassName>` pins for pre-rule jobs. Satisfied or vanished pins fail until removed. |
| `job_root_class` | Base class that identifies queued jobs. Default: `BaseJob`. |
| `job_roots` | App-relative job directories to inspect. Default: `jobs`. The same names are inspected inside each package. |
| `idempotency_field_name` | Class attribute that declares job identity. Default: `idempotency_params`. |
| `side_effect_facade_roots` | Import module roots forbidden inside `pure_modules`, such as DB/Cache/Bus facade modules. |
| `side_effect_facade_names` | Optional imported-name filter within those facade modules. Empty means every imported name is forbidden. |
| `third_party_packages` | Closed third-party import-root inventory. Empty uses a catch-all third-party tier; non-empty makes an unknown dependency a distinct final tier so it cannot enter silently. |
| `deep_import_allowlist` | `(consumer_path, concrete_module)` pairs for dated cycle-breakers that cannot yet use a layer/domain barrel. Stale entries fail. |
| `source_shape_hard_limit` | Hard production-source file budget; Doctrine default is 700 lines. |
| `source_shape_edge_method_limit` | Hard public controller/job method budget; Doctrine default is 40 lines. |
| `source_shape_edge_layers` | Layer names treated as transport edges by `source_shape`; normally `controllers` and `jobs`. |
| `flow_edge_layers` | Layer names that must reach persistence only through a use-case service; normally `controllers` and `jobs`. |
| `atomic_repository_methods` | Exact `path::Class.method` identities for the sole §8 exception: a fully-contained atomic persistence primitive. Stale identities fail. |
| `write_ownership` | Table → `api-owned`, `services-owned` or `shared-gate-owned`. Every model-backed table must be declared. |
| `model_less_write_tables` | Explicit table names whose model-less schema is documented by the product; permits ownership entries without a model class. |

Do not fill a field because it exists. An empty value means “this deployable
does not have this concept”; an allowlist means “tracked debt moving to zero,”
never “approved architecture.”

## 3. Scanner catalog

The command ids below are the exact values accepted by
`craft arch:check --scanner=...`.

### `import_tiers`

Checks the leading top-level import block for the order stdlib → third-party →
framework/kernel → app-local. Relative imports are app-local. With a closed
`third_party_packages` set, an unknown dependency is placed after the normal
tiers so the new dependency cannot blend in unnoticed.

Violation:

```python
from app.services import ProductService
from pathlib import Path
```

Clean:

```python
from pathlib import Path

from cara.facades import Log

from app.services import ProductService
```

This scanner does not alphabetize within a tier and does not inspect imports
after the leading block; formatters and `inline_imports` own those concerns.

### `inline_imports`

Checks every function-local import. It must carry one legal same-line reason:
`# local: envelope body`, `# local: cycle with <module>`, or
`# local: heavy optional dep`. The envelope reason is valid only under a
kernel package's `envelopes/` directory. A cycle reason must name the module.

Violation:

```python
def render():
    import playwright
```

Clean:

```python
def render():
    import playwright  # local: heavy optional dep
```

Use `inline_import_exemptions` only to pin pre-rule debt. Do not use it for a
new import.

### `import_form`

Enforces three import shapes:

1. Consumers outside a layer use its layer/domain barrel, not a leaf module.
2. Siblings inside a layer import each other's direct submodule, not their own
   partially initialized layer barrel.
3. Deployable code consumes the dev-only kernel through `app.*` barrels; only
   the kernel barrel files may import `commons.*`.

Violation:

```python
# app/controllers/catalog/ProductController.py
from app.services.catalog.ProductService import ProductService
from commons.models import Product
```

Clean:

```python
from app.models import Product
from app.services.catalog import ProductService
```

Inside `app/services/catalog/`, the clean sibling form is instead
`from app.services.catalog.OtherService import OtherService`.

### `barrel_completeness`

Walks managed layer and kernel packages recursively. Every package with public
children must declare `__all__`, re-export the complete direct-child surface,
keep `__all__` alphabetical and bind every listed name. Deliberate module-object
bindings remain module-qualified and are exempt from symbol flattening.

Violation:

```python
from .ProductService import ProductService

__all__ = []
```

Clean:

```python
from .ProductService import ProductService

__all__ = ["ProductService"]
```

Do not repair large barrel drift by hand. Run `arch:barrels --write`, inspect
the diff and then rerun both barrel and scanner checks.

### `domain_registry`

Enforces the mirror rule for `domain_layers`: every folder is a declared domain
or flow, every declared domain has a member somewhere, universal domains exist,
charters are non-empty, forbidden names are rejected, and loose layer-root
modules are rejected unless pinned as documented base classes. Stale pins fail.

Violation:

```text
app/controllers/misc/HealthController.py
```

Clean:

```python
DOMAINS = {
    "platform": "Operator health, diagnostics and administrative control.",
}
```

with the controller under `app/controllers/platform/`.

### `kernel_membership`

Runs three checks:

- Direction: `models` imports no other kernel package; `contracts` imports only
  itself and `models`, never `gates` or `shared`.
- Purity: configured pure module stems may not import configured side-effect
  facades/names.
- Shared membership: with at least two checked-out consumer groups, a
  `commons/shared` module consumed by exactly one process is eviction debt.

Violation:

```python
# commons/contracts/Capabilities.py
from commons.gates import Entitlements
```

Clean:

```python
from commons.models import Channel  # typing/value vocabulary only
```

Direction and plug-in seam debts use counted `seam_allowlists`; shared
single-consumer debt uses `single_consumer_allowlist`. Every list is
shrink-only and stale pins are findings.

### `vertical_slice_seams`

Keeps concrete plug-in tokens inside their package or the Four Legal Seams.
Outside `packages/<plugin>`, it checks module paths, definitions, imports and
module/class assignment names. It also checks plug-in strings in
comparisons, default values, dict keys and call arguments. Comments, docstrings
and unrelated string positions are not treated as coupling.

A declared `owned_integration_prefixes` lane is still scanned. Its own provider
tokens are legal inside the lane, while unrelated provider tokens and any core
import of its concrete implementation remain findings. This models discovery
providers that produce thin candidates for later marketplace scraping without
misclassifying the discovery provider as a marketplace.

Violation:

```python
if marketplace == "ebay":
    run_ebay_sync()
```

Clean:

```python
connector = registry.for_marketplace(marketplace)
connector.sync()
```

Concrete slug constants are legal only under configured
`data_vocabulary_prefixes`; mounts and metadata are legal only in the exact
configured composition/manifest files.

### `port_membership`

Checks top-level `*Contract` classes under `app/ports`. A port must have at
least two distinct implementor files in the configured scan roots, or carry a
truthful `# port: <reason>` tag for a genuine external-system edge. A
single-implementation repository mirror is a finding, not an invitation to add
a container binding. Generated database/swappable/external-or-algorithm
boilerplate is rejected; a retained single-implementor port must name its
concrete boundary.

Violation:

```python
class ProductDataContract(ABC):
    ...

# Only ProductDataRepository implements it.
```

Clean:

```python
class ScrapeDriverContract(ABC):
    ...

class DirectDriver(ScrapeDriverContract):
    ...

class ScrapeDoDriver(ScrapeDriverContract):
    ...
```

For one implementation, delete the contract and inject the concrete repository.
Do not add `# port:` to preserve ceremony.

### `job_idempotency`

Finds classes that inherit, directly or through scanned ancestors, from the
configured job root class. Each job must declare the configured idempotency
field, inherit it, or document a real no-risk case with
`# idempotency: none — <reason>`. Package job roots are included when
`roots.packages` exists. Stale exemptions fail.

Violation:

```python
class RefreshProductJob(BaseJob):
    async def execute(self):
        ...
```

Clean:

```python
class RefreshProductJob(BaseJob):
    idempotency_params = ("product_id",)

    async def execute(self):
        ...
```

An empty tuple is still an explicit declaration; use it only when the base
class semantics make that identity truthful.

### `source_shape`

Enforces the 700-line hard file budget, one public top-level class per file
named for that file, and the 40-line public controller/job method budget.
Existing violations use exact, shrink-only `source_shape_lines`,
`source_shape_classes` and `source_shape_edge_methods` counts; growth and stale
pins fail.

### `flow_law`

Keeps transport edges on `controller/job → use-case service → repository`.
Controller/job files may not import repositories, models, kernel gates or the
DB facade, nor resolve repositories from the container. Existing violations
are counted per file in the shrink-only `flow_law` debt census.

### `domain_ownership`

Keeps each domain's repository behind that domain's service door. A service may
call another domain's service, but services and repositories may not import a
sibling domain's repository. Barrel imports are resolved back to the owning
domain, so `from app.repositories import XRepository` cannot hide the reach.
Existing violations use an exact, shrink-only per-file census.

### `transaction_ownership`

Enforces `use-case service → transaction → repository`. Controllers, jobs and
repositories may not open, commit or roll back business transactions. The one
legal repository exception is a fully-contained atomic persistence primitive
named exactly in `atomic_repository_methods`; stale declarations fail. Existing
violations use an exact per-file `transaction_ownership` census.

### `write_ownership`

Requires one declared owner for every model-backed table and scans direct ORM
class writes, query-builder mutations and literal write SQL. The owning
deployable may write its tables; `shared-gate-owned` tables may be written only
through `gates/persistence`. Existing cross-owner writes are exact,
shrink-only `path::table` debt.

## 4. Commands

Run from the deployable root unless `--manifest` points elsewhere:

```bash
./venv/bin/python craft arch:check
./venv/bin/python craft arch:check --scanner=import_form,port_membership
./venv/bin/python craft arch:check --manifest=/absolute/path/to/architecture_manifest.py
```

The default runs all registered scanners in stable name order. Unknown scanner,
missing manifest, load failure or any finding exits non-zero.

Barrels are a separate boot-free command:

```bash
./venv/bin/python craft arch:barrels
./venv/bin/python craft arch:barrels --check
./venv/bin/python craft arch:barrels --write
```

Check is the default. It reports what would change and exits non-zero without
writing. Write regenerates managed barrels, fails on export-name collisions and
returns success only after writing the plan. Passing `--check` and `--write`
together is a usage error.

The generator is idempotent and preserves documented special structure:
module docstrings, `__future__` imports, constants, module-object binds, aliased
relative imports, selected private exports, foreign imports and deliberate
post-`__all__` late binds. Generated public exports and `__all__` remain the
authoritative superset.

Recommended local/CI order:

```bash
./venv/bin/python craft arch:barrels --check
./venv/bin/python craft arch:check
./venv/bin/python -m pytest tests/architecture -q
```

## 5. Adapting the pack to a new product

1. **Create the doctrine shape.** Establish independent `api`, `services`,
   `commons`, docs and surface repos. Create
   `commons/{models,contracts,gates,shared}` and the four `app.*` barrels in
   each deployable.
2. **Declare domains and flows.** Write literal, boot-free `app/domains.py`
   data and, for worker mechanics such as pipeline stages, `app/flows.py`.
   Make layer folders mirror those names.
3. **Create one manifest per deployable.** Resolve every root from the
   manifest file or deployable root; do not depend on the caller's home
   directory. Declare only layers and optional roots that exist.
4. **Scope every scanner explicitly.** Populate `scanner_roots` for the
   scanners that consume it: `import_tiers`, `inline_imports`, `import_form`,
   `vertical_slice_seams` and `port_membership`. The remaining scanners derive
   their scope from layer/kernel/job fields. Include app/config/routes/package
   trees according to each scanner's purpose; verify that no product-owned
   Python tree disappears between them.
5. **Describe the kernel honestly.** Map all checked-out kernel packages,
   split `kernel_barrel_packages`/`seam_kernel_packages` between twin runs, and
   provide separate API/services `consumer_roots` so shared membership can be
   proven when both repos are present.
6. **Declare true plug-in seams.** List plug-in tokens, composition roots,
   manifest files and durable vocabulary prefixes. Enable string-literal
   scanning unless the product has no vertical plug-ins.
7. **Collapse fake ports before pinning anything.** Concrete repositories are
   injected directly. Keep ports only for substitutable strategies/external
   edges; use a `# port:` reason only when that edge is genuine.
8. **Run the pack with empty allowlists.** Fix new violations. If adopting the
   pack on existing code, record only measured pre-rule hits as dated,
   counted, shrink-only debt; never add an allowlist for code introduced in
   the adoption change.
9. **Generate barrels and inspect the diff.** Run `arch:barrels --write`, then
   `--check`. Resolve name collisions at the source; do not hide them with a
   curated `__all__`.
10. **Wire CI and product guards.** Gate on barrel check, the full Guard Pack
    and architecture scanner tests. Keep the non-framework doctrine guards
    for layering/raw SQL, write ownership, queue topology, migrations,
    vendoring dry-run and docs freshness/claims.
11. **Prove both deployables.** Run each manifest independently, then run with
    both consumer trees checked out so single-consumer kernel eviction is not
    silently skipped.
12. **Ratchet debt to zero.** A finding count that shrinks means its allowlist
    must shrink in the same change. A vanished file or now-compliant job means
    its pin must be deleted.

## 6. Review rules

- A scanner change is a framework change and requires framework tests plus
  product proofs. A product cannot locally redefine scanner semantics.
- Adding an allowlist entry weakens enforcement and therefore requires the same
  scrutiny as a Doctrine amendment.
- `arch:barrels --write` is a generator, not a formatter. Review every changed
  public surface.
- A green Guard Pack proves only the rules it scans. It does not prove business
  correctness, authorization completeness, transaction safety or production
  topology.
- Never make a red guard green by deleting the assertion, narrowing a scan root
  without architectural reason, or tagging ceremony as a port.
