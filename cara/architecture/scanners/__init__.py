"""The Guard Pack scanners (DOCTRINE §11): one class per file, each a pure
function of a :class:`~cara.architecture.Manifest.Manifest` returning
``list[Finding]``. ``craft arch:check`` runs the full set (or one, via
``--scanner``); every scanner is also directly importable for a targeted
pytest fixture.
"""

from .BarrelCompleteness import BarrelCompleteness
from .DomainRegistry import DomainRegistry
from .ImportForm import ImportForm
from .ImportTiers import ImportTiers
from .InlineImports import InlineImports
from .JobIdempotency import JobIdempotency
from .KernelMembership import KernelMembership
from .PortMembership import PortMembership
from .VerticalSliceSeams import VerticalSliceSeams

__all__ = [
    "BarrelCompleteness",
    "DomainRegistry",
    "ImportForm",
    "ImportTiers",
    "InlineImports",
    "JobIdempotency",
    "KernelMembership",
    "PortMembership",
    "VerticalSliceSeams",
]

#: name (as accepted by ``craft arch:check --scanner``) -> scanner class
REGISTRY: dict[str, type] = {
    "import_tiers": ImportTiers,
    "inline_imports": InlineImports,
    "import_form": ImportForm,
    "barrel_completeness": BarrelCompleteness,
    "domain_registry": DomainRegistry,
    "kernel_membership": KernelMembership,
    "vertical_slice_seams": VerticalSliceSeams,
    "port_membership": PortMembership,
    "job_idempotency": JobIdempotency,
}
