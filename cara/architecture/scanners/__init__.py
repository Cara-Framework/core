"""The Guard Pack scanners (DOCTRINE §11): one class per file, each a pure
function of a :class:`~cara.architecture.Manifest.Manifest` returning
``list[Finding]``. ``craft arch:check`` runs the full set (or one, via
``--scanner``); every scanner is also directly importable for a targeted
pytest fixture.
"""

from .BarrelCompleteness import BarrelCompleteness
from .DomainOwnership import DomainOwnership
from .DomainRegistry import DomainRegistry
from .FlowLaw import FlowLaw
from .ImportForm import ImportForm
from .ImportTiers import ImportTiers
from .InlineImports import InlineImports
from .JobIdempotency import JobIdempotency
from .KernelMembership import KernelMembership
from .PortMembership import PortMembership
from .SourceShape import SourceShape
from .TransactionOwnership import TransactionOwnership
from .VerticalSliceSeams import VerticalSliceSeams
from .WriteOwnership import WriteOwnership

__all__ = [
    "BarrelCompleteness",
    "DomainOwnership",
    "DomainRegistry",
    "FlowLaw",
    "ImportForm",
    "ImportTiers",
    "InlineImports",
    "JobIdempotency",
    "KernelMembership",
    "PortMembership",
    "SourceShape",
    "TransactionOwnership",
    "VerticalSliceSeams",
    "WriteOwnership",
]

#: name (as accepted by ``craft arch:check --scanner``) -> scanner class
REGISTRY: dict[str, type] = {
    "import_tiers": ImportTiers,
    "inline_imports": InlineImports,
    "import_form": ImportForm,
    "barrel_completeness": BarrelCompleteness,
    "domain_ownership": DomainOwnership,
    "domain_registry": DomainRegistry,
    "flow_law": FlowLaw,
    "kernel_membership": KernelMembership,
    "vertical_slice_seams": VerticalSliceSeams,
    "port_membership": PortMembership,
    "job_idempotency": JobIdempotency,
    "source_shape": SourceShape,
    "transaction_ownership": TransactionOwnership,
    "write_ownership": WriteOwnership,
}
