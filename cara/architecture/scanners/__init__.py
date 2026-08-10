"""The Guard Pack scanners (DOCTRINE §11): one class per file, each a pure
function of a :class:`~cara.architecture.Manifest.Manifest` returning
``list[Finding]``. ``craft arch:check`` runs the full set (or one, via
``--scanner``); every scanner is also directly importable for a targeted
pytest fixture.
"""

from .BarrelCompleteness import BarrelCompleteness
from .BarrelMidLoad import BarrelMidLoad
from .CollaboratorCalls import CollaboratorCalls
from .DomainOwnership import DomainOwnership
from .DomainRegistry import DomainRegistry
from .EnvReadDiscipline import EnvReadDiscipline
from .FlowLaw import FlowLaw
from .HttpInBusinessLogic import HttpInBusinessLogic
from .ImportForm import ImportForm
from .ImportTiers import ImportTiers
from .InlineImports import InlineImports
from .JobIdempotency import JobIdempotency
from .KernelMembership import KernelMembership
from .ModelQueryDiscipline import ModelQueryDiscipline
from .PortMembership import PortMembership
from .RawSqlHome import RawSqlHome
from .SilentExceptSwallow import SilentExceptSwallow
from .SourceShape import SourceShape
from .TransactionOwnership import TransactionOwnership
from .VendorBarrelParity import VendorBarrelParity
from .VerticalSliceSeams import VerticalSliceSeams
from .WriteOwnership import WriteOwnership

__all__ = [
    "BarrelCompleteness",
    "BarrelMidLoad",
    "CollaboratorCalls",
    "DomainOwnership",
    "DomainRegistry",
    "EnvReadDiscipline",
    "FlowLaw",
    "HttpInBusinessLogic",
    "ImportForm",
    "ImportTiers",
    "InlineImports",
    "JobIdempotency",
    "KernelMembership",
    "ModelQueryDiscipline",
    "PortMembership",
    "RawSqlHome",
    "SilentExceptSwallow",
    "SourceShape",
    "TransactionOwnership",
    "VendorBarrelParity",
    "VerticalSliceSeams",
    "WriteOwnership",
]

#: name (as accepted by ``craft arch:check --scanner``) -> scanner class
REGISTRY: dict[str, type] = {
    "import_tiers": ImportTiers,
    "inline_imports": InlineImports,
    "import_form": ImportForm,
    "barrel_completeness": BarrelCompleteness,
    "barrel_mid_load": BarrelMidLoad,
    "domain_ownership": DomainOwnership,
    "domain_registry": DomainRegistry,
    "flow_law": FlowLaw,
    "kernel_membership": KernelMembership,
    "vertical_slice_seams": VerticalSliceSeams,
    "port_membership": PortMembership,
    "job_idempotency": JobIdempotency,
    "source_shape": SourceShape,
    "transaction_ownership": TransactionOwnership,
    "vendor_barrel_parity": VendorBarrelParity,
    "write_ownership": WriteOwnership,
    "collaborator_calls": CollaboratorCalls,
    "raw_sql_home": RawSqlHome,
    "model_query_discipline": ModelQueryDiscipline,
    "http_in_business_logic": HttpInBusinessLogic,
    "env_read_discipline": EnvReadDiscipline,
    "silent_except_swallow": SilentExceptSwallow,
}
