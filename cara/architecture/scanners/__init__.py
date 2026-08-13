"""The Guard Pack scanners (DOCTRINE §11): one class per file, each a pure
function of a :class:`~cara.architecture.Manifest.Manifest` returning
``list[Finding]``. ``craft arch:check`` runs the full set (or one, via
``--scanner``); every scanner is also directly importable for a targeted
pytest fixture.
"""

from cara._LazyExports import _install_lazy_exports

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


_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ABORT_CALL": (".HttpInBusinessLogic", "ABORT_CALL"),
    "BROAD_EXCEPTIONS": (".SilentExceptSwallow", "BROAD_EXCEPTIONS"),
    "CLASSES_KEY": (".SourceShape", "CLASSES_KEY"),
    "COLLABORATOR_CALLS_KEY": (".CollaboratorCalls", "COLLABORATOR_CALLS_KEY"),
    "COMPOSE_METHODS": (".RawSqlHome", "COMPOSE_METHODS"),
    "CURSOR_EXEC_METHODS": (".RawSqlHome", "CURSOR_EXEC_METHODS"),
    "CYCLE_PREFIX": (".InlineImports", "CYCLE_PREFIX"),
    "DOMAIN_OWNERSHIP_KEY": (".DomainOwnership", "DOMAIN_OWNERSHIP_KEY"),
    "EDGE_METHODS_KEY": (".SourceShape", "EDGE_METHODS_KEY"),
    "ENVIRONMENT_MODULE": (".EnvReadDiscipline", "ENVIRONMENT_MODULE"),
    "EXEC_METHODS": (".RawSqlHome", "EXEC_METHODS"),
    "FLOW_KEY": (".FlowLaw", "FLOW_KEY"),
    "LEGAL_PREFIXES": (".InlineImports", "LEGAL_PREFIXES"),
    "LINES_KEY": (".SourceShape", "LINES_KEY"),
    "ORM_METHODS": (".ModelQueryDiscipline", "ORM_METHODS"),
    "OS_ENV_NAMES": (".EnvReadDiscipline", "OS_ENV_NAMES"),
    "OWNERS": (".WriteOwnership", "OWNERS"),
    "PORTS_LAYER": (".PortMembership", "PORTS_LAYER"),
    "QUERY_COMPILER_MARKER": (".RawSqlHome", "QUERY_COMPILER_MARKER"),
    "REPORTING_CALLS": (".SilentExceptSwallow", "REPORTING_CALLS"),
    "ROW_LOCK_METHOD": (".ModelQueryDiscipline", "ROW_LOCK_METHOD"),
    "SQL_START": (".RawSqlHome", "SQL_START"),
    "TAG": (".InlineImports", "TAG"),
    "TRANSACTION_KEY": (".TransactionOwnership", "TRANSACTION_KEY"),
    "TRANSACTION_METHOD": (".ModelQueryDiscipline", "TRANSACTION_METHOD"),
    "WRITE_KEY": (".WriteOwnership", "WRITE_KEY"),
    "query_compiler_classes": (".RawSqlHome", "query_compiler_classes"),
    "raw_sql_findings": (".RawSqlHome", "raw_sql_findings"),
}

__all__ = [
    "ABORT_CALL",
    "BROAD_EXCEPTIONS",
    "BarrelCompleteness",
    "BarrelMidLoad",
    "CLASSES_KEY",
    "COLLABORATOR_CALLS_KEY",
    "COMPOSE_METHODS",
    "CURSOR_EXEC_METHODS",
    "CYCLE_PREFIX",
    "CollaboratorCalls",
    "DOMAIN_OWNERSHIP_KEY",
    "DomainOwnership",
    "DomainRegistry",
    "EDGE_METHODS_KEY",
    "ENVIRONMENT_MODULE",
    "EXEC_METHODS",
    "EnvReadDiscipline",
    "FLOW_KEY",
    "FlowLaw",
    "HttpInBusinessLogic",
    "ImportForm",
    "ImportTiers",
    "InlineImports",
    "JobIdempotency",
    "KernelMembership",
    "LEGAL_PREFIXES",
    "LINES_KEY",
    "ModelQueryDiscipline",
    "ORM_METHODS",
    "OS_ENV_NAMES",
    "OWNERS",
    "PORTS_LAYER",
    "PortMembership",
    "QUERY_COMPILER_MARKER",
    "REGISTRY",
    "REPORTING_CALLS",
    "ROW_LOCK_METHOD",
    "RawSqlHome",
    "SQL_START",
    "SilentExceptSwallow",
    "SourceShape",
    "TAG",
    "TRANSACTION_KEY",
    "TRANSACTION_METHOD",
    "TransactionOwnership",
    "VendorBarrelParity",
    "VerticalSliceSeams",
    "WRITE_KEY",
    "WriteOwnership",
    "query_compiler_classes",
    "raw_sql_findings",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
