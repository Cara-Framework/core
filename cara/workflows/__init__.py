"""
Cara Framework Workflows Module.

Ordered step-sequence pipeline for command workflows.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ConditionalPipeline": (".ConditionalPipeline", "ConditionalPipeline"),
    "Pipeline": (".Pipeline", "Pipeline"),
    "PipelineStep": (".PipelineStep", "PipelineStep"),
    "PipelineType": (".PipelineType", "PipelineType"),
    "StepFailed": (".StepFailed", "StepFailed"),
}

__all__ = [
    "ConditionalPipeline",
    "Pipeline",
    "PipelineStep",
    "PipelineType",
    "StepFailed",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
