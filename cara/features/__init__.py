"""Feature flags — cached, fail-open runtime gate (Pennant-lite)."""

from .FeatureManager import ABSENT, Feature, FeatureManager, bucket

__all__ = ["ABSENT", "Feature", "FeatureManager", "bucket"]
