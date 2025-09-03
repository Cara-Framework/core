"""
Rate Limit Provider for the Cara framework.

This module provides the deferred service provider that configures and registers the rate limiting
functionality, making the RateLimiter available throughout the application based on configuration.
"""

from typing import Any, Dict, List

from cara.configuration import config
from cara.exceptions import RateLimitConfigurationException
from cara.foundation import DeferredProvider
from cara.rates import RateLimiter


class RateLimitProvider(DeferredProvider):
    """
    Deferred provider for rate limiting.

    Reads config and registers the RateLimiter under 'rate'.
    """

    @classmethod
    def provides(cls) -> List[str]:
        return ["rate"]

    def register(self) -> None:
        default_driver = config("rate.default", None)
        drivers_cfg: Dict[str, Any] = config("rate.drivers", {})

        if not default_driver or default_driver not in drivers_cfg:
            raise RateLimitConfigurationException(
                "Missing or invalid 'rate.default' or 'rate.drivers' config."
            )

        driver_opts = drivers_cfg.get(default_driver)
        # For now, we only have a "fixed" driver
        if default_driver != RateLimiter.driver_name:
            raise RateLimitConfigurationException(
                f"Rate limit driver '{default_driver}' not supported."
            )

        limiter = RateLimiter(
            application=self.application,
            options=driver_opts,
        )
        self.application.bind("rate", limiter)

    def boot(self) -> None:
        pass
