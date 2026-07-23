"""Logger message formatting and caller-label contracts."""

from __future__ import annotations

from unittest.mock import patch

from loguru import logger as _loguru_logger

from cara.logging import Logger


class TestInterpolateHelper:
    """The pure interpolation helper that mirrors ``LogFake._record``."""

    def test_printf_single_arg(self) -> None:
        assert Logger._interpolate("failed: %s", ("boom",)) == "failed: boom"

    def test_printf_multiple_args(self) -> None:
        assert (
            Logger._interpolate("user %s -> %s", (42, "ok")) == "user 42 -> ok"
        )

    def test_no_args_passthrough(self) -> None:
        # A bare message with no args must be returned untouched — even if
        # it happens to contain a literal '%' (e.g. "100% done").
        assert Logger._interpolate("100% done", ()) == "100% done"

    def test_mismatch_falls_back_to_append(self) -> None:
        # No placeholder but an arg supplied: never raise — append instead
        # so the datum is preserved.
        out = Logger._interpolate("no placeholder here", ("ctx",))
        assert out == "no placeholder here ctx"

    def test_dict_positional_arg_does_not_raise(self) -> None:
        # Some call sites pass a context dict as the sole positional arg.
        # That can't printf-format against a placeholder-free template;
        # it must degrade to a lossless append, never a TypeError.
        out = Logger._interpolate("failed", ({"user_id": 7},))
        assert "failed" in out
        assert "user_id" in out


class TestModuleDisplayName:
    """Caller labels preserve class-style capitals and normalize snake case."""

    def test_preserves_camel_case(self) -> None:
        assert Logger._module_display_name("app.support.MarketplaceRegistry") == (
            "MarketplaceRegistry"
        )

    def test_normalizes_snake_case(self) -> None:
        assert Logger._module_display_name("app.support.marketplace_registry") == (
            "MarketplaceRegistry"
        )


class TestPublicMethodsForwardArgs:
    """Every public level method must thread ``*args`` into ``_log``."""

    def test_each_level_forwards_message_args(self) -> None:
        logger = Logger()
        for level in ("debug", "info", "warning", "error", "critical", "exception"):
            with patch.object(logger, "_log") as log_spy:
                getattr(logger, level)("v=%s n=%s", "x", 3, category="c")
            assert log_spy.call_count == 1
            kwargs = log_spy.call_args.kwargs
            assert kwargs.get("message_args") == ("x", 3), (
                f"{level} dropped its positional args"
            )


class TestEndToEndInterpolation:
    """The fully-wired logger emits the interpolated text to its sink."""

    def test_emitted_message_is_interpolated(self) -> None:
        logger = Logger()
        captured: list[str] = []
        sink_id = _loguru_logger.add(
            lambda m: captured.append(m.record["message"]), level="DEBUG"
        )
        try:
            logger.error("delivery failed for user %s: %s", 99, "timeout")
        finally:
            _loguru_logger.remove(sink_id)

        assert any("delivery failed for user 99: timeout" in m for m in captured)
        assert not any("%s" in m for m in captured)
