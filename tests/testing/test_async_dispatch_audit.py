"""The discarded-coroutine audit, and every shape it must leave alone.

The rule is narrow on purpose: a bare expression statement is the only place a
dispatch coroutine has no consumer at all. Widening it to "a call with no
``await`` on the line" is what the line-based product copies did, and it made
them guess about `gather` lists, wrapper variables and continuation lines.
"""

from __future__ import annotations

import textwrap

from cara.testing.audits import AsyncDispatchAudit


def _scan(source: str) -> list[str]:
    audit = AsyncDispatchAudit()
    return [str(f) for f in audit.scan_source(textwrap.dedent(source), "m.py")]


class TestDiscardedDispatch:
    def test_a_bare_facade_dispatch_is_flagged(self):
        findings = _scan(
            """
            async def handle(self):
                Event.fire(ProductSaved(1))
            """
        )
        assert len(findings) == 1
        assert "Event.fire" in findings[0]

    def test_every_default_dispatch_name_is_watched(self):
        findings = _scan(
            """
            async def handle(self):
                Event.dispatch(a)
                Event.fire(b)
                Bus.dispatch(c)
                safe_dispatch(d)
            """
        )
        assert len(findings) == 4

    def test_a_sync_method_discarding_a_dispatch_is_the_same_finding(self):
        """The original bug: a sync helper that "fires" an event and does not."""
        findings = _scan(
            """
            def notify(self):
                Event.fire(Thing())
            """
        )
        assert len(findings) == 1

    def test_an_aliased_receiver_is_still_the_same_call(self):
        findings = _scan(
            """
            async def handle(self):
                self.safe_dispatch(job)
                cara.facades.Event.fire(thing)
            """
        )
        assert len(findings) == 2


class TestConsumersAreNotFindings:
    def test_await_is_a_consumer(self):
        assert (
            _scan(
                """
                async def handle(self):
                    await Event.fire(thing)
                """
            )
            == []
        )

    def test_create_task_is_a_consumer(self):
        assert (
            _scan(
                """
                async def handle(self):
                    asyncio.create_task(Event.fire(thing))
                """
            )
            == []
        )

    def test_gathering_into_a_list_is_a_consumer(self):
        assert (
            _scan(
                """
                async def handle(self, things):
                    await asyncio.gather(*[Event.fire(t) for t in things])
                """
            )
            == []
        )

    def test_assignment_is_a_consumer(self):
        assert (
            _scan(
                """
                async def handle(self):
                    coroutine = Event.fire(thing)
                    await coroutine
                """
            )
            == []
        )

    def test_returning_the_coroutine_is_a_consumer(self):
        """Returning a coroutine delegates the await to the caller."""
        assert (
            _scan(
                """
                def notify(self, thing):
                    return Event.fire(thing)
                """
            )
            == []
        )


class TestNonCodeIsNotCode:
    def test_a_docstring_showing_the_call_is_not_a_finding(self):
        assert (
            _scan(
                '''
                async def handle(self):
                    """Callers used to write Event.fire(thing) here."""
                    await Event.fire(thing)
                '''
            )
            == []
        )

    def test_a_comment_is_not_a_finding(self):
        assert (
            _scan(
                """
                async def handle(self):
                    # Event.fire(thing)
                    await Event.fire(thing)
                """
            )
            == []
        )

    def test_a_definition_named_like_the_call_is_not_a_call(self):
        assert (
            _scan(
                """
                async def safe_dispatch(job):
                    return job
                """
            )
            == []
        )

    def test_a_multiline_call_is_still_seen(self):
        findings = _scan(
            """
            async def handle(self):
                Event.fire(
                    ProductSaved(1),
                )
            """
        )
        assert len(findings) == 1

    def test_unparseable_source_is_skipped(self):
        assert _scan("async def broken(:\n") == []


class TestExtensibility:
    def test_a_product_may_add_its_own_wrapper(self):
        audit = AsyncDispatchAudit({"Notifier.push"})
        findings = audit.scan_source("def f():\n    Notifier.push(x)\n", "m.py")
        assert len(findings) == 1
        assert "Notifier.push" in str(findings[0])

    def test_narrowing_the_inventory_narrows_the_rule(self):
        audit = AsyncDispatchAudit({"Bus.dispatch"})
        assert audit.scan_source("def f():\n    Event.fire(x)\n", "m.py") == []


class TestTreeScan:
    def test_only_named_directories_are_scanned(self, tmp_path):
        (tmp_path / "jobs").mkdir()
        (tmp_path / "vendor").mkdir()
        (tmp_path / "jobs" / "A.py").write_text("def f():\n    Event.fire(x)\n")
        (tmp_path / "vendor" / "B.py").write_text("def f():\n    Event.fire(x)\n")
        audit = AsyncDispatchAudit()
        findings = audit.scan_tree(tmp_path, ["jobs"])
        assert [f.path for f in findings] == ["jobs/A.py"]

    def test_no_directories_means_the_whole_tree(self, tmp_path):
        (tmp_path / "jobs").mkdir()
        (tmp_path / "jobs" / "A.py").write_text("def f():\n    Event.fire(x)\n")
        audit = AsyncDispatchAudit()
        assert len(audit.scan_tree(tmp_path)) == 1

    def test_the_report_names_every_site(self, tmp_path):
        (tmp_path / "jobs").mkdir()
        (tmp_path / "jobs" / "A.py").write_text("def f():\n    Event.fire(x)\n")
        audit = AsyncDispatchAudit()
        findings = audit.scan_tree(tmp_path)
        assert "jobs/A.py:2" in audit.report(findings)
