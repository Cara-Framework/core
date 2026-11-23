"""``@scheduled`` gives every class its own spec list.

The decorator appended to the list ``getattr`` found, which on a subclass is
the parent's class attribute: a child's schedule appeared on the parent and
on every sibling.
"""

from __future__ import annotations

from cara.decorators.Schedule import scheduled


def test_a_subclass_schedule_stays_on_the_subclass():
    @scheduled(daily=(3, 0), identifier="parent")
    class Parent:
        pass

    @scheduled(hourly=5, identifier="child")
    class Child(Parent):
        pass

    @scheduled(cron="*/10 * * * *", identifier="sibling")
    class Sibling(Parent):
        pass

    def ids(cls):
        return [spec["identifier"] for spec in cls._schedule_specs]

    assert ids(Parent) == ["parent"]
    assert ids(Child) == ["parent", "child"]  # still inherits the parent's
    assert ids(Sibling) == ["parent", "sibling"]


def test_stacked_decorators_on_one_class_accumulate():
    @scheduled(daily=(1, 0), identifier="a")
    @scheduled(hourly=0, identifier="b")
    class Job:
        pass

    assert [spec["identifier"] for spec in Job._schedule_specs] == ["b", "a"]
