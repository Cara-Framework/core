"""The routing-key matcher and the pattern-overlap test, pinned as rules.

Both functions exist because a topology guard has to answer two questions the
router only answers implicitly: "does this key reach that queue?" and "do two
bindings deliver the same message twice?". Products used to re-implement both
inside their own guards, so a guard whose purpose was proving routing correct
carried its own private model of routing — free to drift from the router it
was proving things about.

``_matches_pattern`` on the router now delegates here, so these pins cover the
dispatch path as well as the guards.
"""

from __future__ import annotations

from cara.queues.routing import QueueRouter, matches_pattern, patterns_overlap


class TestMatchesPattern:
    def test_a_literal_pattern_matches_only_itself(self):
        assert matches_pattern("mail.send.default", "mail.send.default")
        assert not matches_pattern("mail.send.high", "mail.send.default")

    def test_star_stands_for_exactly_one_word(self):
        assert matches_pattern("jobs.import.high", "jobs.*.high")
        assert matches_pattern("jobs.export.high", "jobs.*.high")

    def test_star_does_not_span_a_dot(self):
        """``*`` is one word, so it cannot absorb the separator.

        If it could, ``jobs.*.high`` would also claim ``jobs.a.b.high`` and a
        key would silently reach a queue its author never bound it to.
        """
        assert not matches_pattern("jobs.a.b.high", "jobs.*.high")

    def test_unequal_word_counts_never_match(self):
        assert not matches_pattern("jobs.high", "jobs.*.high")
        assert not matches_pattern("jobs.import.high", "jobs.*")

    def test_the_multi_word_hash_wildcard_is_not_supported(self):
        """``#`` is a plain word here, not a wildcard.

        A broker's topic exchange would treat ``#`` as "zero or more words",
        which makes the set of keys reaching a queue unbounded — and an
        exactly-once topology proof over an unbounded set proves nothing. The
        matcher therefore refuses to model it, and a product whose bindings
        contain ``#`` must know its guard no longer describes the broker.
        """
        assert not matches_pattern("jobs.import.high", "jobs.#")
        assert not matches_pattern("jobs.import.high", "#")
        assert matches_pattern("jobs.#.high", "jobs.#.high")


class TestPatternsOverlap:
    def test_identical_patterns_overlap(self):
        assert patterns_overlap("jobs.*.high", "jobs.*.high")

    def test_a_wildcard_overlaps_the_literal_it_covers(self):
        assert patterns_overlap("jobs.*.high", "jobs.import.high")
        assert patterns_overlap("jobs.import.high", "jobs.*.high")

    def test_two_different_literals_in_the_same_slot_do_not_overlap(self):
        assert not patterns_overlap("jobs.import.high", "jobs.export.high")

    def test_overlap_requires_equal_word_counts(self):
        assert not patterns_overlap("jobs.*", "jobs.*.high")

    def test_overlap_is_symmetric(self):
        pairs = [
            ("jobs.*.high", "jobs.import.*"),
            ("mail.send.default", "mail.*.default"),
            ("jobs.import.high", "jobs.export.high"),
        ]
        for one, other in pairs:
            assert patterns_overlap(one, other) == patterns_overlap(other, one)

    def test_overlap_is_not_the_same_question_as_matching(self):
        """Two wildcard patterns overlap although neither matches the other.

        A guard that reached for the key matcher to answer the fan-out
        question would find ``jobs.*.high`` does not "match" ``jobs.import.*``
        and wrongly conclude the two bindings are disjoint — while
        ``jobs.import.high`` is delivered to both.
        """
        assert not matches_pattern("jobs.*.high", "jobs.import.*")
        assert patterns_overlap("jobs.*.high", "jobs.import.*")
        assert matches_pattern("jobs.import.high", "jobs.*.high")
        assert matches_pattern("jobs.import.high", "jobs.import.*")


class TestRouterDelegatesToTheSharedRule:
    def test_the_router_answers_exactly_what_the_function_answers(self):
        """The dispatch path and the guard path share one implementation."""
        cases = [
            ("jobs.import.high", "jobs.*.high"),
            ("jobs.import.high", "jobs.*.default"),
            ("jobs.a.b.high", "jobs.*.high"),
            ("mail.send.default", "mail.send.default"),
            ("jobs.import.high", "jobs.#"),
        ]
        for routing_key, pattern in cases:
            assert QueueRouter._matches_pattern(
                None, routing_key, pattern
            ) is matches_pattern(routing_key, pattern)
