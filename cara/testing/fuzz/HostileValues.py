"""Values no well-behaved client sends, as Hypothesis strategies."""

from __future__ import annotations

from hypothesis import strategies as st


class HostileValues:
    """Pools and strategies for the hostile half of a fuzz request.

    Every pool entry is here because some handler once mis-handled it: SQL
    metacharacters reaching a raw ``ORDER BY``, an integer past ``int8`` reaching
    a column bind, a NUL byte reaching Postgres text, a lone surrogate reaching
    a UTF-8 encoder, a JSON number past float range reaching ``Decimal``.
    """

    SQL_FRAGMENTS = (
        "' OR '1'='1",
        "'; DROP TABLE users; --",
        "1) OR pg_sleep(30)--",
        '") OR 1=1 --',
        "created_at desc; select pg_sleep(30)",
        "(select pg_sleep(30))",
        "id, (select 1/0)",
        "%",
        "_",
        "\\",
        "'",
        '"',
        "%s",
        "%(x)s",
    )
    STRINGY = (
        "",
        " ",
        "\x00",
        "a\x00b",
        "\ud800",
        "\udfff",
        "‮",
        "﻿",
        "🙂",
        "𝒳" * 300,
        "a" * 5000,
        "null",
        "undefined",
        "NaN",
        "Infinity",
        "-0",
        "0x1F",
        "1e400",
        "../../etc/passwd",
        "<script>alert(1)</script>",
        "{{7*7}}",
        "${7*7}",
        "\r\nX-Injected: 1",
    )
    HUGE_INTS = (
        -1,
        0,
        2**31 - 1,
        2**31,
        2**53 + 1,
        2**63 - 1,
        2**63,
        2**64,
        -(2**63) - 1,
        10**60,
    )
    FLOATS = (
        0.1,
        -0.0,
        1e308,
        -1e308,
        5e-324,
        1e-30,
        float("inf"),
        float("-inf"),
        float("nan"),
    )
    ODD_SHAPES = (None, True, False, [], {}, [[]], [None], {"a": 1}, [{}], {"": ""})
    QUERY_EDGES = (
        "1.5",
        "1e5",
        "-5",
        "+5",
        "%",
        "%zz",
        "%00",
        "%ff",
        "a&b=c",
        "a=b",
        "#",
    )
    PATH_SEGMENTS = (
        "0",
        "-1",
        "1.5",
        str(2**31),
        str(2**63),
        str(10**60),
        "null",
        "undefined",
        "%00",
        "a%00b",
        "%ED%A0%80",
        "%F0%9F%99%82",
        "%27%20OR%20%271%27%3D%271",
        "%25",
        "%2F",
        "..",
        "...",
        "a" * 400,
        "a" * 40000,
        "XXX" + "0" * 26,
    )
    RAW_BODIES = (
        b"",
        b"{",
        b"null",
        b"[]",
        b"[1,2]",
        b'"text"',
        b"0",
        b'{"a":1,"a":2}',
        b"\xff\xfe\x00",
        b'{"x":"\xff"}',
        b"[" * 3000,
        b'{"a":' * 3000,
        b'{"n":' + b"9" * 5000 + b"}",
        b'{"n":1e999999}',
        b'{"n":NaN}',
    )
    RAW_QUERIES = (
        b"",
        b"&",
        b"=",
        b"&&==",
        b"%",
        b"a",
        b"a=%zz",
        b"limit=%00",
        b"[]=1",
        b"a[b][c][d]=1",
        b"%ff=%ff",
    )
    RAW_CONTENT_TYPES = (
        "application/json",
        "text/plain",
        "multipart/form-data",
        "multipart/form-data; boundary=x",
        "application/x-www-form-urlencoded",
        "application/json; charset=latin-1",
        None,
        "",
    )
    #: Header values a proxy or a hostile client can put on the wire; every
    #: one is ASCII with no edge whitespace, or an HTTP client (h11) refuses to
    #: send it and the test fails on the client instead of probing the server.
    HEADER_VALUES = (
        "",
        "a" * 8000,
        "Bearer",
        "Bearer a.b.c",
        "Bearer " + "A" * 4000,
        "Basic Zm9vOmJhcg==",
        "Token null",
        "1.2.3.4, 5.6.7.8, unknown",
        "not-an-ip",
        "::ffff:127.0.0.1",
        "../../",
        "%00",
        "*/*;q=x",
        "en-US;q=1e999,tr;q=-1",
        "gzip;q=NaN",
    )

    @classmethod
    def string(cls) -> st.SearchStrategy:
        return st.one_of(
            st.sampled_from(cls.SQL_FRAGMENTS),
            st.sampled_from(cls.STRINGY),
            st.text(max_size=60),
            st.text(alphabet=st.characters(codec="utf-8"), max_size=20),
        )

    @classmethod
    def scalar(cls) -> st.SearchStrategy:
        return st.one_of(
            cls.string(),
            st.sampled_from(cls.HUGE_INTS),
            st.sampled_from(cls.FLOATS),
            st.sampled_from(cls.ODD_SHAPES),
        )

    @classmethod
    def query_value(cls) -> st.SearchStrategy:
        return st.one_of(
            cls.string(),
            st.sampled_from([str(n) for n in cls.HUGE_INTS]),
            st.sampled_from(cls.QUERY_EDGES),
        )

    @classmethod
    def header_value(cls) -> st.SearchStrategy:
        printable = st.text(
            alphabet=st.characters(min_codepoint=0x20, max_codepoint=0x7E), max_size=200
        )
        return st.one_of(st.sampled_from(cls.HEADER_VALUES), printable.map(str.strip))

    @classmethod
    def for_array(cls, value: list) -> st.SearchStrategy:
        """Empty, duplicated, oversized and mis-typed variants of a bulk array."""
        duplicate = (value * 4)[:40] if value else ["x", "x"]
        return st.sampled_from(
            [
                [],
                duplicate,
                (value or ["x"]) * 2000,
                [None],
                [[]],
                [{}],
                [2**64],
                ["\x00"],
                ["\ud800"],
                [True, False],
                {"0": "a"},
                "a,b",
            ]
        )

    @classmethod
    def mutate_object(cls, body: dict) -> st.SearchStrategy:
        """Make exactly one field of a valid body hostile (or add a foreign one)."""
        options = []
        for key, value in body.items():
            options.append(
                st.tuples(st.just(key), cls.scalar()).map(lambda p: {**body, p[0]: p[1]})
            )
            if isinstance(value, list):
                options.append(
                    st.tuples(st.just(key), cls.for_array(value)).map(
                        lambda p: {**body, p[0]: p[1]}
                    )
                )
            if isinstance(value, dict) and value:
                options.append(
                    st.tuples(st.just(key), cls.mutate_object(value)).map(
                        lambda p: {**body, p[0]: p[1]}
                    )
                )
        options.append(
            st.tuples(
                st.sampled_from(
                    ["id", "user_id", "tenant_id", "__class__", "\x00", "a" * 5000]
                ),
                cls.scalar(),
            ).map(lambda p: {**body, p[0]: p[1]})
        )
        return st.one_of(*options)
