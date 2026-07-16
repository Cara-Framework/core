from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from cara.http import Response


def _payload(response: Response) -> dict:
    return json.loads(response.content)


@pytest.mark.parametrize("through_factory", [False, True])
def test_paginated_response_uses_canonical_cursor_metadata(
    through_factory: bool,
) -> None:
    response = Response(MagicMock())
    target = response.factory if through_factory else response

    configured = target.paginated(
        [{"id": 1}],
        limit=25,
        has_more=True,
        next_cursor="signed-token",
        prev_cursor="signed-previous-token",
        source_counts={"amazon": 1},
    )

    assert configured is response
    assert _payload(response) == {
        "data": [{"id": 1}],
        "meta": {
            "limit": 25,
            "has_more": True,
            "next_cursor": "signed-token",
            "prev_cursor": "signed-previous-token",
            "source_counts": {"amazon": 1},
        },
    }


@pytest.mark.parametrize("through_factory", [False, True])
def test_final_page_omits_optional_previous_cursor(through_factory: bool) -> None:
    response = Response(MagicMock())
    target = response.factory if through_factory else response

    target.paginated([], limit=25, has_more=False, next_cursor=None)

    assert _payload(response)["meta"] == {
        "limit": 25,
        "has_more": False,
        "next_cursor": None,
    }


@pytest.mark.parametrize("through_factory", [False, True])
@pytest.mark.parametrize(
    "kwargs",
    [
        {"limit": True, "has_more": False, "next_cursor": None},
        {"limit": 0, "has_more": False, "next_cursor": None},
        {"limit": 101, "has_more": False, "next_cursor": None},
        {"limit": "25", "has_more": False, "next_cursor": None},
        {"limit": 25, "has_more": 1, "next_cursor": "token"},
        {"limit": 25, "has_more": True, "next_cursor": None},
        {"limit": 25, "has_more": True, "next_cursor": ""},
        {"limit": 25, "has_more": False, "next_cursor": "token"},
        {
            "limit": 25,
            "has_more": False,
            "next_cursor": None,
            "prev_cursor": "",
        },
    ],
)
def test_paginated_response_rejects_inconsistent_metadata(
    through_factory: bool,
    kwargs: dict,
) -> None:
    response = Response(MagicMock())
    target = response.factory if through_factory else response

    with pytest.raises((TypeError, ValueError)):
        target.paginated([], **kwargs)
