"""Shared path-parameter validation bridge for controllers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cara.http import FormRequest, Request


async def validate_path_param(
    request: Request,
    param_name: str,
    form_request_cls: type[FormRequest],
) -> dict[str, Any]:
    """Bridge a route path-parameter into a FormRequest validation flow.

    Stashes the path segment into the request's input view so that the
    FormRequest validator sees it as body/query input, then runs
    validation and returns the validated dict.
    """
    request.set_input(param_name, request.param(param_name))
    return await form_request_cls().validate_request(request)
