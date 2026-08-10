"""``ResponseFactory.file()`` writes headers through its declared collaborator.

The factory declares its collaborator as ``BaseResponse``, but ``file()``
used to call ``self.response.header(...)`` and ``.with_headers(...)`` —
methods that exist only on the ``Response`` subclass. It never blew up in
production because both construction sites happen to hand it a full
``Response``; a bare ``BaseResponse`` would have raised AttributeError after
the headers were half-written and before the body was read. Cara's own
``CollaboratorCalls`` scanner reported exactly these five call sites and
nothing else in the whole framework tree, so this test pins the fix: the
method serves a file through a plain ``BaseResponse``, and the resulting
headers are byte-identical to what the ``Response`` path produced.
"""

from __future__ import annotations

from cara.http.response.BaseResponse import BaseResponse
from cara.http.response.Response import Response
from cara.http.response.ResponseFactory import ResponseFactory


def test_file_serves_through_a_bare_base_response(tmp_path):
    target = tmp_path / "report.csv"
    target.write_bytes(b"id,name\n1,widget\n")

    response = BaseResponse(application=None)
    ResponseFactory(response).file(
        str(target),
        filename="report.csv",
        content_type="text/csv",
        headers={"X-Report-Run": "42"},
    )

    headers = {k.lower(): v for k, v in response.header_bag.all().items()}
    assert headers["content-type"] == "text/csv"
    assert headers["content-length"] == str(target.stat().st_size)
    assert headers["accept-ranges"] == "bytes"
    assert headers["content-disposition"] == 'attachment; filename="report.csv"'
    assert headers["x-report-run"] == "42"
    assert response.content == b"id,name\n1,widget\n"


def test_file_flips_the_explicit_content_type_flag_on_the_response(tmp_path):
    """The flag ``_finalize_response`` reads must be the one the factory set.

    Routing through ``self.headers`` keeps writing to the Response's own
    HeaderManager, so ``ContentTypeDetector`` stays out of the way — the
    same invariant the constructor's docstring describes for ``json()``.
    """
    target = tmp_path / "logo.png"
    target.write_bytes(b"\x89PNG\r\n\x1a\n")

    response = Response(application=None)
    ResponseFactory(response).file(str(target), content_type="image/png")

    assert response.headers._content_type_explicitly_set is True
    assert response.headers.get("Content-Type") == "image/png"


def test_missing_file_reports_404_and_writes_no_headers(tmp_path):
    response = BaseResponse(application=None)
    ResponseFactory(response).file(str(tmp_path / "absent.bin"))

    assert response.get_status_code() == 404
    assert response.content == b"File not found"
    assert response.header_bag.all() == {}
