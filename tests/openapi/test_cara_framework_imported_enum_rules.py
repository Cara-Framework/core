"""An ``in:`` rule built from an imported constant publishes its enum.

``f"in:{','.join(LANE_KEYS)}"`` with ``LANE_KEYS`` from a kernel module, or
``Product.CONDITIONS`` off a model, resolved to nothing: the extractor knew
only constants defined inside the requests tree, so about a dozen Synkronus
requests (Action Center ``lane``, product ``condition``, refund ``reason`` …)
went out with no ``enum`` and a client learned the vocabulary from 422s.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

from cara.openapi import FormRequestSchemaExtractor

VOCAB = "cara_fw_enum_vocab"


def _project(tmp_path: Path, monkeypatch, request_source: str) -> Path:
    package = tmp_path / "src" / VOCAB
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "Lanes.py").write_text(
        textwrap.dedent(
            """
            from decimal import Decimal

            LANE_KEYS = ("selling", "stock", "money")
            PACKS = {"large": (3, Decimal("9.99")), "small": (1, Decimal("4.99"))}
            STATUSES = frozenset({"open", "closed"})

            def live_marketplaces():
                raise AssertionError("the extractor must never call app code")
            """
        )
    )
    (package / "Models.py").write_text(
        textwrap.dedent(
            """
            class _Meta(type):
                def __getattr__(cls, name):
                    raise AssertionError("a model metaclass hook must not run")

            class Product(metaclass=_Meta):
                CONDITIONS = ("new", "used")
            """
        )
    )
    monkeypatch.syspath_prepend(str(tmp_path / "src"))
    requests = tmp_path / "requests"
    requests.mkdir()
    (requests / "Things.py").write_text(textwrap.dedent(request_source))
    return requests


def test_imported_constants_and_model_attributes_become_enums(tmp_path, monkeypatch):
    requests = _project(
        tmp_path,
        monkeypatch,
        f'''
        from {VOCAB}.Lanes import LANE_KEYS, PACKS, STATUSES, live_marketplaces
        from {VOCAB}.Models import Product


        class ThingRequest(FormRequest):
            """POST /things body."""

            def rules(self):
                return {{
                    "lane": f"nullable|string|in:{{','.join(LANE_KEYS)}}",
                    "pack": f"required|string|in:{{','.join(sorted(PACKS))}}",
                    "status": f"nullable|string|in:{{','.join(STATUSES)}}",
                    "condition": f"nullable|string|in:{{','.join(Product.CONDITIONS)}}",
                    "market": f"required|string|in:{{','.join(live_marketplaces())}}",
                }}
        ''',
    )

    schema = FormRequestSchemaExtractor(requests).extract()["ThingRequest"]
    properties = schema["properties"]

    assert properties["lane"]["enum"] == ["selling", "stock", "money"]
    assert properties["pack"]["enum"] == ["large", "small"]
    # An unordered set publishes sorted: the spec never reshuffles per run.
    assert properties["status"]["enum"] == ["closed", "open"]
    assert properties["condition"]["enum"] == ["new", "used"]
    # A function is never called to document a rule; that rule stays partial.
    assert "enum" not in properties["market"]
    assert schema["x-cara-rules-partial"] is True


def test_a_module_level_tuple_of_model_constants_resolves(tmp_path, monkeypatch):
    requests = _project(
        tmp_path,
        monkeypatch,
        f'''
        from {VOCAB}.Models import Product

        _STATES = (Product.CONDITIONS[0],)
        _ALL = tuple(Product.CONDITIONS)


        class StateRequest(FormRequest):
            """GET /things query."""

            def rules(self):
                return {{"state": f"nullable|string|in:{{','.join(_ALL)}}"}}
        ''',
    )

    schema = FormRequestSchemaExtractor(requests).extract()["StateRequest"]

    assert schema["properties"]["state"]["enum"] == ["new", "used"]
    assert "x-cara-rules-partial" not in schema


def test_an_unimportable_module_leaves_the_rule_partial(tmp_path, monkeypatch):
    requests = _project(
        tmp_path,
        monkeypatch,
        '''
        from no_such_vocabulary.anywhere import KEYS


        class GoneRequest(FormRequest):
            """GET /gone query."""

            def rules(self):
                return {"k": f"nullable|string|in:{','.join(KEYS)}"}
        ''',
    )

    schema = FormRequestSchemaExtractor(requests).extract()["GoneRequest"]

    assert "enum" not in schema["properties"]["k"]
    assert schema["x-cara-rules-partial"] is True
