"""ModelQueryDiscipline follows a model reached through its imported MODULE.

The live defect: a model was recognised only when its class name was
imported (``from app.models import Product``). ``import app.models as m``
then ``m.Product.where(...)`` is the same reach, but the chain's root name
``m`` was never a model name, so the query was invisible.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cara.architecture.scanners.ModelQueryDiscipline import ModelQueryDiscipline

from ._fixtures import make_manifest, write


def _messages(root: Path, source: str) -> list[str]:
    write(root / "app" / "services" / "Report.py", source)
    return [str(f) for f in ModelQueryDiscipline.scan(make_manifest(root))]


@pytest.mark.parametrize(
    "source",
    [
        "import app.models as m\n\n\ndef run():\n"
        "    return m.Product.where('status', 'active').get()\n",
        "from app import models\n\n\ndef run():\n"
        "    return models.Product.where('status', 'active').get()\n",
        "import app.models\n\n\ndef run():\n"
        "    return app.models.Product.where('status', 'active').get()\n",
    ],
)
def test_a_query_through_an_imported_models_module_is_a_finding(
    tmp_path: Path, source: str
):
    assert any("Product.where(...)" in m for m in _messages(tmp_path, source))


def test_a_non_model_attribute_of_the_models_module_is_left_alone(tmp_path: Path):
    source = (
        "import app.models as m\n\n\ndef run(kind):\n"
        "    return m.RETENTION_CLASSES.get(kind)\n"
    )

    assert _messages(tmp_path, source) == []


def test_an_unrelated_module_alias_is_left_alone(tmp_path: Path):
    source = (
        "import app.services.reports as m\n\n\ndef run():\n"
        "    return m.Product.where('status', 'active').get()\n"
    )

    assert _messages(tmp_path, source) == []
