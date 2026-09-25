import tomllib
from pathlib import Path

import editguard


def test_version_matches_pyproject() -> None:
    pyproject = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    assert editguard.__version__ == pyproject["project"]["version"]
