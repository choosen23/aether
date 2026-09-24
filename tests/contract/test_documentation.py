from __future__ import annotations

from pathlib import Path


def test_readme_commands_exist_in_makefile() -> None:
    readme = Path("README.md").read_text()
    makefile = Path("Makefile").read_text()
    for command in ("make test", "make lint", "make smoke"):
        assert command in readme
        assert f"{command.removeprefix('make ')}:" in makefile


def test_provenance_names_real_and_derived_fields() -> None:
    text = Path("docs/data-provenance.md").read_text()
    assert "Real: OpenSky" in text
    assert "Derived: interpolation" in text
    assert "Simulated: none in this slice" in text
