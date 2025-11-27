"""Tests for helper parsing functions in ``aiida_epw.tools.parsers``.

These tests deliberately use real EPW output files placed under ``tests/files``
instead of synthetic strings, and rely on ``pytest-regressions`` to freeze
their parsed output.
"""

from pathlib import Path

import numpy

from aiida_epw.tools import parsers


def test_parse_epw_bands(files_path: Path, data_regression):
    """Parse an existing ``band.eig`` file and regress on the main arrays."""
    bands_path = files_path / "tools" / "parsers" / "bands" / "band.eig"
    content = bands_path.read_text()

    parsed = parsers.parse_epw_bands(content)

    # Basic sanity checks
    assert "kpoints" in parsed
    assert "bands" in parsed
    assert isinstance(parsed["kpoints"], numpy.ndarray)
    assert isinstance(parsed["bands"], numpy.ndarray)
    assert parsed["kpoints"].shape[0] == parsed["bands"].shape[0]

    regression_data = {
        "kpoints": parsed["kpoints"].tolist(),
        "bands": parsed["bands"].tolist(),
    }
    data_regression.check(regression_data)


def test_parse_epw_a2f(files_path: Path, data_regression):
    """Parse an existing ``aiida.a2f`` file and regress on arrays and metadata."""
    a2f_path = files_path / "tools" / "parsers" / "a2f" / "aiida.a2f"
    content = a2f_path.read_text()

    parsed = parsers.parse_epw_a2f(content)

    for key in ("frequency", "a2f", "lambda", "phonon_smearing"):
        assert key in parsed
        assert isinstance(parsed[key], numpy.ndarray)

    assert parsed["frequency"].ndim == 1
    assert parsed["a2f"].ndim == 2
    assert parsed["frequency"].shape[0] == parsed["a2f"].shape[0]

    regression_data = {
        "frequency": parsed["frequency"].tolist()[:10],
        "a2f": parsed["a2f"].tolist()[:10],
        "lambda": parsed["lambda"].tolist(),
        "phonon_smearing": parsed["phonon_smearing"].tolist(),
        "electron_smearing": float(parsed["electron_smearing"]),
        "fermi_window": float(parsed["fermi_window"]),
        "summed_elph_coupling": float(parsed["summed_elph_coupling"]),
    }
    data_regression.check(regression_data)


def test_parse_epw_max_eigenvalue(files_path: Path, data_regression):
    """Parse the max eigenvalue table from a real ``aiida.out`` file."""
    stdout_path = files_path / "tools" / "parsers" / "linearized_iso_eliashberg" / "aiida.out"
    content = stdout_path.read_text()

    parsed = parsers.parse_epw_max_eigenvalue(content)

    assert "max_eigenvalue" in parsed
    assert isinstance(parsed["max_eigenvalue"], numpy.ndarray)

    regression_data = {
        "max_eigenvalue": parsed["max_eigenvalue"].tolist(),
    }
    data_regression.check(regression_data)


def test_parse_epw_eldos(files_path: Path, data_regression):
    """Parse an existing ``aiida.dos`` file and regress on the DOS arrays."""
    dos_path = files_path / "tools" / "parsers" / "a2f" / "aiida.dos"
    content = dos_path.read_text()

    parsed = parsers.parse_epw_eldos(content)

    assert set(parsed.keys()) == {"energy", "edos", "integrated_dos"}

    regression_data = {
        "energy": parsed["energy"].tolist()[:10],
        "edos": parsed["edos"].tolist()[:10],
        "integrated_dos": parsed["integrated_dos"].tolist()[:10],
    }
    data_regression.check(regression_data)


def test_parse_epw_phdos(files_path: Path, data_regression):
    """Parse an existing ``aiida.phdos`` file and regress on the PHDOS arrays."""
    phdos_path = files_path / "tools" / "parsers" / "a2f" / "aiida.phdos"
    content = phdos_path.read_text()

    parsed = parsers.parse_epw_phdos(content)

    assert set(parsed.keys()) == {"frequency", "phdos"}

    regression_data = {
        "frequency": parsed["frequency"].tolist()[:10],
        "phdos": parsed["phdos"].tolist()[:10],
    }
    data_regression.check(regression_data)


def test_parse_epw_imag_iso(files_path: Path, data_regression):
    """Parse isotropic ``imag_iso`` files from a folder mapping."""
    iso_dir = files_path / "tools" / "parsers" / "full_iso_eliashberg"

    file_contents = {
        path.name: path.read_text() for path in sorted(iso_dir.iterdir()) if path.is_file()
    }

    parsed = parsers.parse_epw_imag_iso(file_contents, prefix="aiida")

    assert parsed, "No temperatures were parsed from imag_iso files."

    regression_data = {T: parsed[T].tolist()[:10] for T in sorted(parsed.keys())}
    data_regression.check(regression_data)


def test_parse_epw_imag_aniso_gap0(files_path: Path, data_regression):
    """Parse anisotropic ``imag_aniso_gap0`` files from a folder mapping."""
    aniso_dir = files_path / "tools" / "parsers" / "fsr_aniso_eliashberg"

    file_contents = {
        path.name: path.read_text()
        for path in sorted(aniso_dir.iterdir())
        if path.is_file()
    }

    parsed = parsers.parse_epw_imag_aniso_gap0(file_contents, prefix="aiida")

    assert parsed, "No temperatures were parsed from imag_aniso_gap0 files."

    regression_data = {T: parsed[T].tolist()[:10] for T in sorted(parsed.keys())}
    data_regression.check(regression_data)



