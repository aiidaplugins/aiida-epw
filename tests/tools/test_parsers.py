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


def test_parse_epw_a2f_proj(files_path: Path, data_regression):
    """Parse an existing ``aiida.a2f_proj`` file."""
    # Use the a2f directory where we put the dummy a2f_proj
    a2f_proj_path = files_path / "tools" / "parsers" / "a2f" / "aiida.a2f_proj"
    content = a2f_proj_path.read_text()

    parsed = parsers.parse_epw_a2f_proj(content)

    assert "frequency" in parsed
    assert "a2f_proj" in parsed
    
    regression_data = {
        "frequency": parsed["frequency"].tolist(),
        "a2f_proj": parsed["a2f_proj"].tolist(),
    }
    data_regression.check(regression_data)


def test_parse_epw_lambda_FS(files_path: Path, data_regression):
    """Parse an existing ``aiida.lambda_FS`` file."""
    lambda_fs_path = files_path / "tools" / "parsers" / "lambda" / "aiida.lambda_FS"
    content = lambda_fs_path.read_text()

    parsed = parsers.parse_epw_lambda_FS(content)

    for key in ("kpoints", "band", "Enk", "lambda"):
        assert key in parsed
    
    regression_data = {
        "kpoints": parsed["kpoints"].tolist(),
        "band": parsed["band"].tolist(),
        "Enk": parsed["Enk"].tolist(),
        "lambda": parsed["lambda"].tolist(),
    }
    data_regression.check(regression_data)


def test_parse_epw_lambda_k_pairs(files_path: Path, data_regression):
    """Parse an existing ``aiida.lambda_k_pairs`` file."""
    lambda_k_pairs_path = files_path / "tools" / "parsers" / "lambda" / "aiida.lambda_k_pairs"
    content = lambda_k_pairs_path.read_text()

    parsed = parsers.parse_epw_lambda_k_pairs(content)

    assert "lambda_nk" in parsed
    assert "rho" in parsed
    
    regression_data = {
        "lambda_nk": parsed["lambda_nk"].tolist(),
        "rho": parsed["rho"].tolist(),
    }
    data_regression.check(regression_data)


def test_parse_epw_stdout(files_path: Path, data_regression):
    """Parse a fake ``stdout`` file."""
    stdout_path = files_path / "tools" / "parsers" / "default" / "aiida.out"
    content = stdout_path.read_text()

    # Pass version explicitly as string
    parsed = parsers.parse_epw_stdout(content, code_version="6.0")

    # Filter out potential erratic values or just dump everything if stable
    data_regression.check(parsed)


def test_parse_epw_dos(files_path: Path, data_regression):
    """Parse an existing ``aiida.dos`` file using the basic parser."""
    dos_path = files_path / "tools" / "parsers" / "a2f" / "aiida.dos"
    content = dos_path.read_text()

    parsed = parsers.parse_epw_dos(content)

    assert "Energy" in parsed
    assert "EDOS" in parsed
    
    regression_data = {
        "Energy": parsed["Energy"].tolist()[:10],
        "EDOS": parsed["EDOS"].tolist()[:10],
    }
    data_regression.check(regression_data)


def test_parse_epw_phdos(files_path: Path, data_regression):
    """Parse an existing ``aiida.phdos`` file using the basic parser."""
    phdos_path = files_path / "tools" / "parsers" / "a2f" / "aiida.phdos"
    content = phdos_path.read_text()

    parsed = parsers.parse_epw_phdos(content)

    assert "Frequency" in parsed
    assert "PHDOS" in parsed
    
    regression_data = {
        "Frequency": parsed["Frequency"].tolist()[:10],
        "PHDOS": parsed["PHDOS"].tolist()[:10],
    }
    data_regression.check(regression_data)


def test_parse_epw_gap_function(files_path: Path, data_regression):
    """Parse an existing gap function file."""
    # Re-using imag_iso file as a generic gap function file
    gap_path = files_path / "tools" / "parsers" / "full_iso_eliashberg" / "aiida.imag_iso_003.00"
    content = gap_path.read_text()

    # Skipping first row as it has comments usually or just headers
    parsed = parsers.parse_epw_gap_function(content, skiprows=1)

    assert isinstance(parsed, numpy.ndarray)
    
    regression_data = {
        "gap_function": parsed.tolist()[:10],
    }
    data_regression.check(regression_data)



