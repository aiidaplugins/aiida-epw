"""Tests for the `EpwParser`."""

import pytest
from aiida import orm
from aiida.common import LinkType
from aiida.plugins.entry_point import (
    format_entry_point_string,
    get_entry_point_string_from_class,
)

from aiida_epw.calculations.epw import EpwCalculation
from aiida_epw.data import (
    A2fData,
    PA2fData,
    DosData,
    PDosData,
    GapFunctionData,
    LambdaFSData,
)
from aiida_epw.parsers.epw import EpwParser


@pytest.mark.parametrize(
    "test_name",
    (
        "default",
        "isotropic_eliashberg",
        "bands",
    ),
)
def test_epw(parse_from_files, data_regression, test_name):
    """Test the `EpwParser`."""
    results, calcfunction = parse_from_files(EpwParser, test_name=test_name)

    assert calcfunction.is_finished, calcfunction.exception
    assert calcfunction.is_finished_ok, calcfunction.exit_message

    data_regression_dict = {
        "output_parameters": results["output_parameters"].get_dict(),
    }
    if test_name == "default":
        assert isinstance(results["a2f"], A2fData)
        data_regression_dict["a2f"] = results["a2f"].get_array("a2f").tolist()[::50]
        data_regression_dict["lambda"] = results["a2f"].get_array("lambda").tolist()
        data_regression_dict["degaussq"] = results["a2f"].get_array("degaussq").tolist()
        assert results["a2f"].get_spectrum().shape[1] == 10
        assert results["a2f"].get_cumulative_lambda().shape[1] == 10
    if test_name == "isotropic_eliashberg":
        data_regression_dict["max_eigenvalue"] = (
            results["max_eigenvalue"].get_array("max_eigenvalue").tolist()
        )
    if test_name == "bands":
        data_regression_dict["kpoints"] = (
            results["el_band_structure"].get_kpoints().tolist()
        )
        data_regression_dict["el_band_structure"] = (
            results["el_band_structure"].get_bands().tolist()
        )
        data_regression_dict["qpoints"] = (
            results["ph_band_structure"].get_kpoints().tolist()
        )
        data_regression_dict["ph_band_structure"] = (
            results["ph_band_structure"].get_bands().tolist()
        )

    data_regression.check(data_regression_dict)


def test_epw_failed_broyden_factor(parse_from_files, data_regression):
    """Test a `epw.x` that failed due to an error in routine `mix_broyden`."""
    results, calcfunction = parse_from_files(EpwParser, "failed_broyden_factor")
    expected_exit_status = (
        EpwCalculation.exit_codes.ERROR_OUTPUT_STDOUT_INCOMPLETE.status
    )

    assert calcfunction.is_failed
    assert calcfunction.exit_status == expected_exit_status
    data_regression.check(
        {
            "output_parameters": results["output_parameters"].get_dict(),
        }
    )


def test_epw_reads_dos_from_output_subfolder(aiida_localhost, files_path):
    """Test that DOS data is parsed even when retrieved inside the EPW output folder."""
    parser_entry_point = get_entry_point_string_from_class(
        class_module=EpwParser.__module__, class_name=EpwParser.__name__
    )
    calc_entry_point = format_entry_point_string(
        group="aiida.calculations", name=parser_entry_point.split(":")[1]
    )

    node = orm.CalcJobNode(computer=aiida_localhost, process_type=calc_entry_point)
    node.base.attributes.set("output_filename", "aiida.out")
    node.store()

    retrieved = orm.FolderData()
    retrieved.base.repository.put_object_from_tree(
        (files_path / "parsers" / "epw" / "default").as_posix()
    )
    retrieved.base.repository.put_object_from_file(
        (files_path / "tools" / "parsers" / "a2f" / "aiida.dos").as_posix(),
        "out/aiida.dos",
    )
    retrieved.base.links.add_incoming(
        node, link_type=LinkType.CREATE, link_label="retrieved"
    )
    retrieved.store()

    results, calcfunction = EpwParser.parse_from_node(node, store_provenance=False)

    assert calcfunction.is_finished_ok, calcfunction.exit_message
    assert "dos" in results
    assert isinstance(results["dos"], DosData)
    assert results["dos"].get_dos().shape == (160,)


def test_parse_a2f_supports_non_default_grid_lengths():
    """Test that `.a2f` parsing does not assume a fixed number of spectral rows."""
    content = """w[meV] a2f and integrated 2*a2f/w for   2 smearing values
   0.1000000   1.0000000   2.0000000
   0.2000000   3.0000000   4.0000000
 Integrated el-ph coupling
  #            0.1000000   0.2000000
 Phonon smearing (meV)
  #            0.5000000   0.6000000
Electron smearing (eV)   0.0500000
Fermi window (eV)   0.8000000
"""

    a2f, parsed = EpwParser.parse_a2f(content)

    assert isinstance(a2f, A2fData)
    assert a2f.get_array("frequency").tolist() == [0.1, 0.2]
    assert a2f.get_array("a2f").tolist() == [[1.0, 2.0], [3.0, 4.0]]
    assert a2f.get_array("lambda").tolist() == [0.1, 0.2]
    assert a2f.get_array("degaussq").tolist() == [0.5, 0.6]
    assert a2f.electron_smearing == pytest.approx(0.05)
    assert a2f.fermi_window == pytest.approx(0.8)
    assert parsed == {"degaussw": 0.05, "fsthick": 0.8}


def test_parse_a2f_proj_returns_typed_data(files_path):
    """Test the projected a2F parser returns `ProjectedSpectrumData`."""
    content = (files_path / "tools" / "parsers" / "a2f" / "aiida.a2f_proj").read_text()

    a2f_proj = EpwParser.parse_a2f_proj(content)

    assert isinstance(a2f_proj, PA2fData)
    assert a2f_proj.get_frequency().shape[0] == 500
    assert a2f_proj.get_a2f().shape == (500,)
    assert a2f_proj.get_projected_a2f().shape == (500, 3)


def test_parse_phdos_proj_preserves_all_projection_columns():
    """Test that the projected phonon DOS keeps every projected series."""
    content = """w[meV] phdos[states/meV] phdos_modeproj[states/meV]
   0.1000000   1.0000000   2.0000000   3.0000000
   0.2000000   4.0000000   5.0000000   6.0000000
"""

    phdos_proj = EpwParser.parse_phdos_proj(content)

    assert isinstance(phdos_proj, PDosData)
    assert phdos_proj.get_frequency().tolist() == [0.1, 0.2]
    assert phdos_proj.get_phdos().tolist() == [1.0, 4.0]
    assert phdos_proj.get_projected_phdos().tolist() == [[2.0, 3.0], [5.0, 6.0]]


def test_parse_phdos_preserves_all_smearing_columns():
    """Test that the total phonon DOS keeps every smearing series."""
    content = """w[meV] phdos[states/meV] for   3 smearing values
   0.1000000   1.0000000   2.0000000   3.0000000
   0.2000000   4.0000000   5.0000000   6.0000000
"""

    phdos = EpwParser.parse_phdos(content)

    assert phdos.get_frequency().tolist() == [0.1, 0.2]
    assert phdos.get_phdos().tolist() == [
        [1.0, 2.0, 3.0],
        [4.0, 5.0, 6.0],
    ]


def test_parse_lambda_fs_returns_typed_data():
    """Test that `lambda_FS` is parsed into `LambdaFSData`."""
    content = """# kx ky kz band Enk lambda
 0.0000 0.1000 0.2000 1 0.3000 0.9000
 0.5000 0.6000 0.7000 2 0.4000 1.1000
"""

    lambda_fs = EpwParser.parse_lambda_FS(content)

    assert isinstance(lambda_fs, LambdaFSData)
    assert lambda_fs.energy_units == "eV"
    assert lambda_fs.get_kpoints().tolist() == [
        [0.0, 0.1, 0.2],
        [0.5, 0.6, 0.7],
    ]
    assert lambda_fs.get_bands().tolist() == [1.0, 2.0]
    assert lambda_fs.get_energies().tolist() == [0.3, 0.4]
    assert lambda_fs.get_lambda().tolist() == [0.9, 1.1]
    assert lambda_fs.get_array("Enk").tolist() == [0.3, 0.4]


def test_parse_lambda_k_pairs_returns_typed_data():
    """Test that `lambda_k_pairs` is parsed into `LambdaKPairsData`."""
    content = """# lambda_nk rho
 0.1000 1.5000
 0.2000 2.5000
"""

    lambda_k_pairs = EpwParser.parse_lambda_k_pairs(content)

    assert isinstance(lambda_k_pairs, DosData)
    assert lambda_k_pairs.get_energy().tolist() == [0.1, 0.2]
    assert lambda_k_pairs.get_dos().tolist() == [1.5, 2.5]


def test_parse_iso_gap_functions_returns_typed_data(files_path):
    """Test isotropic gap-function files are wrapped in `GapFunctionData`."""
    iso_dir = files_path / "tools" / "parsers" / "full_iso_eliashberg"
    file_contents = {
        path.name: path.read_text()
        for path in sorted(iso_dir.iterdir())
        if path.is_file()
    }

    gap_functions = EpwParser.parse_iso_gap_functions(file_contents)

    assert isinstance(gap_functions, GapFunctionData)
    assert gap_functions.kind == "iso"
    assert gap_functions.get_temperatures().tolist() == [3.0, 4.0, 5.0]
    assert gap_functions.get_gap_function(3.0).shape[1] == 3


def test_parse_aniso_gap_functions_returns_typed_data(files_path):
    """Test anisotropic gap-function files are wrapped in `GapFunctionData`."""
    aniso_dir = files_path / "tools" / "parsers" / "fsr_aniso_eliashberg"
    file_contents = {
        path.name: path.read_text()
        for path in sorted(aniso_dir.iterdir())
        if path.is_file()
    }

    gap_functions = EpwParser.parse_aniso_gap_functions(file_contents)

    assert isinstance(gap_functions, GapFunctionData)
    assert gap_functions.kind == "aniso"
    assert gap_functions.get_temperatures().tolist() == [3.0, 4.0, 5.0]
    assert gap_functions.get_gap_function(3.0).shape[1] == 5


def test_parse_aniso_gap_fs_returns_typed_data():
    """Test parsing of imag_aniso_gap_FS file."""
    content = """# kx ky kz band energy delta
 0.0 0.0 0.0 1 0.1 0.5
 0.0 0.0 0.0 2 0.2 0.6
"""
    aniso_gap_fs = EpwParser.parse_aniso_gap_fs(
        {"aiida.imag_aniso_gap_FS_003.00": content}
    )
    assert isinstance(aniso_gap_fs, orm.ArrayData)
    assert aniso_gap_fs.base.attributes.get("temperatures") == [3.0]
    assert aniso_gap_fs.base.attributes.get("temp_3_00_bands") == [1, 2]
    assert aniso_gap_fs.get_array("temp_3_00_band_1_kpoints").tolist() == [
        [0.0, 0.0, 0.0]
    ]
    assert aniso_gap_fs.get_array("temp_3_00_band_1_energy").tolist() == [0.1]
    assert aniso_gap_fs.get_array("temp_3_00_band_1_delta").tolist() == [0.5]


def test_parse_aniso_imag_returns_typed_data():
    """Test parsing of imag_aniso file."""
    content = """# w energy znorm delta shift
 0.01 0.1 1.0 0.5 0.0
 0.02 0.2 1.1 0.6 0.0
"""
    aniso_imag = EpwParser.parse_aniso_imag({"aiida.imag_aniso_003.00": content})
    assert isinstance(aniso_imag, orm.ArrayData)
    assert aniso_imag.base.attributes.get("temperatures") == [3.0]
    assert aniso_imag.base.attributes.get("temp_3_00_frequencies") == [0.01, 0.02]
    assert aniso_imag.get_array("temp_3_00_freq_0_energy").tolist() == [0.1]
    assert aniso_imag.get_array("temp_3_00_freq_0_znorm").tolist() == [1.0]
    assert aniso_imag.get_array("temp_3_00_freq_0_delta").tolist() == [0.5]
    assert aniso_imag.get_array("temp_3_00_freq_0_shift").tolist() == [0.0]
