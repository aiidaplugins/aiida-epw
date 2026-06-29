"""Tests for the `EpwParser`."""

import textwrap

import pytest
from aiida import orm
from aiida.common import LinkType
from aiida.plugins.entry_point import (
    format_entry_point_string,
    get_entry_point_string_from_class,
)

from aiida_epw.calculations.epw import EpwCalculation
from aiida_epw.data import A2fData, DosData, PA2fData, PDosData, PhDosData
from aiida_epw.parsers.epw import EpwParser


@pytest.mark.parametrize(
    "test_name",
    (
        "default",
        "isotropic/fbw/imag",
        "anisotropic/fsr/pade",
        "anisotropic/fbw/pade",
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
    if test_name == "isotropic/fbw/imag":
        if "max_eigenvalue" in results:
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
    results, calcfunction = parse_from_files(
        EpwParser, "isotropic/fsr/failed_broyden_factor"
    )
    expected_exit_status = (
        EpwCalculation.exit_codes.ERROR_TEMPERATURE_OUT_OF_RANGE.status
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
    assert results["dos"].get_array("dos").shape == (160,)
    assert "integrated_dos" in results["dos"].get_arraynames()


def test_phdos_from_string_preserves_all_smearing_columns():
    """Test that the total phonon DOS keeps every smearing series."""
    content = """w[meV] phdos[states/meV] for   3 smearing values
   0.1000000   1.0000000   2.0000000   3.0000000
   0.2000000   4.0000000   5.0000000   6.0000000
"""

    phdos = PhDosData.from_string(content)

    assert isinstance(phdos, PhDosData)
    assert phdos.get_frequency().tolist() == [0.1, 0.2]
    assert phdos.get_phdos().tolist() == [
        [1.0, 2.0, 3.0],
        [4.0, 5.0, 6.0],
    ]
    assert phdos.num_smearings == 3


def test_epw_reads_phdos_output(aiida_localhost, files_path):
    """Test that EpwParser emits ``PhDosData`` for the total phonon DOS."""
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
        (files_path / "tools" / "parsers" / "a2f" / "aiida.phdos").as_posix(),
        "aiida.phdos",
    )
    retrieved.base.links.add_incoming(
        node, link_type=LinkType.CREATE, link_label="retrieved"
    )
    retrieved.store()

    results, calcfunction = EpwParser.parse_from_node(node, store_provenance=False)

    assert calcfunction.is_finished_ok, calcfunction.exit_message
    assert "phdos" in results
    assert isinstance(results["phdos"], PhDosData)
    assert results["phdos"].get_frequency().shape == (500,)
    assert results["phdos"].get_phdos().shape == (500, 10)
    assert results["phdos"].num_smearings == 10


def test_epw_calculation_registers_memory_exit_code():
    """Test that parser-side memory errors map to a defined calculation exit code."""
    assert EpwCalculation.exit_codes.ERROR_MEMORY_EXCEEDS_MAX_MEMLT.status == 313


def test_epw_calculation_registers_walltime_exit_code():
    """Test that parser-side walltime errors map to a defined calculation exit code."""
    assert EpwCalculation.exit_codes.ERROR_OUT_OF_WALLTIME.status == 400


def test_epw_out_of_walltime(aiida_localhost, tmp_path):
    """Test that internal EPW walltime errors map to `ERROR_OUT_OF_WALLTIME`."""
    parser_entry_point = get_entry_point_string_from_class(
        class_module=EpwParser.__module__, class_name=EpwParser.__name__
    )
    calc_entry_point = format_entry_point_string(
        group="aiida.calculations", name=parser_entry_point.split(":")[1]
    )
    node = orm.CalcJobNode(computer=aiida_localhost, process_type=calc_entry_point)
    node.base.attributes.set("output_filename", "aiida.out")
    node.store()

    stdout_path = tmp_path / "aiida.out"
    stdout_path.write_text(
        textwrap.dedent(
            """\
            Program EPW v.5.7 starts on 15May2023 at  3: 7:50
            Maximum CPU time exceeded
            """
        )
    )

    retrieved = orm.FolderData()
    retrieved.base.repository.put_object_from_file(stdout_path.as_posix(), "aiida.out")
    retrieved.base.links.add_incoming(
        node, link_type=LinkType.CREATE, link_label="retrieved"
    )
    retrieved.store()

    results, calcfunction = EpwParser.parse_from_node(node, store_provenance=False)

    assert calcfunction.is_finished
    assert calcfunction.is_failed
    assert (
        calcfunction.exit_status
        == EpwCalculation.exit_codes.ERROR_OUT_OF_WALLTIME.status
    )
    assert "output_parameters" in results


def test_epw_preserves_scheduler_out_of_walltime(aiida_localhost, tmp_path):
    """Test that scheduler walltime failures are not overridden by parser errors."""
    parser_entry_point = get_entry_point_string_from_class(
        class_module=EpwParser.__module__, class_name=EpwParser.__name__
    )
    calc_entry_point = format_entry_point_string(
        group="aiida.calculations", name=parser_entry_point.split(":")[1]
    )
    node = orm.CalcJobNode(computer=aiida_localhost, process_type=calc_entry_point)
    node.base.attributes.set("output_filename", "aiida.out")
    node.set_exit_status(
        EpwCalculation.exit_codes.ERROR_SCHEDULER_OUT_OF_WALLTIME.status
    )
    node.store()

    stdout_path = tmp_path / "aiida.out"
    stdout_path.write_text(
        textwrap.dedent(
            """\
            Program EPW v.5.7 starts on 15May2023 at  3: 7:50
            """
        )
    )

    retrieved = orm.FolderData()
    retrieved.base.repository.put_object_from_file(stdout_path.as_posix(), "aiida.out")
    retrieved.base.links.add_incoming(
        node, link_type=LinkType.CREATE, link_label="retrieved"
    )
    retrieved.store()

    _, calcfunction = EpwParser.parse_from_node(node, store_provenance=False)

    assert calcfunction.is_finished
    assert calcfunction.is_failed
    assert (
        calcfunction.exit_status
        == EpwCalculation.exit_codes.ERROR_SCHEDULER_OUT_OF_WALLTIME.status
    )


def test_epw_detects_scheduler_out_of_memory_from_stderr(aiida_localhost, tmp_path):
    """Scheduler OOM messages should take precedence over incomplete stdout parsing."""
    parser_entry_point = get_entry_point_string_from_class(
        class_module=EpwParser.__module__, class_name=EpwParser.__name__
    )
    calc_entry_point = format_entry_point_string(
        group="aiida.calculations", name=parser_entry_point.split(":")[1]
    )
    node = orm.CalcJobNode(computer=aiida_localhost, process_type=calc_entry_point)
    node.base.attributes.set("output_filename", "aiida.out")
    node.set_option("scheduler_stderr", "_scheduler-stderr.txt")
    node.store()

    stdout_path = tmp_path / "aiida.out"
    stdout_path.write_text(
        textwrap.dedent(
            """\
            Program EPW v.5.7 starts on 15May2023 at  3: 7:50
            """
        )
    )
    scheduler_stderr_path = tmp_path / "_scheduler-stderr.txt"
    scheduler_stderr_path.write_text(
        textwrap.dedent(
            """\
            slurmstepd: error: Detected 4 oom_kill events in StepId=13178347.0.
            srun: error: cns261: tasks 104,106: Out Of Memory
            """
        )
    )

    retrieved = orm.FolderData()
    retrieved.base.repository.put_object_from_file(stdout_path.as_posix(), "aiida.out")
    retrieved.base.repository.put_object_from_file(
        scheduler_stderr_path.as_posix(), "_scheduler-stderr.txt"
    )
    retrieved.base.links.add_incoming(
        node, link_type=LinkType.CREATE, link_label="retrieved"
    )
    retrieved.store()

    _, calcfunction = EpwParser.parse_from_node(node, store_provenance=False)

    assert calcfunction.is_finished
    assert calcfunction.is_failed
    assert (
        calcfunction.exit_status
        == EpwCalculation.exit_codes.ERROR_SCHEDULER_OUT_OF_MEMORY.status
    )


def test_epw_reads_projected_outputs(aiida_localhost, files_path):
    """Test that EpwParser correctly parses projected phdos and projected a2f files."""
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
        (files_path / "tools" / "parsers" / "a2f" / "aiida.phdos_proj").as_posix(),
        "aiida.phdos_proj",
    )
    retrieved.base.repository.put_object_from_file(
        (files_path / "tools" / "parsers" / "a2f" / "aiida.a2f_proj").as_posix(),
        "aiida.a2f_proj",
    )
    retrieved.base.links.add_incoming(
        node, link_type=LinkType.CREATE, link_label="retrieved"
    )
    retrieved.store()

    results, calcfunction = EpwParser.parse_from_node(node, store_provenance=False)

    assert calcfunction.is_finished_ok, calcfunction.exit_message
    assert "phdos_proj" in results
    assert isinstance(results["phdos_proj"], PDosData)
    assert results["phdos_proj"].get_frequency().shape == (500,)
    assert results["phdos_proj"].get_phdos().shape == (500,)
    assert results["phdos_proj"].get_projected_phdos().shape == (500, 3)

    assert "a2f_proj" in results
    assert isinstance(results["a2f_proj"], PA2fData)
    assert results["a2f_proj"].get_frequency().shape == (500,)
    assert results["a2f_proj"].get_a2f().shape == (500,)
    assert results["a2f_proj"].get_projected_a2f().shape == (500, 3)
    assert results["a2f_proj"].lambda_int == pytest.approx(1.9917789)
    assert results["a2f_proj"].lambda_sum == pytest.approx(1.9853134)


def test_epw_reads_lambda_k_pairs_as_dos(aiida_localhost, files_path):
    """Test that ``lambda_k_pairs`` is exposed as a generic ``DosData`` node."""
    import io

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
    retrieved.base.repository.put_object_from_filelike(
        io.BytesIO(b"# energy dos\n0.1 1.0\n0.2 2.0\n"),
        "aiida.lambda_k_pairs",
    )
    retrieved.base.links.add_incoming(
        node, link_type=LinkType.CREATE, link_label="retrieved"
    )
    retrieved.store()

    results, calcfunction = EpwParser.parse_from_node(node, store_provenance=False)

    assert calcfunction.is_finished_ok, calcfunction.exit_message
    assert "lambda_k_pairs" in results
    assert isinstance(results["lambda_k_pairs"], DosData)
    assert results["lambda_k_pairs"].get_energy().shape == (2,)
    assert results["lambda_k_pairs"].get_dos().shape == (2,)
    assert results["lambda_k_pairs"].get_integrated_dos() is None


def test_epw_factorization_errors(aiida_localhost):
    """Test detection of factorization and temperature out of range errors."""
    import io

    # Common function to run parser on mock stdout
    def run_parser(stdout_content):
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
        retrieved.base.repository.put_object_from_filelike(
            io.BytesIO(stdout_content.encode("utf-8")), "aiida.out"
        )
        retrieved.base.links.add_incoming(
            node, link_type=LinkType.CREATE, link_label="retrieved"
        )
        retrieved.store()

        return EpwParser.parse_from_node(node, store_provenance=False)

    # 1. Standard factorization failure (deltai last value is 0.1 meV > 1E-10)
    stdout_factorization = """
     Program EPW v.6.0 
     EPW v6.0
     Solve isotropic Eliashberg equations
     temp(   1) =    10.00000 K
     Total number of frequency points nsiw(   1) =    200
     Cutoff frequency wscut =    0.500
     broyden mixing factor =    0.700
     startiw =    1, lastiw =   200, nsiw(itemp) =   200
   iter      ethr          znormi      deltai [meV]
     1     0.100E-01     1.100000     0.100000
 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     Error in routine mix_broyden (5):
     factorization
 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    """
    _, calcfunction1 = run_parser(stdout_factorization)
    assert calcfunction1.is_failed
    assert (
        calcfunction1.exit_status
        == EpwCalculation.exit_codes.ERROR_FACTORIZATION.status
    )

    # 2. Temperature out of range (deltai last value is 0.1E-11 < 1E-10)
    stdout_temp_out_of_range = """
     Program EPW v.6.0 
     EPW v6.0
     Solve isotropic Eliashberg equations
     temp(   1) =    10.00000 K
     Total number of frequency points nsiw(   1) =    200
     Cutoff frequency wscut =    0.500
     broyden mixing factor =    0.700
     startiw =    1, lastiw =   200, nsiw(itemp) =   200
   iter      ethr          znormi      deltai [meV]
     1     0.100E-01     1.100000     0.100E-11
 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     Error in routine mix_broyden (5):
     factorization
 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    """
    _, calcfunction2 = run_parser(stdout_temp_out_of_range)
    assert calcfunction2.is_failed
    assert (
        calcfunction2.exit_status
        == EpwCalculation.exit_codes.ERROR_TEMPERATURE_OUT_OF_RANGE.status
    )
