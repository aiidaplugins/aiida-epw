"""Tests for the `EpwParser`."""

import pytest

from aiida_epw.calculations.epw import EpwCalculation
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
        data_regression_dict["a2f"] = results["a2f"].get_array("a2f").tolist()[::50]
        data_regression_dict["lambda"] = results["a2f"].get_array("lambda").tolist()
        data_regression_dict["degaussq"] = results["a2f"].get_array("degaussq").tolist()
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
