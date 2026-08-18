"""Tests for Eliashberg temperature-range error detection."""

import textwrap

import pytest
from aiida_quantumespresso.utils.mapping import get_logging_container
from packaging.version import Version

from aiida_epw.parsers.epw import EpwParser


@pytest.mark.parametrize(
    ("iterations", "failure", "expected_error"),
    (
        (
            "1  1.0E-02  1.1  1.0E-01",
            "Error in routine mix_broyden (5): factorization",
            "ERROR_FACTORIZATION",
        ),
        (
            "1  1.0E-02  1.1  1.0E-11",
            "Error in routine mix_broyden (5): factorization",
            "ERROR_TEMPERATURE_OUT_OF_RANGE",
        ),
        (
            "\n".join(
                (
                    "1  2.0E+00  2.0  8.0E-01",
                    "2  5.0E+00  1.9  8.0E-23",
                    "3  NaN      NaN  NaN",
                )
            ),
            "",
            "ERROR_TEMPERATURE_OUT_OF_RANGE",
        ),
    ),
)
def test_temperature_range_classification(iterations, failure, expected_error):
    """Distinguish ordinary factorization failures from a collapsed gap."""
    stdout = textwrap.dedent(
        f"""\
        Program EPW v.6.0
        temp(1) = 10.0 K
        Solve isotropic Eliashberg equations on imaginary-axis
        Total number of frequency points nsiw(1) = 200
        Cutoff frequency wscut = 0.500 eV
        broyden mixing factor = 0.700
        startiw = 1, lastiw = 200, nsiw(itemp) = 200
        iter  ethr  znormi  deltai [meV]
        {iterations}
        {failure}
        """
    )
    logs = get_logging_container()

    _, logs = EpwParser.parse_stdout(stdout, logs, code_version=Version("6.0"))

    assert expected_error in logs.error
