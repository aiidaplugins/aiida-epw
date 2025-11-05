"""Calculators for the superconductivity analysis."""

import numpy
from numpy.typing import ArrayLike

meV_to_Kelvin = 11.604518121550082


def allen_dynes(lambo, omega_log, mu_star):
    """Calculate the Allen-Dynes critical temperature Tc."""
    if lambo - mu_star * (1 + 0.62 * lambo) < 0:
        return 0
    else:
        return (
            omega_log
            * numpy.exp(-1.04 * (1 + lambo) / (lambo - mu_star * (1 + 0.62 * lambo)))
            / 1.2
        )


def calculate_lambda_omega(frequency: ArrayLike, spectrum: ArrayLike) -> tuple:
    """Calculate lambda and omega_log from the parsed a2F spectrum.

    :param frequency: Frequency array on which the a2F spectrum is defined [meV].
    :param spectrum: a2F spectral function values.

    :returns: Tuple of the calculated lambda and omega_log values.
    """
    lambda_ = 2 * numpy.trapezoid(spectrum / frequency, frequency)  # unitless
    omega_log = numpy.exp(
        2
        / lambda_
        * numpy.trapezoid(spectrum / frequency * numpy.log(frequency), frequency)
    )  # eV
    omega_log = omega_log * meV_to_Kelvin

    return lambda_, omega_log


# This function is taken from https://www.sciencedirect.com/science/article/pii/S0010465516302260 eq.81
def bcs_gap_function(T, Tc, p, Delta_0):
    return Delta_0 * numpy.sqrt(1 - (T / Tc) ** p)
