"""Calculators for the superconductivity analysis."""

import numpy
from aiida import orm
from aiida.engine import calcfunction
from numpy.typing import ArrayLike
from scipy.interpolate import interp1d

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


def _calculate_iso_tc(max_eigenvalue, allow_extrapolation=False):
    """Calculate the isotropic critical temperature Tc from the linearized Eliashberg equations."""
    if max_eigenvalue[:, 1].max() < 1.0:
        return 0.0
    elif max_eigenvalue[:, 1].min() > 1.0:
        if allow_extrapolation:
            print(
                "This Tc is estimated from the extrapolation of the max eigenvalues. Please check whether it's reliable."
            )
            f_extrapolate = interp1d(
                max_eigenvalue[:, 1],
                max_eigenvalue[:, 0],
                kind="linear",  # Can be 'linear', 'quadratic', etc. for interpolation
                bounds_error=False,  # Do not raise an error for out-of-bounds values
                fill_value="extrapolate",  # Extrapolate using a line from the last two points
            )
            return float(f_extrapolate(1.0))
        else:
            return numpy.nan
    else:
        return float(interp1d(max_eigenvalue[:, 1], max_eigenvalue[:, 0])(1.0))


@calcfunction
def calculate_iso_tc(max_eigenvalue: orm.XyData) -> orm.Float:
    """Calculate the isotropic critical temperature Tc from the linearized Eliashberg equations."""
    return orm.Float(_calculate_iso_tc(max_eigenvalue.get_array("max_eigenvalue")))


# This function is taken from https://www.sciencedirect.com/science/article/pii/S0010465516302260 eq.81
def bcs_gap_function(T, Tc, p, Delta_0):
    """BCS gap function."""
    return Delta_0 * numpy.sqrt(1 - (T / Tc) ** p)
