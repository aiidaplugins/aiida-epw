"""Plotting functions copied from EPWpy: https://gitlab.com/epwpy/epwpy/-/blob/develop/EPWpy/plotting/plot_supercond.py?ref_type=heads.

The code is adapted to AiiDA datatypes.
"""

import os

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from aiida import orm
from scipy.optimize import curve_fit

from aiida_epw.data import GapFunctionData
from aiida_epw.tools.calculators import bcs_gap_function


def _iter_gap_functions(gap_functions):
    """Yield `(temperature, table)` pairs from typed or legacy gap-function data."""
    if isinstance(gap_functions, GapFunctionData):
        yield from gap_functions.get_itergap_functions()
        return

    for arrayname, array in gap_functions.get_iterarrays():
        yield float(arrayname.replace("_", ".")), array


#### Isotropic gap (Imaginary, real and ) vs. temeprature


def gap_iso_imag_temp(
    iso_gap_function: orm.ArrayData,
    tempmax,
    font=12,
    prefix="aiida",
    fit=False,
    p0=None,
    destpath=None,
):
    """Plot the isotropic gap (Imaginary) vs. temeprature."""
    imag_delta = []
    imag_temp = []

    for temperature, array in _iter_gap_functions(iso_gap_function):
        gap = array[0, -1] * 1000
        if np.isnan(gap):
            continue
        imag_delta.append(gap)  # Convert to meV
        imag_temp.append(temperature)

    ##Plot
    fig = plt.figure(figsize=(4.5, 3.5))
    ax1 = fig.add_subplot(1, 1, 1)
    ax1.set_title("Superconducting Gap vs. Temperature", fontsize=font)
    ax1.set_xlabel("Temeperature (K)", fontsize=font)
    ax1.set_xlim(0, tempmax)
    ax1.set_ylabel(r"$\Delta_0$ (meV)", fontsize=font)
    ax1.tick_params(axis="y", labelsize=font)
    ax1.tick_params(axis="x", labelsize=font)
    ax1.plot(
        imag_temp,
        imag_delta,
        linestyle="-",
        marker="o",
        c="k",
        label="Im. axis",
    )
    ax1.yaxis.set_minor_locator(ticker.AutoMinorLocator(2))
    plt.tight_layout()
    if fit:
        if p0 is None:
            p0 = [imag_temp[-1], 3.3, imag_delta[0]]
        popt, pcov = curve_fit(bcs_gap_function, imag_temp, imag_delta, p0=p0)
        Tc, p, Delta_0 = popt
        T = np.linspace(0, Tc, 100)
        ax1.plot(
            T,
            bcs_gap_function(T, Tc, p, Delta_0),
            linestyle="--",
            c="r",
            label="Fit",
        )
    if destpath:
        plt.savefig(os.path.join(destpath, f"{prefix}_iso_gap_imag_vs_Temp.pdf"))
    plt.show()


def gap_aniso_temp(
    aniso_gap_functions,
    calc_type="fsr",
    tempmax=30,
    font=12,
    prefix="aiida",
    fit=True,
    p0=None,
    destpath=None,
):
    """Plot the anisotropic gap vs. temeprature."""
    fig = plt.figure(figsize=(4.5, 2.8))
    ax1 = fig.add_subplot(111)

    dict_files = dict(_iter_gap_functions(aniso_gap_functions))
    # Determine the maximum y-limit value from the first file's data
    max_y_value = max(dict_files[list(dict_files.keys())[0]][:, 1]) if dict_files else 1
    Ts = []
    average_deltas = []

    for T, array in dict_files.items():
        ax1.plot(array[:, 0], array[:, 1], color="blue")
        Ts.append(T)
        average_deltas.append(np.average(array[:, 1], weights=array[:, 0]))

    ax1.scatter(Ts, average_deltas, color="red")
    ax1.set_xlabel("Temeperature (K)", fontsize=font)
    ax1.set_xlim(0, tempmax)
    ax1.tick_params(axis="y", labelsize=font)
    ax1.tick_params(axis="x", labelsize=font)
    ax1.set_ylabel(r"$\Delta_{nk}$ (meV)", fontsize=font)
    ax1.set_ylim(0, max_y_value * 1.05)
    ax1.yaxis.set_minor_locator(ticker.AutoMinorLocator(2))
    ax1.set_title(f"Supercond. Im. gap using {calc_type} approx.")
    # ax1.annotate(r'$\pi$', (7.5, 3), fontsize=font)
    # ax1.annotate(r'$\sigma$', (7.5, 10), fontsize=font)

    # plt.title('Superconducting Gap vs. Temperature', fontsize=font)
    plt.tight_layout()
    if fit:
        if p0 is None:
            p0 = [Ts[-1], 3.3, average_deltas[0]]

        while len(Ts) >= 3:
            popt, pcov = curve_fit(
                bcs_gap_function, Ts, average_deltas, p0=p0, maxfev=10000
            )
            Tc, p, Delta_0 = popt
            if np.max(pcov) < 1:
                break
            Ts.pop()
            average_deltas.pop()

        T = np.linspace(0, Tc, 100)
        ax1.plot(
            T,
            bcs_gap_function(T, Tc, p, Delta_0),
            linestyle="--",
            c="r",
            label="Fit",
        )

    if destpath:
        plt.savefig(os.path.join(destpath, f"{prefix}_gap_aniso_vs_temp.pdf"))
    plt.show()
