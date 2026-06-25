"""Regular expression schemas and parsers for EPW stdout files."""

import re


def parse_fortran_float(value: str) -> float:
    """Parse a Fortran-style double float (e.g. 1.23D-04) into a Python float."""
    value = value.replace("D", "E").replace("d", "E")
    value = re.sub(r"([0-9\.]+)([\+\-]\d+)$", r"\1E\2", value)
    return float(value)


def parse_space_separated_ints(value: str) -> list[int]:
    """Parse a space-separated string of integers into a list."""
    return [int(x) for x in value.split()]


_PATTERNS_LEGACY = [
    ("Allen_Dynes_Tc", float, r"\s+Estimated Allen-Dynes Tc =\s+([\d\.]+) K"),
    ("fermi_energy_coarse", float, r"\s+Fermi energy coarse grid =\s+([\d\.-]+)\seV"),
]

_PATTERNS_MODERN = [
    ("nbndsub", int, r"nbndsub\s*=\s*(\d+)"),
    (
        "ws_vectors_electrons",
        int,
        r"^\s*Number of WS vectors for electrons\s+(\d+)",
    ),
    ("ws_vectors_phonons", int, r"^\s*Number of WS vectors for phonons\s+(\d+)"),
    (
        "ws_vectors_electron_phonon",
        int,
        r"^\s*Number of WS vectors for electron-phonon\s+(\d+)",
    ),
    (
        "max_cores_parallelization",
        int,
        r"^\s*Maximum number of cores for efficient parallelization\s+(\d+)",
    ),
    ("ibndmin", int, r"ibndmin\s*=\s*(\d+)"),
    ("ebndmin", float, r"ebndmin\s*=\s*([+-]?[\d\.]+)"),
    ("ibndmax", int, r"ibndmax\s*=\s*(\d+)"),
    ("ebndmax", float, r"ebndmax\s*=\s*([+-]?[\d\.]+)"),
    ("nbnd_skip", int, r"^\s*Skipping\s+(\d+)\s+occupied bands:"),
    (
        "fermi_energy_coarse",
        float,
        r"^\s*Fermi energy coarse grid =\s*([+-]?[\d\.]+)\s+eV",
    ),
    (
        "fermi_energy_fine",
        float,
        r"^\s*Fermi energy is calculated from the fine k-mesh: Ef =\s*([+-]?[\d\.]+)\s+eV",
    ),
    (
        "fine_q_mesh",
        parse_space_separated_ints,
        r"^\s*Using uniform q-mesh:\s+((?:\d+\s*)+)",
    ),
    (
        "fine_k_mesh",
        parse_space_separated_ints,
        r"^\s*Using uniform k-mesh:\s+((?:\d+\s*)+)",
    ),
    ("fermi_level", parse_fortran_float, r"Fermi level \(eV\)\s*=\s*([\d\.D+-]+)"),
    ("DOS", parse_fortran_float, r"DOS\(states/spin/eV/Unit Cell\)\s*=\s*([\d\.D+-]+)"),
    (
        "electron_smearing",
        parse_fortran_float,
        r"Electron smearing \(eV\)\s*=\s*([\d\.D+-]+)",
    ),
    ("fermi_window", parse_fortran_float, r"Fermi window \(eV\)\s*=\s*([\d\.D+-]+)"),
    ("lambda", float, r"Electron-phonon coupling strength\s*=\s*([\d\.]+)"),
    (
        "McMillan_Tc",
        float,
        r"Estimated Tc using McMillan expression\s*=\s*([\d\.]+) K for muc",
    ),
    (
        "Allen_Dynes_Tc",
        float,
        r"Estimated Tc using Allen-Dynes modified McMillan expression\s*=\s*([\d\.]+) K",
    ),
    (
        "SISSO_Tc",
        float,
        r"Estimated Tc using SISSO machine learning model\s*=\s*([\d\.]+) K",
    ),
    ("muc", float, r"for muc\s*=\s*([\d\.]+)"),
    ("w_log", float, r"Estimated w_log\s*=\s*([\d\.]+) meV"),
    (
        "BCS_gap",
        float,
        r"Estimated BCS superconducting gap using McMillan Tc\s*=\s*([\d\.]+) meV",
    ),
]

# Precompile all regular expressions for maximum performance
REGEX_PATTERNS_LEGACY = [
    (key, type_func, re.compile(pat)) for key, type_func, pat in _PATTERNS_LEGACY
]
REGEX_PATTERNS_MODERN = [
    (key, type_func, re.compile(pat)) for key, type_func, pat in _PATTERNS_MODERN
]
