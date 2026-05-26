"""Tools for the EPW workflows."""

from . import parsers

__all__ = ["gap_iso_imag_temp", "parsers"]


def __getattr__(name):
    """Lazily import plotting helpers so parser utilities do not require plotting deps."""
    if name == "gap_iso_imag_temp":
        from .plot import gap_iso_imag_temp

        return gap_iso_imag_temp

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
