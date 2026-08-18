"""Generic EPW error-recovery helpers."""

from copy import deepcopy


def prepare_bracket_ef_recovery(parameters, fermi_energy_coarse):
    """Return updated parameters and recovery action, or ``(None, None)``."""
    if fermi_energy_coarse is None:
        return None, None
    parameters = deepcopy(parameters)
    inputepw = parameters.setdefault("INPUTEPW", {})
    updated = inputepw.get("efermi_read") is not True
    inputepw["efermi_read"] = True
    if (
        inputepw.get("fermi_energy") is None
        or abs(float(inputepw["fermi_energy"]) - float(fermi_energy_coarse)) > 1.0e-6
    ):
        inputepw["fermi_energy"] = fermi_energy_coarse
        updated = True
    if not updated:
        return None, None
    return parameters, (
        f"set `efermi_read = True` and `fermi_energy = {fermi_energy_coarse}`"
    )
