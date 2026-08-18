"""Pade error-recovery helpers for EPW workflows."""

from copy import deepcopy


def prepare_pade_recovery(parameters, output_parameters, max_nsiw, min_nsiw):
    """Return updated inputs and an action for a recoverable Pade failure.

    Returns ``(None, None)`` when neither the Pade order nor the temperature
    list can be adjusted further.
    """
    parameters = deepcopy(parameters)
    outputs = output_parameters
    eliashberg_data = (
        outputs.get("isotropic_eliashberg")
        or outputs.get("anisotropic_eliashberg")
        or {}
    )
    succeeded_temps = _get_succeeded_temperatures(eliashberg_data)

    input_epw = parameters.get("INPUTEPW", {})
    all_temps, is_linear_range = _get_input_temperatures(input_epw, eliashberg_data)
    remaining_temps = [
        temp
        for temp in all_temps
        if not any(abs(temp - succeeded) < 1.0e-4 for succeeded in succeeded_temps)
    ]

    nsiw = _get_first_value(eliashberg_data, remaining_temps, "nsiw")
    if nsiw is None:
        nsiw = input_epw.get("nsiw")
    current_npade = input_epw.get("npade", 90)
    current_iterations = _get_first_value(
        eliashberg_data, remaining_temps, "nsiter", nested_key="pade"
    )
    target_npade = _get_target_npade(
        input_epw, nsiw, current_npade, current_iterations, max_nsiw, min_nsiw
    )

    input_epw_new = parameters.setdefault("INPUTEPW", {})
    new_temps = (
        [remaining_temps[0], remaining_temps[-1]]
        if is_linear_range and len(remaining_temps) >= 2
        else remaining_temps
    )
    input_epw_new["temps"] = (
        " ".join(str(temp) for temp in new_temps)
        if isinstance(input_epw.get("temps"), str)
        else new_temps
    )
    input_epw_new["nstemp"] = len(remaining_temps)
    input_epw_new.pop("tempsmin", None)
    input_epw_new.pop("tempsmax", None)

    action_taken = ""
    if target_npade is not None and target_npade != current_npade:
        input_epw_new["npade"] = target_npade
        action_taken += f"Reduced npade from {current_npade} to {target_npade}. "
    if len(remaining_temps) < len(all_temps):
        succeeded = [
            temp
            for temp in all_temps
            if any(abs(temp - value) < 1.0e-4 for value in succeeded_temps)
        ]
        action_taken += f"Removed successfully calculated temperatures: {succeeded}. "

    if not action_taken:
        return None, None

    return parameters, action_taken


def _get_succeeded_temperatures(eliashberg_data):
    """Return temperatures whose reported Eliashberg data contain no failure marker."""
    succeeded_temps = []
    for temp_str, data in eliashberg_data.items():
        iterations = data.get("iterations", {})
        pade = data.get("pade", {})
        has_failed = False
        if iterations:
            if not iterations.get("ethr") or any(
                value is None for value in iterations.get("ethr", [])
            ):
                has_failed = True
            elif any(value is None for value in iterations.get("znormi", [])) or any(
                value is None for value in iterations.get("deltai", [])
            ):
                has_failed = True
        if pade and any(
            pade.get(key) is None for key in ("delta", "znorm", "shift") if key in pade
        ):
            has_failed = True
        if not iterations and not pade:
            has_failed = True
        if not has_failed:
            succeeded_temps.append(float(temp_str))
    return succeeded_temps


def _get_input_temperatures(input_epw, eliashberg_data):
    """Return the explicit temperature sequence and whether it was a linear range."""
    if "temps" in input_epw:
        temps = input_epw["temps"]
        if isinstance(temps, str):
            original_temps = [float(temp) for temp in temps.replace(",", " ").split()]
        elif isinstance(temps, (int, float)):
            original_temps = [float(temps)]
        else:
            original_temps = [float(temp) for temp in temps]
        nstemp = input_epw.get("nstemp", len(original_temps))
        if len(original_temps) == 2 and nstemp >= 2:
            return (
                [
                    original_temps[0]
                    + index * (original_temps[1] - original_temps[0]) / (nstemp - 1)
                    for index in range(nstemp)
                ],
                True,
            )
        return original_temps, False
    if {"tempsmin", "tempsmax", "nstemp"} <= input_epw.keys():
        temp_min = float(input_epw["tempsmin"])
        temp_max = float(input_epw["tempsmax"])
        nstemp = int(input_epw["nstemp"])
        if nstemp > 1:
            return (
                [
                    temp_min + index * (temp_max - temp_min) / (nstemp - 1)
                    for index in range(nstemp)
                ],
                True,
            )
        return [temp_min], True
    return [float(temp) for temp in eliashberg_data], False


def _get_first_value(eliashberg_data, temperatures, key, nested_key=None):
    """Return the first value matching a remaining input temperature."""
    for temperature in temperatures:
        for data_temperature, data in eliashberg_data.items():
            if abs(float(data_temperature) - temperature) < 1.0e-4:
                value = (
                    data.get(nested_key, {}).get(key) if nested_key else data.get(key)
                )
                if value is not None:
                    return value
    return None


def _get_target_npade(
    input_epw, nsiw, current_npade, current_iterations, max_nsiw, min_nsiw
):
    """Calculate a lower Pade iteration count compatible with the current mesh."""
    if current_iterations is None and nsiw is not None:
        divisor = (
            2
            if input_epw.get("fbw", False) and not input_epw.get("positive_matsu", True)
            else 1
        )
        current_iterations = int(current_npade * (nsiw / divisor) / 100)
    if current_iterations is None or not nsiw:
        return None

    target_iterations = (
        max_nsiw
        if current_iterations > max_nsiw
        else max(min_nsiw, int(current_iterations * 0.5))
    )
    divisor = (
        2
        if input_epw.get("fbw", False) and not input_epw.get("positive_matsu", True)
        else 1
    )
    target_npade = int(target_iterations * 100 / (nsiw / divisor))
    target_npade = max(1, min(100, target_npade))
    if target_npade == current_npade and current_npade > 1:
        return max(1, current_npade - 5)
    return target_npade
