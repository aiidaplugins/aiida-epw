"""Helpers for tracing workflow parent folders."""


def _filter_essential_parameters(params):
    """
    Only keep essential physical parameters for strict comparison.
    Ignores all other differences (e.g., max_seconds, tprnfor, etc.).
    """
    # Define parameters that must be strictly identical (allowlist)
    essential_keys = {
        "SYSTEM": ["smearing", "degauss", "ecutwfc", "ecutrho"],
        "ELECTRONS": ["conv_thr"],
    }

    filtered = {}
    for namelist, keys in essential_keys.items():
        if namelist in params:
            # If the namelist exists in the original dictionary, extract the keys of interest
            extracted_namelist = {}
            for key in keys:
                if key in params[namelist]:
                    extracted_namelist[key] = params[namelist][key]

            # Add to the final comparison dictionary only when essential content is extracted
            if extracted_namelist:
                filtered[namelist] = extracted_namelist

    return filtered


def _normalize_structure_component(value):
    """Normalize nested structure data for tolerant equality checks."""
    if isinstance(value, float):
        return round(value, 12)
    if isinstance(value, list):
        return [_normalize_structure_component(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize_structure_component(item) for item in value)
    if isinstance(value, dict):
        return {
            key: _normalize_structure_component(item)
            for key, item in sorted(value.items())
        }
    return value


def _as_plain_mapping(value):
    """Return an AiiDA or Python mapping as a plain dictionary."""
    if value is None:
        return {}
    if hasattr(value, "get_dict"):
        return value.get_dict()
    if isinstance(value, dict):
        return value
    return dict(value)


def _kpoints_signature(kpoints):
    """Return a normalized signature for a ``KpointsData`` node."""
    try:
        mesh, offset = kpoints.get_kpoints_mesh()
    except AttributeError:
        points, weights = kpoints.get_kpoints(also_weights=True)
        return {
            "mode": "explicit",
            "points": _normalize_structure_component(points.tolist()),
            "weights": _normalize_structure_component(weights.tolist()),
            "labels": _normalize_structure_component(getattr(kpoints, "labels", None)),
        }

    return {
        "mode": "mesh",
        "mesh": tuple(int(value) for value in mesh),
        "offset": _normalize_structure_component(offset),
    }


def kpoints_match(left, right) -> bool:
    """Return whether two ``KpointsData`` nodes describe the same grid."""
    return _kpoints_signature(left) == _kpoints_signature(right)


def _pseudo_signature(pseudo):
    """Return a stable pseudo signature suitable for compatibility checks."""
    try:
        repository_hash = pseudo.base.repository.hash()
    except Exception:  # pragma: no cover - repository access should usually work
        repository_hash = None

    filename = None
    if hasattr(pseudo, "filename"):
        filename = pseudo.filename

    return repository_hash or filename or getattr(pseudo, "uuid", None)


def _pseudos_signature(pseudos):
    """Return a normalized pseudo mapping signature."""
    return {
        key: _pseudo_signature(value)
        for key, value in sorted(_as_plain_mapping(pseudos).items())
    }


def structures_match(left, right) -> bool:
    """Return whether two ``StructureData`` nodes describe the same structure."""
    left_signature = {
        "cell": _normalize_structure_component(left.cell),
        "pbc": tuple(bool(value) for value in left.pbc),
        "kinds": _normalize_structure_component(left.base.attributes.get("kinds", [])),
        "sites": _normalize_structure_component(left.base.attributes.get("sites", [])),
    }
    right_signature = {
        "cell": _normalize_structure_component(right.cell),
        "pbc": tuple(bool(value) for value in right.pbc),
        "kinds": _normalize_structure_component(right.base.attributes.get("kinds", [])),
        "sites": _normalize_structure_component(right.base.attributes.get("sites", [])),
    }
    return left_signature == right_signature


def get_parent_folder_calculation(parent_folder):
    """Return the calculation that produced a remote or stashed parent folder."""
    current_node = parent_folder

    while True:
        creator = current_node.creator
        if creator is None:
            raise ValueError(
                f"The provided node {current_node} does not have a creator."
            )

        if creator.process_label == "move_stash":
            current_node = creator.inputs.stash_data
        else:
            return creator


def get_parent_ph_calculation(parent_folder_ph):
    """Return the original ``PhCalculation`` that produced a phonon parent folder."""
    calculation = get_parent_folder_calculation(parent_folder_ph)
    visited = set()

    while True:
        if calculation.process_label != "PhCalculation":
            raise ValueError(
                "`parent_folder_ph` must be created by a `PhCalculation` or its stashed "
                f"remote folder, got `{calculation.process_label}`."
            )

        identifier = getattr(calculation, "uuid", id(calculation))
        if identifier in visited:
            raise ValueError(
                "Detected a cycle while tracing `parent_folder_ph` back to the original `PhCalculation`."
            )
        visited.add(identifier)

        parent_folder = getattr(calculation.inputs, "parent_folder", None)
        if parent_folder is None:
            return calculation

        parent_calculation = get_parent_folder_calculation(parent_folder)
        if parent_calculation.process_label == "PwCalculation":
            return calculation

        if parent_calculation.process_label != "PhCalculation":
            return calculation

        calculation = parent_calculation


def get_parent_ph_qpoints(parent_folder_ph):
    """Return the q-point mesh associated with a phonon parent folder."""
    calculation = get_parent_ph_calculation(parent_folder_ph)
    qpoints = getattr(calculation.inputs, "qpoints", None)

    if qpoints is None:
        raise ValueError(
            "The provided `parent_folder_ph` does not expose input `qpoints`."
        )

    return qpoints


def get_parent_ph_qpoint_ibz_count(parent_folder_ph):
    """Return the number of irreducible q-points that need to be staged from `ph.x`."""

    calculation = get_parent_folder_calculation(parent_folder_ph)
    qibz_ar = []
    for key, value in sorted(calculation.outputs.output_parameters.get_dict().items()):
        if key.startswith("dynamical_matrix_"):
            qibz_ar.append(value["q_point"])

    return len(qibz_ar)


def get_parent_ph_pw_calculation(parent_folder_ph):
    """Return the original ``PwCalculation`` behind a phonon parent folder."""
    calculation = get_parent_ph_calculation(parent_folder_ph)

    parent_folder = getattr(calculation.inputs, "parent_folder", None)
    if parent_folder is None:
        raise ValueError(
            "The provided `parent_folder_ph` does not expose the parent folder "
            "needed to validate its SCF provenance."
        )

    parent_calculation = get_parent_folder_calculation(parent_folder)

    if parent_calculation.process_label != "PwCalculation":
        raise ValueError(
            "`parent_folder_ph` must trace back through `PhCalculation` restarts "
            f"to a `PwCalculation`, got `{parent_calculation.process_label}`."
        )

    return parent_calculation


def validate_parent_ph_inputs(
    parent_folder_ph,
    structure,
    *,
    scf_kpoints=None,
    scf_parameters=None,
    scf_pseudos=None,
):
    """Validate a phonon parent folder against the target EPW inputs."""
    qpoints = get_parent_ph_qpoints(parent_folder_ph)
    parent_pw_calculation = get_parent_ph_pw_calculation(parent_folder_ph)
    print("Found parent PW calculation:", getattr(parent_pw_calculation, "pk", None))
    parent_structure = getattr(parent_pw_calculation.inputs, "structure", None)
    if parent_structure is None:
        raise ValueError(
            "The PW calculation linked to `parent_folder_ph` does not expose input "
            "`structure`."
        )

    mismatches = []

    if not structures_match(parent_structure, structure):
        mismatches.append(f"structure {parent_structure.uuid} != {structure.uuid}")

    if scf_kpoints is not None:
        parent_kpoints = getattr(parent_pw_calculation.inputs, "kpoints", None)
        if parent_kpoints is None:
            mismatches.append("missing SCF kpoints on the parent PwCalculation")
        elif not kpoints_match(parent_kpoints, scf_kpoints):
            mismatches.append(
                "SCF kpoints "
                f"(parent={_kpoints_signature(parent_kpoints)}, "
                f"current={_kpoints_signature(scf_kpoints)})"
            )

    if scf_parameters is not None:
        parent_parameters = getattr(parent_pw_calculation.inputs, "parameters", None)
        if parent_parameters is None:
            mismatches.append("missing SCF pw.parameters on the parent PwCalculation")
        else:
            # 1. Extract the allowlisted parameters of the parent calculation
            parent_dict = _filter_essential_parameters(
                _normalize_structure_component(parent_parameters.get_dict())
            )
            # 2. Extract the current allowlisted parameters to be passed
            current_dict = _filter_essential_parameters(
                _normalize_structure_component(_as_plain_mapping(scf_parameters))
            )

            # 3. Compare only these core physical quantities
            if parent_dict != current_dict:
                # For easier troubleshooting, we can print the mismatch details
                mismatches.append(
                    f"SCF pw.parameters mismatch in essential keys (Parent: {parent_dict} vs Current: {current_dict})"
                )

    if scf_pseudos is not None:
        parent_pseudos = getattr(parent_pw_calculation.inputs, "pseudos", None)
        if parent_pseudos is None:
            mismatches.append("missing SCF pseudos on the parent PwCalculation")
        elif _pseudos_signature(parent_pseudos) != _pseudos_signature(scf_pseudos):
            mismatches.append("SCF pseudos")

    if mismatches:
        raise ValueError(
            "`parent_folder_ph` is incompatible with the current prep inputs; "
            f"mismatched fields: {', '.join(mismatches)}."
        )

    return qpoints


def find_related_calculation(parent_folder_epw):
    """Find the related calculation from a parent folder of an epw calculation."""
    calculation = get_parent_folder_calculation(parent_folder_epw)

    if not calculation.process_label == "EpwCalculation":
        raise ValueError(
            f"Related calculation is not a valid epw calculation: {calculation.process_label}"
        )

    return calculation


def format_subprocess_failure(node, process_label=None):
    """Return a readable failure message for a subprocess node."""
    label = process_label or getattr(node, "process_label", node.__class__.__name__)
    message = f"{label}<{node.pk}> failed with exit status {node.exit_status}"
    exit_message = getattr(node, "exit_message", None)

    if exit_message:
        message = f"{message}: {exit_message}"

    return message


def get_default_target_basepath(computer):
    """Set the target basepath for the stash folder."""
    from pathlib import Path

    if computer.transport_type == "core.local":
        target_basepath = Path(computer.get_workdir(), "stash").as_posix()
    elif computer.transport_type.startswith("core.ssh"):
        workdir = computer.get_workdir()
        if "{username}" in workdir:
            username = computer.get_configuration().get("username")
            if not username:
                try:
                    from aiida.orm import User

                    auth_info = computer.get_authinfo(User.objects.get_default())
                    username = auth_info.get_auth_params().get("username")
                except Exception:
                    pass
            if not username:
                raise ValueError(
                    f"Could not determine username to format workdir for computer '{computer.label}'"
                )
            target_basepath = Path(
                workdir.format(username=username), "stash"
            ).as_posix()
        else:
            target_basepath = Path(workdir, "stash").as_posix()
    else:
        raise ValueError(f"Unsupported transport type: {computer.transport_type}")
    return target_basepath


def pop_succeeded_temperatures(parameters, output_parameters):
    """Analyze Eliashberg output parameters and pop successfully completed temperatures from input parameters.

    :param parameters: Dict containing input namelist parameters
    :param output_parameters: Dict containing output parameters of the calculation
    :return: A tuple of (updated_parameters_dict, succeeded_temps, remaining_temps, eliashberg_data)
    """
    eliashberg_data = (
        output_parameters.get("isotropic_eliashberg")
        or output_parameters.get("anisotropic_eliashberg")
        or {}
    )

    succeeded_temps = []
    for temp_str, data in eliashberg_data.items():
        temp = float(temp_str)
        has_failed = False

        iterations = data.get("iterations", {})
        if iterations:
            if not iterations.get("ethr") or any(
                v is None for v in iterations.get("ethr", [])
            ):
                has_failed = True
            elif any(v is None for v in iterations.get("znormi", [])) or any(
                v is None for v in iterations.get("deltai", [])
            ):
                has_failed = True

        pade = data.get("pade", {})
        if pade:
            if any(
                pade.get(k) is None for k in ("delta", "znorm", "shift") if k in pade
            ):
                has_failed = True

        if not iterations and not pade:
            has_failed = True

        if not has_failed:
            succeeded_temps.append(temp)

    input_epw = parameters.get("INPUTEPW", {})
    all_temps = []
    is_linear_range = False
    if "temps" in input_epw:
        temps_val = input_epw["temps"]
        if isinstance(temps_val, str):
            original_temps = [float(t) for t in temps_val.replace(",", " ").split()]
        elif isinstance(temps_val, (int, float)):
            original_temps = [float(temps_val)]
        else:
            original_temps = [float(t) for t in temps_val]

        nstemp = input_epw.get("nstemp", len(original_temps))
        if len(original_temps) == 2 and nstemp >= 2:
            is_linear_range = True
            t_min, t_max = original_temps[0], original_temps[1]
            all_temps = [
                t_min + i * (t_max - t_min) / (nstemp - 1) for i in range(nstemp)
            ]
        else:
            all_temps = original_temps
    elif "tempsmin" in input_epw and "tempsmax" in input_epw and "nstemp" in input_epw:
        is_linear_range = True
        t_min = float(input_epw["tempsmin"])
        t_max = float(input_epw["tempsmax"])
        n_temp = int(input_epw["nstemp"])
        if n_temp > 1:
            all_temps = [
                t_min + i * (t_max - t_min) / (n_temp - 1) for i in range(n_temp)
            ]
        else:
            all_temps = [t_min]
    else:
        all_temps = [float(k) for k in eliashberg_data.keys()]

    remaining_temps = [
        t for t in all_temps if not any(abs(t - st) < 1e-4 for st in succeeded_temps)
    ]

    new_temps_list = []
    new_nstemp = len(remaining_temps)
    if is_linear_range and new_nstemp >= 2:
        new_temps_list = [remaining_temps[0], remaining_temps[-1]]
    else:
        new_temps_list = remaining_temps

    updated_parameters = dict(parameters)
    input_epw_new = updated_parameters.setdefault("INPUTEPW", {})

    if isinstance(input_epw.get("temps"), str):
        input_epw_new["temps"] = " ".join(str(t) for t in new_temps_list)
    else:
        input_epw_new["temps"] = new_temps_list
    input_epw_new["nstemp"] = new_nstemp
    input_epw_new.pop("tempsmin", None)
    input_epw_new.pop("tempsmax", None)

    return updated_parameters, succeeded_temps, remaining_temps, eliashberg_data
