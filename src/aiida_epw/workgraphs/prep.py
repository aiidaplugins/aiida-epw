"""WorkGraph implementation of the EPW preparation workflow."""

from __future__ import annotations

from typing import Any

from aiida import orm
from aiida_workgraph import If, WorkGraph, spec, task
from aiida_quantumespresso.workflows.protocols.utils import recursive_merge
from aiida_quantumespresso.workflows.ph.base import PhBaseWorkChain
from aiida_quantumespresso.workflows.pw.base import PwBaseWorkChain
from aiida_workgraph.utils import get_dict_from_builder
from aiida_wannier90_workflows.workflows import (
    Wannier90BandsWorkChain,
    Wannier90OptimizeWorkChain,
)
from aiida_wannier90_workflows.common.types import WannierProjectionType

from aiida_epw.tools.workchain import (
    get_target_basepath,
    validate_parent_ph_inputs,
)
from aiida_epw.workflows.base import EpwBaseWorkChain

PwBaseTask = task(PwBaseWorkChain)
Wannier90OptimizeTask = task(Wannier90OptimizeWorkChain)
Wannier90BandsTask = task(Wannier90BandsWorkChain)
PhBaseTask = task(PhBaseWorkChain)
EpwBaseTask = task(EpwBaseWorkChain)

__all__ = (
    "prep",
    "prep_from_inputs",
    "build_task_inputs",
)


def _copy_nested_containers(value: Any) -> Any:
    """Recursively copy Python containers while preserving AiiDA ORM nodes."""
    if isinstance(value, dict):
        return {key: _copy_nested_containers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_nested_containers(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_copy_nested_containers(item) for item in value)
    return value


def _normalize_nested_python_values(value: Any) -> Any:
    """Normalize nested Python containers to avoid non-JSON-serializable values."""
    if isinstance(value, orm.Dict):
        return orm.Dict(dict=_normalize_nested_python_values(value.get_dict()))
    if isinstance(value, dict):
        return {
            key: _normalize_nested_python_values(item) for key, item in value.items()
        }
    if isinstance(value, list):
        return [_normalize_nested_python_values(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize_nested_python_values(item) for item in value)
    if isinstance(value, range):
        return list(value)
    return value


def _get_nested_value(mapping: dict[str, Any], path: str) -> Any:
    """Return a nested value from a mapping or ``None`` if the path is missing."""
    current: Any = mapping
    for key in path.split("."):
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _set_nested_value(mapping: dict[str, Any], path: str, value: Any) -> None:
    """Set a nested value inside a mapping, creating intermediate dictionaries."""
    keys = path.split(".")
    current = mapping
    for key in keys[:-1]:
        current = current.setdefault(key, {})
    current[keys[-1]] = value


def _restore_nested_fields(
    target: dict[str, Any],
    source: dict[str, Any],
    paths: tuple[str, ...],
) -> dict[str, Any]:
    """Restore selected nested fields from a source mapping into a target mapping."""
    restored = _copy_nested_containers(target)
    for path in paths:
        value = _get_nested_value(source, path)
        if value is not None:
            _set_nested_value(restored, path, _copy_nested_containers(value))
    return restored


def _set_socket_value(namespace: Any, path: str, value: Any) -> None:
    """Set a nested socket value using a dotted path."""
    current = namespace
    keys = path.split(".")
    for key in keys[:-1]:
        current = current[key]
    current[keys[-1]] = value


def _apply_socket_overrides(
    namespace: Any,
    source: dict[str, Any],
    paths: tuple[str, ...],
) -> None:
    """Apply selected values from a source mapping onto a socket namespace."""
    for path in paths:
        value = _get_nested_value(source, path)
        if value is not None:
            _set_socket_value(namespace, path, value)


def validate_inputs(  # pylint: disable=unused-argument,inconsistent-return-statements
    inputs, ctx=None
):
    """Validate the inputs of the `EpwPrepWorkChain`."""
    has_w90_bands = "w90_bands" in inputs
    use_epw_wannierize = should_epw_wannierize(inputs)

    if has_w90_bands and use_epw_wannierize:
        return (
            "`w90_bands` inputs and `epw_base.parameters.INPUTEPW.wannierize = True` "
            "are mutually exclusive."
        )

    if not has_w90_bands and not use_epw_wannierize:
        return (
            "Either provide `w90_bands` inputs or set "
            "`epw_base.parameters.INPUTEPW.wannierize = True`."
        )

    if use_epw_wannierize:
        missing = [
            namespace for namespace in ("scf", "nscf") if namespace not in inputs
        ]
        if missing:
            return (
                "`scf` and `nscf` inputs are required when "
                "`epw_base.parameters.INPUTEPW.wannierize = True`."
            )

    if "parent_folder_ph" not in inputs and "ph_base" not in inputs:
        return "Either provide `ph_base` inputs or set `parent_folder_ph`."


def should_epw_wannierize(inputs) -> bool:
    """Return whether the EPW namespace is configured to run Wannierization directly."""
    epw_base = _as_dict(inputs.get("epw_base"))
    parameters = _as_dict(epw_base.get("parameters"))
    inputepw = _as_dict(parameters.get("INPUTEPW", parameters.get("inputepw")))
    return bool(inputepw.get("wannierize", False))


def _as_bool(value: Any) -> bool:
    """Convert Python and AiiDA booleans into a plain boolean."""
    if isinstance(value, orm.Bool):
        return value.value
    return bool(value)


def _as_dict(value: Any) -> dict[str, Any]:
    """Convert AiiDA and plain mapping inputs into a plain dictionary."""
    if value is None:
        return {}
    if isinstance(value, orm.Dict):
        return value.get_dict()
    if isinstance(value, dict):
        return value
    return dict(value)


def _create_kpoints_from_distance_node(
    structure: orm.StructureData,
    distance: orm.Float,
    force_parity: orm.Bool,
) -> orm.KpointsData:
    """Create a k-point mesh with the same symmetry handling as the QE helper."""
    from numpy import linalg

    epsilon = 1e-5

    kpoints = orm.KpointsData()
    kpoints.set_cell_from_structure(structure)
    kpoints.set_kpoints_mesh_from_density(
        distance.value,
        force_parity=force_parity.value,
    )

    lengths_vector = [linalg.norm(vector) for vector in structure.cell]
    lengths_kpoint = kpoints.get_kpoints_mesh()[0]

    is_symmetric_cell = all(
        abs(length - lengths_vector[0]) < epsilon for length in lengths_vector
    )
    is_symmetric_mesh = all(length == lengths_kpoint[0] for length in lengths_kpoint)

    if is_symmetric_cell and not is_symmetric_mesh:
        nkpoints = max(lengths_kpoint)
        kpoints.set_kpoints_mesh([nkpoints if pbc else 1 for pbc in structure.pbc])

    return kpoints


def get_protocol_inputs(
    protocol: str | None = None,
    overrides: dict | None = None,
) -> dict:
    """Return the inputs for the EPW preparation workflow based on a protocol."""
    from importlib_resources import files
    from aiida_epw.workflows import protocols
    from aiida_quantumespresso.workflows.protocols.utils import ProtocolMixin

    # 1. Load protocol file from workflows side
    filepath = files(protocols) / "prep.yaml"

    AdHocProtocol = type(
        "AdHocProtocol",
        (ProtocolMixin,),
        {
            "get_protocol_filepath": classmethod(lambda cls: filepath),
            "_validate_override_keys": classmethod(lambda cls, overrides: None),
        },
    )

    return AdHocProtocol.get_protocol_inputs(protocol, overrides)


def _add_metadata_stash_target_base(inputs: dict[str, Any], code) -> None:
    """Populate a ``metadata.options.stash`` target base when missing."""
    if code is None:
        return

    stash = (
        inputs.setdefault("metadata", {})
        .setdefault("options", {})
        .setdefault("stash", {})
    )
    if "target_base" not in stash:
        stash["target_base"] = get_target_basepath(code.computer)
        stash["stash_mode"] = stash.get("stash_mode", "copy")


def _add_options_stash_target_base(inputs: dict[str, Any], code) -> None:
    """Populate an ``options.stash`` target base when missing."""
    if code is None:
        return

    stash = inputs.setdefault("options", {}).setdefault("stash", {})
    if "target_base" not in stash:
        stash["target_base"] = get_target_basepath(code.computer)
        stash["stash_mode"] = stash.get("stash_mode", "copy")


def _set_call_link_label(inputs: dict[str, Any], label: str) -> dict[str, Any]:
    """Return inputs with a top-level metadata call link label."""
    updated = _copy_nested_containers(inputs)
    metadata = updated.setdefault("metadata", {})
    metadata["call_link_label"] = label
    return updated


def _validate_reference_bands_projection_type(
    reference_bands: orm.BandsData | None,
    wannier_projection_type: WannierProjectionType,
) -> None:
    """Reject unsupported optimization modes for the selected projection type."""
    if (
        reference_bands is not None
        and wannier_projection_type == WannierProjectionType.ANALYTIC
    ):
        raise ValueError(
            "`reference_bands` with `WannierProjectionType.ANALYTIC` is not "
            "supported in `EpwPrepWorkChain`: the optimize branch uses "
            "`Wannier90OptimizeWorkChain`, which optimizes `dis_proj_min/max`. "
            "Use `Wannier90BandsWorkChain` without `reference_bands` and tune "
            "`dis_win_*`/`dis_froz_*` manually."
        )


def _validate_parent_folder_ph(parent_folder_ph, structure) -> None:
    """Validate that a provided phonon parent folder can be reused safely."""
    if parent_folder_ph is None:
        return

    validate_parent_ph_inputs(parent_folder_ph, structure)


def _build_wannier90_inputs(
    *,
    codes: dict[str, Any],
    structure: orm.StructureData,
    protocol_inputs: dict[str, Any],
    pseudo_family: Any,
    wannier_projection_type,
    reference_bands: orm.BandsData | None,
    bands_kpoints: orm.KpointsData | None,
) -> dict[str, Any]:
    """Build the static inputs for the Wannier90 task."""
    _validate_reference_bands_projection_type(reference_bands, wannier_projection_type)
    w90_overrides = _copy_nested_containers(protocol_inputs.get("w90_bands", {}))
    if reference_bands is not None:
        w90_builder = Wannier90OptimizeWorkChain.get_builder_from_protocol(
            structure=structure,
            codes=codes,
            pseudo_family=pseudo_family,
            overrides=protocol_inputs.get("w90_bands", {}),
            projection_type=wannier_projection_type,
            reference_bands=reference_bands,
            bands_kpoints=bands_kpoints,
        )
        w90_builder.separate_plotting = False
    else:
        w90_builder = Wannier90BandsWorkChain.get_builder_from_protocol(
            structure=structure,
            codes=codes,
            pseudo_family=pseudo_family,
            overrides=protocol_inputs.get("w90_bands", {}),
            projection_type=wannier_projection_type,
            bands_kpoints=bands_kpoints,
        )

    w90_bands = get_dict_from_builder(w90_builder)
    w90_bands = _normalize_nested_python_values(w90_bands)
    w90_bands = _restore_nested_fields(
        w90_bands,
        w90_overrides,
        (
            "scf.pw.metadata",
            "nscf.pw.metadata",
            "pw2wannier90.pw2wannier90.metadata",
            "wannier90.wannier90.metadata",
        ),
    )
    if wannier_projection_type == WannierProjectionType.ATOMIC_PROJECTORS_QE:
        w90_bands.pop("projwfc", None)

    w90_bands.pop("structure", None)
    w90_bands.pop("open_grid", None)
    return w90_bands


def _build_ph_inputs(
    *,
    codes: dict[str, Any],
    protocol: str | None,
    protocol_inputs: dict[str, Any],
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Build the static inputs for the phonon task."""
    if codes.get("ph") is None:
        return {}

    ph_base_inputs = protocol_inputs.get("ph_base", {})
    _add_metadata_stash_target_base(
        ph_base_inputs.setdefault("ph", {}), codes.get("ph")
    )

    ph_base_builder = PhBaseWorkChain.get_builder_from_protocol(
        codes["ph"], None, protocol, overrides=ph_base_inputs, **kwargs
    )
    ph_base = get_dict_from_builder(ph_base_builder)
    ph_base = _normalize_nested_python_values(ph_base)
    ph_base = _restore_nested_fields(ph_base, ph_base_inputs, ("ph.metadata",))
    ph_base.pop("clean_workdir", None)
    ph_base.pop("qpoints_distance", None)
    return ph_base


def _build_pw_inputs(
    *,
    code: Any,
    structure: orm.StructureData,
    protocol: str | None,
    pseudo_family: Any,
    namespace_inputs: dict[str, Any],
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Build static inputs for a direct PW task."""
    pw_inputs = recursive_merge(
        {"pseudo_family": pseudo_family},
        namespace_inputs,
    )
    pw_builder = PwBaseWorkChain.get_builder_from_protocol(
        code=code,
        structure=structure,
        protocol=protocol,
        overrides=pw_inputs,
        **kwargs,
    )
    inputs = get_dict_from_builder(pw_builder)
    inputs = _normalize_nested_python_values(inputs)
    inputs = _restore_nested_fields(inputs, namespace_inputs, ("pw.metadata",))
    inputs.pop("clean_workdir", None)
    inputs.pop("kpoints_distance", None)
    return inputs


def _build_epw_inputs(
    *,
    codes: dict[str, Any],
    structure: orm.StructureData,
    protocol: str | None,
    protocol_inputs: dict[str, Any],
    kwargs: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build the static inputs for the transformation and bands EPW tasks."""
    epw_base: dict[str, Any] = {}
    epw_bands: dict[str, Any] = {}
    epw_code = codes.get("epw")

    for namespace in ("epw_base", "epw_bands"):
        epw_inputs = protocol_inputs.get(namespace, {})
        if namespace == "epw_base":
            _add_options_stash_target_base(epw_inputs, epw_code)

        epw_builder = EpwBaseWorkChain.get_builder_from_protocol(
            code=codes["epw"],
            structure=structure,
            protocol=protocol,
            overrides=epw_inputs,
            **kwargs,
        )

        if "settings" in epw_inputs:
            epw_builder.settings = orm.Dict(epw_inputs["settings"])
        if "parallelization" in epw_inputs:
            epw_builder.parallelization = orm.Dict(epw_inputs["parallelization"])

        if namespace == "epw_base":
            epw_base = get_dict_from_builder(epw_builder)
            epw_base = _normalize_nested_python_values(epw_base)
            epw_base = _restore_nested_fields(epw_base, epw_inputs, ("options",))
        else:
            epw_bands = get_dict_from_builder(epw_builder)
            epw_bands = _normalize_nested_python_values(epw_bands)
            epw_bands = _restore_nested_fields(epw_bands, epw_inputs, ("options",))

    return epw_base, epw_bands


def _drop_epw_mesh_generation_inputs(inputs: dict[str, Any]) -> dict[str, Any]:
    """Drop builder-side mesh generation inputs when explicit meshes are wired in the graph."""
    cleaned = _copy_nested_containers(inputs)
    cleaned.pop("kfpoints_factor", None)
    cleaned.pop("qfpoints_distance", None)
    return cleaned


def _build_wannier90_task_inputs(
    w90_bands: dict[str, Any],
    structure: orm.StructureData,
    kpoints_scf: orm.KpointsData,
    kpoints_nscf: orm.KpointsData,
    wannier90_kpoints: orm.KpointsData,
    parameters: Any,
) -> dict[str, Any]:
    """Build inputs for a Wannier90 workchain task."""
    inputs = _copy_nested_containers(w90_bands)
    inputs["structure"] = structure
    inputs["scf"]["kpoints"] = kpoints_scf
    inputs["nscf"]["kpoints"] = kpoints_nscf
    inputs["wannier90"]["wannier90"]["kpoints"] = wannier90_kpoints
    inputs["wannier90"]["wannier90"]["parameters"] = parameters
    return inputs


def build_task_inputs(
    *,
    codes: dict,
    structure: orm.StructureData,
    protocol: str | None = None,
    overrides: dict | None = None,
    wannier_projection_type=None,
    reference_bands: orm.BandsData | None = None,
    bands_kpoints: orm.KpointsData | None = None,
    parent_folder_ph: orm.RemoteData | orm.RemoteStashFolderData | None = None,
    **kwargs,
) -> dict[str, Any]:
    """Build and validate the static inputs required by the prep WorkGraph."""
    if hasattr(codes, "items"):
        codes = dict(codes.items())
    if overrides and hasattr(overrides, "items"):
        overrides = dict(overrides.items())

    protocol_inputs = get_protocol_inputs(protocol, overrides)
    if parent_folder_ph is not None:
        _validate_parent_folder_ph(parent_folder_ph, structure)
    use_epw_wannierize = should_epw_wannierize(protocol_inputs)
    protocol_inputs = _copy_nested_containers(protocol_inputs)
    if parent_folder_ph is not None:
        protocol_inputs["parent_folder_ph"] = parent_folder_ph
    if use_epw_wannierize:
        protocol_inputs.pop("w90_bands", None)
    else:
        protocol_inputs.pop("scf", None)
        protocol_inputs.pop("nscf", None)

    validation_error = validate_inputs(protocol_inputs)
    if validation_error is not None:
        raise ValueError(validation_error)

    if wannier_projection_type is None:
        wannier_projection_type = WannierProjectionType.ATOMIC_PROJECTORS_QE

    pseudo_family = protocol_inputs.pop("pseudo_family", None)
    w90_bands: dict[str, Any] = {}
    scf: dict[str, Any] = {}
    nscf: dict[str, Any] = {}

    if use_epw_wannierize:
        scf = _build_pw_inputs(
            code=codes["pw"],
            structure=structure,
            protocol=protocol,
            pseudo_family=pseudo_family,
            namespace_inputs=protocol_inputs.get("scf", {}),
            kwargs=kwargs,
        )
        nscf = _build_pw_inputs(
            code=codes["pw"],
            structure=structure,
            protocol=protocol,
            pseudo_family=pseudo_family,
            namespace_inputs=protocol_inputs.get("nscf", {}),
            kwargs=kwargs,
        )
    else:
        w90_bands = _build_wannier90_inputs(
            codes=codes,
            structure=structure,
            protocol_inputs=protocol_inputs,
            pseudo_family=pseudo_family,
            wannier_projection_type=wannier_projection_type,
            reference_bands=reference_bands,
            bands_kpoints=bands_kpoints,
        )
    ph_base = {}
    if parent_folder_ph is None:
        ph_base = _build_ph_inputs(
            codes=codes,
            protocol=protocol,
            protocol_inputs=protocol_inputs,
            kwargs=kwargs,
        )
    epw_base, epw_bands = _build_epw_inputs(
        codes=codes,
        structure=structure,
        protocol=protocol,
        protocol_inputs=protocol_inputs,
        kwargs=kwargs,
    )

    return {
        "w90_bands": w90_bands,
        "scf": scf,
        "nscf": nscf,
        "ph_base": ph_base,
        "epw_base": epw_base,
        "epw_bands": epw_bands,
        "qpoints_distance": orm.Float(protocol_inputs["qpoints_distance"]),
        "kpoints_distance_scf": orm.Float(protocol_inputs["kpoints_distance_scf"]),
        "kpoints_factor_nscf": orm.Int(protocol_inputs["kpoints_factor_nscf"]),
        "kpoints_force_parity": orm.Bool(
            protocol_inputs.get("kpoints_force_parity", False)
        ),
        "parent_folder_ph": parent_folder_ph,
    }


@task.calcfunction(
    outputs=spec.namespace(
        kpoints_scf=Any,
        qpoints=Any,
        kpoints_nscf=Any,
    )
)
def generate_reciprocal_points(
    structure,
    force_parity,
    kpoints_distance_scf,
    qpoints_distance,
    kpoints_factor_nscf,
    parent_folder_ph=None,
):
    """Generate the SCF k-point mesh, Q-point mesh, and NSCF k-point mesh for the EPW parameters."""
    kpoints_scf = _create_kpoints_from_distance_node(
        structure=structure,
        distance=kpoints_distance_scf,
        force_parity=force_parity,
    )
    if parent_folder_ph is not None:
        qpoints = validate_parent_ph_inputs(parent_folder_ph, structure)
    else:
        qpoints = _create_kpoints_from_distance_node(
            structure=structure,
            distance=qpoints_distance,
            force_parity=force_parity,
        )
    qpoints_mesh = qpoints.get_kpoints_mesh()[0]
    kpoints_nscf = orm.KpointsData()
    kpoints_nscf.set_kpoints_mesh(
        [value * kpoints_factor_nscf.value for value in qpoints_mesh]
    )

    return {
        "kpoints_scf": kpoints_scf,
        "qpoints": qpoints,
        "kpoints_nscf": kpoints_nscf,
    }


@task()
def should_run_wannier90(w90_parameters) -> bool:
    """Mirror the outline guard for the Wannier90 branch."""
    bands_plot = False
    if w90_parameters is not None:
        bands_plot = _as_dict(w90_parameters).get("bands_plot", False)
    return orm.Bool(bands_plot)


@task.calcfunction(
    outputs=spec.namespace(
        nscf_kpoints=Any,
        wannier90_kpoints=Any,
        parameters=Any,
    )
)
def prepare_wannier90_runtime_inputs(parameters, kpoints_nscf):
    """Build runtime NSCF and Wannier90 kpoints from the NSCF mesh."""
    from aiida_wannier90_workflows.utils.kpoints import get_explicit_kpoints

    explicit_kpoints = get_explicit_kpoints(kpoints_nscf)
    points, weights = explicit_kpoints.get_kpoints(also_weights=True)
    labels = explicit_kpoints.labels if explicit_kpoints.labels else None

    nscf_kpoints = orm.KpointsData()
    nscf_kpoints.set_kpoints(points, weights=weights, labels=labels)

    wannier90_kpoints = orm.KpointsData()
    wannier90_kpoints.set_kpoints(points, weights=weights, labels=labels)

    updated = parameters.get_dict()
    updated["mp_grid"] = kpoints_nscf.get_kpoints_mesh()[0]
    return {
        "nscf_kpoints": nscf_kpoints,
        "wannier90_kpoints": wannier90_kpoints,
        "parameters": orm.Dict(updated),
    }


@task.calcfunction()
def create_kpoints_gamma():
    """Create the gamma-only mesh used by the transformation EPW run."""
    gamma = orm.KpointsData()
    gamma.set_kpoints_mesh([1, 1, 1])
    return gamma


@task.calcfunction()
def prepare_nscf_runtime_kpoints(kpoints_nscf):
    """Build explicit NSCF k-points from the coarse NSCF mesh."""
    from aiida_wannier90_workflows.utils.kpoints import get_explicit_kpoints

    return get_explicit_kpoints(kpoints_nscf)


@task.workfunction()
def get_seekpath_explicit_kpoints(reference_output):
    """Trace the explicit k-point path back to the internal seekpath calcfunction."""
    from aiida.common.links import LinkType
    from aiida.orm import ProcessNode

    workchain = None
    for link in reference_output.base.links.get_incoming(
        link_type=LinkType.RETURN
    ).all():
        if not isinstance(link.node, ProcessNode):
            continue
        outgoing = link.node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).all()
        if any(out.link_label == "seekpath_structure_analysis" for out in outgoing):
            workchain = link.node
            break

    if workchain is None:
        raise ValueError(
            "Could not find a parent workchain with a `seekpath_structure_analysis` call."
        )

    for link in workchain.base.links.get_outgoing(link_type=LinkType.CALL_CALC).all():
        if link.link_label == "seekpath_structure_analysis":
            return link.node.outputs.explicit_kpoints

    raise ValueError(
        "The parent workchain does not expose an `explicit_kpoints` output from seekpath."
    )


@task.calcfunction()
def extract_kpoints_path(band_structure):
    """Convert a ``BandsData`` output into a standalone ``KpointsData`` node."""
    kpoints = orm.KpointsData()
    kpoints.set_cell(band_structure.cell, band_structure.pbc)

    points, weights = band_structure.get_kpoints(also_weights=True)
    labels = band_structure.labels if band_structure.labels else None
    kpoints.set_kpoints(points, weights=weights, labels=labels)

    return kpoints


@task.workfunction()
def generate_seekpath_explicit_kpoints(structure):
    """Generate an explicit band path directly from the input structure."""
    from aiida_quantumespresso.calculations.functions.seekpath_structure_analysis import (
        seekpath_structure_analysis,
    )

    result = seekpath_structure_analysis(
        structure=structure,
        metadata={"call_link_label": "seekpath_structure_analysis"},
    )
    return result["explicit_kpoints"]


@task(
    outputs=spec.namespace(
        retrieved=Any,
        epw_folder=Any,
        epw_stash=Any,
    )
)
def results(retrieved, epw_folder, epw_stash=None, epw_kpoints=None, epw_qpoints=None):
    """Compatibility task retained so previously saved prep graphs can still be loaded."""
    return {
        "retrieved": retrieved,
        "epw_folder": epw_folder,
        "epw_stash": epw_stash,
        "epw_kpoints": epw_kpoints,
        "epw_qpoints": epw_qpoints,
    }


def prep_from_inputs(
    *,
    structure: orm.StructureData,
    inputs: dict[str, Any],
    use_wannier90_optimize: bool,
    w90_chk_to_ukk_script: orm.RemoteData | None = None,
):
    """Create the prep WorkGraph from pre-built static task inputs."""
    use_epw_wannierize = should_epw_wannierize(inputs)
    w90_bands = inputs.get("w90_bands", {})
    scf = inputs.get("scf", {})
    nscf = inputs.get("nscf", {})
    ph_base = inputs.get("ph_base", {})
    epw_base = inputs["epw_base"]
    epw_bands = inputs["epw_bands"]

    with WorkGraph(
        name="prep",
        outputs=spec.namespace(
            retrieved=Any,
            epw_folder=Any,
            epw_stash=Any,
        ),
    ) as wg:
        parent_folder_ph = inputs.get("parent_folder_ph")
        reciprocal_points = generate_reciprocal_points(
            structure=structure,
            force_parity=inputs["kpoints_force_parity"],
            kpoints_distance_scf=inputs["kpoints_distance_scf"],
            qpoints_distance=inputs["qpoints_distance"],
            kpoints_factor_nscf=inputs["kpoints_factor_nscf"],
            parent_folder_ph=parent_folder_ph,
        )

        if use_epw_wannierize:
            scf_inputs = recursive_merge(
                _copy_nested_containers(scf),
                {
                    "pw": {"structure": structure},
                    "kpoints": reciprocal_points.kpoints_scf,
                },
            )
            scf_run = PwBaseTask(**_set_call_link_label(scf_inputs, "scf"))
            _apply_socket_overrides(
                scf_run._task.inputs,
                scf,
                ("pw.code", "pw.metadata"),
            )

            nscf_kpoints = prepare_nscf_runtime_kpoints(
                kpoints_nscf=reciprocal_points.kpoints_nscf,
            )
            nscf_inputs = recursive_merge(
                _copy_nested_containers(nscf),
                {
                    "pw": {
                        "structure": structure,
                        "parent_folder": scf_run.remote_folder,
                    },
                    "kpoints": nscf_kpoints.result,
                },
            )
            nscf_run = PwBaseTask(**_set_call_link_label(nscf_inputs, "nscf"))
            _apply_socket_overrides(
                nscf_run._task.inputs,
                nscf,
                ("pw.code", "pw.metadata"),
            )

            scf_remote = scf_run.remote_folder
            nscf_remote = nscf_run.remote_folder
            chk_folder = None
        else:
            should_run_w90 = should_run_wannier90(
                w90_parameters=w90_bands.get("wannier90", {})
                .get("wannier90", {})
                .get("parameters")
            )
            with If(should_run_w90.result):
                wannier90_runtime_inputs = prepare_wannier90_runtime_inputs(
                    parameters=w90_bands["wannier90"]["wannier90"]["parameters"],
                    kpoints_nscf=reciprocal_points.kpoints_nscf,
                )
                if use_wannier90_optimize:
                    wannier90_inputs = _build_wannier90_task_inputs(
                        w90_bands=w90_bands,
                        structure=structure,
                        kpoints_scf=reciprocal_points.kpoints_scf,
                        kpoints_nscf=wannier90_runtime_inputs.nscf_kpoints,
                        wannier90_kpoints=wannier90_runtime_inputs.wannier90_kpoints,
                        parameters=wannier90_runtime_inputs.parameters,
                    )
                    wannier90_run_proxy = Wannier90OptimizeTask(
                        **_set_call_link_label(wannier90_inputs, "w90_bands"),
                    )
                    _apply_socket_overrides(
                        wannier90_run_proxy._task.inputs,
                        w90_bands,
                        (
                            "scf.pw.code",
                            "scf.pw.metadata",
                            "nscf.pw.code",
                            "nscf.pw.metadata",
                            "pw2wannier90.pw2wannier90.code",
                            "pw2wannier90.pw2wannier90.metadata",
                            "wannier90.wannier90.code",
                            "wannier90.wannier90.metadata",
                        ),
                    )

                    scf_remote = wannier90_run_proxy.scf.remote_folder
                    nscf_remote = wannier90_run_proxy.nscf.remote_folder

                    if _as_bool(w90_bands.get("optimize_disproj", False)):
                        chk_folder = wannier90_run_proxy.wannier90_optimal.remote_folder
                    else:
                        chk_folder = wannier90_run_proxy.wannier90.remote_folder
                else:
                    wannier90_inputs = _build_wannier90_task_inputs(
                        w90_bands=w90_bands,
                        structure=structure,
                        kpoints_scf=reciprocal_points.kpoints_scf,
                        kpoints_nscf=wannier90_runtime_inputs.nscf_kpoints,
                        wannier90_kpoints=wannier90_runtime_inputs.wannier90_kpoints,
                        parameters=wannier90_runtime_inputs.parameters,
                    )
                    wannier90_run_proxy = Wannier90BandsTask(
                        **_set_call_link_label(wannier90_inputs, "w90_bands"),
                    )
                    _apply_socket_overrides(
                        wannier90_run_proxy._task.inputs,
                        w90_bands,
                        (
                            "scf.pw.code",
                            "scf.pw.metadata",
                            "nscf.pw.code",
                            "nscf.pw.metadata",
                            "pw2wannier90.pw2wannier90.code",
                            "pw2wannier90.pw2wannier90.metadata",
                            "wannier90.wannier90.code",
                            "wannier90.wannier90.metadata",
                        ),
                    )

                    scf_remote = wannier90_run_proxy.scf.remote_folder
                    nscf_remote = wannier90_run_proxy.nscf.remote_folder
                    chk_folder = wannier90_run_proxy.wannier90.remote_folder

        if parent_folder_ph is None:
            phonons_inputs = recursive_merge(
                ph_base,
                {
                    "qpoints": reciprocal_points.qpoints,
                    "ph": {"parent_folder": scf_remote},
                },
            )
            phonons_run = PhBaseTask(**_set_call_link_label(phonons_inputs, "ph_base"))
            _apply_socket_overrides(
                phonons_run._task.inputs,
                ph_base,
                ("ph.code", "ph.metadata"),
            )
            parent_ph_for_epw = phonons_run.remote_folder
        else:
            parent_ph_for_epw = parent_folder_ph

        kfpoints = create_kpoints_gamma()
        epw_inputs = recursive_merge(
            _drop_epw_mesh_generation_inputs(epw_base),
            {
                "structure": structure,
                "parent_folder_ph": parent_ph_for_epw,
                "parent_folder_nscf": nscf_remote,
                "kpoints": reciprocal_points.kpoints_nscf,
                "kfpoints": kfpoints.result,
                "qpoints": reciprocal_points.qpoints,
                "qfpoints": kfpoints.result,
            },
        )
        if chk_folder is not None:
            epw_inputs["parent_folder_chk"] = chk_folder
        if w90_chk_to_ukk_script is not None:
            epw_inputs["w90_chk_to_ukk_script"] = w90_chk_to_ukk_script

        epw_run = EpwBaseTask(**_set_call_link_label(epw_inputs, "epw_base"))
        _apply_socket_overrides(
            epw_run._task.inputs,
            epw_base,
            ("code", "options"),
        )

        if epw_bands:
            epw_bands_inputs = recursive_merge(
                _drop_epw_mesh_generation_inputs(epw_bands),
                {
                    "structure": structure,
                    "parent_folder_epw": epw_run.remote_stash,
                    "kpoints": reciprocal_points.kpoints_nscf,
                    "qpoints": reciprocal_points.qpoints,
                },
            )

            if "bands_kpoints" in w90_bands:
                bands_kpoints_source = w90_bands["bands_kpoints"]
            elif w90_bands:
                bands_kpoints_source = get_seekpath_explicit_kpoints(
                    reference_output=nscf_remote
                ).result
            else:
                bands_kpoints_source = generate_seekpath_explicit_kpoints(
                    structure=structure
                ).result

            epw_bands_inputs["qfpoints"] = bands_kpoints_source
            epw_bands_inputs["kfpoints"] = bands_kpoints_source

            epw_bands_run = EpwBaseTask(
                **_set_call_link_label(epw_bands_inputs, "epw_bands"),
            )
            _apply_socket_overrides(
                epw_bands_run._task.inputs,
                epw_bands,
                ("code", "options"),
            )

        wg.outputs.retrieved = epw_run.retrieved
        wg.outputs.epw_folder = epw_run.remote_folder
        wg.outputs.epw_stash = epw_run.remote_stash

    return wg


def prep(
    codes: dict,
    structure: orm.StructureData,
    protocol: str | None = None,
    overrides: dict | None = None,
    wannier_projection_type=None,
    reference_bands: orm.BandsData | None = None,
    bands_kpoints: orm.KpointsData | None = None,
    parent_folder_ph: orm.RemoteData | orm.RemoteStashFolderData | None = None,
    w90_chk_to_ukk_script: orm.RemoteData | None = None,
    **kwargs,
):
    """Build the EPW preparation WorkGraph."""
    inputs = build_task_inputs(
        codes=codes,
        structure=structure,
        protocol=protocol,
        overrides=overrides,
        wannier_projection_type=wannier_projection_type,
        reference_bands=reference_bands,
        bands_kpoints=bands_kpoints,
        parent_folder_ph=parent_folder_ph,
        **kwargs,
    )
    return prep_from_inputs(
        structure=structure,
        inputs=inputs,
        use_wannier90_optimize=reference_bands is not None,
        w90_chk_to_ukk_script=w90_chk_to_ukk_script,
    )
