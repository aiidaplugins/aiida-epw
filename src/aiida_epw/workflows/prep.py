"""Work chain for doing the coarse-grid calculations."""

import logging
from pathlib import Path

from aiida import orm
from aiida.common import AttributeDict

from aiida.engine import WorkChain, ToContext, if_
from aiida_quantumespresso.workflows.ph.base import PhBaseWorkChain
from aiida_quantumespresso.workflows.protocols.utils import ProtocolMixin
from aiida_quantumespresso.common.types import ElectronicType

from aiida_quantumespresso.calculations.functions.create_kpoints_from_distance import (
    create_kpoints_from_distance,
)

from aiida_wannier90_workflows.workflows import (
    Wannier90BandsWorkChain,
    Wannier90OptimizeWorkChain,
)
from aiida_wannier90_workflows.workflows.bands import (
    validate_inputs as validate_inputs_bands,
)
from aiida_wannier90_workflows.utils.workflows.builder.setter import set_kpoints
from aiida_wannier90_workflows.common.types import WannierProjectionType

from aiida_epw.workflows.base import EpwBaseWorkChain

try:
    from aiida_quantumespresso_ph.workflows.phonon_bands import PhononBandsWorkChain
    from aiida_quantumespresso_ph.workflows.dynamical_matrix import (
        DynamicalMatrixWorkChain,
    )
except ImportError:
    PhononBandsWorkChain = None
    DynamicalMatrixWorkChain = None

logger = logging.getLogger(__name__)


def _as_wannier_projection_list(projections):
    """Return projections as an AiiDA list for the Wannier90 input port."""
    if isinstance(projections, orm.List):
        return projections
    return orm.List(list=list(projections))


def apply_manual_wannierization(w90_bands, manual_wannierization=None):
    """Apply manual Wannier90 projections and energy windows to a W90 builder.

    Manual projections must be passed through the ``projections`` input port of
    ``Wannier90Calculation``.  The plugin blocks a ``projections`` key in the
    parameters dictionary because it writes the projections block separately.
    This helper also accepts that legacy location and moves it to the correct
    input port.
    """
    manual_wannierization = dict(manual_wannierization or {})

    w90_inputs = w90_bands.wannier90
    wannier90_inputs = w90_inputs.wannier90
    parameters = wannier90_inputs.parameters.get_dict()

    manual_parameters = dict(manual_wannierization.get("parameters", {}))
    parameter_projections = parameters.pop("projections", None)
    manual_parameter_projections = manual_parameters.pop("projections", None)
    explicit_projections = manual_wannierization.get("projections", None)

    projection_sources = [
        value
        for value in (
            explicit_projections,
            manual_parameter_projections,
            parameter_projections,
        )
        if value is not None
    ]
    normalized_projection_sources = [
        value.get_list() if isinstance(value, orm.List) else list(value)
        for value in projection_sources
    ]
    if len({repr(value) for value in normalized_projection_sources}) > 1:
        raise ValueError(
            "Conflicting manual Wannier projections were provided in multiple locations."
        )

    projections = projection_sources[0] if projection_sources else None
    parameters.update(manual_parameters)

    manual_requested = bool(
        manual_wannierization or manual_parameters or projections is not None
    )
    if not manual_requested:
        wannier90_inputs.parameters = orm.Dict(dict=parameters)
        return False

    if projections is not None:
        wannier90_inputs.projections = _as_wannier_projection_list(projections)
        parameters.pop("auto_projections", None)

    if "shift_energy_windows" in manual_wannierization:
        w90_inputs.shift_energy_windows = orm.Bool(
            manual_wannierization["shift_energy_windows"]
        )
    else:
        w90_inputs.shift_energy_windows = orm.Bool(False)

    if "auto_energy_windows" in manual_wannierization:
        w90_inputs.auto_energy_windows = orm.Bool(
            manual_wannierization["auto_energy_windows"]
        )
    else:
        w90_inputs.auto_energy_windows = orm.Bool(False)

    if "guiding_centres_projections" in manual_wannierization:
        w90_inputs.guiding_centres_projections = _as_wannier_projection_list(
            manual_wannierization["guiding_centres_projections"]
        )

    wannier90_inputs.parameters = orm.Dict(dict=parameters)
    return True


def has_manual_wannierization_inputs(w90_bands_inputs, manual_wannierization=None):
    """Return whether the protocol inputs request manual Wannier projections."""
    if manual_wannierization:
        return True

    parameters = (
        w90_bands_inputs.get("wannier90", {}).get("wannier90", {}).get("parameters", {})
    )
    return "projections" in parameters


class EpwPrepWorkChain(ProtocolMixin, WorkChain):
    """Main work chain to start calculating properties using EPW.

    Has support for both the selected columns of the density matrix (SCDM) and
    (projectability-disentangled Wannier function) PDWF projection types.
    """

    @classmethod
    def define(cls, spec):
        """Define the work chain specification."""
        super().define(spec)

        spec.input(
            "structure",
            valid_type=orm.StructureData,
            help=(
                "Structure used to generate k-point and q-point meshes and passed to all "
                "child workflows (`Wannier90BandsWorkChain`/`Wannier90OptimizeWorkChain`, "
                "`PhBaseWorkChain`, and `EpwBaseWorkChain`)."
            ),
        )
        spec.input(
            "clean_workdir",
            valid_type=orm.Bool,
            default=lambda: orm.Bool(False),
            help=(
                "Whether the remote working directories of all child calculations will be "
                "cleaned up after the workchain terminates."
            ),
        )
        spec.input(
            "qpoints_distance",
            valid_type=orm.Float,
            default=lambda: orm.Float(0.5),
            help=(
                "Distance between q-points in the coarse q-point mesh used for the `PhBaseWorkChain`."
            ),
        )
        spec.input(
            "kpoints_distance_scf",
            valid_type=orm.Float,
            default=lambda: orm.Float(0.15),
            help=(
                "Distance between k-points in the k-point mesh used for the "
                "`Wannier90OptimizeWorkChain`/`Wannier90BandsWorkChain`."
            ),
        )
        spec.input(
            "kpoints_factor_nscf",
            valid_type=orm.Int,
            default=lambda: orm.Int(2),
            help=(
                "Factor applied to each dimension of the coarse q-point mesh to build the "
                "coarse k-point mesh for the `Wannier90OptimizeWorkChain`/`Wannier90BandsWorkChain`. "
                "For example, a q-mesh [4, 4, 4] with `kpoints_factor_nscf=2` becomes a k-mesh [8, 8, 8]. "
            ),
        )
        spec.input(
            "w90_chk_to_ukk_script",
            valid_type=(orm.RemoteData, orm.SinglefileData),
            help=(
                "Julia script that converts `prefix.chk` from `wannier90.x` to the "
                "`epw.x`-readable `prefix.ukk` (and adapts `prefix.mmn` for EPW >= v6.0). "
                "Run as a prepend command before launching `epw.x`."
            ),
        )
        spec.input("bandplot", valid_type=orm.Int, default=lambda: orm.Int(0))
        if PhononBandsWorkChain is not None:
            spec.expose_inputs(
                PhononBandsWorkChain,
                namespace="ph_bands",
                exclude=(
                    "dynamical_matrix.parent_folder",
                    "dynamical_matrix.ph_main.qpoints",
                ),
                namespace_options={"required": False, "populate_defaults": False},
            )
        # Optional parent folders for restart
        spec.input(
            "parent_folder_ph",
            valid_type=orm.RemoteData,
            required=False,
            help=(
                "Optional RemoteData from a PhCalculation to restart from. "
                "Skips the phonon step; combine with "
                "parent_folder_nscf/parent_folder_chk to skip Wannier90 as well."
            ),
        )
        spec.input(
            "parent_folder_nscf",
            valid_type=orm.RemoteData,
            required=False,
            help=(
                "RemoteData from an NSCF calculation. When provided together "
                "with parent_folder_chk, skips Wannier90; combine with "
                "parent_folder_ph to skip phonon as well."
            ),
        )
        spec.input(
            "parent_folder_chk",
            valid_type=orm.RemoteData,
            required=False,
            help=(
                "RemoteData from a Wannier90 calculation containing the .chk "
                "file. When provided together with parent_folder_nscf, skips "
                "Wannier90; combine with parent_folder_ph to skip phonon as well."
            ),
        )

        spec.expose_inputs(
            Wannier90OptimizeWorkChain,
            namespace="w90_bands",
            exclude=(
                "structure",
                "clean_workdir",
            ),
            namespace_options={
                "help": "Inputs for the `Wannier90OptimizeWorkChain/Wannier90BandsWorkChain`."
            },
        )
        spec.inputs["w90_bands"].validator = validate_inputs_bands
        spec.expose_inputs(
            PhBaseWorkChain,
            namespace="ph_base",
            exclude=(
                "clean_workdir",
                "ph.parent_folder",
                "qpoints",
                "qpoints_distance",
            ),
            namespace_options={
                "help": "Inputs for the `PhBaseWorkChain` that does the `ph.x` calculation.",
                "required": False,
                "populate_defaults": False,
            },
        )
        # The optional ph_bands namespace is exposed above. run_ph only injects
        # runtime parent_folder/qpoints before submitting PhononBandsWorkChain.
        spec.expose_inputs(
            EpwBaseWorkChain,
            namespace="epw_base",
            exclude=(
                "structure",
                "clean_workdir",
                "kpoints",
                "qpoints",
                "kfpoints",
                "qfpoints",
                "qfpoints_distance",
                "kfpoints_factor",
                "parent_folder_ph",
                "parent_folder_nscf",
                "parent_folder_epw",
                "parent_folder_chk",
            ),
            namespace_options={"help": "Inputs for the `EpwBaseWorkChain`."},
        )
        spec.expose_inputs(
            EpwBaseWorkChain,
            namespace="epw_bands",
            exclude=(
                "structure",
                "clean_workdir",
                "kpoints",
                "qpoints",
                "kfpoints",
                "qfpoints",
                "qfpoints_distance",
                "kfpoints_factor",
                "parent_folder_epw",
            ),
            namespace_options={
                "help": "Inputs namespace for `EpwBaseWorkChain` that runs the `epw.x` calculation in interpolation mode, i.e. the interpolated electron and phonon band structures."
            },
        )
        spec.output("retrieved", valid_type=orm.FolderData)
        spec.output("epw_folder", valid_type=orm.RemoteStashFolderData)
        spec.output("el_band_structure", valid_type=orm.BandsData, required=False)
        spec.output("ph_band_structure", valid_type=orm.BandsData, required=False)
        spec.output("phonon_bands", valid_type=orm.BandsData, required=False)

        spec.outline(
            cls.generate_reciprocal_points,
            if_(cls.should_run_wannier90)(
                cls.run_wannier90,
                cls.inspect_wannier90,
            ),
            if_(cls.should_run_ph)(
                cls.run_ph,
                cls.inspect_ph,
            ),
            cls.run_epw,
            cls.inspect_epw,
            if_(cls.should_run_epw_bands)(
                cls.run_epw_bands,
                cls.inspect_epw_bands,
            ),
            cls.results,
        )
        spec.exit_code(
            403,
            "ERROR_SUB_PROCESS_FAILED_PHONON",
            message="The electron-phonon `PhBaseWorkChain` sub process failed",
        )
        spec.exit_code(
            404,
            "ERROR_SUB_PROCESS_FAILED_WANNIER90",
            message="The `Wannier90BandsWorkChain` sub process failed",
        )
        spec.exit_code(
            405,
            "ERROR_SUB_PROCESS_FAILED_EPW",
            message="The `EpwWorkChain` sub process failed",
        )
        spec.exit_code(
            406,
            "ERROR_SUB_PROCESS_FAILED_EPW_BANDS",
            message="The `EpwBaseWorkChain` sub process failed",
        )

    @classmethod
    def get_protocol_filepath(cls, filename="prep.yaml"):
        """Return ``pathlib.Path`` to the ``.yaml`` file that defines the protocols.

        :param filename: Name of the protocol file (default: "prep.yaml").
        """
        from importlib_resources import files
        from . import protocols

        return files(protocols) / filename

    @classmethod
    def get_protocol_inputs(cls, protocol=None, overrides=None, filename="prep.yaml"):
        """Load inputs from the correct protocol file.

        :param protocol: Protocol to use (e.g., 'moderate', 'precise').
        :param overrides: Dictionary of overrides.
        :param filename: Name of the yaml file to load.
        """
        import yaml
        from aiida_quantumespresso.workflows.protocols.utils import recursive_merge

        with cls.get_protocol_filepath(filename).open() as handle:
            data = yaml.safe_load(handle)

        protocol = protocol or data.get("default_protocol")

        try:
            protocol_data = data["protocols"][protocol]
        except KeyError:
            raise ValueError(
                f"Protocol '{protocol}' not found in {filename}. "
                f"Available protocols: {', '.join(data['protocols'].keys())}"
            )

        inputs = recursive_merge(data["default_inputs"], protocol_data)

        if overrides:
            inputs = recursive_merge(inputs, overrides)

        return inputs

    @classmethod
    def _build_phonon_bands_builder_from_config(
        cls,
        codes,
        structure,
        protocol,
        ph_bands_config,
        bands_kpoints=None,
    ):
        """Build PhononBandsWorkChain inputs from protocol/YAML data."""
        if PhononBandsWorkChain is None or DynamicalMatrixWorkChain is None:
            raise ImportError(
                "bandplot mode requires the aiida_quantumespresso_ph package"
            )

        required_codes = ("pw", "ph", "q2r", "matdyn")
        missing_codes = [name for name in required_codes if name not in codes]
        if missing_codes:
            raise ValueError(
                f"bandplot mode requires explicit codes for {', '.join(missing_codes)}"
            )

        dm_config = cls._get_phonon_bands_dynamical_matrix_config(
            ph_bands_config.get("dynamical_matrix", {})
        )
        interpolate_config = ph_bands_config.get("interpolate", {})

        builder = PhononBandsWorkChain.get_builder()
        dynamical_matrix = DynamicalMatrixWorkChain.get_builder_from_protocol(
            pw_code=codes["pw"],
            ph_code=codes["ph"],
            structure=structure,
            protocol=protocol,
            overrides=dm_config,
        )
        dynamical_matrix.pop("parent_folder", None)
        dynamical_matrix.ph_main.pop("qpoints", None)
        for key, value in dynamical_matrix.items():
            builder.dynamical_matrix[key] = value

        q2r_config = interpolate_config.get("q2r", {})
        cls._apply_calcjob_config(
            builder.interpolate.q2r.q2r,
            q2r_config.get("q2r", {}),
            codes["q2r"],
        )
        if "clean_workdir" in q2r_config:
            builder.interpolate.q2r.clean_workdir = orm.Bool(
                q2r_config["clean_workdir"]
            )

        matdyn_config = interpolate_config.get("matdyn", {})
        cls._apply_calcjob_config(
            builder.interpolate.matdyn.matdyn,
            matdyn_config.get("matdyn", {}),
            codes["matdyn"],
        )
        if "clean_workdir" in matdyn_config:
            builder.interpolate.matdyn.clean_workdir = orm.Bool(
                matdyn_config["clean_workdir"]
            )
        if bands_kpoints is not None:
            builder.interpolate.matdyn.matdyn.kpoints = bands_kpoints

        return builder

    @staticmethod
    def _get_phonon_bands_dynamical_matrix_config(config):
        """Map user-facing ph_base config to PhononBandsWorkChain inputs."""
        config = dict(config or {})
        if "ph_base" in config:
            config["ph_main"] = config.pop("ph_base")
        return config

    @classmethod
    def _get_ph_base_config(cls, inputs):
        """Return PhBaseWorkChain overrides from the unified ph_bands section."""
        ph_bands_config = inputs.get("ph_bands", {})
        dynamical_matrix_config = ph_bands_config.get("dynamical_matrix", {})
        ph_base_config = dynamical_matrix_config.get("ph_base")
        if ph_base_config is None:
            return None

        ph_base_config = dict(ph_base_config)
        ph_base_config.pop("parallelize_qpoints", None)
        return ph_base_config

    @classmethod
    def get_builder_from_protocol(
        cls,
        codes,
        structure,
        protocol=None,
        overrides=None,
        wannier_projection_type=WannierProjectionType.ATOMIC_PROJECTORS_QE,
        reference_bands=None,
        bands_kpoints=None,
        workflow_type="mob",
        electronic_type=ElectronicType.METAL,
        bandplot=False,
        **kwargs,
    ):
        """Return a builder prepopulated with inputs selected according to the chosen protocol.

        :param structure: the ``StructureData`` instance to use.
        :param protocol: protocol to use, if not specified, the default will be used.
        :param overrides: optional dictionary of inputs to override the defaults of the protocol.
        :param workflow_type: Type of workflow, either "mob" (mobility) or "sc" (superconductivity).
            This determines which protocol files to use:
            - "mob": uses prep_mob.yaml and base_mob.yaml
            - "sc": uses prep_sc.yaml and base_sc.yaml
        :param electronic_type: indicate the electronic character of the system through ``ElectronicType`` instance.
            Use ``ElectronicType.INSULATOR`` for valence bands only (e.g., for insulators/semiconductors),
            or ``ElectronicType.METAL`` (default) to include conduction bands.
        :param kwargs: additional keyword arguments that will be passed to the ``get_builder_from_protocol`` of all the
            sub processes that are called by this workchain.
        :return: a process builder instance with all inputs defined ready for launch.
        """
        # Determine protocol filenames based on workflow_type
        if workflow_type == "mob":
            prep_filename = "prep_mob.yaml"
            base_filename = "base_mob.yaml"
        elif workflow_type == "sc":
            prep_filename = "prep_sc.yaml"
            base_filename = "base_sc.yaml"
        else:
            raise ValueError(
                f"Invalid workflow_type '{workflow_type}'. Must be 'mob' or 'sc'."
            )

        inputs = cls.get_protocol_inputs(protocol, overrides, filename=prep_filename)

        builder = cls.get_builder()
        builder.structure = structure

        w90_bands_inputs = inputs.get("w90_bands", {})
        pseudo_family = inputs.pop("pseudo_family", None)
        manual_wannierization = kwargs.pop("manual_wannierization", None)
        manual_wannier_requested = has_manual_wannierization_inputs(
            w90_bands_inputs, manual_wannierization
        )
        if manual_wannier_requested:
            kwargs.setdefault("print_summary", False)
            wannier_projection_type = WannierProjectionType.ANALYTIC

        if wannier_projection_type == WannierProjectionType.ATOMIC_PROJECTORS_QE:
            if reference_bands is None:
                raise ValueError(
                    f"reference_bands must be specified for {wannier_projection_type}"
                )
            # Validate: ATOMIC_PROJECTORS_QE + INSULATOR is incompatible
            if electronic_type == ElectronicType.INSULATOR:
                raise ValueError(
                    f"The combination of {wannier_projection_type} and ElectronicType.INSULATOR is not supported. "
                    "ATOMIC_PROJECTORS_QE uses auto_projections which generates projections for all atomic orbitals, "
                    "but INSULATOR mode sets num_wann to only the valence band count. This causes a mismatch. "
                    "Please use one of the following alternatives:\n"
                    "  1. Use WannierProjectionType.SCDM with ElectronicType.INSULATOR (recommended for valence-only)\n"
                    "  2. Use ElectronicType.METAL to include conduction bands"
                )
            w90_bands = Wannier90OptimizeWorkChain.get_builder_from_protocol(
                structure=structure,
                codes=codes,
                pseudo_family=pseudo_family,
                overrides=w90_bands_inputs,
                reference_bands=reference_bands,
                bands_kpoints=bands_kpoints,
                electronic_type=electronic_type,
                **kwargs,
            )
            w90_bands.separate_plotting = False
            # pop useless inputs, otherwise the builder validation will fail
            # at validating empty inputs
            w90_bands.pop("projwfc", None)
            from aiida_epw.tools.band_analysis import detect_bandgap_from_bands

            # ── Insulator-aware dis_froz_max adjustment ──
            # For insulators/semiconductors using ElectronicType.METAL + auto_projections,
            # the default dis_froz_max = Ef + 2.0 eV may fall inside the bandgap and miss
            # the CBM entirely.  The upstream get_homo_lumo function fails to detect the
            # true gap because Ef = VBM for 'fixed' occupations.  Here we use the electron
            # count to reliably identify VBM/CBM and override dis_froz_max accordingly.
            _pf = pseudo_family or w90_bands_inputs.get("meta_parameters", {}).get(
                "pseudo_family", "PseudoDojo/0.5/PBE/SR/standard/upf"
            )
            w90_params = w90_bands.wannier90.wannier90.parameters.get_dict()
            gap_info = detect_bandgap_from_bands(
                reference_bands,
                structure,
                _pf,
                exclude_bands=w90_params.get("exclude_bands", None),
            )
            if gap_info is not None:
                # System is an insulator – set dis_froz_max relative to CBM
                # The value in w90 parameters is *relative* (shifted by Ef or LUMO at
                # runtime).  Since shift_energy_windows is True and the runtime logic
                # in Wannier90BaseWorkChain.prepare_inputs() will shift by either Ef or
                # LUMO, but LUMO detection also fails (same get_homo_lumo bug), we
                # instead set an *absolute* dis_froz_max and disable the shift.
                DIS_FROZ_MARGIN = 1.0  # eV above CBM
                abs_dis_froz_max = gap_info["cbm"] + DIS_FROZ_MARGIN
                w90_params["dis_froz_max"] = abs_dis_froz_max
                w90_bands.wannier90.wannier90.parameters = orm.Dict(w90_params)
                # Disable shift_energy_windows so that dis_froz_max is used as-is
                w90_bands.wannier90.shift_energy_windows = orm.Bool(False)
                logger.info(
                    "Insulator detected (bandgap=%.3f eV, CBM=%.3f eV). "
                    "Set absolute dis_froz_max=%.3f eV (CBM + %.1f eV), "
                    "shift_energy_windows=False.",
                    gap_info["bandgap"],
                    gap_info["cbm"],
                    abs_dis_froz_max,
                    DIS_FROZ_MARGIN,
                )
        elif wannier_projection_type == WannierProjectionType.SCDM:
            w90_codes = {
                k: v
                for k, v in codes.items()
                if k in ["pw", "pw2wannier90", "wannier90", "projwfc", "open_grid"]
            }
            w90_bands = Wannier90BandsWorkChain.get_builder_from_protocol(
                structure=structure,
                codes=w90_codes,
                pseudo_family=pseudo_family,
                overrides=w90_bands_inputs,
                electronic_type=electronic_type,
                projection_type=wannier_projection_type,
                bands_kpoints=bands_kpoints,
                **kwargs,
            )
        elif wannier_projection_type == WannierProjectionType.ANALYTIC:
            w90_codes = {
                k: v
                for k, v in codes.items()
                if k in ["pw", "pw2wannier90", "wannier90", "projwfc", "open_grid"]
            }
            w90_bands = Wannier90BandsWorkChain.get_builder_from_protocol(
                structure=structure,
                codes=w90_codes,
                pseudo_family=pseudo_family,
                overrides=w90_bands_inputs,
                electronic_type=electronic_type,
                projection_type=wannier_projection_type,
                bands_kpoints=bands_kpoints,
                **kwargs,
            )
        else:
            raise ValueError(
                f"Unsupported wannier_projection_type: {wannier_projection_type}"
            )

        if manual_wannier_requested:
            apply_manual_wannierization(w90_bands, manual_wannierization)

        w90_bands.pop("structure", None)
        w90_bands.pop("open_grid", None)
        # projwfc is not needed for analytic/manual or external projectors
        if wannier_projection_type in (
            WannierProjectionType.ANALYTIC,
            WannierProjectionType.ATOMIC_PROJECTORS_EXTERNAL,
        ):
            w90_bands.pop("projwfc", None)

        builder.w90_bands = w90_bands

        if bandplot:
            builder.pop("ph_base", None)
        else:
            args = (codes["ph"], None, protocol)
            ph_base = PhBaseWorkChain.get_builder_from_protocol(
                *args, overrides=cls._get_ph_base_config(inputs), **kwargs
            )
            ph_base.pop("clean_workdir", None)
            ph_base.pop("qpoints_distance")
            builder.ph_base = ph_base

        epw_builder_namespaces = ("epw_base", "epw_bands")
        for namespace in epw_builder_namespaces:
            epw_inputs = inputs.get(namespace, None)
            if epw_inputs is None:
                continue

            epw_options = epw_inputs.get("options")
            if epw_options is None:
                epw_options = epw_inputs.get("metadata", {}).get("options")

            if namespace == "epw_base":
                stash_options = epw_options.get("stash", {}) if epw_options else {}
                if "target_base" not in stash_options:
                    epw_computer = codes["epw"].computer
                    target_basepath = None
                    if epw_computer.transport_type == "core.local":
                        target_basepath = Path(
                            epw_computer.get_workdir(), "stash"
                        ).as_posix()
                    elif epw_computer.transport_type == "core.ssh":
                        target_basepath = Path(
                            epw_computer.get_workdir().format(
                                username=epw_computer.get_configuration()["username"]
                            ),
                            "stash",
                        ).as_posix()

                    if target_basepath is not None:
                        if epw_options is None:
                            epw_options = epw_inputs.setdefault("options", {})
                        stash = epw_options.setdefault("stash", {})
                        stash["target_base"] = target_basepath
                        if stash.get("stash_mode", None) != "copy":
                            stash["stash_mode"] = "copy"

            epw_builder = EpwBaseWorkChain.get_builder_from_protocol(
                code=codes["epw"],
                structure=structure,
                protocol=protocol,
                overrides=epw_inputs,
                options=epw_options,
                protocol_filename=base_filename,
                **kwargs,
            )

            if "settings" in epw_inputs:
                epw_builder.settings = orm.Dict(epw_inputs["settings"])
            if "parallelization" in epw_inputs:
                epw_builder.parallelization = orm.Dict(epw_inputs["parallelization"])
            builder[namespace] = epw_builder

        builder.qpoints_distance = orm.Float(inputs["qpoints_distance"])
        builder.kpoints_distance_scf = orm.Float(inputs["kpoints_distance_scf"])
        builder.kpoints_factor_nscf = orm.Int(inputs["kpoints_factor_nscf"])
        builder.clean_workdir = orm.Bool(inputs["clean_workdir"])

        # Set bandplot flag and prebuild PhononBandsWorkChain inputs from YAML.
        if bandplot:
            builder.bandplot = orm.Int(1)
            ph_bands_config = inputs.get("ph_bands")
            if ph_bands_config is None:
                raise ValueError(
                    "bandplot mode requires a 'ph_bands' section in the prep overrides"
                )
            ph_bands = cls._build_phonon_bands_builder_from_config(
                codes=codes,
                structure=structure,
                protocol=protocol,
                ph_bands_config=ph_bands_config,
                bands_kpoints=bands_kpoints,
            )
            for key, value in ph_bands.items():
                builder.ph_bands[key] = value
        else:
            builder.pop("ph_bands", None)

        return builder

    def generate_reciprocal_points(self):
        """Generate the qpoints and kpoints meshes for the `ph.x` and `pw.x` calculations."""

        inputs = {
            "structure": self.inputs.structure,
            "distance": self.inputs.qpoints_distance,
            "force_parity": self.inputs.get("kpoints_force_parity", orm.Bool(False)),
            "metadata": {"call_link_label": "create_qpoints_from_distance"},
        }
        qpoints = create_kpoints_from_distance(**inputs)  # pylint: disable=unexpected-keyword-arg
        inputs = {
            "structure": self.inputs.structure,
            "distance": self.inputs.kpoints_distance_scf,
            "force_parity": self.inputs.get("kpoints_force_parity", orm.Bool(False)),
            "metadata": {"call_link_label": "create_kpoints_scf_from_distance"},
        }
        kpoints_scf = create_kpoints_from_distance(**inputs)

        qpoints_mesh = qpoints.get_kpoints_mesh()[0]
        kpoints_nscf = orm.KpointsData()
        kpoints_nscf.set_kpoints_mesh(
            [v * self.inputs.kpoints_factor_nscf.value for v in qpoints_mesh]
        )

        self.ctx.qpoints = qpoints
        self.ctx.kpoints_scf = kpoints_scf
        self.ctx.kpoints_nscf = kpoints_nscf

    def should_run_wannier90(self):
        """Check if the Wannier90 workflow should be run.

        Returns False if lower-level Wannier90 parent folders are provided directly.
        """
        if "parent_folder_nscf" in self.inputs and "parent_folder_chk" in self.inputs:
            self.report(
                "Skipping Wannier90: using provided parent_folder_nscf and parent_folder_chk"
            )
            return False
        return True

    def should_run_ph(self):
        """Check if the Ph workflow should be run.

        Returns False if a phonon parent folder is provided.
        """
        if "parent_folder_ph" in self.inputs:
            self.report("Skipping Ph: using provided parent_folder_ph")
            return False
        return True

    def run_wannier90(self):
        """Run the wannier90 workflow."""
        if self._uses_wannier90_bands_workchain():
            w90_class = Wannier90BandsWorkChain
        else:
            w90_class = Wannier90OptimizeWorkChain

        self.ctx.w90_class_name = w90_class.get_name()
        self.report(f"Running a {self.ctx.w90_class_name}.")

        inputs = AttributeDict(
            self.exposed_inputs(Wannier90OptimizeWorkChain, namespace="w90_bands")
        )
        if w90_class is Wannier90BandsWorkChain:
            for key in (
                "separate_plotting",
                "optimize_disproj",
                "optimize_disprojmax_range",
                "optimize_disprojmin_range",
                "optimize_reference_bands",
                "optimize_bands_distance_threshold",
                "optimize_spreads_imbalence_threshold",
            ):
                inputs.pop(key, None)
        inputs.setdefault("metadata", AttributeDict())
        inputs.metadata.call_link_label = "w90_bands"
        inputs.structure = self.inputs.structure

        set_kpoints(inputs, self.ctx.kpoints_nscf, w90_class)
        inputs["scf"]["kpoints"] = self.ctx.kpoints_scf

        workchain_node = self.submit(w90_class, **inputs)
        self.report(f"launching {w90_class.get_name()}<{workchain_node.pk}>")

        return ToContext(workchain_w90_bands=workchain_node)

    def _uses_wannier90_bands_workchain(self):
        """Return whether the w90 namespace matches Wannier90BandsWorkChain."""
        if "projwfc" in self.inputs.w90_bands:
            return True

        try:
            wannier90_inputs = self.inputs.w90_bands.wannier90.wannier90
            if "projections" in wannier90_inputs:
                return True
        except (AttributeError, KeyError):
            pass

        try:
            pw2wannier90_inputs = self.inputs.w90_bands.pw2wannier90.pw2wannier90
        except (AttributeError, KeyError):
            return False

        return "external_projectors_path" in pw2wannier90_inputs

    def inspect_wannier90(self):
        """Verify that the wannier90 workflow finished successfully."""
        workchain = self.ctx.workchain_w90_bands

        if not workchain.is_finished_ok:
            self.report(
                f"{self.ctx.w90_class_name}<{workchain.pk}> failed with exit status {workchain.exit_status}"
            )
            return self.exit_codes.ERROR_SUB_PROCESS_FAILED_WANNIER90

    @staticmethod
    def _uses_optimized_w90_folder(w90_workchain):
        """Return whether the optimized Wannier90 folder should be used."""
        try:
            optimize_disproj = w90_workchain.inputs.optimize_disproj
        except (AttributeError, KeyError):
            return False

        if isinstance(optimize_disproj, orm.Bool):
            return optimize_disproj.value
        return bool(optimize_disproj)

    def _get_w90_chk_folder(self, w90_workchain):
        """Return the Wannier90 remote folder that contains the chk file."""
        if self._uses_optimized_w90_folder(w90_workchain):
            for label in ("wannier90_optimal", "wannier90_optimal__remote_folder"):
                try:
                    output = getattr(w90_workchain.outputs, label)
                except AttributeError:
                    continue

                try:
                    return output.remote_folder
                except AttributeError:
                    return output

        return w90_workchain.outputs.wannier90.remote_folder

    def _get_w90_nscf_and_chk_folders(self):
        """Return NSCF and Wannier90 chk parent folders for EPW."""
        if "parent_folder_nscf" in self.inputs and "parent_folder_chk" in self.inputs:
            self.report(
                "Using directly provided parent_folder_nscf and parent_folder_chk"
            )
            return self.inputs.parent_folder_nscf, self.inputs.parent_folder_chk

        if "workchain_w90_bands" not in self.ctx:
            self.report("ERROR: Cannot find Wannier90 outputs for EPW restart")
            return None, None

        w90_workchain = self.ctx.workchain_w90_bands
        self.ctx.parent_w90_workchain = w90_workchain
        return (
            w90_workchain.outputs.nscf.remote_folder,
            self._get_w90_chk_folder(w90_workchain),
        )

    def _get_w90_scf_base_workchain(self):
        """Return the SCF base workchain from the current Wannier90 run."""
        if "workchain_w90_bands" in self.ctx:
            try:
                return (
                    self.ctx.workchain_w90_bands.base.links.get_outgoing(
                        link_label_filter="scf"
                    )
                    .first()
                    .node
                )
            except AttributeError:
                pass

        return None

    def _get_w90_scf_parent_folder(self):
        """Return the SCF remote folder needed to start a fresh phonon step."""
        scf_base_wc = EpwPrepWorkChain._get_w90_scf_base_workchain(self)
        if scf_base_wc is not None:
            try:
                return scf_base_wc.outputs.remote_folder
            except AttributeError:
                pass

        if "parent_folder_nscf" in self.inputs:
            try:
                return self.inputs.parent_folder_nscf.creator.inputs.parent_folder
            except AttributeError:
                pass

        return None

    @staticmethod
    def _apply_calcjob_config(calcjob_builder, config, code=None):
        """Apply YAML-derived code, parameters, settings, and options."""
        if code is not None:
            calcjob_builder.code = code
        if "parameters" in config:
            calcjob_builder.parameters = orm.Dict(dict=config["parameters"])
        if "settings" in config:
            calcjob_builder.settings = orm.Dict(dict=config["settings"])

        options = config.get("metadata", {}).get("options")
        if options:
            calcjob_builder.metadata.options = options

    def _get_ph_parent_folder(self):
        """Return the phonon remote folder for EPW."""
        if "parent_folder_ph" in self.inputs:
            return self.inputs.parent_folder_ph

        if "workchain_ph" not in self.ctx:
            self.report("ERROR: Cannot find phonon outputs for EPW restart")
            return None

        if self.ctx.workchain_ph.process_label == "PhononBandsWorkChain":
            return self._get_phonon_remote_folder(self.ctx.workchain_ph)

        return self.ctx.workchain_ph.outputs.remote_folder

    def run_ph(self):
        """Run the `PhBaseWorkChain` or `PhononBandsWorkChain` depending on bandplot."""
        if self.inputs.bandplot.value != 0:
            if PhononBandsWorkChain is None or DynamicalMatrixWorkChain is None:
                self.report(
                    "ERROR: bandplot is enabled but aiida_quantumespresso_ph is "
                    "not installed."
                )
                return self.exit_codes.ERROR_SUB_PROCESS_FAILED_PHONON

            if "ph_bands" not in self.inputs:
                self.report(
                    "ERROR: bandplot is enabled but no 'ph_bands' inputs were "
                    "provided. Build them from the prep overrides and explicit "
                    "codes in get_builder_from_protocol()."
                )
                return self.exit_codes.ERROR_SUB_PROCESS_FAILED_PHONON

            parent_folder = self._get_w90_scf_parent_folder()
            if parent_folder is None:
                self.report(
                    "ERROR: Cannot find SCF parent folder for "
                    "PhononBandsWorkChain. If Wannier90 is skipped with "
                    "parent_folder_nscf/parent_folder_chk, provide "
                    "parent_folder_ph as well or use an NSCF parent with "
                    "creator.inputs.parent_folder."
                )
                return self.exit_codes.ERROR_SUB_PROCESS_FAILED_PHONON

            inputs = AttributeDict(
                self.exposed_inputs(PhononBandsWorkChain, namespace="ph_bands")
            )
            inputs.dynamical_matrix.parent_folder = parent_folder
            inputs.dynamical_matrix.ph_main.qpoints = self.ctx.qpoints
            inputs.metadata.call_link_label = "ph_bands"
            workchain_node = self.submit(PhononBandsWorkChain, **inputs)
            self.report(f"launching PhononBandsWorkChain<{workchain_node.pk}>")
            return ToContext(workchain_ph=workchain_node)

        inputs = AttributeDict(
            self.exposed_inputs(PhBaseWorkChain, namespace="ph_base")
        )

        # scf_base_wc = (
        #     self.ctx.workchain_w90_bands.base.links.get_outgoing(
        #         link_label_filter="scf"
        #     )
        #     .first()
        #     .node
        # )
        # inputs.ph.parent_folder = scf_base_wc.outputs.remote_folder

        parent_folder = self._get_w90_scf_parent_folder()
        if parent_folder is None:
            self.report(
                "ERROR: Cannot find SCF parent folder for PhBaseWorkChain. "
                "If Wannier90 is skipped with parent_folder_nscf/parent_folder_chk, "
                "provide parent_folder_ph as well or use an NSCF parent with "
                "creator.inputs.parent_folder."
            )
            return self.exit_codes.ERROR_SUB_PROCESS_FAILED_PHONON
        inputs.ph.parent_folder = parent_folder

        inputs.qpoints = self.ctx.qpoints

        inputs.metadata.call_link_label = "ph_base"
        workchain_node = self.submit(PhBaseWorkChain, **inputs)
        self.report(f"launching PhBaseWorkChain<{workchain_node.pk}>")

        return ToContext(workchain_ph=workchain_node)

    def inspect_ph(self):
        """Verify that the `PhBaseWorkChain` finished successfully."""
        workchain = self.ctx.workchain_ph

        if not workchain.is_finished_ok:
            self.report(
                f"PhBaseWorkChain<{workchain.pk}> failed with exit status {workchain.exit_status}"
            )
            return self.exit_codes.ERROR_SUB_PROCESS_FAILED_PHONON

    def _get_phonon_remote_folder(self, ph_bands_workchain):
        """Extract remote_folder from PhononBandsWorkChain by searching descendants.

        The PhononBandsWorkChain doesn't directly output a remote_folder. Instead, we need
        to traverse the call stack to find the PhBaseWorkChain which contains the remote_folder.

        Structure: PhononBandsWorkChain -> DynamicalMatrixWorkChain -> PhWorkChain -> PhBaseWorkChain
        """
        self.report(
            f"Searching for remote_folder in PhononBandsWorkChain<{ph_bands_workchain.pk}>"
        )

        def find_remote_folder_recursive(node, depth=0):
            """Recursively search for remote_folder in node and its descendants."""
            indent = "  " * depth
            self.report(f"{indent}Checking {node.process_label}<{node.pk}>...")

            # Check if this node has remote_folder output
            for link in node.base.links.get_outgoing():
                if link.link_label == "remote_folder":
                    self.report(f"{indent}  -> Found remote_folder (pk={link.node.pk})")
                    return link.node

            # Recursively check children
            for child in node.called:
                result = find_remote_folder_recursive(child, depth + 1)
                if result:
                    return result

            return None

        remote_folder = find_remote_folder_recursive(ph_bands_workchain)
        if remote_folder is None:
            raise ValueError(
                f"Could not find remote_folder in PhononBandsWorkChain<{ph_bands_workchain.pk}> or its descendants"
            )

        self.report(
            f"Successfully found remote_folder<{remote_folder.pk}> from PhononBandsWorkChain"
        )
        return remote_folder

    def _fetch_quadrupole_file(self, quad_dir):
        """Return a SinglefileData containing quadrupole.fmt from a RemoteData node.

        This is needed when the quadrupole data lives on a different computer than the EPW
        code: AiiDA's remote_copy_list cannot transfer files across machines.

        Strategy (in order):
        1. Check if the RemoteData's creator (QuadrupoleWorkChain / ShellJob) already
           produced a ``quadrupole_fmt`` SinglefileData output — this is already in the
           AiiDA repository, no transport needed.
        2. Check if the creator retrieved ``quadrupole.fmt`` in its ``retrieved`` FolderData.
           If present, materialize it to a new SinglefileData in the AiiDA repository.
        3. Fall back to downloading via AiiDA transport (requires daemon SSH access to
           the remote computer).

        :param quad_dir: RemoteData node whose remote directory contains quadrupole.fmt.
        :returns: SinglefileData node, or None if all strategies fail.
        """
        import os
        import tempfile

        # Strategy 1: check if the creator already has a quadrupole_fmt SinglefileData output
        creator = quad_dir.creator
        if creator is not None:
            try:
                quad_file = creator.outputs.quadrupole_fmt
                self.report(
                    f"Using quadrupole_fmt SinglefileData<{quad_file.pk}> from "
                    f"{creator.process_label}<{creator.pk}> (no transport needed)"
                )
                return quad_file
            except AttributeError:
                pass  # creator has no quadrupole_fmt output, try next strategy

        # Strategy 2: download via AiiDA transport
        remote_path = os.path.join(quad_dir.get_remote_path(), "quadrupole.fmt")
        try:
            with quad_dir.computer.get_transport() as transport:
                with tempfile.NamedTemporaryFile(suffix=".fmt", delete=False) as tmp:
                    tmp_path = tmp.name
                try:
                    transport.getfile(remote_path, tmp_path)
                    quad_file = orm.SinglefileData(
                        filepath=tmp_path, filename="quadrupole.fmt"
                    )
                    quad_file.store()
                    return quad_file
                finally:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
        except Exception as exc:
            self.report(f"Failed to download quadrupole.fmt from {remote_path}: {exc}")

        return None

    def run_epw(self):
        """Run the `EpwBaseWorkChain`."""
        inputs = AttributeDict(
            self.exposed_inputs(EpwBaseWorkChain, namespace="epw_base")
        )

        inputs.parent_folder_ph = self._get_ph_parent_folder()
        inputs.parent_folder_nscf, inputs.parent_folder_chk = (
            self._get_w90_nscf_and_chk_folders()
        )
        if (
            inputs.parent_folder_ph is None
            or inputs.parent_folder_nscf is None
            or inputs.parent_folder_chk is None
        ):
            return self.exit_codes.ERROR_SUB_PROCESS_FAILED_EPW

        # Here we explicitly specify the coarse k/q grid so the EpwBaseWorkChain will not deduce it from the parent
        # folders. This EpwBaseWorkChain is only used for the transition from coarse Bloch representation to Wannier
        # representation. Thus the fine grid is always [1, 1, 1].
        fine_points = orm.KpointsData()
        fine_points.set_kpoints_mesh([1, 1, 1])

        # Pass w90_chk_to_ukk_script to EpwBaseWorkChain if provided
        if "w90_chk_to_ukk_script" in self.inputs:
            inputs.w90_chk_to_ukk_script = self.inputs.w90_chk_to_ukk_script

        # Resolve quadrupole_dir into quadrupole_file whenever possible. This makes
        # the transfer robust against missing source remote folders and cross-computer
        # limitations because SinglefileData is uploaded from the AiiDA repository.
        if "quadrupole_dir" in inputs:
            quad_dir = inputs.quadrupole_dir
            if isinstance(quad_dir, orm.RemoteData):
                quad_file = self._fetch_quadrupole_file(quad_dir)
                if quad_file is not None:
                    inputs.quadrupole_file = quad_file
                    inputs.pop("quadrupole_dir", None)
                    self.report(
                        f"Resolved quadrupole_dir RemoteData<{quad_dir.pk}> to "
                        f"quadrupole_file SinglefileData<{quad_file.pk}>"
                    )
                else:
                    epw_computer = inputs.code.computer
                    if quad_dir.computer.pk != epw_computer.pk:
                        self.report(
                            "WARNING: quadrupole_dir is on a different computer than EPW and "
                            "quadrupole.fmt could not be materialized from repository/transport. "
                            "EPW will run without quadrupole correction."
                        )
                        inputs.pop("quadrupole_dir", None)
                    else:
                        self.report(
                            "WARNING: failed to materialize quadrupole_file from quadrupole_dir. "
                            "Falling back to direct remote copy from quadrupole_dir."
                        )

        inputs.kpoints = self.ctx.kpoints_nscf
        inputs.kfpoints = fine_points
        inputs.qpoints = self.ctx.qpoints
        inputs.qfpoints = fine_points

        inputs.metadata.call_link_label = "epw_base"

        workchain_node = self.submit(EpwBaseWorkChain, **inputs)
        self.report(
            f"launching EpwBaseWorkChain<{workchain_node.pk}> in transformation mode"
        )

        return ToContext(workchain_epw=workchain_node)

    def inspect_epw(self):
        """Verify that the `EpwBaseWorkChain` finished successfully."""
        workchain = self.ctx.workchain_epw

        if not workchain.is_finished_ok:
            self.report(
                f"EpwBaseWorkChain<{workchain.pk}> failed with exit status {workchain.exit_status}"
            )
            return self.exit_codes.ERROR_SUB_PROCESS_FAILED_EPW

    def should_run_epw_bands(self):
        """Check if the `EpwBaseWorkChain` should be run in bands mode.

        Returns True if bandplot is non-zero (meaning both phonon bands and EPW bands should be calculated).
        """
        return self.inputs.bandplot.value != 0

    def _get_w90_workchain_for_bands(self):
        """Get the Wannier90 workchain node, from ctx or from provenance graph."""
        # Normal flow: w90 ran in this workflow
        if "workchain_w90_bands" in self.ctx:
            return self.ctx.workchain_w90_bands
        # Restart flow: w90 reference saved by run_epw from provenance
        if "parent_w90_workchain" in self.ctx:
            return self.ctx.parent_w90_workchain
        # Last resort: navigate from parent_folder_ph provenance
        if "parent_folder_ph" in self.inputs:
            ph_calc = self.inputs.parent_folder_ph.creator
            node = ph_calc
            for _ in range(10):
                node = node.caller if node.caller else None
                if node is None:
                    break
                if node.process_label == "EpwPrepWorkChain":
                    return (
                        node.base.links.get_outgoing(link_label_filter="w90_bands")
                        .first()
                        .node
                    )
        return None

    def run_epw_bands(self):
        """Run the `EpwBaseWorkChain` in bands mode."""
        inputs = AttributeDict(
            self.exposed_inputs(EpwBaseWorkChain, namespace="epw_bands")
        )

        w90_workchain = self._get_w90_workchain_for_bands()
        if w90_workchain is None:
            self.report("ERROR: Cannot find Wannier90 workchain for bands_kpoints")
            return self.exit_codes.ERROR_SUB_PROCESS_FAILED_EPW_BANDS

        if "bands_kpoints" in w90_workchain.inputs:
            bands_kpoints = w90_workchain.inputs.bands_kpoints
        else:
            bands_kpoints = (
                w90_workchain.base.links.get_outgoing(
                    link_label_filter="seekpath_structure_analysis"
                )
                .first()
                .node.outputs.explicit_kpoints
            )

        inputs.kpoints = self.ctx.kpoints_nscf
        inputs.qpoints = self.ctx.qpoints
        inputs.qfpoints = bands_kpoints
        inputs.kfpoints = bands_kpoints
        inputs.parent_folder_epw = self.ctx.workchain_epw.outputs.remote_folder
        inputs.metadata.call_link_label = "epw_bands"
        workchain_node = self.submit(EpwBaseWorkChain, **inputs)
        self.report(
            f"launching EpwBaseWorkChain<{workchain_node.pk}> in bands interpolation mode"
        )

        return {"workchain_epw_bands": workchain_node}

    def inspect_epw_bands(self):
        """Verify that the `EpwBaseWorkChain` finished successfully."""
        workchain = self.ctx.workchain_epw_bands
        if not workchain.is_finished_ok:
            self.report(
                f"EpwBaseWorkChain<{workchain.pk}> failed with exit status {workchain.exit_status}"
            )
            return self.exit_codes.ERROR_SUB_PROCESS_FAILED_EPW_BANDS

    def results(self):
        """Add the most important results to the outputs of the work chain."""
        self.out("retrieved", self.ctx.workchain_epw.outputs.retrieved)
        self.out("epw_folder", self.ctx.workchain_epw.outputs.remote_stash)

        if "workchain_epw_bands" in self.ctx:
            if "el_band_structure" in self.ctx.workchain_epw_bands.outputs:
                self.out(
                    "el_band_structure",
                    self.ctx.workchain_epw_bands.outputs.el_band_structure,
                )
            if "ph_band_structure" in self.ctx.workchain_epw_bands.outputs:
                self.out(
                    "ph_band_structure",
                    self.ctx.workchain_epw_bands.outputs.ph_band_structure,
                )

        if "workchain_ph" in self.ctx and self.inputs.bandplot.value == 3:
            if "output_phonon_bands" in self.ctx.workchain_ph.outputs:
                self.out(
                    "phonon_bands",
                    self.ctx.workchain_ph.outputs.output_phonon_bands,
                )

    def on_terminated(self):
        """Clean the working directories of all child calculations if `clean_workdir=True` in the inputs."""
        super().on_terminated()

        if self.inputs.clean_workdir.value is False:
            self.report("remote folders will not be cleaned")
            return

        cleaned_calcs = []

        for called_descendant in self.node.called_descendants:
            if isinstance(called_descendant, orm.CalcJobNode):
                try:
                    called_descendant.outputs.remote_folder._clean()  # pylint: disable=protected-access
                    cleaned_calcs.append(called_descendant.pk)
                except (IOError, OSError, KeyError):
                    pass

        if cleaned_calcs:
            self.report(
                f"cleaned remote folders of calculations: {' '.join(map(str, cleaned_calcs))}"
            )
