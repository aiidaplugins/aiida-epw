"""Plugin to create a Quantum Espresso epw.x input file."""

import numbers
import warnings
from pathlib import Path

from aiida import orm
from aiida.common import datastructures, exceptions
from aiida.common.warnings import AiidaDeprecationWarning
from aiida.orm.nodes.data.base import to_aiida_type
from aiida_quantumespresso.calculations import (
    BasePwCpInputGenerator,
    _pop_parser_options,
    _case_transform_dict,
    _uppercase_dict,
)
from aiida_quantumespresso.calculations.namelists import NamelistsCalculation
from aiida_quantumespresso.calculations.ph import PhCalculation
from aiida_quantumespresso.calculations.pw import PwCalculation
from aiida_quantumespresso.utils.convert import convert_input_to_namelist_entry

from aiida_epw.data import (
    A2fData,
    PA2fData,
    DosData,
    PDosData,
    PhDosData,
    GapFunctionData,
    LambdaFSData,
)

from aiida_epw.tools.workchain import get_parent_ph_qpoint_ibz_count


def _lowercase_dict(dictionary, dict_name):
    return _case_transform_dict(dictionary, dict_name, "_lowercase_dict", str.lower)


class EpwCalculation(NamelistsCalculation):
    """`CalcJob` implementation for the epw.x code of Quantum ESPRESSO."""

    # Keywords that cannot be set by the user but will be set by the plugin
    _blocked_keywords = [
        ("INPUTEPW", "outdir"),
        ("INPUTEPW", "prefix"),
        ("INPUTEPW", "dvscf_dir"),
        ("INPUTEPW", "nq1"),
        ("INPUTEPW", "nq2"),
        ("INPUTEPW", "nq3"),
        ("INPUTEPW", "nk1"),
        ("INPUTEPW", "nk2"),
        ("INPUTEPW", "nk3"),
        ("INPUTEPW", "nqf1"),
        ("INPUTEPW", "nqf2"),
        ("INPUTEPW", "nqf3"),
        ("INPUTEPW", "nkf1"),
        ("INPUTEPW", "nkf2"),
        ("INPUTEPW", "nkf3"),
        ("INPUTEPW", "eliashberg"),
        ("INPUTEPW", "liso"),
        ("INPUTEPW", "laniso"),
        ("INPUTEPW", "fbw"),
        ("INPUTEPW", "lreal"),
        ("INPUTEPW", "limag"),
        ("INPUTEPW", "lpade"),
        ("INPUTEPW", "lacon"),
    ]

    _use_kpoints = True

    _default_namelists = ["INPUTEPW"]
    _default_parser = "epw.epw"

    # Default input and output files
    _PREFIX = "aiida"
    _DEFAULT_INPUT_FILE = "aiida.in"
    _DEFAULT_OUTPUT_FILE = "aiida.out"
    _OUTPUT_SUBFOLDER = "./out/"
    _FOLDER_SAVE = "save"
    _FOLDER_DYNAMICAL_MATRIX = "DYN_MAT"
    _kfpoints_input_file = "kfpoints.kpt"
    _qfpoints_input_file = "qfpoints.kpt"
    _OUTPUT_XML_TENSOR_FILE_NAME = "tensors.xml"
    _OUTPUT_DOS_FILE = _PREFIX + ".dos"
    _OUTPUT_PHDOS_FILE = _PREFIX + ".phdos"
    _OUTPUT_PHDOS_PROJ_FILE = _PREFIX + ".phdos_proj"
    _OUTPUT_A2F_FILE = _PREFIX + ".a2f"
    _OUTPUT_A2F_PROJ_FILE = _PREFIX + ".a2f_proj"
    _OUTPUT_LAMBDA_FS_FILE = _PREFIX + ".lambda_FS"
    _OUTPUT_LAMBDA_K_PAIRS_FILE = _PREFIX + ".lambda_k_pairs"
    _output_elbands_file = "band.eig"
    _output_phbands_file = "phband.freq"

    _MAX_NSTEMP = 1000

    # Not using symlink in pw to allow multiple nscf to run on top of the same scf
    _default_symlink_usage = False
    _PARALLELIZATION_FLAGS = BasePwCpInputGenerator._PARALLELIZATION_FLAGS
    _ENABLED_PARALLELIZATION_FLAGS = (
        "nimage",
        "npool",
    )
    _PARALLELIZATION_FLAG_ALIASES = BasePwCpInputGenerator._PARALLELIZATION_FLAG_ALIASES

    @classmethod
    def define(cls, spec):
        """Define the process specification."""
        super().define(spec)
        spec.input(
            "parameters",
            valid_type=orm.Dict,
            help="Parameters for the `epw.x` input file.",
        )
        spec.input(
            "momentum_dependence",
            valid_type=orm.Bool,
            required=False,
            serializer=to_aiida_type,
            help="Isotropic (False) or anisotropic (True) Eliashberg calculation.",
        )
        spec.input(
            "full_bandwidth",
            valid_type=orm.Bool,
            required=False,
            serializer=to_aiida_type,
            help="Solve full bandwidth (True) or restrict to Fermi surface (False).",
        )
        spec.input(
            "real_axis",
            valid_type=orm.Bool,
            required=False,
            serializer=to_aiida_type,
            help="Solve the Eliashberg equations on the real axis (True) or imaginary axis (False).",
        )
        spec.input(
            "analytical_continuation",
            valid_type=orm.Str,
            required=False,
            serializer=to_aiida_type,
            help="Analytical continuation method: 'pade' or 'acon'.",
        )
        spec.input(
            "kpoints",
            valid_type=orm.KpointsData,
            help=(
                "The coarse kpoint mesh from nscf calculation."
                "should be consistent with kpoints from parent_folder_nscf "
                "or `parent_folder_epw` if restarting."
            ),
        )
        spec.input(
            "qpoints",
            valid_type=orm.KpointsData,
            help=(
                "The coarse qpoint mesh from ph calculation."
                "should be consistent with qpoints from `parent_folder_ph` "
                "or `parent_folder_epw` if restarting."
            ),
        )
        spec.input(
            "kfpoints",
            valid_type=orm.KpointsData,
            help=(
                "The fine kpoint mesh for interpolation of electron bands."
                "should be consistent with kpoints from `parent_folder_epw` if restarting."
            ),
        )
        spec.input(
            "qfpoints",
            valid_type=orm.KpointsData,
            help=(
                "The fine qpoint mesh for interpolation of phonon bands."
                "should be consistent with qpoints from `parent_folder_epw` if restarting."
            ),
        )
        spec.input(
            "parallelization",
            valid_type=orm.Dict,
            required=False,
            validator=cls.validate_parallelization,
            help=(
                "Parallelization options. The following flags are allowed:\n"
                + "\n".join(
                    f"{flag_name:<7}: {cls._PARALLELIZATION_FLAGS[flag_name]}"
                    for flag_name in cls._ENABLED_PARALLELIZATION_FLAGS
                )
            ),
        )
        spec.input(
            "parent_folder_nscf",
            required=False,
            valid_type=orm.RemoteData,
            help="the folder of a completed nscf `PwCalculation`",
        )
        spec.input(
            "parent_folder_chk",
            required=False,
            valid_type=orm.RemoteData,
            help="the folder of a completed wannier90 `Wannier90Calculation`",
        )
        spec.input(
            "parent_folder_ph",
            required=False,
            valid_type=(orm.RemoteData, orm.RemoteStashFolderData),
            help="the folder of a completed `PhCalculation`",
        )
        spec.input(
            "parent_folder_epw",
            required=False,
            valid_type=(orm.RemoteData, orm.RemoteStashFolderData),
            help="folder that contains all files required to restart an `EpwCalculation`",
        )
        spec.input(
            "quadrupole_dir",
            valid_type=(orm.Str, orm.RemoteData),
            required=False,
            help=(
                "RemoteData containing the quadrupole.fmt file, or its absolute path string."
            ),
        )
        spec.input(
            "quadrupole_file",
            valid_type=orm.SinglefileData,
            required=False,
            help=(
                "SinglefileData containing the quadrupole.fmt file. "
                "Use this instead of `quadrupole_dir` when the quadrupole data lives on a "
                "different computer than the EPW code (cross-computer transfer)."
            ),
        )

        spec.inputs.validator = cls.validate_inputs

        spec.output(
            "output_parameters",
            valid_type=orm.Dict,
            help="The `output_parameters` output node of the successful calculation.",
        )
        spec.output(
            "dos",
            valid_type=DosData,
            required=False,
            help="The electron density of states.",
        )
        spec.output(
            "phdos",
            valid_type=PhDosData,
            required=False,
            help="The phonon density of states.",
        )
        spec.output(
            "phdos_proj",
            valid_type=PDosData,
            required=False,
            help="The phonon density of states projected on the atomic orbitals.",
        )
        spec.output(
            "max_eigenvalue",
            valid_type=orm.XyData,
            required=False,
            help="The temperature dependence of the max eigenvalue.",
        )
        spec.output(
            "a2f",
            valid_type=A2fData,
            required=False,
            help="The contents of the `.a2f` file.",
        )
        spec.output(
            "a2f_proj",
            valid_type=PA2fData,
            required=False,
            help="The contents of the `.a2f_proj` file.",
        )
        spec.output(
            "lambda_FS",
            valid_type=LambdaFSData,
            required=False,
            help="The electron-phonon coupling on the Fermi surface.",
        )
        spec.output(
            "lambda_k_pairs",
            valid_type=DosData,
            required=False,
            help="The density of the electron-phonon coupling on the k-points.",
        )
        spec.output(
            "el_band_structure",
            valid_type=orm.BandsData,
            required=False,
            help="The interpolated electronic band structure.",
        )
        spec.output(
            "ph_band_structure",
            valid_type=orm.BandsData,
            required=False,
            help="The interpolated phonon band structure.",
        )
        spec.output(
            "iso_gap_functions",
            valid_type=GapFunctionData,
            required=False,
            help="The interpolated isotropic gap function.",
        )
        spec.output(
            "aniso_gap_functions",
            valid_type=GapFunctionData,
            required=False,
            help="The interpolated anisotropic gap function.",
        )
        spec.output(
            "aniso_gap_FS",
            valid_type=orm.ArrayData,
            required=False,
            help="The anisotropic gap on the Fermi surface.",
        )
        spec.output(
            "aniso_gap_imag",
            valid_type=orm.ArrayData,
            required=False,
            help="The anisotropic gap on the imaginary axis.",
        )

        spec.exit_code(
            300,
            "ERROR_NO_RETRIEVED_FOLDER",
            message="The retrieved folder data node could not be accessed.",
        )
        spec.exit_code(
            400,
            "ERROR_OUT_OF_WALLTIME",
            message="The calculation stopped prematurely because it ran out of walltime.",
        )
        spec.exit_code(
            310,
            "ERROR_OUTPUT_STDOUT_READ",
            message="The stdout output file could not be read.",
        )
        spec.exit_code(
            312,
            "ERROR_OUTPUT_STDOUT_INCOMPLETE",
            message="The stdout output file was incomplete probably because the calculation got interrupted.",
        )
        spec.exit_code(
            313,
            "ERROR_MEMORY_EXCEEDS_MAX_MEMLT",
            message="The required memory exceeds the EPW `max_memlt` setting.",
        )
        # yapf: enable
        spec.exit_code(
            314,
            "ERROR_PARAMETERS_NOT_VALID",
            message="The parameters are not valid.",
        )
        spec.exit_code(
            320,
            "ERROR_CANNOT_BRACKET_EF",
            message="Internal error, cannot bracket Ef.",
        )

    @classmethod
    def normalize_parameters(cls, parameters):
        """Return a copy of the input parameters with normalized namelist keys."""
        parameters = _uppercase_dict(parameters, dict_name="parameters")
        return {k: _lowercase_dict(v, dict_name=k) for k, v in parameters.items()}

    @classmethod
    def validate_blocked_keywords(cls, parameters):
        """Raise if users try to override keywords that are managed by the plugin."""
        for namelist, key in cls._blocked_keywords:
            if key in parameters.get(namelist, {}):
                raise exceptions.InputValidationError(
                    f"`parameters.{namelist}.{key}` is set automatically by the plugin."
                )

    @classmethod
    def validate_inputs(cls, value, _):
        """Validate the top-level inputs for the calculation."""
        if "parameters" not in value:
            return "required value was not provided for the `parameters` input."

        if "parent_folder" in value:
            return (
                "`parent_folder` is not supported for `EpwCalculation`; use "
                "`parent_folder_nscf`, `parent_folder_chk`, `parent_folder_ph`, "
                "or `parent_folder_epw` instead."
            )

        parameters = cls.normalize_parameters(value["parameters"].get_dict())

        try:
            cls.validate_parameters_inputs(parameters, value)
        except exceptions.InputValidationError as exception:
            return str(exception)

    @classmethod
    def validate_restart_inputs(cls, parameters, inputs):
        """Validate restart-related input combinations against the EPW parameters."""
        inputepw = parameters["INPUTEPW"]

        if not inputepw.get("wannierize", False):
            return

        for input_name in ("parent_folder_epw", "parent_folder_chk"):
            if input_name in inputs:
                raise exceptions.InputValidationError(
                    f"`{input_name}` cannot be specified when "
                    "`parameters.INPUTEPW.wannierize` is true."
                )

        if "parent_folder_nscf" not in inputs:
            raise exceptions.InputValidationError(
                "`parent_folder_nscf` must be specified when "
                "`parameters.INPUTEPW.wannierize` is true."
            )

    @staticmethod
    def has_manual_projections(inputepw):
        """Return whether manual projection entries were provided for EPW Wannierization."""
        projections = inputepw.get("proj")
        if projections is not None:
            return projections

        return any(key.startswith("proj(") for key in inputepw)

    @classmethod
    def validate_parameters_inputs(cls, parameters, inputs):
        """Validate normalized EPW parameters against the provided inputs."""
        if "INPUTEPW" not in parameters:
            raise exceptions.InputValidationError(
                "Required namelist `INPUTEPW` not in `parameters` input."
            )

        cls.set_blocked_keywords(parameters)
        cls.validate_restart_inputs(parameters, inputs)

        inputepw = parameters["INPUTEPW"]
        if inputepw.get("wannierize", False):
            if inputepw.get("auto_projections", False):
                raise exceptions.InputValidationError(
                    "`parameters.INPUTEPW.auto_projections` is not supported; "
                    "provide manual `proj` entries instead."
                )

            if inputepw.get("scdm_proj", False):
                raise exceptions.InputValidationError(
                    "`parameters.INPUTEPW.scdm_proj` is not supported; "
                    "provide manual `proj` entries instead."
                )

            if "proj" in inputepw and not isinstance(inputepw["proj"], (list, tuple)):
                raise exceptions.InputValidationError(
                    "`parameters.INPUTEPW.proj` must be a list or tuple so it can "
                    "be written as `proj(i)` entries."
                )

            if not cls.has_manual_projections(inputepw):
                raise exceptions.InputValidationError(
                    "Manual `proj` entries must be provided when "
                    "`parameters.INPUTEPW.wannierize` is true."
                )

        # Validate Eliashberg parameters
        momentum_dependence = inputs.get("momentum_dependence", None)
        full_bandwidth = inputs.get("full_bandwidth", None)
        real_axis = inputs.get("real_axis", None)
        analytical_continuation = inputs.get("analytical_continuation", None)

        if analytical_continuation is not None:
            ac_val = analytical_continuation.value
            if ac_val.lower() not in ("pade", "acon"):
                raise exceptions.InputValidationError(
                    f"Invalid `analytical_continuation`: '{ac_val}' is not supported. Must be 'pade' or 'acon'."
                )

        tc_linear = inputepw.get("tc_linear", False)

        if tc_linear:
            if real_axis is not None and real_axis.value:
                raise exceptions.InputValidationError(
                    "Linearized Eliashberg (tc_linear=True) cannot be used with real_axis=True."
                )
            if momentum_dependence is not None and momentum_dependence.value:
                raise exceptions.InputValidationError(
                    "Linearized Eliashberg (tc_linear=True) cannot be used with momentum_dependence=True (anisotropic)."
                )
            if full_bandwidth is not None and full_bandwidth.value:
                raise exceptions.InputValidationError(
                    "Linearized Eliashberg (tc_linear=True) cannot be used with full_bandwidth=True."
                )

        if real_axis is not None and real_axis.value:
            if momentum_dependence is not None and momentum_dependence.value:
                raise exceptions.InputValidationError(
                    "Real axis solver (real_axis=True) is only implemented for the isotropic case (momentum_dependence=False)."
                )
            if analytical_continuation is not None:
                raise exceptions.InputValidationError(
                    "Analytical continuation (analytical_continuation) cannot be used when solving on the real axis (real_axis=True)."
                )

        if full_bandwidth is not None and full_bandwidth.value:
            if (
                analytical_continuation is not None
                and analytical_continuation.value.lower() == "acon"
            ):
                raise exceptions.InputValidationError(
                    "Analytic continuation method 'acon' is not implemented when full_bandwidth is True."
                )

    @classmethod
    def set_blocked_keywords(cls, parameters):
        """Validate plugin-managed keywords without mutating the parameter dictionary."""
        cls.validate_blocked_keywords(parameters)
        return parameters

    @classmethod
    def validate_parallelization(cls, value, _):
        """Validate the optional `parallelization` input."""
        if not value:
            return None

        value_dict = value.get_dict()
        unknown_flags = set(value_dict) - set(cls._ENABLED_PARALLELIZATION_FLAGS)
        if unknown_flags:
            return (
                f"Unknown flags in `parallelization`: {unknown_flags}, allowed flags "
                f"are {cls._ENABLED_PARALLELIZATION_FLAGS}."
            )

        invalid_values = [
            flag_value
            for flag_value in value_dict.values()
            if isinstance(flag_value, bool)
            or not isinstance(flag_value, numbers.Integral)
            or flag_value < 1
        ]
        if invalid_values:
            return (
                "Parallelization values must be positive integers; "
                f"got invalid values {invalid_values}."
            )

    @staticmethod
    def generate_input_file(parameters):
        """Generate the EPW input file from normalized namelist parameters."""
        file_lines = []
        for namelist_name, namelist in parameters.items():
            file_lines.append(f"&{namelist_name}")
            for key, value in sorted(namelist.items()):
                entry = convert_input_to_namelist_entry(key, value).rstrip()
                if key == "temps":
                    entry = entry.replace("'", "")
                file_lines.append(entry)
            file_lines.append("/")

        return "\n".join(file_lines) + "\n"

    @staticmethod
    def test_mesh_offset(offset):
        """Validate that a mesh does not use an offset, which EPW cannot handle here."""
        if any(value != 0.0 for value in offset):
            raise NotImplementedError(
                "Computation of electron-phonon on a mesh with non zero offset is not implemented, "
                "at the level of epw.x"
            )

    @staticmethod
    def write_kpoints_list_file(folder, filename, kpoints):
        """Write an explicit k-point list to the EPW auxiliary file format."""
        with folder.open(filename, "w") as handle:
            handle.write(f"{len(kpoints)} crystal\n")
            for kpoint in kpoints:
                handle.write(
                    " ".join(f"{coordinate:.12}" for coordinate in kpoint) + "   1.0\n"
                )

    def set_coarse_mesh_parameters(
        self, parameters, kpoints, mesh_parameter_names, error_message
    ):
        """Populate coarse mesh parameters from a required mesh input."""
        try:
            mesh, offset = kpoints.get_kpoints_mesh()
            self.test_mesh_offset(offset)
        except NotImplementedError as exception:
            raise exceptions.InputValidationError(error_message) from exception

        for parameter_name, value in zip(mesh_parameter_names, mesh):
            parameters["INPUTEPW"][parameter_name] = value

    def set_fine_mesh_parameters(
        self,
        folder,
        parameters,
        kpoints,
        mesh_parameter_names,
        filename_parameter_name,
        filename,
        error_message,
    ):
        """Populate fine-grid parameters from a mesh or explicit point list."""
        try:
            mesh, offset = kpoints.get_kpoints_mesh()
            self.test_mesh_offset(offset)
            for parameter_name, value in zip(mesh_parameter_names, mesh):
                parameters["INPUTEPW"][parameter_name] = value
        except AttributeError:
            kpoints_list = kpoints.get_kpoints()
            self.write_kpoints_list_file(folder, filename, kpoints_list)
            parameters["INPUTEPW"][filename_parameter_name] = filename
        except NotImplementedError as exception:
            raise exceptions.InputValidationError(error_message) from exception

    def get_namelists_to_print(self, settings):
        """Return the namelists that should be written to the EPW input file."""
        try:
            namelists_toprint = settings.pop("NAMELISTS")
            if not isinstance(namelists_toprint, list):
                raise exceptions.InputValidationError(
                    "The 'NAMELISTS' value, if specified in the settings input "
                    "node, must be a list of strings"
                )
        except KeyError:
            namelists_toprint = self._default_namelists

        return namelists_toprint

    def get_settings(self):
        """Return normalized settings, matching the QE namelist calculation contract."""
        if "settings" in self.inputs:
            settings = _uppercase_dict(
                self.inputs.settings.get_dict(), dict_name="settings"
            )
            if "ADDITIONAL_RETRIEVE_LIST" in settings:
                warnings.warn(
                    "The key `ADDITIONAL_RETRIEVE_LIST` in the settings input is "
                    "deprecated and will be removed in the future. Use the "
                    "`CalcJob.metadata.options.additional_retrieve_list` input instead.",
                    AiidaDeprecationWarning,
                )
        else:
            settings = {}

        return settings

    def get_parameters(self):
        """Return normalized calculation parameters."""
        if "parameters" in self.inputs:
            parameters = self.normalize_parameters(self.inputs.parameters.get_dict())
        else:
            parameters = {}

        self.validate_parameters_inputs(parameters, self.inputs)

        return parameters

    def cap_nstemp(self, inputepw_parameters):
        """Clamp `nstemp` to the maximum value supported by this plugin."""
        nstemp = inputepw_parameters.get("nstemp")

        if nstemp and nstemp > self._MAX_NSTEMP:
            self.report(
                f"nstemp too large, reset it to maximum allowed: {self._MAX_NSTEMP}"
            )
            inputepw_parameters["nstemp"] = self._MAX_NSTEMP

    def prepare_input_parameters(self, folder, parameters):
        """Populate plugin-managed EPW parameters before writing the input file."""
        inputepw_parameters = parameters["INPUTEPW"]

        self.cap_nstemp(inputepw_parameters)

        # Override Eliashberg settings in parameters if inputs are specified
        eliashberg_any = any(
            f in self.inputs
            for f in (
                "momentum_dependence",
                "full_bandwidth",
                "real_axis",
                "analytical_continuation",
            )
        )
        if eliashberg_any:
            inputepw_parameters["eliashberg"] = True

            if "momentum_dependence" in self.inputs:
                momentum_dependence = self.inputs.momentum_dependence.value
                inputepw_parameters["laniso"] = momentum_dependence
                inputepw_parameters["liso"] = not momentum_dependence

            if "full_bandwidth" in self.inputs:
                inputepw_parameters["fbw"] = self.inputs.full_bandwidth.value

            if "real_axis" in self.inputs:
                real_axis = self.inputs.real_axis.value
                inputepw_parameters["lreal"] = real_axis
                inputepw_parameters["limag"] = not real_axis

            if "analytical_continuation" in self.inputs:
                ac_method = self.inputs.analytical_continuation.value.lower()
                if ac_method == "pade":
                    inputepw_parameters["lpade"] = True
                    inputepw_parameters["lacon"] = False
                elif ac_method == "acon":
                    inputepw_parameters["lpade"] = True
                    inputepw_parameters["lacon"] = True
                inputepw_parameters["limag"] = True
                inputepw_parameters["lreal"] = False

        inputepw_parameters["outdir"] = self._OUTPUT_SUBFOLDER
        inputepw_parameters["dvscf_dir"] = self._FOLDER_SAVE
        inputepw_parameters["prefix"] = self._PREFIX

        self.set_coarse_mesh_parameters(
            parameters,
            self.inputs.qpoints,
            ("nq1", "nq2", "nq3"),
            "Cannot get the coarse q-point grid",
        )
        self.set_coarse_mesh_parameters(
            parameters,
            self.inputs.kpoints,
            ("nk1", "nk2", "nk3"),
            "Cannot get the coarse k-point grid",
        )
        user_filqf = inputepw_parameters.get("filqf")
        if user_filqf:
            try:
                kpoints_list = self.inputs.qfpoints.get_kpoints()
            except AttributeError as exception:
                raise exceptions.InputValidationError(
                    f"Cannot get explicit q-points to write to '{user_filqf}'"
                ) from exception
            self.write_kpoints_list_file(folder, user_filqf, kpoints_list)
        else:
            self.set_fine_mesh_parameters(
                folder,
                parameters,
                self.inputs.qfpoints,
                ("nqf1", "nqf2", "nqf3"),
                "filqf",
                self._qfpoints_input_file,
                "Cannot get the fine q-point grid",
            )

        user_filkf = inputepw_parameters.get("filkf")
        if user_filkf:
            try:
                kpoints_list = self.inputs.kfpoints.get_kpoints()
            except AttributeError as exception:
                raise exceptions.InputValidationError(
                    f"Cannot get explicit k-points to write to '{user_filkf}'"
                ) from exception
            self.write_kpoints_list_file(folder, user_filkf, kpoints_list)
        else:
            self.set_fine_mesh_parameters(
                folder,
                parameters,
                self.inputs.kfpoints,
                ("nkf1", "nkf2", "nkf3"),
                "filkf",
                self._kfpoints_input_file,
                "Cannot get the fine k-point grid",
            )

        return parameters

    @staticmethod
    def get_parent_folder_path(parent_folder):
        """Return the filesystem path for a remote or stashed parent folder."""
        if isinstance(parent_folder, orm.RemoteStashFolderData):
            return Path(parent_folder.target_basepath)

        return Path(parent_folder.get_remote_path())

    def stage_nscf_parent(self, remote_copy_list):
        """Stage the NSCF output directory into the EPW working directory."""
        if "parent_folder_nscf" not in self.inputs:
            return

        parent_folder_nscf = self.inputs.parent_folder_nscf
        remote_copy_list.append(
            (
                parent_folder_nscf.computer.uuid,
                Path(
                    parent_folder_nscf.get_remote_path(),
                    PwCalculation._OUTPUT_SUBFOLDER,
                ).as_posix(),
                self._OUTPUT_SUBFOLDER,
            )
        )

    def stage_chk_parent(self, remote_list):
        """Stage Wannier checkpoint files required by EPW."""
        if "parent_folder_chk" not in self.inputs:
            return

        parent_folder_chk = self.inputs.parent_folder_chk

        for suffix in ["chk", "bvec"]:
            remote_list.append(
                (
                    parent_folder_chk.computer.uuid,
                    Path(
                        parent_folder_chk.get_remote_path(),
                        f"{self._PREFIX}.{suffix}",
                    ).as_posix(),
                    f"{self._PREFIX}.{suffix}",
                )
            )

        remote_list.append(
            (
                parent_folder_chk.computer.uuid,
                Path(
                    parent_folder_chk.get_remote_path(), f"{self._PREFIX}.mmn"
                ).as_posix(),
                f"{self._PREFIX}.wannier90.mmn",
            )
        )

    def stage_ph_parent(self, folder, settings, remote_list):
        """Stage `ph.x` data needed by EPW into the local `save` directory."""
        if "parent_folder_ph" not in self.inputs:
            return

        parent_folder_ph = self.inputs.parent_folder_ph
        folder.get_subfolder(self._FOLDER_SAVE, create=True)

        if "NUMBER_OF_QPOINTS" in settings:
            nqpt = settings.pop("NUMBER_OF_QPOINTS")
        else:
            nqpt = get_parent_ph_qpoint_ibz_count(parent_folder_ph)

        prefix = self._PREFIX
        outdir = PhCalculation._OUTPUT_SUBFOLDER
        fildvscf = PhCalculation._DVSCF_PREFIX
        fildyn = PhCalculation._OUTPUT_DYNAMICAL_MATRIX_PREFIX
        ph_path = self.get_parent_folder_path(parent_folder_ph)

        remote_list.append(
            (
                parent_folder_ph.computer.uuid,
                Path(ph_path, outdir, "_ph0", f"{prefix}.phsave").as_posix(),
                self._FOLDER_SAVE,
            )
        )

        for iqpt in range(1, nqpt + 1):
            remote_list.append(
                (
                    parent_folder_ph.computer.uuid,
                    Path(
                        ph_path,
                        outdir,
                        "_ph0",
                        "" if iqpt == 1 else f"{prefix}.q_{iqpt}",
                        f"{prefix}.{fildvscf}1",
                    ).as_posix(),
                    Path(self._FOLDER_SAVE, f"{prefix}.dvscf_q{iqpt}").as_posix(),
                )
            )
            remote_list.append(
                (
                    parent_folder_ph.computer.uuid,
                    Path(ph_path, f"{fildyn}{iqpt}").as_posix(),
                    Path(self._FOLDER_SAVE, f"{prefix}.dyn_q{iqpt}").as_posix(),
                )
            )

    def stage_epw_parent(self, folder, parameters, remote_list, remote_symlink_list):
        """Stage restart files from a previous EPW calculation."""
        if "parent_folder_epw" not in self.inputs:
            return

        folder.get_subfolder(self._OUTPUT_SUBFOLDER, create=True)

        parent_folder_epw = self.inputs.parent_folder_epw
        epw_path = self.get_parent_folder_path(parent_folder_epw)

        file_list = [
            "selecq.fmt",
            "crystal.fmt",
            "epwdata.fmt",
            "dmedata.fmt",
            "vmedata.fmt",
            "wigner.fmt",
            "quadrupole.fmt",
            "decay.H",
            "decay.v",
            "decay.P",
            "decay.dynmat",
            "decay.epmate",
            "decay.epmatp",
            f"{self._PREFIX}.kgmap",
            f"{self._PREFIX}.kmap",
            f"{self._PREFIX}.ukk",
            f"{self._PREFIX}.mmn",
            f"{self._PREFIX}.bvec",
            self._FOLDER_SAVE,
        ]
        if parameters["INPUTEPW"].get("restart", False):
            file_list.append("restart.fmt")

        if parameters["INPUTEPW"].get("epwread", False) and parameters["INPUTEPW"].get(
            "elph", False
        ):
            remote_symlink_list.append(
                (
                    parent_folder_epw.computer.uuid,
                    Path(
                        epw_path,
                        f"{self._OUTPUT_SUBFOLDER}/{self._PREFIX}.epmatwp",
                    ).as_posix(),
                    Path(f"{self._OUTPUT_SUBFOLDER}/{self._PREFIX}.epmatwp").as_posix(),
                )
            )

        if parameters["INPUTEPW"].get("eliashberg", False):
            if parameters["INPUTEPW"].get("ephwrite", True):
                if parameters["INPUTEPW"].get("restart", False):
                    remote_symlink_list.append(
                        (
                            parent_folder_epw.computer.uuid,
                            Path(
                                epw_path,
                                f"{self._OUTPUT_SUBFOLDER}/{self._PREFIX}.ephmat",
                            ).as_posix(),
                            Path(
                                f"{self._OUTPUT_SUBFOLDER}/{self._PREFIX}.ephmat"
                            ).as_posix(),
                        )
                    )
            else:
                remote_symlink_list.append(
                    (
                        parent_folder_epw.computer.uuid,
                        Path(
                            epw_path,
                            f"{self._OUTPUT_SUBFOLDER}/{self._PREFIX}.ephmat",
                        ).as_posix(),
                        Path(
                            f"{self._OUTPUT_SUBFOLDER}/{self._PREFIX}.ephmat"
                        ).as_posix(),
                    )
                )
                file_list.append(f"{self._PREFIX}.a2f")

        for filename in file_list:
            remote_list.append(
                (
                    parent_folder_epw.computer.uuid,
                    Path(epw_path, filename).as_posix(),
                    Path(filename).as_posix(),
                )
            )

    def stage_quadrupole(self, local_copy_list, remote_list):
        """Stage quadrupole file/directory if provided as inputs."""
        if "quadrupole_file" in self.inputs:
            # SinglefileData: upload directly from the AiiDA repository (cross-computer safe)
            quadrupole_file = self.inputs.quadrupole_file
            local_copy_list.append(
                (quadrupole_file.uuid, quadrupole_file.filename, "quadrupole.fmt")
            )
        elif "quadrupole_dir" in self.inputs:
            quadrupole_dir = self.inputs.quadrupole_dir
            if isinstance(quadrupole_dir, orm.RemoteData):
                source_path = quadrupole_dir.get_remote_path()
                # Use the computer where the quadrupole data actually lives, not the EPW computer
                quad_computer_uuid = quadrupole_dir.computer.uuid
            else:
                source_path = quadrupole_dir.value
                quad_computer_uuid = self.inputs.code.computer.uuid

            remote_list.append(
                (
                    quad_computer_uuid,
                    Path(source_path, "quadrupole.fmt").as_posix(),
                    "quadrupole.fmt",
                )
            )

    def stage_parent_folders(
        self, folder, parameters, settings, remote_copy_list, remote_symlink_list
    ):
        """Stage all supported parent-folder inputs for the EPW calculation."""
        remote_list = (
            remote_symlink_list
            if settings.pop("PARENT_FOLDER_SYMLINK", self._default_symlink_usage)
            else remote_copy_list
        )

        self.stage_nscf_parent(remote_copy_list)
        self.stage_chk_parent(remote_list)
        self.stage_ph_parent(folder, settings, remote_list)
        self.stage_epw_parent(folder, parameters, remote_list, remote_symlink_list)

    def _add_parallelization_flags_to_cmdline_params(self, cmdline_params):
        """Return cmdline parameters with validated parallelization flags appended."""
        cmdline_params_result = list(cmdline_params)
        cmdline_params_normalized = []

        for param in cmdline_params:
            cmdline_params_normalized.extend(param.split())

        parallelization_dict = (
            self.inputs.parallelization.get_dict()
            if "parallelization" in self.inputs
            else {}
        )

        for flag_name in self._ENABLED_PARALLELIZATION_FLAGS:
            aliases = list(self._PARALLELIZATION_FLAG_ALIASES[flag_name]) + [flag_name]
            aliases_in_cmdline = [
                alias for alias in aliases if f"-{alias}" in cmdline_params_normalized
            ]

            if aliases_in_cmdline:
                if len(aliases_in_cmdline) > 1:
                    raise exceptions.InputValidationError(
                        "Conflicting parallelization flags "
                        f"{aliases_in_cmdline} in settings['CMDLINE']"
                    )
                if flag_name in parallelization_dict:
                    raise exceptions.InputValidationError(
                        "Parallelization flag "
                        f"'{aliases_in_cmdline[0]}' specified in settings['CMDLINE'] "
                        f"conflicts with '{flag_name}' in the `parallelization` input."
                    )
                warnings.warn(
                    "Specifying the parallelization flags through settings['CMDLINE'] is "
                    "deprecated, use the `parallelization` input instead.",
                    AiidaDeprecationWarning,
                )
                continue

            if flag_name in parallelization_dict:
                cmdline_params_result += [
                    f"-{flag_name}",
                    str(parallelization_dict[flag_name]),
                ]

        return cmdline_params_result

    def create_codeinfo(self, settings):
        """Create the `CodeInfo` for this EPW calculation."""
        codeinfo = datastructures.CodeInfo()
        codeinfo.cmdline_params = self._add_parallelization_flags_to_cmdline_params(
            list(settings.pop("CMDLINE", []))
        ) + ["-in", self.metadata.options.input_filename]
        codeinfo.stdout_name = self.metadata.options.output_filename
        codeinfo.code_uuid = self.inputs.code.uuid

        return codeinfo

    def write_input_file(self, folder, parameters, settings):
        """Write the EPW input file to the sandbox folder."""
        namelists_toprint = self.get_namelists_to_print(settings)
        file_content = self.generate_input_file(
            self.filter_namelists(parameters, namelists_toprint)
        )
        with folder.open(self.metadata.options.input_filename, "w") as infile:
            infile.write(file_content)

    def create_calcinfo(
        self,
        settings,
        codeinfo,
        local_copy_list,
        remote_copy_list,
        remote_symlink_list,
        retrieve_list,
    ):
        """Create the `CalcInfo` for this EPW calculation."""
        calcinfo = datastructures.CalcInfo()
        calcinfo.uuid = str(self.uuid)
        calcinfo.codes_info = [codeinfo]
        calcinfo.local_copy_list = local_copy_list
        calcinfo.remote_copy_list = remote_copy_list
        calcinfo.remote_symlink_list = remote_symlink_list
        calcinfo.retrieve_list = [self.metadata.options.output_filename]
        calcinfo.retrieve_list += retrieve_list
        calcinfo.retrieve_list += settings.pop("ADDITIONAL_RETRIEVE_LIST", [])
        calcinfo.retrieve_list += self._internal_retrieve_list
        calcinfo.retrieve_temporary_list = self._retrieve_temporary_list
        calcinfo.retrieve_singlefile_list = self._retrieve_singlefile_list

        return calcinfo

    def validate_remaining_settings(self, settings):
        """Remove parser options and fail on any remaining unknown settings."""
        _pop_parser_options(self, settings)

        if settings:
            unknown_keys = ", ".join(list(settings.keys()))
            raise exceptions.InputValidationError(
                f"`settings` contained unexpected keys: {unknown_keys}"
            )

    def prepare_for_submission(self, folder):
        """Prepare the calculation job for submission by transforming input nodes into input files.

        In addition to the input files being written to the sandbox folder, a `CalcInfo` instance will be returned that
        contains lists of files that need to be copied to the remote machine before job submission, as well as file
        lists that are to be retrieved after job completion.

        :param folder: a sandbox folder to temporarily write files on disk.
        :return: :class:`~aiida.common.datastructures.CalcInfo` instance.
        """
        # pylint: disable=too-many-statements,too-many-branches, protected-access

        local_copy_list = []
        remote_copy_list = []
        remote_symlink_list = []

        settings = self.get_settings()
        parameters = self.prepare_input_parameters(folder, self.get_parameters())

        self.stage_parent_folders(
            folder, parameters, settings, remote_copy_list, remote_symlink_list
        )

        retrieve_list = []
        self.write_input_file(folder, parameters, settings)

        # Stage quadrupole files if present
        remote_list = (
            remote_symlink_list
            if settings.pop("PARENT_FOLDER_SYMLINK", self._default_symlink_usage)
            else remote_copy_list
        )
        self.stage_quadrupole(local_copy_list, remote_list)

        codeinfo = self.create_codeinfo(settings)
        calcinfo = self.create_calcinfo(
            settings,
            codeinfo,
            local_copy_list,
            remote_copy_list,
            remote_symlink_list,
            retrieve_list,
        )
        self.validate_remaining_settings(settings)

        return calcinfo
