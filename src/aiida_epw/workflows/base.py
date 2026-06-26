# -*- coding: utf-8 -*-
from aiida import orm
from aiida.common import AttributeDict
from pathlib import Path

from aiida.engine import (
    BaseRestartWorkChain,
    ProcessHandlerReport,
    process_handler,
    while_,
)
from aiida.plugins import CalculationFactory
from aiida.common.lang import type_check

from aiida_quantumespresso.calculations.functions.create_kpoints_from_distance import (
    create_kpoints_from_distance,
)
from aiida_quantumespresso.workflows.protocols.utils import ProtocolMixin
from aiida.orm.nodes.data.base import to_aiida_type

from aiida_epw.tools.kpoints import check_kpoints_qpoints_compatibility

EpwCalculation = CalculationFactory("epw.epw")


def get_kpoints_from_chk_folder(chk_folder):
    """
    This method tries different strategies to find the k-point mesh from a parent nscf folder.

    :param chk_folder: A RemoteData node from a Wannier90Calculation (chk).
    :return: A KpointsData node that has mesh information.
    :raises ValueError: If the mesh cannot be found through any strategy.
    """

    wannier_params = chk_folder.creator.inputs.parameters

    if "mp_grid" in wannier_params:
        mp_grid = wannier_params["mp_grid"]
        kpoints = orm.KpointsData()
        kpoints.set_kpoints_mesh(mp_grid)
        return kpoints
    else:
        raise ValueError(
            "Could not deduce mesh from the parent folder of the nscf calculation."
        )


def validate_inputs(  # pylint: disable=unused-argument,inconsistent-return-statements
    inputs, ctx=None
):
    """Validate the inputs of the entire input namespace of `EpwBaseWorkChain`."""
    # Usually at the creation of the inputs, the coarse and fine k/q grid is already determined.
    #
    # Cannot specify both `kfpoints` and `kfpoints_factor`
    if all(_ in inputs for _ in ["kfpoints", "kfpoints_factor"]):
        return "Can only specify one of the `kfpoints`, `kfpoints_factor`."

    if not any([_ in inputs for _ in ["kfpoints", "kfpoints_factor"]]):
        return "Either `kfpoints` or `kfpoints_factor` must be specified."

    # Cannot specify both `kfpoints` and `kfpoints_factor`
    if all(_ in inputs for _ in ["qfpoints", "qfpoints_distance"]):
        return "Can only specify one of the `qfpoints`, `qfpoints_distance`."

    if not any([_ in inputs for _ in ["qfpoints", "qfpoints_distance"]]):
        return "Either `qfpoints` or `qfpoints_distance` must be specified."

    return None


class EpwBaseWorkChain(ProtocolMixin, BaseRestartWorkChain):
    """BaseWorkchain to run a epw.x calculation."""

    _MAX_NSIW = 200
    _MIN_NSIW = 20

    _process_class = EpwCalculation

    @classmethod
    def define(cls, spec):
        """Define the process specification."""
        # yapf: disable
        super().define(spec)

        # Here we exclude the `metadata` input of the EpwCalculation
        # to avoid the conflict between it with the `metadata` of this WorkChain.
        # We also exclude the `qfpoints` and `kfpoints` inputs of the EpwCalculation
        # because they are marked as required in the EpwCalculation.
        # but will only be provided when the EpwBaseWorkChain is run.
        spec.expose_inputs(
            EpwCalculation,
            exclude=('metadata', 'qfpoints', 'kfpoints')
        )

        spec.input(
            'options',
            valid_type=orm.Dict,
            required=True,
            serializer=to_aiida_type,
            help=(
                "Dictionary containing the `metadata.options` for the `EpwCalculation`."
                )
            )

        spec.input(
            'structure',
            valid_type=orm.StructureData,
            required=False,
            help=(
                "Structure used to generate k-point and q-point meshes. Should match the "
                "one used in the previous `Wannier90BandsWorkChain`. Only required when "
                "fine k/q meshes are built from a distance."
                )
            )

        spec.input(
            'qfpoints_distance',
            valid_type=orm.Float,
            serializer=to_aiida_type,
            required=False,
            help=(
                "Distance between q-points in the fine mesh. Mutually exclusive with "
                "`qfpoints`; provide only one. When set, fine q-points are generated "
                "from the input structure and this distance. Otherwise, supply fine "
                "q-points explicitly."
                )
            )


        spec.input(
            'kfpoints_factor',
            valid_type=orm.Int,
            serializer=to_aiida_type,
            required=False,
            help=(
                "Factor applied to each dimension of the fine q-point mesh to obtain the "
                "fine k-point mesh. Mutually exclusive with `kfpoints`; provide only one. "
                "For example, a fine q-mesh [40, 40, 40] with `kfpoints_factor=2` becomes "
                "[80, 80, 80]."
                )
            )

        spec.input(
            "w90_chk_to_ukk_script",
            valid_type=orm.RemoteData,
            required=False,
            help="Julia script that converts `prefix.chk` from `wannier90.x` to the `epw.x`-readable `prefix.ukk`.",
        )

        spec.inputs.validator = validate_inputs

        spec.outline(
            cls.setup,
            cls.validate_kpoints,
            while_(cls.should_run_process)(
                cls.prepare_process,
                cls.run_process,
                cls.inspect_process,
            ),
            cls.results,
        )

        spec.expose_outputs(EpwCalculation)

        spec.exit_code(202, 'ERROR_COARSE_GRID_NOT_VALID',
            message='The specification of coarse k/q grid is not valid.')
        spec.exit_code(300, 'ERROR_UNRECOVERABLE_FAILURE',
            message='The calculation failed with an unidentified unrecoverable error.')
        spec.exit_code(310, 'ERROR_KNOWN_UNRECOVERABLE_FAILURE',
            message='The calculation failed with a known unrecoverable error.')

    @classmethod
    def get_protocol_filepath(cls, filename="base.yaml"):
        """Return ``pathlib.Path`` to the ``.yaml`` file that defines the protocols.

        :param filename: Name of the protocol file (default: "base.yaml").
        """
        from importlib_resources import files

        from . import protocols

        return files(protocols) / filename

    @classmethod
    def get_protocol_inputs(cls, protocol=None, overrides=None, filename="base.yaml"):
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
    def get_builder_from_protocol(
        cls,
        code,
        structure,
        protocol=None,
        overrides=None,
        options=None,
        w90_chk_to_ukk_script=None,
        quadrupole_dir=None,
        protocol_filename="base.yaml",
        **_,
    ):
        """Return a builder prepopulated with inputs selected according to the chosen protocol.

        :param code: the ``Code`` instance configured for the ``quantumespresso.epw`` plugin.
        :param protocol: protocol to use, if not specified, the default will be used.
        :param overrides: optional dictionary of inputs to override the defaults of the protocol.
        :param options: optional dictionary of options to override the metadata options.
        :param w90_chk_to_ukk_script: a julia script to convert the prefix.chk file.
        :param protocol_filename: Name of the protocol file to use (default: "base.yaml").
        :return: a process builder instance with all inputs defined ready for launch.
        """
        from aiida_quantumespresso.workflows.protocols.utils import recursive_merge

        type_check(code, orm.Code)
        type_check(structure, orm.StructureData)

        inputs = cls.get_protocol_inputs(
            protocol, overrides, filename=protocol_filename
        )

        # Update the parameters based on the protocol inputs
        parameters = inputs["parameters"]

        # If overrides are provided, they are considered absolute
        if overrides:
            parameter_overrides = overrides.get("parameters", {})
            parameters = recursive_merge(parameters, parameter_overrides)

        if options:
            inputs["options"] = recursive_merge(inputs["options"], options)

        # pylint: disable=no-member
        builder = cls.get_builder()
        builder.structure = structure
        builder.code = code
        builder.parameters = orm.Dict(parameters)
        builder.options = orm.Dict(inputs["options"])

        if w90_chk_to_ukk_script:
            type_check(w90_chk_to_ukk_script, orm.RemoteData)
            builder.w90_chk_to_ukk_script = w90_chk_to_ukk_script

        if "settings" in inputs:
            builder.settings = orm.Dict(inputs["settings"])
        if "parallelization" in inputs:
            builder.parallelization = orm.Dict(inputs["parallelization"])

        builder.clean_workdir = orm.Bool(inputs["clean_workdir"])

        builder.qfpoints_distance = orm.Float(inputs["qfpoints_distance"])
        builder.kfpoints_factor = orm.Int(inputs["kfpoints_factor"])
        builder.max_iterations = orm.Int(inputs["max_iterations"])
        builder.max_iterations = orm.Int(inputs["max_iterations"])

        if quadrupole_dir:
            if isinstance(quadrupole_dir, (str, Path)):
                builder.quadrupole_dir = orm.Str(str(quadrupole_dir))
            else:
                builder.quadrupole_dir = quadrupole_dir

        # pylint: enable=no-member

        return builder

    def setup(self):
        """Call the ``setup`` of the ``BaseRestartWorkChain`` and create the inputs dictionary in ``self.ctx.inputs``.
        This ``self.ctx.inputs`` dictionary will be used by the ``EpwCalculation`` in the internal loop.
        """
        super().setup()

        self.ctx.inputs = AttributeDict(self.exposed_inputs(EpwCalculation))

        # Initialize here an empty metadata dictionary.
        # Now there is only options in it.

        # TODO: Should check if we need to append more
        # information into it.
        metadata = {}

        metadata["options"] = self.inputs.options.get_dict()

        ## Didn't find the way to modify `metadata` in EpwCalculation.
        ## It should be migrated into EpwCalculation in the future.
        if (
            "w90_chk_to_ukk_script" in self.inputs
            and "parent_folder_chk" in self.inputs
        ):
            prepend_text = metadata["options"].get("prepend_text", "")
            prepend_text += f"\n{self.inputs.w90_chk_to_ukk_script.get_remote_path()} {EpwCalculation._PREFIX}.chk {EpwCalculation._OUTPUT_SUBFOLDER}{EpwCalculation._PREFIX}.xml {EpwCalculation._PREFIX}.ukk {EpwCalculation._PREFIX}.wannier90.mmn {EpwCalculation._PREFIX}.mmn"

            metadata["options"]["prepend_text"] = prepend_text

        self.ctx.inputs.metadata = metadata

        # Update of the parameters should be done here instead of in EpwCalculation
        # so that all the changes of parameters are saved!
        # IMPORTANT: I notice that since now EpwCalculation is not encapsulated, the parameters exposed to
        # the EpwBaseWorkChain and the parameters inside EpwCalculation are not the same.
        parameters = self.ctx.inputs.parameters.get_dict()

        if "parent_folder_chk" in self.inputs:
            w90_params = (
                self.inputs.parent_folder_chk.creator.inputs.parameters.get_dict()
            )
            exclude_bands = w90_params.get("exclude_bands", None)  # TODO check this!

            if exclude_bands:
                parameters["INPUTEPW"]["bands_skipped"] = (
                    f"exclude_bands = {exclude_bands[0]}:{exclude_bands[-1]}"
                )

            parameters["INPUTEPW"]["nbndsub"] = w90_params["num_wann"]

        if "parent_folder_epw" in self.inputs:
            epw_params = (
                self.inputs.parent_folder_epw.creator.inputs.parameters.get_dict()
            )
            parameters["INPUTEPW"]["use_ws"] = epw_params["INPUTEPW"].get(
                "use_ws", False
            )
            parameters["INPUTEPW"]["nbndsub"] = epw_params["INPUTEPW"]["nbndsub"]
            if "bands_skipped" in epw_params["INPUTEPW"]:
                parameters["INPUTEPW"]["bands_skipped"] = epw_params["INPUTEPW"].get(
                    "bands_skipped"
                )

        self.ctx.inputs.parameters = orm.Dict(parameters)

    # We should validate the kpoints and qpoints on the fly
    # because they are usually not determined at the creation of the inputs.
    def validate_kpoints(self):
        """
        Validate the inputs related to k-points.
        `epw.x` requires coarse k-points and q-points to be compatible, which means the kpoints should be multiple of qpoints.
        e.g. if qpoints are [2,2,2], kpoints should be [2*l,2*m,2*n] for integer l,m,n.
        We firstly construct qpoints. Either an explicit `KpointsData` with given mesh/path, or a desired qpoints distance should be specified.
        In the case of the latter, the `KpointsData` will be constructed for the input `StructureData` using the `create_kpoints_from_distance` calculation function.
        Then we construct kpoints by multiplying the qpoints mesh by the `kpoints_factor`.
        """

        # If there is already the parent folder of a previous EPW calculation, the coarse k/q grid is already there and must be valid.
        # We only need to take the kpointsdata from it and continue to generate the find grid.

        if "parent_folder_epw" in self.inputs:
            epw_calc = self.inputs.parent_folder_epw.creator
            kpoints = epw_calc.inputs.kpoints
            qpoints = epw_calc.inputs.qpoints

        # If there is no parent folder of a previous EPW calculation, it must be the case that we are running the transition from coarse BLoch representation to Wannier representation.
        # This means that we are using coarse k grid from a previous nscf calculation and
        else:
            if "kpoints" in self.inputs:
                kpoints = self.inputs.kpoints
            elif "parent_folder_chk" in self.inputs:
                kpoints = get_kpoints_from_chk_folder(self.inputs.parent_folder_chk)
            else:
                self.report(
                    "Could not determine the coarse k-points from the inputs or the parent folder of the wannier90 calculation."
                )
                return self.exit_codes.ERROR_COARSE_GRID_NOT_VALID

            if "qpoints" in self.inputs:
                qpoints = self.inputs.qpoints
            elif "parent_folder_ph" in self.inputs:
                qpoints = self.inputs.parent_folder_ph.creator.inputs.qpoints
            else:
                self.report(
                    "Could not determine the coarse q-points from the inputs or the parent folder of the ph calculation."
                )
                return self.exit_codes.ERROR_COARSE_GRID_NOT_VALID

        self.report(
            f"Successfully determined coarse k-points from the inputs: {kpoints.get_kpoints_mesh()[0]}"
        )
        self.report(
            f"Successfully determined coarse q-points from the inputs: {qpoints.get_kpoints_mesh()[0]}"
        )

        is_compatible, message = check_kpoints_qpoints_compatibility(kpoints, qpoints)

        self.ctx.inputs.kpoints = kpoints
        self.ctx.inputs.qpoints = qpoints

        if not is_compatible:
            self.report(message)
            return self.exit_codes.ERROR_COARSE_GRID_NOT_VALID

        ## TODO: If we are restarting from .ephmat folder, we should use the same
        ## qfpoints and kfpoints as the creator of 'parent_folder_epw'.
        if "qfpoints" in self.inputs:
            qfpoints = self.inputs.qfpoints
        else:
            inputs = {
                "structure": self.inputs.structure,
                "distance": self.inputs.qfpoints_distance,
                "force_parity": self.inputs.get(
                    "qfpoints_force_parity", orm.Bool(False)
                ),
                "metadata": {"call_link_label": "create_qfpoints_from_distance"},
            }
            qfpoints = create_kpoints_from_distance(**inputs)  # pylint: disable=unexpected-keyword-arg

        if "kfpoints" in self.inputs:
            kfpoints = self.inputs.kfpoints
        else:
            qfpoints_mesh = qfpoints.get_kpoints_mesh()[0]
            kfpoints = orm.KpointsData()
            kfpoints.set_kpoints_mesh(
                [v * self.inputs.kfpoints_factor.value for v in qfpoints_mesh]
            )

        self.ctx.inputs.qfpoints = qfpoints
        self.ctx.inputs.kfpoints = kfpoints

    def prepare_process(self):
        """
        Prepare inputs for the next calculation.

        Currently, no modifications to `self.ctx.inputs` are needed before
        submission. We rely on the parent `run_process` to create the builder.
        """
        pass

    def report_error_handled(self, calculation, action):
        """Report an action taken for a calculation that has failed.

        This should be called in a registered error handler if its condition is met and an action was taken.

        :param calculation: the failed calculation node
        :param action: a string message with the action taken
        """
        arguments = [
            calculation.process_label,
            calculation.pk,
            calculation.exit_status,
            calculation.exit_message,
        ]
        self.report("{}<{}> failed with exit status {}: {}".format(*arguments))
        self.report(f"Action taken: {action}")

    @process_handler(priority=10)
    def handle_unrecoverable_failure(self, calculation):
        """Handle calculations with an exit status below 400 which are unrecoverable, so abort the work chain."""
        if calculation.is_failed and calculation.exit_status < 400:
            self.report_error_handled(calculation, "unrecoverable error, aborting...")
            return ProcessHandlerReport(
                True, self.exit_codes.ERROR_UNRECOVERABLE_FAILURE
            )

    @process_handler(
        priority=500,
        exit_codes=[EpwCalculation.exit_codes.ERROR_OUTPUT_STDOUT_INCOMPLETE],
    )
    def handle_mesh_refinement(self, calculation):
        """Handle exit code 312 (incomplete output) by adjusting mesh parameters.

        This error can be caused by:
        1. Mesh too coarse: "LU factorization failed" in prtmeff (effective mass calculation)
        2. Mesh too dense: Out of Memory (process killed)

        The handler inspects the output to diagnose and adjusts parameters accordingly.
        """
        # Try to read output to diagnose
        stdout_content = ""
        scheduler_stderr = ""

        if "retrieved" in calculation.outputs:
            retrieved = calculation.outputs.retrieved
            try:
                output_filename = (
                    calculation.get_option("output_filename") or "aiida.out"
                )
                if output_filename in retrieved.list_object_names():
                    stdout_content = retrieved.get_object_content(output_filename)
            except Exception:
                pass
            try:
                if "_scheduler-stderr.txt" in retrieved.list_object_names():
                    scheduler_stderr = retrieved.get_object_content(
                        "_scheduler-stderr.txt"
                    )
            except Exception:
                pass

        is_lu_failure = "LU factorization failed" in stdout_content
        is_oom = any(
            x in scheduler_stderr.upper()
            for x in ["KILLED", "SIGKILL", "OOM", "OUT OF MEMORY"]
        )

        action_taken = None

        if is_lu_failure:
            # Mesh too coarse - need finer mesh
            if "kfpoints_factor" in self.ctx.inputs:
                current_factor = self.ctx.inputs.kfpoints_factor.value
                if current_factor == 1:
                    # Increase factor from 1 to 2
                    self.ctx.inputs.kfpoints_factor = orm.Int(2)
                    action_taken = f"Increased kfpoints_factor from {current_factor} to 2 due to LU factorization failure."
                else:
                    # Factor already > 1, refine qfpoints_distance instead
                    if "qfpoints_distance" in self.ctx.inputs:
                        current_dist = self.ctx.inputs.qfpoints_distance.value
                        new_dist = current_dist * 0.8
                        if new_dist >= 0.03:  # Don't go below minimum
                            self.ctx.inputs.qfpoints_distance = orm.Float(new_dist)
                            action_taken = f"Decreased qfpoints_distance from {current_dist:.3f} to {new_dist:.3f} due to LU factorization failure."
            elif "qfpoints_distance" in self.ctx.inputs:
                current_dist = self.ctx.inputs.qfpoints_distance.value
                new_dist = current_dist * 0.8
                if new_dist >= 0.03:
                    self.ctx.inputs.qfpoints_distance = orm.Float(new_dist)
                    action_taken = f"Decreased qfpoints_distance from {current_dist:.3f} to {new_dist:.3f} due to LU factorization failure."

        elif is_oom:
            # Mesh too dense - need coarser mesh
            if "qfpoints_distance" in self.ctx.inputs:
                current_dist = self.ctx.inputs.qfpoints_distance.value
                new_dist = current_dist * 1.25
                if new_dist <= 0.15:  # Don't go above maximum
                    self.ctx.inputs.qfpoints_distance = orm.Float(new_dist)
                    action_taken = f"Increased qfpoints_distance from {current_dist:.3f} to {new_dist:.3f} due to OOM."
            elif "kfpoints_factor" in self.ctx.inputs:
                current_factor = self.ctx.inputs.kfpoints_factor.value
                if current_factor > 1:
                    new_factor = current_factor - 1
                    self.ctx.inputs.kfpoints_factor = orm.Int(new_factor)
                    action_taken = f"Decreased kfpoints_factor from {current_factor} to {new_factor} due to OOM."

        if action_taken:
            # Regenerate kfpoints/qfpoints with updated parameters
            if "qfpoints_distance" in self.ctx.inputs:
                qf_inputs = {
                    "structure": self.inputs.structure,
                    "distance": self.ctx.inputs.qfpoints_distance,
                    "force_parity": self.inputs.get(
                        "qfpoints_force_parity", orm.Bool(False)
                    ),
                    "metadata": {
                        "call_link_label": "create_qfpoints_from_distance_retry"
                    },
                }
                qfpoints = create_kpoints_from_distance(**qf_inputs)
                self.ctx.inputs.qfpoints = qfpoints

                # Regenerate kfpoints based on qfpoints
                if "kfpoints_factor" in self.ctx.inputs:
                    qfpoints_mesh = qfpoints.get_kpoints_mesh()[0]
                    kfpoints = orm.KpointsData()
                    kfpoints.set_kpoints_mesh(
                        [
                            v * self.ctx.inputs.kfpoints_factor.value
                            for v in qfpoints_mesh
                        ]
                    )
                    self.ctx.inputs.kfpoints = kfpoints
            elif "kfpoints_factor" in self.ctx.inputs and "qfpoints" in self.ctx.inputs:
                # Just regenerate kfpoints from existing qfpoints with new factor
                qfpoints_mesh = self.ctx.inputs.qfpoints.get_kpoints_mesh()[0]
                kfpoints = orm.KpointsData()
                kfpoints.set_kpoints_mesh(
                    [v * self.ctx.inputs.kfpoints_factor.value for v in qfpoints_mesh]
                )
                self.ctx.inputs.kfpoints = kfpoints

            self.report_error_handled(calculation, action_taken)
            return ProcessHandlerReport(True)

        # Could not diagnose or fix - let it fail
        self.report_error_handled(
            calculation, "Could not diagnose cause of exit code 312. Aborting."
        )
        return ProcessHandlerReport(
            True, self.exit_codes.ERROR_KNOWN_UNRECOVERABLE_FAILURE
        )

    @process_handler(
        priority=400,
        exit_codes=[EpwCalculation.exit_codes.ERROR_PADE_APPROXIMANTS],
    )
    def handle_pade_approximants(self, calculation):
        """Handle exit code 322 (Pade NaN failure) by reducing nsiw and popping successful temperatures."""

        outputs = calculation.outputs.output_parameters.get_dict()
        eliashberg_data = (
            outputs.get("isotropic_eliashberg")
            or outputs.get("anisotropic_eliashberg")
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
                    pade.get(k) is None
                    for k in ("delta", "znorm", "shift")
                    if k in pade
                ):
                    has_failed = True

            if not iterations and not pade:
                has_failed = True

            if not has_failed:
                succeeded_temps.append(temp)

        input_params = self.ctx.inputs.parameters.get_dict()
        input_epw = input_params.get("INPUTEPW", {})

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
            if len(original_temps) == 2 and nstemp > 2:
                is_linear_range = True
                t_min, t_max = original_temps[0], original_temps[1]
                all_temps = [
                    t_min + i * (t_max - t_min) / (nstemp - 1) for i in range(nstemp)
                ]
            else:
                all_temps = original_temps
        elif (
            "tempsmin" in input_epw
            and "tempsmax" in input_epw
            and "nstemp" in input_epw
        ):
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
            t
            for t in all_temps
            if not any(abs(t - st) < 1e-4 for st in succeeded_temps)
        ]

        nsiw = None
        for t in remaining_temps:
            for k, data in eliashberg_data.items():
                if abs(float(k) - t) < 1e-4:
                    nsiw = data.get("nsiw")
                    break
            if nsiw is not None:
                break

        if nsiw is None:
            nsiw = input_epw.get("nsiw")

        current_npade = input_epw.get("npade", 90)

        current_N = None
        for t in remaining_temps:
            for k, data in eliashberg_data.items():
                if abs(float(k) - t) < 1e-4:
                    current_N = data.get("pade", {}).get("nsiter")
                    break
            if current_N is not None:
                break

        if current_N is None and nsiw is not None:
            fbw = input_epw.get("fbw", False)
            positive_matsu = input_epw.get("positive_matsu", True)
            if fbw and not positive_matsu:
                current_N = int(current_npade * (nsiw / 2) / 100)
            else:
                current_N = int(current_npade * nsiw / 100)

        target_npade = None
        if current_N is not None and nsiw:
            if current_N > self._MAX_NSIW:
                target_N = self._MAX_NSIW
            else:
                target_N = max(self._MIN_NSIW, int(current_N * 0.5))

            fbw = input_epw.get("fbw", False)
            positive_matsu = input_epw.get("positive_matsu", True)
            if fbw and not positive_matsu:
                target_npade = int(target_N * 100 / (nsiw / 2))
            else:
                target_npade = int(target_N * 100 / nsiw)

            target_npade = max(1, min(100, target_npade))
            if target_npade == current_npade and current_npade > 1:
                target_npade = max(1, current_npade - 5)

        action_taken = ""
        parameters = self.ctx.inputs.parameters.get_dict()
        input_epw_new = parameters.setdefault("INPUTEPW", {})

        new_temps_list = []
        new_nstemp = len(remaining_temps)
        if is_linear_range and new_nstemp > 2:
            new_temps_list = [remaining_temps[0], remaining_temps[-1]]
        else:
            new_temps_list = remaining_temps

        if isinstance(input_epw.get("temps"), str):
            input_epw_new["temps"] = " ".join(str(t) for t in new_temps_list)
        else:
            input_epw_new["temps"] = new_temps_list
        input_epw_new["nstemp"] = new_nstemp
        input_epw_new.pop("tempsmin", None)
        input_epw_new.pop("tempsmax", None)

        if target_npade is not None and target_npade != current_npade:
            input_epw_new["npade"] = target_npade
            action_taken += f"Reduced npade from {current_npade} to {target_npade}. "

        if len(remaining_temps) < len(all_temps):
            succeeded_list = [
                t
                for t in all_temps
                if any(abs(t - st) < 1e-4 for st in succeeded_temps)
            ]
            action_taken += (
                f"Removed successfully calculated temperatures: {succeeded_list}. "
            )

        if not action_taken:
            self.report_error_handled(
                calculation,
                "Cannot reduce npade further or pop temperatures. Aborting.",
            )
            return ProcessHandlerReport(
                True, self.exit_codes.ERROR_KNOWN_UNRECOVERABLE_FAILURE
            )

        try:
            from aiida_epw.common.types import RestartType

            self.ctx.inputs.restart_type = RestartType.EPHREAD
        except ImportError:
            input_epw_new["epwread"] = True

        self.ctx.inputs.parameters = orm.Dict(parameters)
        self.ctx.inputs.parent_folder_epw = calculation.outputs.remote_folder

        self.report_error_handled(calculation, action_taken)
        return ProcessHandlerReport(True)
