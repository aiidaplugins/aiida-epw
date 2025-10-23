# -*- coding: utf-8 -*-
from aiida import orm
from aiida.common import AttributeDict, NotExistent

from aiida.engine import BaseRestartWorkChain, ProcessHandlerReport, process_handler, while_
from aiida.plugins import CalculationFactory
from aiida.common.lang import type_check

from aiida_quantumespresso.calculations.functions.create_kpoints_from_distance import create_kpoints_from_distance
from aiida_quantumespresso.workflows.protocols.utils import ProtocolMixin
from aiida.orm.nodes.data.base import to_aiida_type


from pathlib import Path

EpwCalculation = CalculationFactory('epw.epw')

def get_kpoints_from_chk_folder(chk_folder):
    """
    This method tries different strategies to find the k-point mesh from a parent nscf folder.

    :param chk_folder: A RemoteData node from a Wannier90Calculation (chk).
    :return: A KpointsData node that has mesh information.
    :raises ValueError: If the mesh cannot be found through any strategy.
    """

    wannier_params = chk_folder.creator.inputs.parameters

    if 'mp_grid' in wannier_params:
        mp_grid = wannier_params['mp_grid']
        kpoints = orm.KpointsData()
        kpoints.set_kpoints_mesh(mp_grid)
        return kpoints
    else:
        raise ValueError("Could not deduce mesh from the parent folder of the nscf calculation.")
            
def check_kpoints_qpoints_compatibility(
    kpoints,
    qpoints,
    ) -> tuple[bool, str ]:
    """Check if the kpoints and qpoints are compatible."""
    
    kpoints_mesh, kpoints_shift = kpoints.get_kpoints_mesh()
    qpoints_mesh, qpoints_shift = qpoints.get_kpoints_mesh()
    
    multiplicities = []
    remainder = []
    
    for k, q in zip(kpoints_mesh, qpoints_mesh):
        multiplicities.append(k // q)
        remainder.append(k % q)

    if kpoints_shift != [0.0, 0.0, 0.0] or qpoints_shift != [0.0, 0.0, 0.0]:
        return (False, "Shift grid is not supported.")
    else:
        if remainder == [0, 0, 0]:
            return (True, f"The kpoints and qpoints are compatible with multiplicities {multiplicities}.")
        else:
            return (False, "The kpoints and qpoints are not compatible.")

def validate_parallelization(inputs, ctx=None):
    """Validate the parallelization settings. `epw.x` requires npool == nprocs.
    """
    try:
        resources = inputs['epw']['metadata']['options']['resources']
        num_machines = resources['num_machines']
        num_mpiprocs_per_machine = resources['num_mpiprocs_per_machine']
        total_procs = num_machines * num_mpiprocs_per_machine
    except KeyError as e:
        # If resource options are not defined, we cannot perform the check.
        # This is unlikely as AiiDA requires them, but it is a safe fallback.
        return f'Could not determine total MPI processes from metadata.options.resources: {e}.'

    # 2. Extract the `npool` value from either `parallelization` or `settings.CMDLINE`
    npool = None
    parallelization = inputs['epw'].get('parallelization', {})
    settings = inputs['epw'].get('settings', {})
    cmdline = settings.get('cmdline', [])

    if '-npool' in parallelization:
        try:
            npool = int(parallelization['-npool'])
        except (ValueError, TypeError):
            npool = None # Treat non-integer value as not set

    elif '-npool' in cmdline:
        try:
            # Find the index of '-npool' and get the next item in the list
            idx = cmdline.index('-npool')
            npool = int(cmdline[idx + 1])
        except (ValueError, IndexError, TypeError):
            # This handles cases like: ['-npool'] (at the end) or ['-npool', 'four']
            npool = None # Treat malformed cmdline as not set

    # 3. Perform the validation checks
    # if npool is None:
    #     parallelization['-npool'] = total_procs
    #     inputs['parallelization'] = orm.Dict(parallelization)
    if npool != total_procs:
        return f'Validation failed: `npool` value ({npool}) is not equal to the total number of MPI processes ({total_procs}).'
    
    return None

def validate_inputs(  # pylint: disable=unused-argument,inconsistent-return-statements
    inputs, ctx=None
    ):
    """Validate the inputs of the entire input namespace of `EpwBaseWorkChain`."""
    # Usually at the creation of the inputs, the coarse and fine k/q grid is already determined.
    # 
    # Cannot specify both `kfpoints` and `kfpoints_factor`
    result = validate_parallelization(inputs)
    if result:
        return result

    if (
        all(
            _ in inputs
            for _ in ["kfpoints", "kfpoints_factor"]
        )
    ):
        return "Can only specify one of the `kfpoints`, `kfpoints_factor`."

    if not any(
        [_ in inputs for _ in ["kfpoints", "kfpoints_factor"]]
        ):
        return "Either `kfpoints` or `kfpoints_factor` must be specified."

    # Cannot specify both `kfpoints` and `kfpoints_factor`
    if (
        all(
            _ in inputs
            for _ in ["qfpoints", "qfpoints_distance"]
        )
    ):
        return "Can only specify one of the `qfpoints`, `qfpoints_distance`."

    if not any(
        [_ in inputs for _ in ["qfpoints", "qfpoints_distance"]]
        ):
        return "Either `qfpoints` or `qfpoints_distance` must be specified."

    return None

class EpwBaseWorkChain(ProtocolMixin, BaseRestartWorkChain):
    """BaseWorkchain to run a epw.x calculation."""
    _process_class = EpwCalculation

    @classmethod
    def define(cls, spec):
        """Define the process specification."""
        # yapf: disable
        super().define(spec)

        # EpwBaseWorkChain will take over the determination of coarse grid according to the parent folders.
        # It will automatically generate fine grid k/q points according to qpoints distance and kfpoints factor.
        spec.expose_inputs(
            EpwCalculation, 
            namespace='epw',
            exclude=(
                'kpoints',
                'qpoints',
                'kfpoints',
                'qfpoints',
                'parent_folder_nscf',
                'parent_folder_ph',
                'parent_folder_epw',
            )
        )
        spec.input(
            'structure',
            valid_type=orm.StructureData,
            required=False,
            help=(
                "The structure data to use for the generation of k/q points by `create_kpoints_from_distance` calcfunction."
                "In principle, we should take the structure as the one we used in the previous calculation."
                "However, it is a bit difficult to take all the restart cases into account if we have a long chain of EPW calculations."
                "Therefore, for now we just provide it manually as an input."
                "But in the future, it will be removed."
                "In cases that the coarse and fine k/q points are explicitly speficied, this input is not necessary anymore."
                )
            )
        spec.input(
            'kpoints',
            valid_type=orm.KpointsData,
            required=False,
            help=(
                "The coarse k-points data to use for the epw calculation"
                "If not specified, the coarse kpoints will be generated from the parent folder of the nscf calculation."
                )
            )
        spec.input(
            'qpoints',
            valid_type=orm.KpointsData,
            required=False,
            help=(
                "The coarse q-points data to use for the epw calculation"
                "If not specified, the coarse qpoints will be generated from the parent folder of the ph calculation."
                )
            )
        spec.input(
            'qfpoints', 
            valid_type=orm.KpointsData, 
            required=False,
            help=(
                "The explicit q-points data to use for the fine qpoints"
                "If not specified, the fine qpoints will be generated from the parent folder of the ph calculation."
                )
            )

        spec.input(
            'qfpoints_distance', 
            valid_type=orm.Float,
            serializer=to_aiida_type,
            required=False,
            help=(
                "The q-points distance to generate the find qpoints"
                "If specified, the fine qpoints will be generated from `create_kpoints_from_distance` calcfunction."
                "If not specified, the fine qpoints will be read from the inputs.qfpoints input."
                )
            )

        spec.input(
            'kfpoints', 
            valid_type=orm.KpointsData, 
            required=False,
            serializer=to_aiida_type,
            help=(
                "The explicit k-points data to use for the fine kpoints"
                "If not specified, the fine kpoints will be generated from the parent folder of the nscf calculation."
                )
            )

        spec.input(
            'kfpoints_factor',
            valid_type=orm.Int,
            serializer=to_aiida_type,
            required=False,
            help=(
                "The factor to multiply the q-point mesh to get the fine k-point mesh"
                "If not specified, the fine kpoints will be generated from the parent folder of the nscf calculation."
                )
            )

        spec.input(
            'parent_folder_nscf', 
            valid_type=orm.RemoteData,
            required=False,
            help=(
                "The parent folder of the nscf calculation"
                "If not specified, the nscf calculation will be run from the inputs.structure input."
                )
            )

        spec.input(
            'parent_folder_ph',
            valid_type=(orm.RemoteData, orm.RemoteStashFolderData),
            required=False,
            help=(
                "The parent folder of the ph calculation"
                "If not specified, the ph calculation will be run from the inputs.structure input."
                )
            )

        spec.input(
            'parent_folder_epw',
            valid_type=(orm.RemoteData, orm.RemoteStashFolderData),
            required=False,
            help=(
                "The parent folder of the epw calculation"
                "If not specified, the epw calculation will be run from the inputs.structure input."
                )
            )

        # TODO: Maybe it's better to also have this input port in EpwCalculation.
        spec.input(
            'parent_folder_chk',
            valid_type=orm.RemoteData,
            required=False,
            help=(
                "The parent folder of the chk file"
                "If not specified, the chk file will be converted to a ukk file using the inputs.w90_chk_to_ukk_script input."
                )
            )

        spec.input(
            'w90_chk_to_ukk_script',
            valid_type=orm.RemoteData,
            required=False,
            help=(
                "The script to convert the chk file to a ukk file"
                "If not specified, the wannierization will be skipped."
                )
            )

        spec.inputs.validator = validate_inputs

        spec.outline(
            cls.setup,
            cls.validate_parent_folders,
            cls.validate_kpoints,
            while_(cls.should_run_process)(
                cls.prepare_process,
                cls.run_process,
                cls.inspect_process,
            ),
            cls.results,
        )

        spec.expose_outputs(EpwCalculation)

        spec.exit_code(201, 'ERROR_PARENT_FOLDER_NOT_VALID',
            message='The specification of parent folders is not valid.')
        spec.exit_code(202, 'ERROR_COARSE_GRID_NOT_VALID',
            message='The specification of coarse k/q grid is not valid.')
        spec.exit_code(203, 'ERROR_PARAMETERS_NOT_VALID',
            message='The parameters are not valid.')
        spec.exit_code(300, 'ERROR_UNRECOVERABLE_FAILURE',
            message='The calculation failed with an unidentified unrecoverable error.')
        spec.exit_code(310, 'ERROR_KNOWN_UNRECOVERABLE_FAILURE',
            message='The calculation failed with a known unrecoverable error.')
    
    @classmethod
    def get_protocol_filepath(cls):
        """Return ``pathlib.Path`` to the ``.yaml`` file that defines the protocols."""
        from importlib_resources import files

        from . import protocols
        return files(protocols) / 'base.yaml'

    @classmethod
    def get_builder_from_protocol(
        cls,
        code,
        structure,
        protocol=None,
        overrides=None,
        options=None,
        w90_chk_to_ukk_script=None,
        **_
        ):
        """Return a builder prepopulated with inputs selected according to the chosen protocol.

        :param code: the ``Code`` instance configured for the ``quantumespresso.epw`` plugin.
        :param protocol: protocol to use, if not specified, the default will be used.
        :param overrides: optional dictionary of inputs to override the defaults of the protocol.
        :param w90_chk_to_ukk_script: a julia script to convert the prefix.chk file (generated by wannier90.x) to a prefix.ukk file (to be used by epw.x)
        :return: a process builder instance with all inputs defined ready for launch.
        """
        from aiida_quantumespresso.workflows.protocols.utils import recursive_merge

        type_check(code, orm.Code)
        type_check(structure, orm.StructureData)
        
        inputs = cls.get_protocol_inputs(protocol, overrides)

        # Update the parameters based on the protocol inputs
        parameters = inputs['epw']['parameters']

        # If overrides are provided, they are considered absolute
        if overrides:
            parameter_overrides = overrides.get('epw', {}).get('parameters', {})
            parameters = recursive_merge(parameters, parameter_overrides)

        metadata = inputs['epw']['metadata']
        if options:
            metadata['options'] = recursive_merge(inputs['pw']['metadata']['options'], options)

        # pylint: disable=no-member
        builder = cls.get_builder()
        builder.structure = structure
        builder.epw['code'] = code
        builder.epw['parameters'] = orm.Dict(parameters)
        builder.epw['metadata'] = metadata

        if w90_chk_to_ukk_script:
            type_check(w90_chk_to_ukk_script, orm.RemoteData)
            builder.w90_chk_to_ukk_script = w90_chk_to_ukk_script

        if 'settings' in inputs['epw']:
            builder.epw['settings'] = orm.Dict(inputs['epw']['settings'])
        if 'parallelization' in inputs['epw']:
            builder.epw['parallelization'] = orm.Dict(inputs['epw']['parallelization'])

        builder.clean_workdir = orm.Bool(inputs['clean_workdir'])

        builder.qfpoints_distance = orm.Float(inputs['qfpoints_distance'])
        builder.kfpoints_factor = orm.Int(inputs['kfpoints_factor'])
        builder.max_iterations = orm.Int(inputs['max_iterations'])
        # pylint: enable=no-member

        return builder

    def setup(self):
        """Call the ``setup`` of the ``BaseRestartWorkChain`` and create the inputs dictionary in ``self.ctx.inputs``.
        This ``self.ctx.inputs`` dictionary will be used by the ``EpwCalculation`` in the internal loop.
        """
        super().setup()
        self.ctx.inputs = AttributeDict(
            self.exposed_inputs(
                EpwCalculation, namespace='epw')
                )

    # TODO: Should move this validation to EpwCalculation.
    def validate_parent_folders(self):
        """Validate the parent folders for different possible cases.
        """
        if sum(
            _ in self.inputs
            for _ in ["parent_folder_chk", 'w90_chk_to_ukk_script']
        ) == 1:
            self.report("If you specify the `parent_folder_chk`. You must also specify the `w90_chk_to_ukk_script` for the conversion.")
            return self.exit_codes.ERROR_PARENT_FOLDER_NOT_VALID

        if 'parent_folder_epw' in self.inputs:
            if any([
                _ in self.inputs
                for _ in ["parent_folder_nscf", "parent_folder_ph", "parent_folder_chk"]
            ]):
                self.report("You cannot provide the `parent_folder_epw` at the same time as any of the `parent_folder_nscf`, `parent_folder_ph`, `parent_folder_chk`.")
                return self.exit_codes.ERROR_PARENT_FOLDER_NOT_VALID

            self.ctx.inputs.parent_folder_epw = self.inputs.parent_folder_epw

        parent_folders_exist = [
            parent_folder in self.inputs for parent_folder in [
                'parent_folder_nscf', 'parent_folder_ph', 'parent_folder_chk'
            ]]

        if sum(parent_folders_exist) in [1, 2]:
            self.report(
                "You must speficy either all of the `parent_folder_nscf`, `parent_folder_ph`, `parent_folder_chk` or none of them."
                )
            return self.exit_codes.ERROR_PARENT_FOLDER_NOT_VALID

        elif sum(parent_folders_exist) == 3:
            self.ctx.inputs.parent_folder_nscf = self.inputs.parent_folder_nscf
            self.ctx.inputs.parent_folder_ph = self.inputs.parent_folder_ph

        for parent_folder, process_label in (
                ('parent_folder_nscf', 'PwCalculation'),
                ('parent_folder_ph', 'PhCalculation'),
                ('parent_folder_chk', 'Wannier90Calculation'),
                ('parent_folder_epw', 'EpwCalculation')
        ):
            # 1. check if 'parent_folder' is in inputs
            if parent_folder in self.inputs:
                # 2. check if the parent folder is cleaned
                if self.inputs[parent_folder].is_cleaned:
                    self.report(f"{parent_folder} is cleaned.")
                    return self.exit_codes.ERROR_PARENT_FOLDER_NOT_VALID
                # 3. check if the creator process_label is valid
                if self.inputs[parent_folder].creator.process_label != process_label:
                    self.report(f"{parent_folder} is created by a {self.inputs[parent_folder].creator.process_label}, not a {process_label}.")
                    return self.exit_codes.ERROR_PARENT_FOLDER_NOT_VALID
                    
        if 'w90_chk_to_ukk_script' in self.inputs:
            if not self.inputs['w90_chk_to_ukk_script'].computer.uuid == self.inputs.epw.code.computer.uuid:
                self.report("W90 chk to ukk script is not on the same computer as the epw calculation.")
                return self.exit_codes.ERROR_PARENT_FOLDER_NOT_VALID

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

        if 'parent_folder_epw' in self.inputs:
            epw_calc = self.inputs.parent_folder_epw.creator
            kpoints = epw_calc.inputs.kpoints
            qpoints = epw_calc.inputs.qpoints

        # If there is no parent folder of a previous EPW calculation, it must be the case that we are running the transition from coarse BLoch representation to Wannier representation.
        # This means that we are using coarse k grid from a previous nscf calculation and 
        else:
            if 'kpoints' in self.inputs:
                kpoints = self.inputs.kpoints
            elif 'parent_folder_chk' in self.inputs:
                kpoints = get_kpoints_from_chk_folder(self.inputs.parent_folder_chk)
            else:
                self.report("Could not determine the coarse k-points from the inputs or the parent folder of the wannier90 calculation.")
                return self.exit_codes.ERROR_COARSE_GRID_NOT_VALID
            
            if 'qpoints' in self.inputs:
                qpoints = self.inputs.qpoints
            elif 'parent_folder_ph' in self.inputs:
                qpoints = self.inputs.parent_folder_ph.creator.inputs.qpoints
            else:
                self.report("Could not determine the coarse q-points from the inputs or the parent folder of the ph calculation.")
                return self.exit_codes.ERROR_COARSE_GRID_NOT_VALID
            
        self.report(f"Successfully determined coarse k-points from the inputs: {kpoints.get_kpoints_mesh()[0]}")
        self.report(f"Successfully determined coarse q-points from the inputs: {qpoints.get_kpoints_mesh()[0]}")


        is_compatible, message = check_kpoints_qpoints_compatibility(kpoints, qpoints)
        
        self.ctx.inputs.kpoints = kpoints
        self.ctx.inputs.qpoints = qpoints
        
        if not is_compatible:
            self.report(message)
            return self.exit_codes.ERROR_COARSE_GRID_NOT_VALID

        if 'qfpoints' in self.inputs:
            qfpoints = self.inputs.qfpoints
        else:
            inputs = {
                'structure': self.inputs.structure,
                'distance': self.inputs.qfpoints_distance,
                'force_parity': self.inputs.get('qfpoints_force_parity', orm.Bool(False)),
                'metadata': {
                    'call_link_label': 'create_qfpoints_from_distance'
                }
            }
            qfpoints = create_kpoints_from_distance(**inputs)  # pylint: disable=unexpected-keyword-arg

        if 'kfpoints' in self.inputs:
            kfpoints = self.inputs.kfpoints
        else:
            qfpoints_mesh = qfpoints.get_kpoints_mesh()[0]
            kfpoints = orm.KpointsData()
            kfpoints.set_kpoints_mesh([v * self.inputs.kfpoints_factor.value for v in qfpoints_mesh])

        self.ctx.inputs.qfpoints = qfpoints
        self.ctx.inputs.kfpoints = kfpoints

    def prepare_process(self):
        """
        Prepare inputs for the next calculation.

        Currently, no modifications to `self.ctx.inputs` are needed before
        submission. We rely on the parent `run_process` to create the builder.
        """
        parameters = self.ctx.inputs.parameters.get_dict()
        nstemp = parameters['INPUTEPW'].get('nstemp', None)
        if nstemp and nstemp > self._MAX_NSTEMP:
            self.report(f'nstemp too large, reset it to maximum allowed: {self._MAX_NSTEMP}')
            parameters['INPUTEPW']['nstemp'] = self._MAX_NSTEMP

        wannierize = parameters['INPUTEPW'].get('wannierize', False)

        if wannierize and any(
            _ in self.inputs
            for _ in ["parent_folder_epw", "parent_folder_chk"]
        ):
            self.report("Should not have a parent folder of epw or chk if wannierize is True")
            return self.exit_codes.ERROR_PARAMETERS_NOT_VALID

        if 'parent_folder_chk' in self.inputs:
            w90_params = self.inputs.parent_folder_chk.creator.inputs.parameters.get_dict()
            exclude_bands = w90_params.get('exclude_bands', None) #TODO check this!

            if exclude_bands:
                parameters['INPUTEPW']['bands_skipped'] = f'exclude_bands = {exclude_bands[0]}:{exclude_bands[-1]}'

            parameters['INPUTEPW']['nbndsub'] = w90_params['num_wann']

            wannier_chk_path = Path(self.inputs.parent_folder_chk.get_remote_path(), f'{self._process_class._PREFIX}.chk')
            wannier_mmn_path = Path(self.inputs.parent_folder_chk.get_remote_path(), f'{self._process_class._PREFIX}.mmn')
            wannier_bvec_path = Path(self.inputs.parent_folder_chk.get_remote_path(), f'{self._process_class._PREFIX}.bvec')
            nscf_xml_path = Path(self.inputs.parent_folder_nscf.get_remote_path(), f'out/{self._process_class._PREFIX}.xml')

            prepend_text = self.ctx.inputs.metadata.options.get('prepend_text', '')
            prepend_text += f'\n{self.inputs.w90_chk_to_ukk_script.get_remote_path()} {wannier_chk_path} {nscf_xml_path} {self._process_class._PREFIX}.ukk {wannier_mmn_path} {self._process_class._PREFIX}.mmn'
            prepend_text += f'\ncp {wannier_bvec_path} {self._process_class._PREFIX}.bvec'

            self.ctx.inputs.metadata.options.prepend_text = prepend_text

        self.ctx.inputs.parameters = orm.Dict(parameters)

    def report_error_handled(self, calculation, action):
        """Report an action taken for a calculation that has failed.

        This should be called in a registered error handler if its condition is met and an action was taken.

        :param calculation: the failed calculation node
        :param action: a string message with the action taken
        """
        arguments = [calculation.process_label, calculation.pk, calculation.exit_status, calculation.exit_message]
        self.report('{}<{}> failed with exit status {}: {}'.format(*arguments))
        self.report(f'Action taken: {action}')

    # @process_handler(priority=590, exit_codes=EpwCalculation.exit_codes.ERROR_MEMORY_EXCEEDS_MAX_MEMLT)
    # def handle_memory_exceeds_max_memlt(self, calculation):
    #     """Handle calculations with an exit status below 400 which are unrecoverable, so abort the work chain."""

    #     parameters = calculation.inputs.epw.parameters.get_dict()
    #     parameters['INPUTEPW']['max_memlt'] = 2 * parameters['INPUTEPW']['max_memlt']
    #     self.ctx.inputs.parameters = orm.Dict(parameters)
    #     action = f'memory exceeds max_memlt, increasing max_memlt to {parameters["INPUTEPW"]["max_memlt"]} GB'

    #     self.report_error_handled(calculation, action)
    #     return ProcessHandlerReport(True)

    @process_handler(priority=600)
    def handle_unrecoverable_failure(self, calculation):
        """Handle calculations with an exit status below 400 which are unrecoverable, so abort the work chain."""
        if calculation.is_failed and calculation.exit_status < 400:
            self.report_error_handled(calculation, 'unrecoverable error, aborting...')
            return ProcessHandlerReport(True, self.exit_codes.ERROR_UNRECOVERABLE_FAILURE)

    # @process_handler(priority=590, exit_codes=[])
    # def handle_known_unrecoverable_failure(self, calculation):
    #     """Handle calculations with an exit status that correspond to a known failure mode that are unrecoverable.

    #     These failures may always be unrecoverable or at some point a handler may be devised.
    #     """
    #     self.report_error_handled(calculation, 'known unrecoverable failure detected, aborting...')
    #     return ProcessHandlerReport(True, self.exit_codes.ERROR_KNOWN_UNRECOVERABLE_FAILURE)

    # @process_handler(priority=413, exit_codes=[
    #     EpwCalculation.exit_codes.ERROR_CONVERGENCE_TC_LINEAR_NOT_REACHED,
    # ])
    # def handle_convergence_tc_linear_not_reached(self, calculation):
    #     """Handle `ERROR_CONVERGENCE_TC_LINEAR_NOT_REACHED`: consider finished."""
    #     self.ctx.is_finished = True
    #     action = 'Convergence (tc_linear) was not reached. But it is acceptable if Tc can be estimated.'
    #     self.report_error_handled(calculation, action)
    #     self.results()  # Call the results method to attach the output nodes
    #     return ProcessHandlerReport(True, self.exit_codes.ERROR_CONVERGENCE_TC_LINEAR_NOT_REACHED)
