# -*- coding: utf-8 -*-
"""Work chain for computing the critical temperature based off an `EpwWorkChain`."""
from aiida import orm
from aiida.common import AttributeDict
from aiida.engine import WorkChain, ToContext, while_, if_, append_

from aiida_quantumespresso.workflows.protocols.utils import ProtocolMixin

from aiida_quantumespresso.calculations.epw import EpwCalculation
from aiida_quantumespresso.calculations.functions.create_kpoints_from_distance import create_kpoints_from_distance

from aiida.engine import calcfunction


@calcfunction
def stash_to_remote(stash_data: orm.RemoteStashFolderData) -> orm.RemoteData:
    """Convert a ``RemoteStashFolderData`` into a ``RemoteData``."""

    if stash_data.get_attribute("stash_mode") != "copy":
        raise NotImplementedError("Only the `copy` stash mode is supported.")

    remote_data = orm.RemoteData()
    remote_data.set_attribute(
        "remote_path", stash_data.get_attribute("target_basepath")
    )
    remote_data.computer = stash_data.computer

    return remote_data


@calcfunction
def split_list(list_node: orm.List) -> dict:
    return {f'el_{no}': orm.Float(el) for no, el in enumerate(list_node.get_list())}

from scipy.interpolate import interp1d


@calcfunction
def calculate_tc(max_eigenvalue: orm.XyData) -> orm.Float:
    me_array = max_eigenvalue.get_array('max_eigenvalue')
    try:
        return orm.Float(float(interp1d(me_array[:, 1], me_array[:, 0])(1.0)))
    except ValueError:
        return orm.Float(40.0)


class SuperConWorkChain(ProtocolMixin, WorkChain):
    """Work chain to compute the electron-phonon coupling."""

    @classmethod
    def define(cls, spec):
        """Define the work chain specification."""
        super().define(spec)

        spec.input('structure', valid_type=orm.StructureData)
        spec.input('clean_workdir', valid_type=orm.Bool, default=lambda: orm.Bool(False))
        spec.input('epw_folder', valid_type=(orm.RemoteData, orm.RemoteStashFolderData))
        spec.input('interpolation_distance', valid_type=(orm.Float, orm.List))
        spec.input('convergence_threshold', valid_type=orm.Float, required=False)
        spec.input('always_run_final', valid_type=orm.Bool, default=lambda: orm.Bool(False))

        spec.expose_inputs(
            EpwCalculation, namespace='epw_interp', exclude=(
                'parent_folder_ph', 'parent_folder_nscf', 'kfpoints', 'qfpoints'
            ),
            namespace_options={
                'help': 'Inputs for the interpolation `EpwCalculation`s.'
            }
        )
        spec.expose_inputs(
            EpwCalculation, namespace='epw_final', exclude=(
                'parent_folder_ph', 'parent_folder_nscf', 'kfpoints', 'qfpoints'
            ),
            namespace_options={
                'help': 'Inputs for the final `EpwCalculation`.'
            }
        )
        spec.outline(
            cls.setup,
            while_(cls.should_run_conv)(
                cls.generate_reciprocal_points,
                cls.interp_epw,
                cls.inspect_epw,
            ),
            if_(cls.should_run_final)(
                cls.final_epw,
            ),
            cls.results
        )
        spec.output('parameters', valid_type=orm.Dict,
                    help='The `output_parameters` output node of the final EPW calculation.')
        spec.output('max_eigenvalue', valid_type=orm.XyData,
                    help='The temperature dependence of the max eigenvalue for the final EPW.')
        spec.output('a2f', valid_type=orm.XyData,
                    help='The contents of the `.a2f` file for the final EPW.')
        spec.output('Tc', valid_type=orm.Float,
                    help='The isotropic linearised Eliashberg Tc interpolated from the max eigenvalue curve.')

        spec.exit_code(401, 'ERROR_SUB_PROCESS_EPW_INTERP',
            message='The interpolation `epw.x` sub process failed')

    @classmethod
    def get_protocol_filepath(cls):
        """Return ``pathlib.Path`` to the ``.yaml`` file that defines the protocols."""
        from importlib_resources import files
        from . import protocols
        return files(protocols) / 'supercon.yaml'

    @classmethod
    def get_builder_from_protocol(
            cls, epw_code, parent_epw, protocol=None, overrides=None, scon_epw_code=None, epw_folder=None, **kwargs
        ):
        """Return a builder prepopulated with inputs selected according to the chosen protocol.

        :TODO:
        """
        inputs = cls.get_protocol_inputs(protocol, overrides)

        builder = cls.get_builder()

        epw_source = parent_epw.base.links.get_outgoing(link_label_filter='epw').first().node

        if epw_folder is None:

            if epw_source.inputs.code.computer.hostname != epw_code.computer.hostname:
                raise ValueError(
                    'The `epw_code` must be configured on the same computer as that where the `parent_epw` was run.'
                )
            epw_folder = parent_epw.outputs.epw_folder
        else:
            # TODO: Add check to make sure epw_folder is on same computer as epw_code
            pass

        for epw_namespace in ('epw_interp', 'epw_final'):

            epw_inputs = inputs.get(epw_namespace, None)

            parameters = epw_inputs['parameters']
            parameters["INPUTEPW"]["use_ws"] = epw_source.inputs.parameters["INPUTEPW"].get("use_ws", False)
            parameters['INPUTEPW']['nbndsub'] = epw_source.inputs.parameters['INPUTEPW']['nbndsub']
            if 'bands_skipped' in epw_source.inputs.parameters['INPUTEPW']:
                parameters['INPUTEPW']['bands_skipped'] = epw_source.inputs.parameters['INPUTEPW'].get('bands_skipped')

            epw_builder = EpwCalculation.get_builder()

            if epw_namespace == 'epw_interp' and scon_epw_code is not None:
                epw_builder.code = scon_epw_code
            else:
                epw_builder.code = epw_code

            epw_builder.kpoints = epw_source.inputs.kpoints
            epw_builder.qpoints = epw_source.inputs.qpoints

            epw_builder.parameters = orm.Dict(parameters)
            epw_builder.metadata = epw_inputs['metadata']
            if 'settings' in epw_inputs:
                epw_builder.settings = orm.Dict(epw_inputs['settings'])

            builder[epw_namespace]= epw_builder

        if isinstance(inputs['interpolation_distance'], float):
            builder.interpolation_distance = orm.Float(inputs['interpolation_distance'])
        if isinstance(inputs['interpolation_distance'], list):
            qpoints_distance = parent_epw.inputs.qpoints_distance
            interpolation_distance = [v for v in inputs['interpolation_distance'] if v < qpoints_distance / 2]
            builder.interpolation_distance = orm.List(interpolation_distance)

        builder.convergence_threshold = orm.Float(inputs['convergence_threshold'])
        builder.always_run_final = orm.Bool(inputs.get('always_run_final', False))
        builder.structure = parent_epw.inputs.structure
        builder.epw_folder = epw_folder
        # builder.epw_folder = epw_source.outputs.remote_folder
        builder.clean_workdir = orm.Bool(inputs['clean_workdir'])

        return builder

    def setup(self):
        """Setup steps, i.e. initialise context variables."""
        intp = self.inputs.get('interpolation_distance')
        if isinstance(intp, orm.List):
            self.ctx.interpolation_list = list(split_list(intp).values())
        else:
            self.ctx.interpolation_list = [intp]

        self.ctx.interpolation_list.sort()
        self.ctx.final_interp = None
        self.ctx.allen_dynes_values = []
        self.ctx.is_converged = False
        self.ctx.degaussq = None

    def should_run_conv(self):
        """Check if the convergence loop should continue or not."""
        if 'convergence_threshold' in self.inputs:
            try:
                self.ctx.epw_interp[-3].outputs.output_parameters['allen_dynes']  # This is to check that we have at least 3 allen-dynes
                prev_allen_dynes = self.ctx.epw_interp[-2].outputs.output_parameters['allen_dynes']
                new_allen_dynes = self.ctx.epw_interp[-1].outputs.output_parameters['allen_dynes']
                self.ctx.is_converged = (
                    abs(prev_allen_dynes - new_allen_dynes)
                    < self.inputs.convergence_threshold
                )
                self.report(f'Checking convergence: old {prev_allen_dynes}; new {new_allen_dynes} -> Converged = {self.ctx.is_converged.value}')
            except (AttributeError, IndexError, KeyError):
                self.report('Not enough data to check convergence.')

        else:
            self.report('No `convergence_threshold` input was provided, convergence automatically achieved.')
            self.ctx.is_converged = True

        return len(self.ctx.interpolation_list) > 0 and not self.ctx.is_converged

    def generate_reciprocal_points(self):
        """Generate the qpoints and kpoints meshes for the interpolation."""

        inputs = {
            'structure': self.inputs.structure,
            'distance': self.ctx.interpolation_list.pop(),
            'force_parity': orm.Bool(False),
            'metadata': {
                'call_link_label': 'create_kpoints_from_distance'
            }
        }
        inter_points = create_kpoints_from_distance(**inputs)  # pylint: disable=unexpected-keyword-arg

        self.ctx.inter_points = inter_points

    def interp_epw(self):
        """Run the ``restart`` EPW calculation for the current interpolation distance."""
        inputs = AttributeDict(self.exposed_inputs(EpwCalculation, namespace='epw_interp'))

        inputs.parent_folder_epw = self.inputs.epw_folder
        inputs.kfpoints = self.ctx.inter_points
        inputs.qfpoints = self.ctx.inter_points

        try:
            settings = inputs.settings.get_dict()
        except AttributeError:
            settings = {}

        settings['ADDITIONAL_RETRIEVE_LIST'] = ['aiida.a2f']
        inputs.settings = orm.Dict(settings)

        if self.ctx.degaussq:
            parameters = inputs.parameters.get_dict()
            parameters['INPUTEPW']['degaussq'] = self.ctx.degaussq
            inputs.parameters = orm.Dict(parameters)

        inputs.metadata.call_link_label = 'epw_interp'
        calcjob_node = self.submit(EpwCalculation, **inputs)
        mesh = 'x'.join(str(i) for i in self.ctx.inter_points.get_kpoints_mesh()[0])
        self.report(f'launching interpolation `epw` with PK {calcjob_node.pk} and interpolation mesh {mesh}')

        return ToContext(epw_interp=append_(calcjob_node))

    def inspect_epw(self):
        """Verify that the epw.x workflow finished successfully."""
        epw_calculation = self.ctx.epw_interp[-1]

        if not epw_calculation.is_finished_ok:
            self.report(f'`epw.x` failed with exit status {epw_calculation.exit_status}')
            self.ctx.epw_interp.pop()
            # return self.exit_codes.ERROR_SUB_PROCESS_EPW_INTERP
        else:
            self.ctx.final_interp = self.ctx.inter_points
            try:
                self.report(f"Allen-Dynes: {epw_calculation.outputs.output_parameters['allen_dynes']}")
            except KeyError:
                self.report(f"Could not find Allen-Dynes temperature in parsed output parameters!")

            if self.ctx.degaussq is None:
                frequency = epw_calculation.outputs.a2f.get_array('frequency')
                self.ctx.degaussq = frequency[-1] / 100

    def should_run_final(self):
        """Check if the final ``epw.x`` calculation should be run."""
        # if not self.inputs.always_run_final and 'convergence_threshold' in self.inputs:
        #     return self.ctx.is_converged
        if self.ctx.final_interp is None:
            return False

        return True

    def final_epw(self):
        """Run the final ``epw.x`` calculation."""
        inputs = AttributeDict(self.exposed_inputs(EpwCalculation, namespace='epw_final'))

        inputs.parent_folder_epw = self.ctx.epw_interp[-1].outputs.remote_folder
        inputs.kfpoints = self.ctx.final_interp
        inputs.qfpoints = self.ctx.final_interp

        try:
            settings = inputs.settings.get_dict()
        except AttributeError:
            settings = {}

        settings['ADDITIONAL_RETRIEVE_LIST'] = ['aiida.a2f']
        inputs.settings = orm.Dict(settings)

        if self.ctx.degaussq:
            parameters = inputs.parameters.get_dict()
            parameters['INPUTEPW']['degaussq'] = self.ctx.degaussq
            inputs.parameters = orm.Dict(parameters)

        inputs.metadata.call_link_label = 'epw_final'

        calcjob_node = self.submit(EpwCalculation, **inputs)
        self.report(f'launching final `epw` {calcjob_node.pk}')

        return ToContext(final_epw=calcjob_node)

    def inspect_final_epw(self):
        """Verify that the final epw.x workflow finished successfully."""
        epw_calculation = self.ctx.final_epw

        if not epw_calculation.is_finished_ok:
            self.report(f'Final `epw.x` failed with exit status {epw_calculation.exit_status}')
            return self.exit_codes.ERROR_SUB_PROCESS_EPW_INTERP

    def results(self):
        """TODO"""
        self.out('Tc', calculate_tc(self.ctx.final_epw.outputs.max_eigenvalue))
        self.out('parameters', self.ctx.final_epw.outputs.output_parameters)
        self.out('max_eigenvalue', self.ctx.final_epw.outputs.max_eigenvalue)
        self.out('a2f', self.ctx.final_epw.outputs.a2f)

    def on_terminated(self):
        """Clean the working directories of all child calculations if `clean_workdir=True` in the inputs."""
        super().on_terminated()

        if self.inputs.clean_workdir.value is False:
            self.report('remote folders will not be cleaned')
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
            self.report(f"cleaned remote folders of calculations: {' '.join(map(str, cleaned_calcs))}")

