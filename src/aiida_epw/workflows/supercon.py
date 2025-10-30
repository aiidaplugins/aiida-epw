"""Work chain for computing the critical temperature based on an `EpwWorkChain`."""
from aiida import orm
from aiida.common import AttributeDict
from aiida.engine import WorkChain, ToContext, while_, if_, append_

from aiida_quantumespresso.workflows.protocols.utils import ProtocolMixin

from aiida_epw.workflows.base import EpwBaseWorkChain
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
    """This workchain will run a series of `EpwBaseWorkChain`s in interpolation mode to converge 
    the Allen-Dynes Tc according to the interpolation distance, if converged or forced by `always_run_final`, 
    it will then run the final isotropic and anisotropic `EpwBaseWorkChain`s to compute the 
    critical temperature solving the isotropic and anisotropic Migdal-Eliashberg equations."""

    @classmethod
    def define(cls, spec):
        """Define the work chain specification."""
        super().define(spec)

        spec.input('structure', valid_type=orm.StructureData)
        spec.input('clean_workdir', valid_type=orm.Bool, default=lambda: orm.Bool(False))
        spec.input('parent_folder_epw', valid_type=(orm.RemoteData, orm.RemoteStashFolderData))
        spec.input('interpolation_distance', valid_type=(orm.Float, orm.List))
        spec.input('convergence_threshold', valid_type=orm.Float, required=False)
        spec.input('always_run_final', valid_type=orm.Bool, default=lambda: orm.Bool(False))

        # TODO: We can choose how we solve the Migdal-Eliashberg equations.
        # We can have another input port `do_fbw` such that if we want to do a full-bandwidth calculation, 
        # we can set it to true.

        # We can have another input port `do_ir` such that if we want to do solve it in the 
        # intermediate representation space, we can set it to true.

        spec.expose_inputs(
            EpwBaseWorkChain, namespace='epw_interp', exclude=(
                'clean_workdir', 'parent_folder_ph', 'parent_folder_nscf', 'parent_folder_chk', 'qfpoints', 'kfpoints'
            ),
            namespace_options={
                'help': 'Inputs for the interpolation `EpwBaseWorkChain`s.'
            }
        )
        spec.expose_inputs(
            EpwBaseWorkChain, namespace='epw_final_iso', exclude=(
                'clean_workdir', 'parent_folder_ph', 'parent_folder_nscf', 'parent_folder_chk', 'qfpoints_distance', 'kfpoints_factor'
            ),
            namespace_options={
                'help': 'Inputs for the final isotropic `EpwBaseWorkChain`.'
            }
        )
        spec.expose_inputs(
            EpwBaseWorkChain, namespace='epw_final_aniso', exclude=(
                'clean_workdir', 'parent_folder_ph', 'parent_folder_nscf', 'parent_folder_chk', 'qfpoints_distance', 'kfpoints_factor'
            ),
            namespace_options={
                'help': 'Inputs for the final anisotropic `EpwBaseWorkChain`.'
            }
        )
        spec.outline(
            cls.setup,
            while_(cls.should_run_conv)(
                cls.run_conv,
                cls.inspect_conv,
            ),
            if_(cls.should_run_final)(
                cls.run_final_epw_iso,
                cls.inspect_final_epw_iso,
                cls.run_final_epw_aniso,
                cls.inspect_final_epw_aniso,
            ),

            cls.results
        )
        spec.output('parameters', valid_type=orm.Dict,
                    help='The `output_parameters` output node of the final EPW calculation.')
        spec.output('max_eigenvalue', valid_type=orm.XyData,
                    help='The temperature dependence of the max eigenvalue for the final EPW.')
        spec.output('a2f', valid_type=orm.XyData,
                    help='The contents of the `.a2f` file for the final EPW.')
        spec.output('Tc_iso', valid_type=orm.Float,
                    help='The critical temperature.')

        spec.exit_code(401, 'ERROR_SUB_PROCESS_EPW_INTERP',
            message='The interpolation `EpwBaseWorkChain` sub process failed')
        spec.exit_code(402, 'ERROR_ALLEN_DYNES_NOT_CONVERGED',
            message='Allen-Dynes Tc is not converged.')
        spec.exit_code(403, 'ERROR_SUB_PROCESS_EPW_ISO',
            message='The isotropic `EpwBaseWorkChain` sub process failed')
        spec.exit_code(404, 'ERROR_SUB_PROCESS_EPW_ANISO',
            message='The anisotropic `EpwBaseWorkChain` sub process failed')

    @classmethod
    def get_protocol_filepath(cls):
        """Return ``pathlib.Path`` to the ``.yaml`` file that defines the protocols."""
        from importlib_resources import files
        from . import protocols
        return files(protocols) / 'supercon.yaml'

    @classmethod
    def get_builder_from_protocol(
            cls, 
            epw_code, 
            parent_epw, 
            protocol=None, 
            overrides=None, 
            scon_epw_code=None, 
            parent_folder_epw=None, 
            **kwargs
        ):
        """Return a builder prepopulated with inputs selected according to the chosen protocol.

        :TODO:
        """
        inputs = cls.get_protocol_inputs(protocol, overrides)

        builder = cls.get_builder()

        if parent_epw.process_label == 'EpwPrepWorkChain':
            epw_source = parent_epw.base.links.get_outgoing(link_label_filter='epw_base').first().node
        elif parent_epw.process_label == 'EpwBaseWorkChain':
            epw_source = parent_epw
        else:
            raise ValueError(f'Invalid parent_epw process: {parent_epw.process_label}')

        if parent_folder_epw is None:

            if epw_source.inputs.epw.code.computer.hostname != epw_code.computer.hostname:
                raise ValueError(
                    'The `epw_code` must be configured on the same computer as that where the `parent_epw` was run.'
                )
            parent_folder_epw = parent_epw.outputs.epw_folder
        else:
            # TODO: Add check to make sure parent_folder_epw is on same computer as epw_code
            pass

        for epw_namespace in ('epw_interp', 'epw_final_iso', 'epw_final_aniso'):

            epw_inputs = inputs.get(epw_namespace, None)

            epw_builder = EpwBaseWorkChain.get_builder_from_protocol(
                code=epw_code,
                structure=epw_source.inputs.structure,
                protocol=protocol,
                overrides=epw_inputs
            )

            if epw_namespace == 'epw_interp' and scon_epw_code is not None:
                epw_builder.code = scon_epw_code
            else:
                epw_builder.code = epw_code

            epw_builder.kpoints = epw_source.inputs.kpoints
            epw_builder.qpoints = epw_source.inputs.qpoints

            if 'settings' in epw_inputs:
                epw_builder.settings = orm.Dict(epw_inputs['settings'])

            builder[epw_namespace]= epw_builder

        if isinstance(inputs['interpolation_distance'], float):
            builder.interpolation_distance = orm.Float(inputs['interpolation_distance'])
        if isinstance(inputs['interpolation_distance'], list):
            # qpoints_distance = parent_epw.inputs.qpoints_distance
            # interpolation_distance = [v for v in inputs['interpolation_distance'] if v < qpoints_distance / 2]
            builder.interpolation_distance = orm.List(inputs['interpolation_distance'])

        builder.convergence_threshold = orm.Float(inputs['convergence_threshold'])
        builder.always_run_final = orm.Bool(inputs.get('always_run_final', False))
        builder.structure = parent_epw.inputs.structure
        builder.parent_folder_epw = parent_folder_epw
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
        self.ctx.iteration = 0
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
            # 
            if (
                len(self.ctx.interpolation_list) == 0 and 
                not self.ctx.is_converged and 
                self.inputs.always_run_final.value
                ):
                self.report(
                    'Allen-Dynes Tc is not converged, '
                    'but will run the subsequent isotropic and anisotropic workchains as required.'
                    )
        else:
            self.report('No `convergence_threshold` input was provided, convergence automatically achieved.')
            self.ctx.is_converged = True

        return len(self.ctx.interpolation_list) > 0 and not self.ctx.is_converged

    def run_conv(self):
        """Run the EpwBaseWorkChain in interpolation mode for the current interpolation distance."""
        
        self.ctx.iteration += 1

        inputs = AttributeDict(self.exposed_inputs(EpwBaseWorkChain, namespace='epw_interp'))

        inputs.parent_folder_epw = self.inputs.parent_folder_epw
        inputs.kfpoints_factor = self.inputs.epw_interp.kfpoints_factor
        inputs.qfpoints_distance = self.ctx.interpolation_list.pop()

        if self.ctx.degaussq:
            parameters = inputs.parameters.get_dict()
            parameters['INPUTEPW']['degaussq'] = self.ctx.degaussq
            inputs.parameters = orm.Dict(parameters)

        inputs.setdefault('metadata', {})['call_link_label'] = f'conv_{self.ctx.iteration:02d}'
        workchain_node  = self.submit(EpwBaseWorkChain, **inputs)

        self.report(f'launching EpwBaseWorkChain<{workchain_node.pk}> in a2f mode: convergence #{self.ctx.iteration}')

        return ToContext(epw_interp=append_(workchain_node))

    def inspect_conv(self):
        """Verify that the EpwBaseWorkChain in interpolation mode finished successfully."""
        workchain = self.ctx.epw_interp[-1]

        if not workchain.is_finished_ok:
            self.report(f'EpwBaseWorkChain<{workchain.pk}> failed with exit status {workchain.exit_status}')
            self.ctx.epw_interp.pop()
            # return self.exit_codes.ERROR_SUB_PROCESS_EPW_INTERP
        else:
            # self.ctx.final_interp = workchain.inputs.qfpoints_distance
            try:
                self.report(f"Allen-Dynes: {workchain.outputs.output_parameters['Allen_Dynes_Tc']}")
            except KeyError:
                self.report(f"Could not find Allen-Dynes temperature in parsed output parameters!")

            if self.ctx.degaussq is None:
                frequency = workchain.outputs.a2f.get_array('frequency')
                self.ctx.degaussq = frequency[-1] / 100

    def should_run_final(self):
        """Check if the final EpwBaseWorkChain should be run."""
        # if not self.inputs.always_run_final and 'convergence_threshold' in self.inputs:
        #     return self.ctx.is_converged
        # if self.ctx.final_interp is None:
        #     return False
        if self.ctx.is_converged or self.inputs.always_run_final.value:
            return True
        else:
            self.report(f'Allen-Dynes Tc is not converged.')
            return self.exit_codes.ERROR_ALLEN_DYNES_NOT_CONVERGED

    def run_final_epw_iso(self):
        """Run the final EpwBaseWorkChain in isotropic mode."""
        inputs = AttributeDict(self.exposed_inputs(EpwBaseWorkChain, namespace='epw_final_iso'))

        parent_folder_epw = self.ctx.epw_interp[-1].outputs.remote_folder
        inputs.parent_folder_epw = parent_folder_epw
        inputs.kfpoints = parent_folder_epw.creator.inputs.kfpoints
        inputs.qfpoints = parent_folder_epw.creator.inputs.qfpoints

        if self.ctx.degaussq:
            parameters = inputs.parameters.get_dict()
            parameters['INPUTEPW']['degaussq'] = self.ctx.degaussq
            inputs.parameters = orm.Dict(parameters)

        inputs.metadata.call_link_label = 'epw_final_iso'

        workchain_node = self.submit(EpwBaseWorkChain, **inputs)
        self.report(f'launching EpwBaseWorkChain<{workchain_node.pk}> in isotropic mode')

        return ToContext(final_epw_iso=workchain_node)

    def inspect_final_epw_iso(self):
        """Verify that the final EpwBaseWorkChain in isotropic mode finished successfully."""
        workchain = self.ctx.final_epw_iso

        if not workchain.is_finished_ok:
            self.report(f'EpwBaseWorkChain<{workchain.pk}> failed with exit status {workchain.exit_status}')
            return self.exit_codes.ERROR_SUB_PROCESS_EPW_ISO

    def run_final_epw_aniso(self):
        """Run the EpwBaseWorkChain in anisotropic mode for the current interpolation distance."""
        inputs = AttributeDict(self.exposed_inputs(EpwBaseWorkChain, namespace='epw_final_aniso'))

        parent_folder_epw = self.ctx.epw_interp[-1].outputs.remote_folder
        inputs.parent_folder_epw = parent_folder_epw
        inputs.kfpoints = parent_folder_epw.creator.inputs.kfpoints
        inputs.qfpoints = parent_folder_epw.creator.inputs.qfpoints

        inputs.metadata.call_link_label = 'epw_final_aniso'
        workchain_node = self.submit(EpwBaseWorkChain, **inputs)
        self.report(f'launching EpwBaseWorkChain<{workchain_node.pk}> in anisotropic mode')

        return ToContext(final_epw_aniso=workchain_node)    

    def inspect_final_epw_aniso(self):
        """Verify that the final EpwBaseWorkChain in anisotropic mode finished successfully."""
        workchain = self.ctx.final_epw_aniso

        if not workchain.is_finished_ok:
            self.report(f'EpwBaseWorkChain<{workchain.pk}> failed with exit status {workchain.exit_status}')
            return self.exit_codes.ERROR_SUB_PROCESS_EPW_ANISO

    def results(self):
        """TODO"""
        self.out('Tc_iso', calculate_tc(self.ctx.final_epw_iso.outputs.max_eigenvalue))
        self.out('parameters', self.ctx.final_epw_iso.outputs.output_parameters)
        self.out('max_eigenvalue', self.ctx.final_epw_iso.outputs.max_eigenvalue)
        self.out('a2f', self.ctx.final_epw_iso.outputs.a2f)

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

