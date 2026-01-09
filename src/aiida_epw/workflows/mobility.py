"""Work chain for computing the critical temperature based on an `EpwWorkChain`."""

from scipy.interpolate import interp1d

from aiida import orm
from aiida.common import AttributeDict
from aiida.engine import WorkChain, ToContext, while_, if_, append_

from aiida_quantumespresso.workflows.protocols.utils import ProtocolMixin

from aiida_epw.workflows.base import EpwBaseWorkChain

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
    return {f"el_{no}": orm.Float(el) for no, el in enumerate(list_node.get_list())}


class MobilityWorkChain(ProtocolMixin, WorkChain):
    """
    Workchain to compute carrier mobility by converging results with respect to EPW interpolation.

    This workchain runs a series of `EpwBaseWorkChain` calculations to converge the
    carrier mobility with respect to the fine k-point and q-point mesh density used for
    electron-phonon interpolation. Once converged, it runs a final `EpwBaseWorkChain`
    to obtain the final mobility values.
    """

    @classmethod
    def define(cls, spec):
        """Define the work chain specification."""
        super().define(spec)

        spec.input("structure", valid_type=orm.StructureData)
        spec.input(
            "clean_workdir", valid_type=orm.Bool, default=lambda: orm.Bool(False)
        )
        spec.input(
            "parent_folder_epw", valid_type=(orm.RemoteData, orm.RemoteStashFolderData)
        )
        spec.input("interpolation_distance", valid_type=(orm.Float, orm.List))
        spec.input("convergence_threshold", valid_type=orm.Float, required=False)
        spec.input(
            "always_run_final", valid_type=orm.Bool, default=lambda: orm.Bool(False)
        )
        spec.input(
            "quadruple_dir", valid_type=orm.Str, # orm.RemoteData, orm.RemoteStashFolderData)
             required=False, 
             help="Absolute path to the quadrupole.fmt file."
            )
        
      #  spec.input(
      #      "quadruple",
      #      valid_type=orm.XyData,
      #      required=False,
      #      help="The contents of the `quadruple.mft` file for the e-ph interpolation.",
      #  )

      #  spec.expose_inputs(
      #      EpwBaseWorkChain,
      #      namespace="epw_interp",
      #      exclude=(
      #          "clean_workdir",
      #          "parent_folder_ph",
      #          "parent_folder_nscf",
      #          "parent_folder_chk",
      #          "qfpoints",
      #          "kfpoints",
      #      ),
      #      namespace_options={
      #          "help": "Inputs for the interpolation `EpwBaseWorkChain`s."
      #      },
      #  )

        spec.expose_inputs(
            EpwBaseWorkChain,
            namespace="epw_mobility",
            exclude=(
                "clean_workdir",
                "parent_folder_ph",
                "parent_folder_nscf",
                "parent_folder_chk",

            ),
            namespace_options={
                "help": "Inputs for the carrier mobility `EpwBaseWorkChain`."
            },
        )

        spec.outline(
            cls.setup,
            #cls.generate_reciprocal_points,
            while_(cls.should_run_conv_test)(
                cls.run_conv,
                cls.inspect_conv,
            ),
            cls.results,
        )
        spec.output(
            "parameters",
            valid_type=orm.Dict,
            help="The `output_parameters` output node of the final EPW calculation.",
        )
        #spec.output(
        #    "carrier_mobility",
        #    valid_type=orm.XyData,
        #    help="The carrier mobility of both iBTE and SERTA calculations from EPW.",
        #)

        spec.output(
            "carrier_mobility",
            valid_type=orm.Dict,
            help="The carrier mobility of both iterative iBTE and SERTA calculations from EPW.",
        )

        spec.exit_code(
            401,
            "ERROR_SUB_PROCESS_EPW_INTERP",
            message="The interpolation `EpwBaseWorkChain` sub process failed",
        )
        spec.exit_code(
            402,
            "ERROR_MOBILITY_NOT_CONVERGED",
            message="Carrier mobility is not converged.",
        )
        spec.exit_code(
            403,
            "ERROR_NOT_SUFFICIENT_SPACE",
            message="The space for running the calculation is not sufficient.",
        )
        spec.exit_code(
            404,
            "ERROR_EPW_INTERPOLATION",
            message="The interpolated bands or e-ph matrix from epw is not available.",
        )

    @classmethod
    def get_protocol_filepath(cls):
        """Return ``pathlib.Path`` to the ``.yaml`` file that defines the protocols."""
        from importlib_resources import files
        from . import protocols

        return files(protocols) / "mobility.yaml"

    @classmethod
    def get_builder_from_protocol(
        cls,
        epw_code,
        parent_epw,
        protocol=None,
        overrides=None,
        scon_epw_code=None,
        parent_folder_epw=None,
        **kwargs,
    ):
        """Return a builder prepopulated with inputs selected according to the chosen protocol.

        :TODO:
        """
        inputs = cls.get_protocol_inputs(protocol, overrides)

        builder = cls.get_builder()

        if parent_epw.process_label == "EpwPrepWorkChain":
            epw_source = (
                parent_epw.base.links.get_outgoing(link_label_filter="epw_base")
                .first()
                .node
            )
        elif parent_epw.process_label == "EpwCalculation":
            epw_source = parent_epw
        elif parent_epw.process_label == "EpwBaseWorkChain":
            epw_source = parent_epw
        else:
            raise ValueError(f"Invalid parent_epw process: {parent_epw.process_label}")

        if parent_folder_epw is None:
            if (
                epw_source.inputs.epw.code.computer.hostname
                != epw_code.computer.hostname
            ):
                raise ValueError(
                    "The `epw_code` must be configured on the same computer as that where the `parent_epw` was run."
                )
            parent_folder_epw = parent_epw.outputs.epw_folder
        else:
            # TODO: Add check to make sure parent_folder_epw is on same computer as epw_code
            pass

        # for epw_namespace in ("epw_mobility","epw_interpo"):
        epw_namespace = "epw_mobility"
        if epw_namespace == "epw_mobility":
            epw_inputs = inputs.get(epw_namespace, None)

            epw_builder = EpwBaseWorkChain.get_builder_from_protocol(
                code=epw_code,
                structure=epw_source.caller.caller.inputs.structure,
                protocol=protocol,
                overrides=epw_inputs,
            )


            epw_builder.code = epw_code

            epw_builder.kpoints = epw_source.inputs.kpoints
            epw_builder.qpoints = epw_source.inputs.qpoints
            if epw_inputs and "settings" in epw_inputs:
                epw_builder.settings = orm.Dict(epw_inputs["settings"])
            # if "settings" in epw_inputs:
            #     epw_builder.settings = orm.Dict(epw_inputs["settings"])

            builder[epw_namespace] = epw_builder

        if isinstance(inputs["interpolation_distance"], float):
            builder.interpolation_distance = orm.Float(inputs["interpolation_distance"])
        if isinstance(inputs["interpolation_distance"], list):
            builder.interpolation_distance = orm.List(inputs["interpolation_distance"])

        builder.convergence_threshold = orm.Float(inputs["convergence_threshold"])
        builder.structure = epw_source.caller.caller.inputs.structure
        builder.parent_folder_epw = parent_folder_epw
        # builder.qpoints_distance = orm.Float(inputs["qpoints_distance"])
        # builder.kpoints_distance_scf = orm.Float(inputs["kpoints_distance_scf"])
        # builder.kpoints_factor_nscf = orm.Int(inputs["kpoints_factor_nscf"])
        builder.clean_workdir = orm.Bool(inputs["clean_workdir"])

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


    def setup(self):
        """Setup steps, i.e. initialise context variables."""
        intp = self.inputs.get("interpolation_distance")
        if isinstance(intp, orm.List):
            self.ctx.interpolation_list = list(split_list(intp).values())
        else:
            self.ctx.interpolation_list = [intp]

        self.ctx.interpolation_list.sort()
        self.ctx.iteration = 0
        self.ctx.final_interp = None
        self.ctx.mobility_values = []
        self.ctx.is_converged = False
        self.ctx.degaussq = None

    def should_run_conv_test(self):
        """Return True if there are still distances left in the list to test."""
        # 只要列表不为空，就返回 True，继续进入 run_conv
        return len(self.ctx.interpolation_list) > 0


    def should_run_conv(self):
        """Check if the convergence loop should continue or not."""
        if "convergence_threshold" in self.inputs:
            try:
                # Need at least 2 runs to check for convergence
                if len(self.ctx.epw_interp) < 2:
                    return True

                prev_mobility = self.ctx.epw_interp[-2].outputs.output_parameters.get_dict().get("mobility_iBTE")
                new_mobility = self.ctx.epw_interp[-1].outputs.output_parameters.get_dict().get("mobility_iBTE")

                if prev_mobility is None or new_mobility is None:
                    self.report("Could not retrieve mobility from one of the previous calculations.")
                    return True # Continue running, maybe it will appear in the next one

                # Check for convergence, assuming mobility is a dictionary with values to compare.
                # This part might need adjustment depending on the exact structure of `mobility_iBTE`.
                # For simplicity, let's assume it's a single value for now.
                is_converged = abs(prev_mobility - new_mobility) < self.inputs.convergence_threshold.value
                self.ctx.is_converged = is_converged
                self.report(f"Checking convergence: old={prev_mobility:.4f}, new={new_mobility:.4f} -> Converged: {is_converged}")

            except (AttributeError, IndexError, KeyError) as e:
                self.report(f"Not enough data to check convergence, continuing. Error: {e}")

            if not self.ctx.is_converged and not self.ctx.interpolation_list:
                 # Ran out of distances to try
                if self.inputs.always_run_final:
                    self.report("Mobility not converged, but running final calculation as requested.")
                else:
                    return self.exit_codes.ERROR_MOBILITY_NOT_CONVERGED

        else:
            self.report("No `convergence_threshold` input provided, convergence is assumed.")
            self.ctx.is_converged = True

        return not self.ctx.is_converged and self.ctx.interpolation_list


    def run_conv(self):
        """Run the EpwBaseWorkChain in interpolation mode for the current interpolation distance."""
        self.ctx.iteration += 1

        inputs = AttributeDict(self.exposed_inputs(EpwBaseWorkChain, namespace="epw_mobility"))
        inputs.parent_folder_epw = self.inputs.parent_folder_epw
        inputs.qfpoints_distance = self.ctx.interpolation_list.pop(0) # Take the smallest distance first

    # === 处理 Quadrupole 文件的软链接 (ln -s) ===
        # 假设你的 WorkChain 输入里有名为 'quadruple_dir' 的 Str 类型的绝对路径
        if 'quadruple_dir' in self.inputs:
            source_path = self.inputs.quadruple_dir.value
            
            # 获取 EPW 计算所用的计算机 UUID
            # 注意：EpwBaseWorkChain 通常把计算的 inputs 放在 'epw' 命名空间下
            # 确保 inputs.epw.code 存在。如果你的 structure 不一样，请调整这里获取 computer 的方式
            computer = inputs.epw.code.computer
            
            # 准备 symlink 配置: [(uuid, 远程绝对路径, 目标文件名)]
            symlink_item = (computer.uuid, source_path, 'quadrupole.fmt')
            
            # 确保字典路径存在，然后赋值
            if 'metadata' not in inputs.epw:
                inputs.epw.metadata = {}
            if 'options' not in inputs.epw.metadata:
                inputs.epw.metadata.options = {}
                
            # 设置 remote_symlink_list
            # 这会让 AiiDA 在计算开始前执行 ln -s /path/to/quadrupole.fmt ./quadrupole.fmt
            inputs.epw.metadata.options['remote_symlink_list'] = [symlink_item]
        # ===============================================

        if 'kfpoints_factor' in self.inputs:
            inputs.kfpoints_factor = self.inputs.kfpoints_factor
        # 选项 B: 或者是硬编码一个默认值 (比如 1.0 表示和粗网格一样，或者更高的倍数)
        else:
            inputs.kfpoints_factor = int(1.0)


        if self.ctx.degaussq:
            parameters = inputs.parameters.get_dict()
            parameters.setdefault("INPUTEPW", {})["degaussq"] = self.ctx.degaussq
            inputs.parameters = orm.Dict(parameters)

        inputs.metadata.call_link_label = f"conv_{self.ctx.iteration:02d}"
        running = self.submit(EpwBaseWorkChain, **inputs)
        self.report(f"Launched EpwBaseWorkChain<{running.pk}> for convergence run #{self.ctx.iteration}")

        return ToContext(epw_interp=append_(running))

    def inspect_conv(self):
        """Verify that the EpwBaseWorkChain in interpolation mode finished successfully."""
        workchain = self.ctx.epw_interp[-1]

        if not workchain.is_finished_ok:
            self.report(f"Convergence EpwBaseWorkChain<{workchain.pk}> failed with exit status {workchain.exit_status}")
            return self.exit_codes.ERROR_SUB_PROCESS_EPW_INTERP

        try:
            mobility = workchain.outputs.output_parameters.get_dict().get("mobility_iBTE", "N/A")
            self.report(f"Convergence run #{self.ctx.iteration} finished with mobility: {mobility}")
            self.ctx.mobility_values.append(mobility)

            # Set degaussq from the first successful run if not already set
            if self.ctx.degaussq is None and 'a2f' in workchain.outputs:
                frequency = workchain.outputs.a2f.get_array("frequency")
                self.ctx.degaussq = frequency[-1] / 100
                self.report(f"Set degaussq for subsequent runs to {self.ctx.degaussq}")

        except KeyError:
            self.report("Could not find 'mobility_iBTE' in the output parameters of the convergence run.")


    def should_run_final(self):
        """Check if the final EpwBaseWorkChain should be run."""
        return self.ctx.is_converged or self.inputs.always_run_final.value

    def run_final_epw_mobility(self):
        """Run the final EpwBaseWorkChain in mobility mode."""
        inputs = AttributeDict(self.exposed_inputs(EpwBaseWorkChain, namespace="epw_mobility"))

        # Use the remote folder from the last successful convergence run as the parent
        parent_folder_epw = self.ctx.epw_interp[-1].outputs.remote_folder
        inputs.parent_folder_epw = parent_folder_epw
        inputs.kfpoints = parent_folder_epw.creator.inputs.kfpoints
        inputs.qfpoints = parent_folder_epw.creator.inputs.qfpoints

        if self.ctx.degaussq:
            parameters = inputs.parameters.get_dict()
            parameters.setdefault("INPUTEPW", {})["degaussq"] = self.ctx.degaussq
            inputs.parameters = orm.Dict(parameters)

        inputs.metadata.call_link_label = "final_mobility_run"
        running = self.submit(EpwBaseWorkChain, **inputs)
        self.report(f"Launched final EpwBaseWorkChain<{running.pk}> for mobility calculation.")

        return ToContext(final_epw_mobility=running)


    def inspect_final_epw_mobility(self):
        """Verify that the final EpwBaseWorkChain for mobility finished successfully."""
        workchain = self.ctx.final_epw_mobility

        if not workchain.is_finished_ok:
            self.report(f"Final mobility EpwBaseWorkChain<{workchain.pk}> failed with exit status {workchain.exit_status}")
            return self.exit_codes.ERROR_EPW_INTERPOLATION


    def results(self):
        """Attach the final results to the outputs."""
        self.report("Workchain finished successfully, attaching final outputs.")
#        final_results = self.ctx.final_epw_mobility.outputs.output_parameters
 #       self.out("parameters", final_results)
        # The mobility data is expected to be within the main output parameters Dict
  #      self.out("carrier_mobility", final_results)

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
