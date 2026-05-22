"""Work chain for computing carrier mobility with convergence control."""

from aiida import orm
from aiida.engine import WorkChain, while_, append_, calcfunction

from aiida_quantumespresso.workflows.protocols.utils import ProtocolMixin

from aiida_epw.tools.band_analysis import extract_band_edges_from_epw_prep
from aiida_epw.workflows.base import EpwBaseWorkChain


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


@calcfunction
def create_mobility_output(
    output_parameters: orm.Dict,
    convergence_history: orm.List,
    is_converged: orm.Bool,
) -> orm.Dict:
    """Create the carrier mobility output Dict with proper provenance.

    This calcfunction is used to create the carrier_mobility output node,
    ensuring proper data provenance in the workflow.
    """
    output_params = output_parameters.get_dict()
    mobility_data = {
        "mobility_iBTE": output_params.get("mobility_iBTE"),
        "mobility_SERTA": output_params.get("mobility_SERTA"),
        "convergence_history": convergence_history.get_list(),
        "is_converged": is_converged.value,
    }
    return orm.Dict(mobility_data)


class MobilityWorkChain(ProtocolMixin, WorkChain):
    """
    Workchain to compute carrier mobility by converging results with respect to EPW interpolation.

    This workchain runs a series of `EpwBaseWorkChain` calculations to converge the
    carrier mobility with respect to the fine k-point and q-point mesh density used for
    electron-phonon interpolation. It can reuse externally provided quadrupole data but
    does not generate it internally.
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
        spec.input(
            "parent_epw_prep_pk",
            valid_type=orm.Int,
            required=False,
            help="The PK of the EpwPrepWorkChain node to extract CBM/VBM information from.",
        )
        spec.input(
            "carrier_type",
            valid_type=orm.Str,
            required=False,
            help="Carrier type: 'ele' for electrons, 'hole' for holes.",
        )
        spec.input("interpolation_distance", valid_type=(orm.Float, orm.List))
        spec.input("convergence_threshold", valid_type=orm.Float, required=False)
        spec.input(
            "kfpoints_factor",
            valid_type=orm.Int,
            default=lambda: orm.Int(1),
            help="Factor to multiply q-point mesh to get fine k-point mesh.",
        )
        # Optional pre-calculated quadrupole data.
        spec.input(
            "quadrupole_data",
            valid_type=(orm.Str, orm.RemoteData, orm.SinglefileData),
            required=False,
            help="Pre-calculated quadrupole data: path string, RemoteData, or SinglefileData.",
        )

        # Expose select EpwBaseWorkChain inputs under epw_mobility namespace
        spec.input_namespace(
            "epw_mobility",
            help="Inputs for the carrier mobility EpwBaseWorkChain.",
        )
        spec.expose_inputs(
            EpwBaseWorkChain,
            namespace="epw_mobility",
            include=(
                "code",
                "structure",
                "parent_folder_epw",
                "parameters",
                "qfpoints_distance",
                "kfpoints_factor",
                "options",
                "settings",
                "kpoints",
                "qpoints",
                "max_iterations",
                "clean_workdir",
            ),
            namespace_options={
                "required": False,
                "populate_defaults": False,
                "help": "Inputs for the carrier mobility EpwBaseWorkChain.",
            },
        )

        spec.outline(
            cls.setup,
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
        parent_folder_epw=None,
        structure=None,
        quadrupole_data=None,
        quadruple_dir=None,  # Backwards compatibility alias
        **kwargs,
    ):
        """Return a builder prepopulated with inputs selected according to the chosen protocol.

        :param epw_code: the ``Code`` instance configured for the ``epw.x`` plugin.
        :param parent_epw: a completed EpwPrepWorkChain, EpwCalculation, or EpwBaseWorkChain.
        :param protocol: protocol to use, if not specified, the default will be used.
        :param overrides: optional dictionary of inputs to override the defaults of the protocol.
        :param parent_folder_epw: optional RemoteData/RemoteStashFolderData to use as parent.
        :param structure: optional StructureData, if not provided will be inferred from parent_epw.
        :param quadrupole_data: optional path (str) or RemoteData/SinglefileData for quadrupole.fmt.
        :param quadruple_dir: alias for quadrupole_data for backwards compatibility.
        :return: a process builder instance with all inputs defined ready for launch.
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
            # For EpwBaseWorkChain / EpwCalculation the code lives at
            # ``epw_source.inputs.code``; the old path through
            # ``epw_source.inputs.epw.code`` only works when epw_source is an
            # EpwCalculation accessed via an ``epw`` namespace, which is not
            # the case here.
            source_computer = epw_source.inputs.code.computer.hostname
            if source_computer != epw_code.computer.hostname:
                raise ValueError(
                    "The `epw_code` must be configured on the same computer as that where the `parent_epw` was run."
                )
            parent_folder_epw = parent_epw.outputs.epw_folder

        # Determine structure – walk up the caller chain looking for
        # ``inputs.structure``.  Works for any depth of nesting.
        if structure is None:
            node = epw_source
            while node is not None:
                if "structure" in node.inputs:
                    structure = node.inputs.structure
                    break
                node = node.caller
            if structure is None:
                raise ValueError(
                    "Could not determine the structure from the parent node chain. "
                    "Please pass `structure` explicitly."
                )

        epw_namespace = "epw_mobility"
        epw_inputs = inputs.get(epw_namespace, None)

        epw_builder = EpwBaseWorkChain.get_builder_from_protocol(
            code=epw_code,
            structure=structure,
            protocol=protocol,
            overrides=epw_inputs,
            protocol_filename="base_mob.yaml",
        )

        # Copy individual fields from the sub-builder into the
        # ``epw_mobility`` namespace (assigning the builder object directly
        # is rejected because the namespace expects Data nodes, not a
        # ProcessBuilderNamespace).
        epw_mob = builder.epw_mobility
        epw_mob.code = epw_code
        epw_mob.parameters = epw_builder.parameters
        epw_mob.kpoints = epw_source.inputs.kpoints
        epw_mob.qpoints = epw_source.inputs.qpoints

        # Forward optional / protocol-derived inputs
        if "options" in epw_builder and epw_builder.options is not None:
            epw_mob.options = epw_builder.options
        if "max_iterations" in epw_builder and epw_builder.max_iterations is not None:
            epw_mob.max_iterations = epw_builder.max_iterations
        if "clean_workdir" in epw_builder and epw_builder.clean_workdir is not None:
            epw_mob.clean_workdir = epw_builder.clean_workdir
        if "kfpoints_factor" in epw_builder and epw_builder.kfpoints_factor is not None:
            epw_mob.kfpoints_factor = epw_builder.kfpoints_factor
        if "qfpoints_distance" in epw_builder and epw_builder.qfpoints_distance is not None:
            epw_mob.qfpoints_distance = epw_builder.qfpoints_distance

        if epw_inputs and "settings" in epw_inputs:
            epw_mob.settings = orm.Dict(epw_inputs["settings"])

        if isinstance(inputs["interpolation_distance"], float):
            builder.interpolation_distance = orm.Float(inputs["interpolation_distance"])
        if isinstance(inputs["interpolation_distance"], list):
            builder.interpolation_distance = orm.List(inputs["interpolation_distance"])

        builder.convergence_threshold = orm.Float(inputs["convergence_threshold"])
        builder.structure = structure
        builder.parent_folder_epw = parent_folder_epw
        builder.clean_workdir = orm.Bool(inputs["clean_workdir"])

        # Extract kfpoints_factor from inputs (protocol merged with overrides)
        if "kfpoints_factor" in inputs:
            builder.kfpoints_factor = orm.Int(inputs["kfpoints_factor"])

        # Handle quadrupole data (support both names for backwards compatibility)
        quad_data = quadrupole_data or quadruple_dir
        if quad_data is not None:
            if isinstance(quad_data, str):
                builder.quadrupole_data = orm.Str(quad_data)
            else:
                builder.quadrupole_data = quad_data

        return builder

    @staticmethod
    def _get_quadrupole_input(quadrupole_data):
        """Return the `EpwBaseWorkChain` input name/value for quadrupole data."""
        if isinstance(quadrupole_data, orm.SinglefileData):
            return "quadrupole_file", quadrupole_data
        return "quadrupole_dir", quadrupole_data

    def setup(self):
        """Setup steps, i.e. initialise context variables."""
        self.report("Starting setup")
        intp = self.inputs.get("interpolation_distance")
        if isinstance(intp, orm.List):
            self.ctx.interpolation_list = list(split_list(intp).values())
        else:
            self.ctx.interpolation_list = [intp]

        # Sort in descending order (start with coarse mesh, go to fine)
        self.ctx.interpolation_list.sort(reverse=True)
        self.ctx.iteration = 0
        self.ctx.final_interp = None
        self.ctx.mobility_values = []
        self.ctx.is_converged = False
        self.ctx.degaussq = None
        self.ctx.quadrupole_dir = None
        self.ctx.quadrupole_file = None
        self.ctx.epw_interp = []

        if "quadrupole_data" in self.inputs:
            key, value = self._get_quadrupole_input(self.inputs.quadrupole_data)
            if key == "quadrupole_file":
                self.ctx.quadrupole_file = value
            else:
                self.ctx.quadrupole_dir = value
            self.report(f"Using pre-calculated quadrupole data as `{key}`.")

        # Extract band edges and set fermi energy if parent_epw_prep_pk and carrier_type are provided
        self.ctx.fermi_energy_override = None
        if "parent_epw_prep_pk" in self.inputs and "carrier_type" in self.inputs:
            try:
                prep_pk = self.inputs.parent_epw_prep_pk.value
                prep_wc = orm.load_node(prep_pk)
                self.report(f"Loaded EpwPrepWorkChain <{prep_pk}>")

                # Extract band edges from EpwPrepWorkChain
                self.report("Attempting to extract band edges from NSCF data")
                band_edges_dict = extract_band_edges_from_epw_prep(prep_wc)
                if band_edges_dict and 'method' in band_edges_dict:
                    vbm_log = band_edges_dict.get('vbm')
                    cbm_log = band_edges_dict.get('cbm')
                    gap_log = band_edges_dict.get('bandgap')
                    vbm_str = f"{vbm_log:.6f}" if vbm_log is not None else "None"
                    cbm_str = f"{cbm_log:.6f}" if cbm_log is not None else "None"
                    gap_str = f"{gap_log:.6f}" if gap_log is not None else "None"
                    self.report(
                        f"Band edges from {band_edges_dict['method']}: "
                        f"VBM={vbm_str} eV, "
                        f"CBM={cbm_str} eV, "
                        f"gap={gap_str} eV"
                    )

                if band_edges_dict:  # Check if band edges were successfully extracted
                    if 'warning' in band_edges_dict:
                        self.report(f"WARNING: {band_edges_dict['warning']}")

                    carrier_type = self.inputs.carrier_type.value.lower()

                    vbm = band_edges_dict.get('vbm')
                    cbm = band_edges_dict.get('cbm')

                    if carrier_type == 'ele':
                        # For electrons, set fermi energy slightly above CBM
                        if cbm is not None:
                            self.ctx.fermi_energy_override = cbm - 0.1
                        else:
                            self.report(
                                "Warning: CBM is unavailable from band-edge extraction. "
                                "Cannot set electron fermi_energy override."
                            )
                    elif carrier_type == 'hole':
                        # For holes, set fermi energy slightly below VBM
                        if vbm is not None:
                            self.ctx.fermi_energy_override = vbm + 0.1
                        else:
                            self.report(
                                "Warning: VBM is unavailable from band-edge extraction. "
                                "Cannot set hole fermi_energy override."
                            )
                    elif vbm is not None and cbm is not None:
                        # Unknown carrier type, use mid-gap if both edges are available
                        self.ctx.fermi_energy_override = (vbm + cbm) / 2.0
                    else:
                        self.report(
                            "Warning: Incomplete band-edge data (missing VBM/CBM). "
                            "Cannot set fermi_energy override."
                        )

                    if self.ctx.fermi_energy_override is not None:
                        cbm_str = f"{cbm:.6f}" if cbm is not None else "None"
                        vbm_str = f"{vbm:.6f}" if vbm is not None else "None"
                        self.report(
                            f"Set fermi_energy to {self.ctx.fermi_energy_override:.6f} eV "
                            f"for carrier type '{carrier_type}' "
                            f"(CBM={cbm_str} eV, VBM={vbm_str} eV, "
                            f"method={band_edges_dict.get('method', 'unknown')})"
                        )
                else:
                    self.report(
                        "Warning: Could not extract band edges from parent_epw_prep. "
                        "Using default fermi_energy from protocol."
                    )
            except Exception as e:
                import traceback
                self.report(
                    f"Warning: Error extracting band edges: {e}. "
                    f"Traceback: {traceback.format_exc()}"
                    "Using default fermi_energy from protocol."
                )

    def should_run_conv_test(self):
        """Return True if there are still distances left in the list to test and not converged."""
        if self.ctx.is_converged:
            return False
        return len(self.ctx.interpolation_list) > 0

    def run_conv(self):
        """Run the EpwBaseWorkChain for the current interpolation distance."""
        self.ctx.iteration += 1

        # Manually construct inputs to EpwBaseWorkChain to avoid exposing unwanted inputs
        parameters = self.inputs.epw_mobility.parameters.get_dict()

        inputs = {
            "code": self.inputs.epw_mobility.code,
            "structure": self.inputs.structure,
            "parent_folder_epw": self.inputs.parent_folder_epw,
            "qfpoints_distance": self.ctx.interpolation_list.pop(0),
            "kfpoints_factor": self.inputs.kfpoints_factor,
            "parameters": orm.Dict(parameters),
        }

        # Forward options (required by EpwBaseWorkChain)
        if "options" in self.inputs.epw_mobility:
            inputs["options"] = self.inputs.epw_mobility.options

        # Forward optional inputs from epw_mobility namespace
        if "settings" in self.inputs.epw_mobility:
            inputs["settings"] = self.inputs.epw_mobility.settings
        if "kpoints" in self.inputs.epw_mobility:
            inputs["kpoints"] = self.inputs.epw_mobility.kpoints
        if "qpoints" in self.inputs.epw_mobility:
            inputs["qpoints"] = self.inputs.epw_mobility.qpoints
        if "max_iterations" in self.inputs.epw_mobility:
            inputs["max_iterations"] = self.inputs.epw_mobility.max_iterations
        if "clean_workdir" in self.inputs.epw_mobility:
            inputs["clean_workdir"] = self.inputs.epw_mobility.clean_workdir

        if self.ctx.quadrupole_dir is not None:
            inputs["quadrupole_dir"] = self.ctx.quadrupole_dir
        elif self.ctx.quadrupole_file is not None:
            inputs["quadrupole_file"] = self.ctx.quadrupole_file

        # Override fermi_energy if it was set from band edges
        if self.ctx.fermi_energy_override is not None:
            parameters = inputs["parameters"].get_dict()
            parameters.setdefault("INPUTEPW", {})["fermi_energy"] = self.ctx.fermi_energy_override
            parameters.setdefault("INPUTEPW", {})["efermi_read"] = True
            inputs["parameters"] = orm.Dict(parameters)
            self.report(f"Using fermi_energy override: {self.ctx.fermi_energy_override:.6f} eV")

        # Override ncarrier sign based on carrier_type
        if "carrier_type" in self.inputs:
            carrier_type = self.inputs.carrier_type.value.lower()
            parameters = inputs["parameters"].get_dict()
            ncarrier = parameters.get("INPUTEPW", {}).get("ncarrier", 1.0e13)
            if carrier_type == "hole":
                ncarrier = -abs(ncarrier)
                self.report(f"Setting ncarrier to negative for hole: {ncarrier:.2e}")
            elif carrier_type == "ele":
                ncarrier = abs(ncarrier)
                self.report(f"Setting ncarrier to positive for electron: {ncarrier:.2e}")
            parameters.setdefault("INPUTEPW", {})["ncarrier"] = ncarrier
            inputs["parameters"] = orm.Dict(parameters)

        # Safety: remove any None values from INPUTEPW to prevent
        # ``conv_to_fortran`` from crashing on NoneType.
        params_dict = inputs["parameters"].get_dict()
        inputepw = params_dict.get("INPUTEPW", {})
        none_keys = [k for k, v in inputepw.items() if v is None]
        if none_keys:
            for k in none_keys:
                del inputepw[k]
            self.report(f"Removed None-valued keys from INPUTEPW: {none_keys}")
            # If fermi_energy was removed, also disable efermi_read
            if "fermi_energy" in none_keys:
                inputepw.pop("efermi_read", None)
                self.report("Warning: fermi_energy is not set. Disabled efermi_read.")
            params_dict["INPUTEPW"] = inputepw
            inputs["parameters"] = orm.Dict(params_dict)

        # Add metadata with call_link_label
        inputs["metadata"] = {"call_link_label": f"conv_{self.ctx.iteration:02d}"}

        running = self.submit(EpwBaseWorkChain, **inputs)
        self.report(
            f"Launched EpwBaseWorkChain<{running.pk}> for convergence run #{self.ctx.iteration}"
        )

        return {"epw_interp": append_(running)}

    def inspect_conv(self):
        """Verify that the EpwBaseWorkChain finished successfully and check convergence."""
        workchain = self.ctx.epw_interp[-1]

        if not workchain.is_finished_ok:
            self.report(
                f"Convergence EpwBaseWorkChain<{workchain.pk}> failed with exit status {workchain.exit_status}"
            )
            return self.exit_codes.ERROR_SUB_PROCESS_EPW_INTERP

        try:
            output_params = workchain.outputs.output_parameters.get_dict()
            mobility = output_params.get("mobility_iBTE", None)
            self.report(
                f"Convergence run #{self.ctx.iteration} finished with mobility: {mobility}"
            )
            self.ctx.mobility_values.append(mobility)

            # Set degaussq from the first successful run if not already set
            if self.ctx.degaussq is None and "a2f" in workchain.outputs:
                frequency = workchain.outputs.a2f.get_array("frequency")
                self.ctx.degaussq = frequency[-1] / 100
                self.report(f"Set degaussq for subsequent runs to {self.ctx.degaussq}")

            # Check convergence using relative difference
            if (
                "convergence_threshold" in self.inputs
                and len(self.ctx.mobility_values) >= 2
            ):
                prev_mobility = self.ctx.mobility_values[-2]
                new_mobility = self.ctx.mobility_values[-1]

                if prev_mobility is not None and new_mobility is not None and prev_mobility != 0:
                    relative_diff = abs((new_mobility - prev_mobility) / prev_mobility)
                    is_converged = relative_diff < self.inputs.convergence_threshold.value
                    self.ctx.is_converged = is_converged
                    self.report(
                        f"Convergence check: old={prev_mobility:.4f}, new={new_mobility:.4f}, "
                        f"relative_diff={relative_diff:.6f} -> Converged: {is_converged}"
                    )
                else:
                    self.report(
                        "Could not compute relative difference for convergence check."
                    )

        except KeyError:
            self.report(
                "Could not find 'mobility_iBTE' in the output parameters of the convergence run."
            )

        # Check if we ran out of distances without converging
        if (
            not self.ctx.is_converged
            and not self.ctx.interpolation_list
            and "convergence_threshold" in self.inputs
        ):
            return self.exit_codes.ERROR_MOBILITY_NOT_CONVERGED

    def results(self):
        """Attach the final results to the outputs."""
        self.report("Workchain finished successfully, attaching final outputs.")

        if self.ctx.epw_interp:
            final_workchain = self.ctx.epw_interp[-1]
            self.out("parameters", final_workchain.outputs.output_parameters)

            # Create carrier mobility output Dict using calcfunction for proper provenance
            carrier_mobility = create_mobility_output(
                output_parameters=final_workchain.outputs.output_parameters,
                convergence_history=orm.List(self.ctx.mobility_values),
                is_converged=orm.Bool(self.ctx.is_converged),
            )
            self.out("carrier_mobility", carrier_mobility)

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
