"""Work chain for doing the coarse-grid calculations."""

from pathlib import Path

from aiida import orm
from aiida.common import AttributeDict

from aiida.engine import WorkChain, ToContext
from aiida_quantumespresso.workflows.ph.base import PhBaseWorkChain
from aiida_quantumespresso.workflows.protocols.utils import ProtocolMixin

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


class EpwPrepWorkChain(ProtocolMixin, WorkChain):
    """Main work chain to start calculating properties using EPW.

    Has support for both the selected columns of the density matrix (SCDM) and
    (projectability-disentangled Wannier function) PDWF projection types.
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
            "qpoints_distance", valid_type=orm.Float, default=lambda: orm.Float(0.5)
        )
        spec.input(
            "kpoints_distance_scf",
            valid_type=orm.Float,
            default=lambda: orm.Float(0.15),
        )
        spec.input(
            "kpoints_factor_nscf", valid_type=orm.Int, default=lambda: orm.Int(2)
        )
        spec.input(
            "w90_chk_to_ukk_script", valid_type=(orm.RemoteData, orm.SinglefileData)
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
                "help": "Inputs for the `PhBaseWorkChain` that does the `ph.x` calculation."
            },
        )
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

        spec.output("retrieved", valid_type=orm.FolderData)
        spec.output("epw_folder", valid_type=orm.RemoteStashFolderData)

        spec.outline(
            cls.generate_reciprocal_points,
            cls.run_wannier90,
            cls.inspect_wannier90,
            cls.run_ph,
            cls.inspect_ph,
            cls.run_epw,
            cls.inspect_epw,
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

    @classmethod
    def get_protocol_filepath(cls):
        """Return ``pathlib.Path`` to the ``.yaml`` file that defines the protocols."""
        from importlib_resources import files
        from . import protocols

        return files(protocols) / "prep.yaml"

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
        **kwargs,
    ):
        """Return a builder prepopulated with inputs selected according to the chosen protocol.

        :param structure: the ``StructureData`` instance to use.
        :param protocol: protocol to use, if not specified, the default will be used.
        :param overrides: optional dictionary of inputs to override the defaults of the protocol.
        :param kwargs: additional keyword arguments that will be passed to the ``get_builder_from_protocol`` of all the
            sub processes that are called by this workchain.
        :return: a process builder instance with all inputs defined ready for launch.
        """
        inputs = cls.get_protocol_inputs(protocol, overrides)

        builder = cls.get_builder()
        builder.structure = structure

        w90_bands_inputs = inputs.get("w90_bands", {})
        pseudo_family = w90_bands_inputs.pop("pseudo_family", None)

        if wannier_projection_type == WannierProjectionType.ATOMIC_PROJECTORS_QE:
            if reference_bands is None:
                raise ValueError(
                    f"reference_bands must be specified for {wannier_projection_type}"
                )
            w90_bands = Wannier90OptimizeWorkChain.get_builder_from_protocol(
                structure=structure,
                codes=codes,
                pseudo_family=pseudo_family,
                overrides=w90_bands_inputs,
                reference_bands=reference_bands,
                bands_kpoints=bands_kpoints,
            )
            w90_bands.separate_plotting = False
            # pop useless inputs, otherwise the builder validation will fail
            # at validating empty inputs
            w90_bands.pop("projwfc", None)
        elif wannier_projection_type == WannierProjectionType.SCDM:
            w90_bands = Wannier90BandsWorkChain.get_builder_from_protocol(
                structure=structure,
                codes=codes,
                pseudo_family=pseudo_family,
                overrides=w90_bands_inputs,
            )
        else:
            raise ValueError(
                f"Unsupported wannier_projection_type: {wannier_projection_type}"
            )

        w90_bands.pop("structure", None)
        w90_bands.pop("open_grid", None)

        builder.w90_bands = w90_bands

        args = (codes["ph"], None, protocol)
        ph_base = PhBaseWorkChain.get_builder_from_protocol(
            *args, overrides=inputs.get("ph_base", None), **kwargs
        )
        ph_base.pop("clean_workdir", None)
        ph_base.pop("qpoints_distance")

        builder.ph_base = ph_base

        # TODO: Here I have a loop for the epw builders for future extension of another epw bands interpolation
        for namespace in [
            "epw_base",
        ]:
            epw_inputs = inputs.get(namespace, None)
            if namespace == "epw_base":
                if "target_base" not in epw_inputs["metadata"]["options"]["stash"]:
                    epw_computer = codes["epw"].computer
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

                    epw_inputs["metadata"]["options"]["stash"]["target_base"] = (
                        target_basepath
                    )

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
            builder[namespace] = epw_builder

        builder.qpoints_distance = orm.Float(inputs["qpoints_distance"])
        builder.kpoints_distance_scf = orm.Float(inputs["kpoints_distance_scf"])
        builder.kpoints_factor_nscf = orm.Int(inputs["kpoints_factor_nscf"])
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

    def run_wannier90(self):
        """Run the wannier90 workflow."""
        if "projwfc" in self.inputs.w90_bands:
            w90_class = Wannier90BandsWorkChain
        else:
            w90_class = Wannier90OptimizeWorkChain

        self.ctx.w90_class_name = w90_class.get_name()
        self.report(f"Running a {self.ctx.w90_class_name}.")

        inputs = AttributeDict(
            self.exposed_inputs(Wannier90OptimizeWorkChain, namespace="w90_bands")
        )
        inputs.metadata.call_link_label = "w90_bands"
        inputs.structure = self.inputs.structure

        set_kpoints(inputs, self.ctx.kpoints_nscf, w90_class)
        inputs["scf"]["kpoints"] = self.ctx.kpoints_scf

        workchain_node = self.submit(w90_class, **inputs)
        self.report(f"launching {w90_class.get_name()}<{workchain_node.pk}>")

        return ToContext(workchain_w90_bands=workchain_node)

    def inspect_wannier90(self):
        """Verify that the wannier90 workflow finished successfully."""
        workchain = self.ctx.workchain_w90_bands

        if not workchain.is_finished_ok:
            self.report(
                f"{self.ctx.w90_class_name}<{workchain.pk}> failed with exit status {workchain.exit_status}"
            )
            return self.exit_codes.ERROR_SUB_PROCESS_FAILED_WANNIER90

    def run_ph(self):
        """Run the `PhBaseWorkChain`."""
        inputs = AttributeDict(
            self.exposed_inputs(PhBaseWorkChain, namespace="ph_base")
        )

        scf_base_wc = (
            self.ctx.workchain_w90_bands.base.links.get_outgoing(
                link_label_filter="scf"
            )
            .first()
            .node
        )
        inputs.ph.parent_folder = scf_base_wc.outputs.remote_folder

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

    def run_epw(self):
        """Run the `EpwBaseWorkChain`."""
        inputs = AttributeDict(
            self.exposed_inputs(EpwBaseWorkChain), namespace="epw_base"
        )

        # The EpwBaseWorkChain will take the parent folder of the previous
        # PhCalculation, PwCalculation, and Wannier90Calculation.
        inputs.parent_folder_ph = self.ctx.workchain_ph.outputs.remote_folder

        w90_workchain = self.ctx.workchain_w90_bands
        inputs.parent_folder_nscf = w90_workchain.outputs.nscf.remote_folder
        if (
            self.ctx.w90_class_name == "Wannier90OptimizeWorkChain"
            and w90_workchain.inputs.optimize_disproj
        ):
            inputs.parent_folder_chk = (
                w90_workchain.outputs.wannier90_optimal__remote_folder
            )
        else:
            inputs.parent_folder_chk = w90_workchain.outputs.wannier90.remote_folder

        # Here we explicitly specify the coarse k/q grid so the EpwBaseWorkChain will not deduce it from the parent
        # folders. This EpwBaseWorkChain is only used for the transition from coarse Bloch representation to Wannier
        # representation. Thus the fine grid is always [1, 1, 1].
        fine_points = orm.KpointsData()
        fine_points.set_kpoints_mesh([1, 1, 1])

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

    def results(self):
        """Add the most important results to the outputs of the work chain."""
        self.out("retrieved", self.ctx.workchain_epw.outputs.retrieved)
        self.out("epw_folder", self.ctx.workchain_epw.outputs.remote_stash)

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
