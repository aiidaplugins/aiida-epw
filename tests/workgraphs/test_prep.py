"""Tests for ``aiida_epw.workgraphs.prep``."""

import json
from importlib import import_module

import numpy as np
import pytest
from aiida import orm
from aiida.common import AttributeDict
from aiida.common.links import LinkType
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from aiida.engine import ToContext, WorkChain, calcfunction
from aiida_workgraph import WorkGraph
from aiida_workgraph import task
from aiida_workgraph.engine.task_manager import TaskManager
from aiida_workgraph.utils import restore_workgraph_data_from_raw_inputs
from aiida_quantumespresso.utils.mapping import prepare_process_inputs
from aiida_wannier90_workflows.common.types import WannierProjectionType

prep_module = import_module("aiida_epw.workgraphs.prep")


class NestedMetadataWorkChain(WorkChain):
    """Minimal nested workchain used to probe runtime process-task submission."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.expose_inputs(ArithmeticAddCalculation, namespace="scf")
        spec.outline(cls.noop)

    def noop(self):
        """Do nothing."""


class InnerPwWorkChain(WorkChain):
    """Intermediate workchain exposing a nested calcjob namespace."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.expose_inputs(ArithmeticAddCalculation, namespace="pw")
        spec.outline(cls.noop)

    def noop(self):
        """Do nothing."""


class DoubleNestedMetadataWorkChain(WorkChain):
    """Minimal double-nested workchain mirroring ``scf.pw.metadata`` style inputs."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.expose_inputs(InnerPwWorkChain, namespace="scf")
        spec.outline(cls.noop)

    def noop(self):
        """Do nothing."""


class InnerSubmitArithmeticWorkChain(WorkChain):
    """Intermediate workchain that forwards nested metadata to a CalcJob."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.expose_inputs(ArithmeticAddCalculation, namespace="pw")
        spec.outline(cls.run_pw, cls.inspect_pw)

    def run_pw(self):
        inputs = AttributeDict(
            self.exposed_inputs(ArithmeticAddCalculation, namespace="pw")
        )
        inputs.metadata.call_link_label = "pw"
        inputs = prepare_process_inputs(ArithmeticAddCalculation, inputs)
        return ToContext(workchain_pw=self.submit(ArithmeticAddCalculation, **inputs))

    def inspect_pw(self):
        """Do nothing."""


class OuterSubmitInnerWorkChain(WorkChain):
    """Outer workchain that forwards ``scf.pw.metadata`` style inputs to an inner workchain."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.expose_inputs(InnerSubmitArithmeticWorkChain, namespace="scf")
        spec.outline(cls.run_scf, cls.inspect_scf)

    def run_scf(self):
        inputs = AttributeDict(
            self.exposed_inputs(InnerSubmitArithmeticWorkChain, namespace="scf")
        )
        inputs.metadata.call_link_label = "scf"
        return ToContext(
            workchain_scf=self.submit(InnerSubmitArithmeticWorkChain, **inputs)
        )

    def inspect_scf(self):
        """Do nothing."""


class FakeWannier90BandsRuntimeWorkChain(WorkChain):
    """Lightweight substitute for the high-level Wannier bands task."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input("structure", valid_type=orm.StructureData)
        spec.input("clean_workdir", valid_type=orm.Bool, required=False)
        spec.input_namespace("scf", dynamic=True)
        spec.input_namespace("nscf", dynamic=True)
        spec.input_namespace("pw2wannier90", dynamic=True)
        spec.input_namespace("wannier90", dynamic=True)
        spec.outline(cls.generate_outputs)
        spec.output("scf.remote_folder", valid_type=orm.RemoteData)
        spec.output("nscf.remote_folder", valid_type=orm.RemoteData)
        spec.output("wannier90.remote_folder", valid_type=orm.RemoteData)
        spec.output("primitive_structure", valid_type=orm.StructureData)
        spec.output("band_structure", valid_type=orm.BandsData)

    def generate_outputs(self):
        """Emit the outputs that the prep graph expects downstream."""
        code = self.inputs.scf["pw"]["code"]
        seekpath = _fake_seekpath_structure_analysis(
            structure=self.inputs.structure,
            metadata={"call_link_label": "seekpath_structure_analysis"},
        )
        self.out(
            "scf.remote_folder",
            _make_remote_data(code, orm.Str(f"{self.node.uuid}-scf")),
        )
        self.out(
            "nscf.remote_folder",
            _make_remote_data(code, orm.Str(f"{self.node.uuid}-nscf")),
        )
        self.out(
            "wannier90.remote_folder",
            _make_remote_data(code, orm.Str(f"{self.node.uuid}-wannier90")),
        )
        self.out("primitive_structure", seekpath["primitive_structure"])
        self.out("band_structure", _make_bands_data(self.inputs.structure))


class FakePhBaseRuntimeWorkChain(WorkChain):
    """Lightweight substitute for the high-level phonon task."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input_namespace("ph", dynamic=True)
        spec.input("qpoints", valid_type=orm.KpointsData)
        spec.outline(cls.generate_outputs)
        spec.output("remote_folder", valid_type=orm.RemoteData)

    def generate_outputs(self):
        """Emit the remote folder expected by the EPW task."""
        code = self.inputs.ph["code"]
        self.out(
            "remote_folder", _make_remote_data(code, orm.Str(f"{self.node.uuid}-ph"))
        )


class FakePwBaseRuntimeWorkChain(WorkChain):
    """Lightweight substitute for the high-level PW task."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input_namespace("pw", dynamic=True)
        spec.input("kpoints", valid_type=orm.KpointsData)
        spec.outline(cls.generate_outputs)
        spec.output("remote_folder", valid_type=orm.RemoteData)

    def generate_outputs(self):
        """Emit the remote folder expected downstream."""
        code = self.inputs.pw["code"]
        self.out(
            "remote_folder", _make_remote_data(code, orm.Str(f"{self.node.uuid}-pw"))
        )


class FakeEpwBaseRuntimeWorkChain(WorkChain):
    """Lightweight substitute for the high-level EPW task."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input("code", valid_type=orm.AbstractCode)
        spec.input("structure", valid_type=orm.StructureData)
        spec.input("parameters", valid_type=orm.Dict, required=False)
        spec.input("options", valid_type=orm.Dict, required=False)
        spec.input("settings", valid_type=orm.Dict, required=False)
        spec.input("parallelization", valid_type=orm.Dict, required=False)
        spec.input("clean_workdir", valid_type=orm.Bool, required=False)
        spec.input("max_iterations", valid_type=orm.Int, required=False)
        spec.input("qfpoints_distance", valid_type=orm.Float, required=False)
        spec.input("kfpoints_factor", valid_type=orm.Int, required=False)
        spec.input("parent_folder_ph", valid_type=orm.RemoteData, required=False)
        spec.input("parent_folder_nscf", valid_type=orm.RemoteData, required=False)
        spec.input("parent_folder_chk", valid_type=orm.RemoteData, required=False)
        spec.input("parent_folder_epw", valid_type=orm.RemoteData, required=False)
        spec.input("w90_chk_to_ukk_script", valid_type=orm.RemoteData, required=False)
        spec.input("kpoints", valid_type=orm.KpointsData, required=False)
        spec.input("kfpoints", valid_type=orm.KpointsData, required=False)
        spec.input("qpoints", valid_type=orm.KpointsData, required=False)
        spec.input("qfpoints", valid_type=orm.KpointsData, required=False)
        spec.outline(cls.generate_outputs)
        spec.output("retrieved", valid_type=orm.FolderData)
        spec.output("remote_folder", valid_type=orm.RemoteData)
        spec.output("remote_stash", valid_type=orm.RemoteData)

    def generate_outputs(self):
        """Emit the outputs that the prep graph expects downstream."""
        code = self.inputs.code
        self.out("retrieved", _make_folder_data())
        self.out(
            "remote_folder", _make_remote_data(code, orm.Str(f"{self.node.uuid}-epw"))
        )
        self.out(
            "remote_stash", _make_remote_data(code, orm.Str(f"{self.node.uuid}-stash"))
        )


def _metadata_dict(
    *,
    account: str = "elph",
    queue_name: str = "debug",
    num_mpiprocs_per_machine: int = 128,
    max_wallclock_seconds: int = 3600,
) -> dict:
    """Return scheduler metadata used in the workgraph tests."""
    return {
        "options": {
            "resources": {
                "num_machines": 1,
                "num_mpiprocs_per_machine": num_mpiprocs_per_machine,
            },
            "max_wallclock_seconds": max_wallclock_seconds,
            "withmpi": True,
            "account": account,
            "queue_name": queue_name,
        }
    }


def _fake_wannier90_builder(codes):
    """Return a minimal Wannier90 builder-like namespace."""
    builder = AttributeDict()
    builder.structure = orm.StructureData(
        cell=[[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
    )
    builder.open_grid = {}
    builder.clean_workdir = orm.Bool(False)
    builder.scf = AttributeDict(
        {
            "max_iterations": orm.Int(2),
            "kpoints_distance": orm.Float(0.15),
            "kpoints_force_parity": orm.Bool(False),
            "pw": AttributeDict(
                {
                    "code": codes["pw"],
                    "metadata": _metadata_dict(),
                    "parameters": orm.Dict({"SYSTEM": {"occupations": "smearing"}}),
                    "pseudos": {},
                }
            ),
        }
    )
    builder.nscf = AttributeDict(
        {
            "max_iterations": orm.Int(2),
            "kpoints_force_parity": orm.Bool(False),
            "pw": AttributeDict(
                {
                    "code": codes["pw"],
                    "metadata": _metadata_dict(),
                    "parameters": orm.Dict({"SYSTEM": {"occupations": "smearing"}}),
                    "pseudos": {},
                }
            ),
        }
    )
    builder.pw2wannier90 = AttributeDict(
        {
            "max_iterations": orm.Int(2),
            "scdm_sigma_factor": orm.Float(3.0),
            "pw2wannier90": AttributeDict(
                {
                    "code": codes["pw2wannier90"],
                    "metadata": _metadata_dict(num_mpiprocs_per_machine=8),
                    "parameters": orm.Dict({"INPUTPP": {}}),
                }
            ),
        }
    )
    builder.wannier90 = AttributeDict(
        {
            "max_iterations": orm.Int(2),
            "shift_energy_windows": orm.Bool(True),
            "auto_energy_windows": orm.Bool(False),
            "auto_energy_windows_threshold": orm.Float(0.5),
            "wannier90": AttributeDict(
                {
                    "code": codes["wannier90"],
                    "metadata": _metadata_dict(num_mpiprocs_per_machine=1),
                    "parameters": orm.Dict(
                        {
                            "bands_plot": True,
                            "num_bands": 8,
                            "num_wann": 4,
                            "exclude_bands": range(1, 3),
                        }
                    ),
                    "settings": orm.Dict({}),
                }
            ),
        }
    )
    return builder


def _fake_ph_builder(code, *_args, **_kwargs):
    """Return a minimal phonon builder-like namespace."""
    builder = AttributeDict()
    builder.clean_workdir = orm.Bool(False)
    builder.qpoints_distance = orm.Float(0.4)
    builder.ph = AttributeDict(
        {
            "code": code,
            "metadata": _metadata_dict(),
            "parameters": orm.Dict({"INPUTPH": {}}),
        }
    )
    return builder


def _fake_pw_builder(code, structure, *_args, **_kwargs):
    """Return a minimal PW builder-like namespace."""
    builder = AttributeDict()
    builder.clean_workdir = orm.Bool(False)
    builder.kpoints_distance = orm.Float(0.2)
    builder.pw = AttributeDict(
        {
            "code": code,
            "metadata": _metadata_dict(),
            "parameters": orm.Dict({"SYSTEM": {"occupations": "smearing"}}),
            "pseudos": {},
            "structure": structure,
        }
    )
    return builder


def _fake_epw_builder(code, structure, *_args, **kwargs):
    """Return a minimal EPW builder-like namespace."""
    overrides = kwargs.get("overrides", {})
    builder = AttributeDict()
    builder.code = code
    builder.structure = structure
    builder.parameters = orm.Dict(
        overrides.get("parameters", {"INPUTEPW": {"band_plot": True}})
    )
    builder.options = orm.Dict(overrides.get("options", _metadata_dict()["options"]))
    builder.settings = orm.Dict(overrides.get("settings", {}))
    builder.parallelization = orm.Dict(overrides.get("parallelization", {}))
    builder.clean_workdir = orm.Bool(False)
    builder.max_iterations = orm.Int(2)
    builder.qfpoints_distance = orm.Float(0.1)
    builder.kfpoints_factor = orm.Int(2)
    return builder


def _create_explicit_kpoints(mesh):
    """Create an explicit KpointsData list from a mesh."""
    kpoints = orm.KpointsData()
    points = []
    for i in range(mesh[0]):
        for j in range(mesh[1]):
            for k in range(mesh[2]):
                points.append(
                    [
                        i / mesh[0],
                        j / mesh[1],
                        k / mesh[2],
                    ]
                )
    kpoints.set_kpoints(points)
    return kpoints


def _mesh_kpoints(mesh):
    """Create a mesh-style KpointsData node."""
    kpoints = orm.KpointsData()
    kpoints.set_kpoints_mesh(mesh)
    return kpoints


@calcfunction
def _make_remote_data(code, label):
    """Create a lightweight remote data node for tests."""
    return orm.RemoteData(
        computer=code.computer,
        remote_path=f"/tmp/{label.value}",
    )


@calcfunction
def _make_bands_data(structure):
    """Create a simple band structure node for tests."""
    bands_kpoints = orm.KpointsData()
    bands_kpoints.set_cell(structure.cell, structure.pbc)
    bands_kpoints.set_kpoints([[0.0, 0.0, 0.0], [0.5, 0.0, 0.0]])

    band_structure = orm.BandsData()
    band_structure.set_kpointsdata(bands_kpoints)
    band_structure.set_bands(np.array([[0.0, 1.0], [0.5, 1.5]]))
    return band_structure


@calcfunction
def _make_folder_data():
    """Create a folder data node for tests."""
    return orm.FolderData()


@calcfunction
def _fake_seekpath_structure_analysis(structure):
    """Create lightweight seekpath-like outputs with provenance for tests."""
    primitive_structure = orm.StructureData(ase=structure.get_ase())
    return {
        "primitive_structure": primitive_structure,
        "explicit_kpoints": _create_explicit_kpoints([2, 2, 2]),
    }


def test_prepare_wannier90_runtime_inputs_returns_distinct_nodes():
    """The runtime helper should not reuse the same node for two calcfunction outputs."""
    kpoints_nscf = orm.KpointsData()
    kpoints_nscf.set_kpoints_mesh([2, 2, 2])

    result = prep_module.prepare_wannier90_runtime_inputs._callable(
        parameters=orm.Dict({"bands_plot": True}),
        kpoints_nscf=kpoints_nscf,
    )

    nscf_points, nscf_weights = result["nscf_kpoints"].get_kpoints(also_weights=True)
    wannier_points, wannier_weights = result["wannier90_kpoints"].get_kpoints(
        also_weights=True
    )

    assert result["nscf_kpoints"].uuid != result["wannier90_kpoints"].uuid
    assert np.array_equal(nscf_points, wannier_points)
    assert np.array_equal(nscf_weights, wannier_weights)
    assert result["parameters"].get_dict()["mp_grid"] == [2, 2, 2]


def test_drop_epw_mesh_generation_inputs_removes_builder_side_mesh_hints():
    """Explicit EPW meshes in the graph should override builder-side mesh-generation hints."""
    original = {
        "options": {"account": "elph"},
        "kfpoints_factor": orm.Int(2),
        "qfpoints_distance": orm.Float(0.1),
    }

    cleaned = prep_module._drop_epw_mesh_generation_inputs(original)

    assert "kfpoints_factor" not in cleaned
    assert "qfpoints_distance" not in cleaned
    assert cleaned["options"]["account"] == "elph"
    assert "kfpoints_factor" in original
    assert "qfpoints_distance" in original


def test_validate_inputs_accepts_direct_epw_wannierize_inputs():
    """The workgraph should accept the direct EPW Wannierization entry point."""
    message = prep_module.validate_inputs(
        {
            "scf": {},
            "nscf": {},
            "ph_base": {},
            "epw_base": {
                "parameters": {"INPUTEPW": {"wannierize": True, "proj": ["Si:s"]}}
            },
        }
    )

    assert message is None


def test_validate_inputs_accepts_direct_epw_wannierize_with_epw_bands():
    """Direct EPW Wannierization can still request the EPW bands branch."""
    message = prep_module.validate_inputs(
        {
            "scf": {},
            "nscf": {},
            "ph_base": {},
            "epw_base": {
                "parameters": {"INPUTEPW": {"wannierize": True, "proj": ["Si:s"]}}
            },
            "epw_bands": {},
        }
    )

    assert message is None


def test_validate_inputs_accepts_parent_folder_ph_without_ph_base():
    """A reusable phonon parent folder should make the phonon namespace optional."""
    message = prep_module.validate_inputs(
        {
            "w90_bands": {},
            "parent_folder_ph": object(),
            "epw_base": {},
        }
    )

    assert message is None


def test_generate_reciprocal_points_prefers_restart_qpoints_from_parent_ph(
    fixture_localhost,
    generate_remote_data,
    monkeypatch,
):
    """Restarting from an existing PhCalculation should make its q-points authoritative."""
    restart_qpoints = orm.KpointsData()
    restart_qpoints.set_kpoints_mesh([5, 5, 5])
    restart_parent = generate_remote_data(fixture_localhost, "/remote/ph-restart")

    monkeypatch.setattr(
        prep_module,
        "_create_kpoints_from_distance_node",
        lambda *args, **kwargs: _mesh_kpoints([4, 4, 4]),
    )
    monkeypatch.setattr(
        prep_module,
        "validate_parent_ph_inputs",
        lambda _folder, _structure: restart_qpoints,
    )

    result = prep_module.generate_reciprocal_points._callable(
        structure=orm.StructureData(cell=[[1, 0, 0], [0, 1, 0], [0, 0, 1]]),
        force_parity=orm.Bool(False),
        kpoints_distance_scf=orm.Float(0.15),
        qpoints_distance=orm.Float(0.3),
        kpoints_factor_nscf=orm.Int(2),
        parent_folder_ph=restart_parent,
    )

    assert result["qpoints"] == restart_qpoints
    assert result["kpoints_scf"].get_kpoints_mesh()[0] == [4, 4, 4]
    assert result["kpoints_nscf"].get_kpoints_mesh()[0] == [10, 10, 10]


def test_prep_passes_w90_chk_to_ukk_script_only_to_epw_base(
    fixture_code,
    generate_structure,
    monkeypatch,
):
    """The optional chk-to-ukk script should only be wired to the transformation EPW step."""
    codes = {
        "pw": fixture_code("quantumespresso.pw"),
        "pw2wannier90": fixture_code("quantumespresso.pw2wannier90"),
        "wannier90": fixture_code("wannier90.wannier90"),
        "ph": fixture_code("quantumespresso.ph"),
        "epw": fixture_code("epw.epw"),
    }
    structure = generate_structure()
    script = orm.RemoteData(
        computer=codes["epw"].computer, remote_path="/tmp/w90_chk_to_ukk.jl"
    )

    monkeypatch.setattr(
        prep_module,
        "get_protocol_inputs",
        lambda protocol=None, overrides=None: {
            "pseudo_family": "PseudoDojo/0.5/PBE/SR/standard/upf",
            "qpoints_distance": 0.5,
            "kpoints_distance_scf": 0.15,
            "kpoints_factor_nscf": 2,
            "kpoints_force_parity": False,
            "w90_bands": {
                "scf": {"pw": {"metadata": _metadata_dict()}},
                "nscf": {"pw": {"metadata": _metadata_dict()}},
                "pw2wannier90": {
                    "pw2wannier90": {
                        "metadata": _metadata_dict(num_mpiprocs_per_machine=8)
                    }
                },
                "wannier90": {
                    "wannier90": {
                        "metadata": _metadata_dict(num_mpiprocs_per_machine=1)
                    }
                },
            },
            "ph_base": {"ph": {"metadata": _metadata_dict()}},
            "epw_base": {"options": _metadata_dict()["options"]},
            "epw_bands": {"options": _metadata_dict()["options"]},
        },
    )
    monkeypatch.setattr(
        prep_module.Wannier90BandsWorkChain,
        "get_builder_from_protocol",
        lambda **kwargs: _fake_wannier90_builder(kwargs["codes"]),
    )
    monkeypatch.setattr(
        prep_module.Wannier90OptimizeWorkChain,
        "get_builder_from_protocol",
        lambda **kwargs: _fake_wannier90_builder(kwargs["codes"]),
    )
    monkeypatch.setattr(
        prep_module.PhBaseWorkChain, "get_builder_from_protocol", _fake_ph_builder
    )
    monkeypatch.setattr(
        prep_module.EpwBaseWorkChain, "get_builder_from_protocol", _fake_epw_builder
    )

    wg = prep_module.prep(
        codes=codes,
        structure=structure,
        protocol="fast",
        overrides={},
        w90_chk_to_ukk_script=script,
    )
    engine_inputs = wg.to_engine_inputs(metadata=None)

    assert (
        engine_inputs["tasks"]["epw_base"]["w90_chk_to_ukk_script"].uuid == script.uuid
    )
    assert "w90_chk_to_ukk_script" not in engine_inputs["tasks"]["epw_bands"]


def test_prep_workgraph_runs_with_fake_high_level_workchains(
    fixture_code,
    generate_structure,
    monkeypatch,
):
    """Run the real prep graph with lightweight stand-ins for the heavy workchains."""
    codes = {
        "pw": fixture_code("quantumespresso.pw"),
        "pw2wannier90": fixture_code("quantumespresso.pw2wannier90"),
        "wannier90": fixture_code("wannier90.wannier90"),
        "ph": fixture_code("quantumespresso.ph"),
        "epw": fixture_code("epw.epw"),
    }
    structure = generate_structure()

    monkeypatch.setattr(
        prep_module,
        "get_protocol_inputs",
        lambda protocol=None, overrides=None: {
            "pseudo_family": "PseudoDojo/0.5/PBE/SR/standard/upf",
            "qpoints_distance": 0.5,
            "kpoints_distance_scf": 0.15,
            "kpoints_factor_nscf": 2,
            "kpoints_force_parity": False,
            "w90_bands": {
                "scf": {"pw": {"metadata": _metadata_dict()}},
                "nscf": {"pw": {"metadata": _metadata_dict()}},
                "pw2wannier90": {
                    "pw2wannier90": {
                        "metadata": _metadata_dict(num_mpiprocs_per_machine=8)
                    }
                },
                "wannier90": {
                    "wannier90": {
                        "metadata": _metadata_dict(num_mpiprocs_per_machine=1)
                    }
                },
            },
            "ph_base": {"ph": {"metadata": _metadata_dict()}},
            "epw_base": {"options": _metadata_dict()["options"]},
            "epw_bands": {"options": _metadata_dict()["options"]},
        },
    )
    monkeypatch.setattr(
        prep_module.Wannier90BandsWorkChain,
        "get_builder_from_protocol",
        lambda **kwargs: _fake_wannier90_builder(kwargs["codes"]),
    )
    monkeypatch.setattr(
        prep_module.PhBaseWorkChain,
        "get_builder_from_protocol",
        _fake_ph_builder,
    )
    monkeypatch.setattr(
        prep_module.EpwBaseWorkChain,
        "get_builder_from_protocol",
        _fake_epw_builder,
    )
    monkeypatch.setattr(
        prep_module, "_apply_socket_overrides", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        prep_module,
        "Wannier90BandsTask",
        task(FakeWannier90BandsRuntimeWorkChain),
    )
    monkeypatch.setattr(
        prep_module,
        "PhBaseTask",
        task(FakePhBaseRuntimeWorkChain),
    )
    monkeypatch.setattr(
        prep_module,
        "EpwBaseTask",
        task(FakeEpwBaseRuntimeWorkChain),
    )

    wg = prep_module.prep(
        codes=codes,
        structure=structure,
        protocol="fast",
        overrides={},
    )
    wg.run()

    assert wg.process is not None
    assert wg.process.is_finished_ok
    assert (
        wg.process.outputs.epw_stash.uuid
        == wg.tasks["epw_base"].process.outputs.remote_stash.uuid
    )

    wannier_node = wg.tasks["w90_bands"].process
    phonon_node = wg.tasks["ph_base"].process
    epw_node = wg.tasks["epw_base"].process
    epw_bands_node = wg.tasks["epw_bands"].process
    assert wannier_node is not None
    assert phonon_node is not None
    assert epw_node is not None
    assert epw_bands_node is not None

    nscf_kpoints = wannier_node.inputs.nscf.kpoints
    wannier_kpoints = wannier_node.inputs.wannier90.wannier90.kpoints
    with pytest.raises(AttributeError):
        nscf_kpoints.get_kpoints_mesh()
    with pytest.raises(AttributeError):
        wannier_kpoints.get_kpoints_mesh()
    assert len(nscf_kpoints.get_kpoints()) == len(wannier_kpoints.get_kpoints())

    assert (
        phonon_node.inputs.ph.parent_folder.uuid
        == wannier_node.outputs.scf.remote_folder.uuid
    )
    assert (
        epw_node.inputs.parent_folder_ph.uuid == phonon_node.outputs.remote_folder.uuid
    )
    assert (
        epw_node.inputs.parent_folder_nscf.uuid
        == wannier_node.outputs.nscf.remote_folder.uuid
    )
    assert (
        epw_node.inputs.parent_folder_chk.uuid
        == wannier_node.outputs.wannier90.remote_folder.uuid
    )
    reciprocal_points_node = wg.tasks["generate_reciprocal_points"].process
    assert reciprocal_points_node is not None
    assert (
        reciprocal_points_node.outputs.kpoints_nscf.get_kpoints_mesh()[0]
        == epw_node.inputs.kpoints.get_kpoints_mesh()[0]
    )
    assert (
        reciprocal_points_node.outputs.qpoints.get_kpoints_mesh()[0]
        == epw_node.inputs.qpoints.get_kpoints_mesh()[0]
    )
    assert epw_node.inputs.kfpoints.get_kpoints_mesh()[0] == [1, 1, 1]
    assert epw_node.inputs.qfpoints.get_kpoints_mesh()[0] == [1, 1, 1]
    with pytest.raises(AttributeError):
        epw_bands_node.inputs.kfpoints.get_kpoints_mesh()
    with pytest.raises(AttributeError):
        epw_bands_node.inputs.qfpoints.get_kpoints_mesh()
    epw_bands_kf = epw_bands_node.inputs.kfpoints.get_kpoints()
    epw_bands_qf = epw_bands_node.inputs.qfpoints.get_kpoints()
    assert len(epw_bands_kf) > 0
    assert np.array_equal(epw_bands_kf, epw_bands_qf)
    seekpath_node = next(
        link.node
        for link in wannier_node.base.links.get_outgoing(
            link_type=LinkType.CALL_CALC
        ).all()
        if link.link_label == "seekpath_structure_analysis"
    )
    assert (
        epw_bands_node.inputs.kfpoints.uuid
        == seekpath_node.outputs.explicit_kpoints.uuid
    )


def test_build_task_inputs_uses_direct_pw_namespaces_for_epw_wannierize(
    fixture_code,
    generate_structure,
    monkeypatch,
):
    """Direct EPW Wannierization should build standalone SCF/NSCF namespaces instead of `w90_bands`."""
    codes = {
        "pw": fixture_code("quantumespresso.pw"),
        "epw": fixture_code("epw.epw"),
    }
    structure = generate_structure()

    monkeypatch.setattr(
        prep_module,
        "get_protocol_inputs",
        lambda protocol=None, overrides=None: {
            "pseudo_family": "PseudoDojo/0.5/PBE/SR/standard/upf",
            "qpoints_distance": 0.5,
            "kpoints_distance_scf": 0.15,
            "kpoints_factor_nscf": 2,
            "kpoints_force_parity": False,
            "scf": {"pw": {"metadata": _metadata_dict()}},
            "nscf": {"pw": {"metadata": _metadata_dict()}},
            "ph_base": {"ph": {"metadata": _metadata_dict()}},
            "epw_base": {
                "options": _metadata_dict()["options"],
                "parameters": {"INPUTEPW": {"wannierize": True, "proj": ["Si:s"]}},
            },
        },
    )
    monkeypatch.setattr(
        prep_module.PwBaseWorkChain, "get_builder_from_protocol", _fake_pw_builder
    )
    monkeypatch.setattr(
        prep_module.PhBaseWorkChain, "get_builder_from_protocol", _fake_ph_builder
    )
    monkeypatch.setattr(
        prep_module.EpwBaseWorkChain, "get_builder_from_protocol", _fake_epw_builder
    )

    prepared_inputs = prep_module.build_task_inputs(
        codes=codes,
        structure=structure,
        protocol="fast",
        overrides={},
    )

    assert prepared_inputs["w90_bands"] == {}
    assert "pw" in prepared_inputs["scf"]
    assert "pw" in prepared_inputs["nscf"]
    assert "code" in prepared_inputs["epw_bands"]


def test_build_task_inputs_supports_analytic_wannier_projections(
    fixture_code,
    generate_structure,
    monkeypatch,
):
    """The workgraph helper should preserve analytic Wannier90 projections."""
    codes = {
        "pw": fixture_code("quantumespresso.pw"),
        "pw2wannier90": fixture_code("quantumespresso.pw2wannier90"),
        "wannier90": fixture_code("wannier90.wannier90"),
        "ph": fixture_code("quantumespresso.ph"),
        "epw": fixture_code("epw.epw"),
    }
    structure = generate_structure()
    captured = {}

    monkeypatch.setattr(
        prep_module,
        "get_protocol_inputs",
        lambda protocol=None, overrides=None: {
            "pseudo_family": "PseudoDojo/0.5/PBE/SR/standard/upf",
            "qpoints_distance": 0.5,
            "kpoints_distance_scf": 0.15,
            "kpoints_factor_nscf": 2,
            "kpoints_force_parity": False,
            "w90_bands": {
                "scf": {"pw": {"metadata": _metadata_dict()}},
                "nscf": {"pw": {"metadata": _metadata_dict()}},
                "pw2wannier90": {"pw2wannier90": {"metadata": _metadata_dict()}},
                "wannier90": {"wannier90": {"metadata": _metadata_dict()}},
            },
            "ph_base": {"ph": {"metadata": _metadata_dict()}},
            "epw_base": {"options": _metadata_dict()["options"]},
        },
    )

    def fake_w90_builder(**kwargs):
        captured["projection_type"] = kwargs["projection_type"]
        builder = _fake_wannier90_builder(kwargs["codes"])
        builder.wannier90["wannier90"]["projections"] = orm.List(list=["Si:s", "Si:p"])
        return builder

    monkeypatch.setattr(
        prep_module.Wannier90BandsWorkChain,
        "get_builder_from_protocol",
        fake_w90_builder,
    )
    monkeypatch.setattr(
        prep_module.Wannier90OptimizeWorkChain,
        "get_builder_from_protocol",
        fake_w90_builder,
    )
    monkeypatch.setattr(
        prep_module.PhBaseWorkChain,
        "get_builder_from_protocol",
        _fake_ph_builder,
    )
    monkeypatch.setattr(
        prep_module.EpwBaseWorkChain,
        "get_builder_from_protocol",
        _fake_epw_builder,
    )

    prepared_inputs = prep_module.build_task_inputs(
        codes=codes,
        structure=structure,
        protocol="fast",
        overrides={},
        wannier_projection_type=WannierProjectionType.ANALYTIC,
    )

    assert captured["projection_type"] == WannierProjectionType.ANALYTIC
    assert prepared_inputs["w90_bands"]["wannier90"]["wannier90"]["projections"] == [
        "Si:s",
        "Si:p",
    ]


def test_build_task_inputs_rejects_reference_bands_for_analytic_projections(
    fixture_code,
    generate_structure,
    monkeypatch,
):
    """Analytic projections should not enter the optimization builder path."""
    codes = {
        "pw": fixture_code("quantumespresso.pw"),
        "pw2wannier90": fixture_code("quantumespresso.pw2wannier90"),
        "wannier90": fixture_code("wannier90.wannier90"),
        "ph": fixture_code("quantumespresso.ph"),
        "epw": fixture_code("epw.epw"),
    }

    monkeypatch.setattr(
        prep_module,
        "get_protocol_inputs",
        lambda protocol=None, overrides=None: {
            "pseudo_family": "PseudoDojo/0.5/PBE/SR/standard/upf",
            "qpoints_distance": 0.5,
            "kpoints_distance_scf": 0.15,
            "kpoints_factor_nscf": 2,
            "kpoints_force_parity": False,
            "w90_bands": {
                "scf": {"pw": {"metadata": _metadata_dict()}},
                "nscf": {"pw": {"metadata": _metadata_dict()}},
                "pw2wannier90": {"pw2wannier90": {"metadata": _metadata_dict()}},
                "wannier90": {"wannier90": {"metadata": _metadata_dict()}},
            },
            "ph_base": {"ph": {"metadata": _metadata_dict()}},
            "epw_base": {"options": _metadata_dict()["options"]},
        },
    )

    with pytest.raises(ValueError, match="Wannier90OptimizeWorkChain"):
        prep_module.build_task_inputs(
            codes=codes,
            structure=generate_structure(),
            protocol="fast",
            overrides={},
            wannier_projection_type=WannierProjectionType.ANALYTIC,
            reference_bands=orm.BandsData(),
        )


def test_prep_workgraph_runs_direct_epw_wannierize_with_fake_high_level_workchains(
    fixture_code,
    fixture_localhost,
    generate_remote_data,
    generate_structure,
    monkeypatch,
):
    """The workgraph should mirror the classical direct EPW Wannierization path."""
    codes = {
        "pw": fixture_code("quantumespresso.pw"),
        "ph": fixture_code("quantumespresso.ph"),
        "epw": fixture_code("epw.epw"),
    }
    structure = generate_structure()
    restart_qpoints = _mesh_kpoints([3, 3, 3])
    parent_folder_ph = generate_remote_data(fixture_localhost, "/remote/ph-restart")

    monkeypatch.setattr(
        prep_module,
        "get_protocol_inputs",
        lambda protocol=None, overrides=None: {
            "pseudo_family": "PseudoDojo/0.5/PBE/SR/standard/upf",
            "qpoints_distance": 0.5,
            "kpoints_distance_scf": 0.15,
            "kpoints_factor_nscf": 2,
            "kpoints_force_parity": False,
            "scf": {"pw": {"metadata": _metadata_dict()}},
            "nscf": {"pw": {"metadata": _metadata_dict()}},
            "ph_base": {"ph": {"metadata": _metadata_dict()}},
            "epw_base": {
                "options": _metadata_dict()["options"],
                "parameters": {"INPUTEPW": {"wannierize": True, "proj": ["Si:s"]}},
            },
        },
    )
    monkeypatch.setattr(
        prep_module.PwBaseWorkChain, "get_builder_from_protocol", _fake_pw_builder
    )
    monkeypatch.setattr(
        prep_module.EpwBaseWorkChain, "get_builder_from_protocol", _fake_epw_builder
    )
    monkeypatch.setattr(
        prep_module,
        "validate_parent_ph_inputs",
        lambda _folder, _structure: restart_qpoints,
    )
    monkeypatch.setattr(
        prep_module, "_apply_socket_overrides", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(prep_module, "PwBaseTask", task(FakePwBaseRuntimeWorkChain))
    monkeypatch.setattr(prep_module, "EpwBaseTask", task(FakeEpwBaseRuntimeWorkChain))

    wg = prep_module.prep(
        codes=codes,
        structure=structure,
        protocol="fast",
        overrides={},
        parent_folder_ph=parent_folder_ph,
    )
    wg.run()

    assert wg.process is not None
    assert wg.process.is_finished_ok
    assert "w90_bands" not in {task.name for task in wg.tasks}

    scf_node = wg.tasks["scf"].process
    nscf_node = wg.tasks["nscf"].process
    epw_node = wg.tasks["epw_base"].process
    epw_bands_node = wg.tasks["epw_bands"].process
    reciprocal_points_node = wg.tasks["generate_reciprocal_points"].process

    assert scf_node is not None
    assert nscf_node is not None
    assert epw_node is not None
    assert epw_bands_node is not None
    assert reciprocal_points_node is not None

    assert "ph_base" not in wg.tasks
    assert epw_node.inputs.parent_folder_ph.uuid == parent_folder_ph.uuid
    assert (
        epw_node.inputs.parent_folder_nscf.uuid == nscf_node.outputs.remote_folder.uuid
    )
    assert "parent_folder_chk" not in epw_node.inputs
    assert reciprocal_points_node.outputs.qpoints.uuid == restart_qpoints.uuid
    assert epw_node.inputs.qpoints.uuid == restart_qpoints.uuid
    assert (
        epw_node.inputs.kpoints.get_kpoints_mesh()[0]
        == reciprocal_points_node.outputs.kpoints_nscf.get_kpoints_mesh()[0]
    )
    assert (
        epw_bands_node.inputs.parent_folder_epw.uuid
        == epw_node.outputs.remote_stash.uuid
    )
    assert epw_bands_node.inputs.kfpoints.uuid == epw_bands_node.inputs.qfpoints.uuid


def test_prep_workgraph_runs_direct_epw_bands_without_w90(
    fixture_code,
    generate_structure,
    monkeypatch,
):
    """Direct EPW Wannierization should still build the bands branch from seekpath."""
    codes = {
        "pw": fixture_code("quantumespresso.pw"),
        "ph": fixture_code("quantumespresso.ph"),
        "epw": fixture_code("epw.epw"),
    }
    structure = generate_structure()
    monkeypatch.setattr(
        prep_module,
        "get_protocol_inputs",
        lambda protocol=None, overrides=None: {
            "pseudo_family": "PseudoDojo/0.5/PBE/SR/standard/upf",
            "qpoints_distance": 0.5,
            "kpoints_distance_scf": 0.15,
            "kpoints_factor_nscf": 2,
            "kpoints_force_parity": False,
            "scf": {"pw": {"metadata": _metadata_dict()}},
            "nscf": {"pw": {"metadata": _metadata_dict()}},
            "ph_base": {"ph": {"metadata": _metadata_dict()}},
            "epw_base": {
                "options": _metadata_dict()["options"],
                "parameters": {"INPUTEPW": {"wannierize": True, "proj": ["Si:s"]}},
            },
            "epw_bands": {"options": _metadata_dict()["options"]},
        },
    )
    monkeypatch.setattr(
        prep_module.PwBaseWorkChain, "get_builder_from_protocol", _fake_pw_builder
    )
    monkeypatch.setattr(
        prep_module.PhBaseWorkChain, "get_builder_from_protocol", _fake_ph_builder
    )
    monkeypatch.setattr(
        prep_module.EpwBaseWorkChain, "get_builder_from_protocol", _fake_epw_builder
    )
    monkeypatch.setattr(
        prep_module, "_apply_socket_overrides", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(prep_module, "PwBaseTask", task(FakePwBaseRuntimeWorkChain))
    monkeypatch.setattr(prep_module, "PhBaseTask", task(FakePhBaseRuntimeWorkChain))
    monkeypatch.setattr(prep_module, "EpwBaseTask", task(FakeEpwBaseRuntimeWorkChain))

    wg = prep_module.prep(
        codes=codes,
        structure=structure,
        protocol="fast",
        overrides={},
    )
    wg.run()

    epw_bands_node = wg.tasks["epw_bands"].process

    assert epw_bands_node is not None
    assert epw_bands_node.inputs.kfpoints.uuid == epw_bands_node.inputs.qfpoints.uuid


def test_prep_workgraph_preserves_nested_scheduler_options_and_codes(
    fixture_code,
    generate_structure,
    monkeypatch,
):
    """The prep workgraph should keep nested scheduler options on high-level tasks."""
    codes = {
        "pw": fixture_code("quantumespresso.pw"),
        "pw2wannier90": fixture_code("quantumespresso.pw2wannier90"),
        "wannier90": fixture_code("wannier90.wannier90"),
        "ph": fixture_code("quantumespresso.ph"),
        "epw": fixture_code("epw.epw"),
    }
    structure = generate_structure()

    monkeypatch.setattr(
        prep_module,
        "get_protocol_inputs",
        lambda protocol=None, overrides=None: {
            "pseudo_family": "PseudoDojo/0.5/PBE/SR/standard/upf",
            "qpoints_distance": 0.5,
            "kpoints_distance_scf": 0.15,
            "kpoints_factor_nscf": 2,
            "kpoints_force_parity": False,
            "w90_bands": {
                "scf": {"pw": {"metadata": _metadata_dict()}},
                "nscf": {"pw": {"metadata": _metadata_dict()}},
                "pw2wannier90": {
                    "pw2wannier90": {
                        "metadata": _metadata_dict(num_mpiprocs_per_machine=8)
                    }
                },
                "wannier90": {
                    "wannier90": {
                        "metadata": _metadata_dict(num_mpiprocs_per_machine=1)
                    }
                },
            },
            "ph_base": {"ph": {"metadata": _metadata_dict()}},
            "epw_base": {"options": _metadata_dict()["options"]},
        },
    )
    monkeypatch.setattr(
        prep_module.Wannier90BandsWorkChain,
        "get_builder_from_protocol",
        lambda **kwargs: _fake_wannier90_builder(kwargs["codes"]),
    )
    monkeypatch.setattr(
        prep_module.Wannier90OptimizeWorkChain,
        "get_builder_from_protocol",
        lambda **kwargs: _fake_wannier90_builder(kwargs["codes"]),
    )
    monkeypatch.setattr(
        prep_module.PhBaseWorkChain,
        "get_builder_from_protocol",
        _fake_ph_builder,
    )
    monkeypatch.setattr(
        prep_module.EpwBaseWorkChain,
        "get_builder_from_protocol",
        _fake_epw_builder,
    )

    prepared_inputs = prep_module.build_task_inputs(
        codes=codes,
        structure=structure,
        protocol="fast",
        overrides={},
    )
    assert prepared_inputs["w90_bands"]["wannier90"]["wannier90"]["parameters"][
        "exclude_bands"
    ] == [1, 2]

    wg = prep_module.prep(
        codes=codes, structure=structure, protocol="fast", overrides={}
    )
    engine_inputs = wg.to_engine_inputs(metadata=None)
    restored = WorkGraph.from_dict(
        restore_workgraph_data_from_raw_inputs(engine_inputs)
    )

    wannier_task = next(task for task in wg.tasks if task.name == "w90_bands")
    phonon_task = next(task for task in wg.tasks if task.name == "ph_base")
    epw_task = next(task for task in wg.tasks if task.name == "epw_base")
    restored_wannier_task = next(
        task for task in restored.tasks if task.name == "w90_bands"
    )
    restored_phonon_task = next(
        task for task in restored.tasks if task.name == "ph_base"
    )
    restored_epw_task = next(task for task in restored.tasks if task.name == "epw_base")

    assert wannier_task.inputs["scf"]["pw"]["code"].value == codes["pw"]
    assert (
        wannier_task.inputs["scf"]["pw"]["metadata"]["options"]["account"].value
        == "elph"
    )
    assert (
        wannier_task.inputs["scf"]["pw"]["metadata"]["options"]["queue_name"].value
        == "debug"
    )
    assert (
        wannier_task.inputs["nscf"]["pw"]["metadata"]["options"]["account"].value
        == "elph"
    )
    assert (
        wannier_task.inputs["pw2wannier90"]["pw2wannier90"]["metadata"]["options"][
            "account"
        ].value
        == "elph"
    )
    assert (
        wannier_task.inputs["wannier90"]["wannier90"]["metadata"]["options"][
            "account"
        ].value
        == "elph"
    )
    assert phonon_task.inputs["ph"]["metadata"]["options"]["account"].value == "elph"
    assert epw_task.inputs["options"].value["account"] == "elph"
    assert epw_task.inputs["options"].value["queue_name"] == "debug"
    assert (
        restored_wannier_task.inputs["scf"]["pw"]["metadata"]["options"][
            "account"
        ].value
        == "elph"
    )
    assert (
        restored_wannier_task.inputs["nscf"]["pw"]["metadata"]["options"][
            "account"
        ].value
        == "elph"
    )
    assert (
        restored_wannier_task.inputs["pw2wannier90"]["pw2wannier90"]["metadata"][
            "options"
        ]["account"].value
        == "elph"
    )
    assert (
        restored_wannier_task.inputs["wannier90"]["wannier90"]["metadata"]["options"][
            "account"
        ].value
        == "elph"
    )
    assert (
        restored_phonon_task.inputs["ph"]["metadata"]["options"]["account"].value
        == "elph"
    )
    assert restored_epw_task.inputs["options"].value["account"] == "elph"

    wannier_engine = engine_inputs["tasks"]["w90_bands"]
    phonon_engine = engine_inputs["tasks"]["ph_base"]
    epw_engine = engine_inputs["tasks"]["epw_base"]

    assert wannier_engine["scf"]["pw"]["metadata"]["options"]["account"] == "elph"
    assert wannier_engine["nscf"]["pw"]["metadata"]["options"]["account"] == "elph"
    assert (
        wannier_engine["pw2wannier90"]["pw2wannier90"]["metadata"]["options"]["account"]
        == "elph"
    )
    assert (
        wannier_engine["wannier90"]["wannier90"]["metadata"]["options"]["account"]
        == "elph"
    )
    assert phonon_engine["ph"]["metadata"]["options"]["account"] == "elph"
    assert epw_engine["options"]["account"] == "elph"

    manager = TaskManager.__new__(TaskManager)
    manager.process = AttributeDict({"wg": restored})
    manager.ctx = AttributeDict(
        {
            "_task_results": {
                "generate_reciprocal_points": {
                    "kpoints_scf": orm.KpointsData(),
                    "qpoints": orm.KpointsData(),
                    "kpoints_nscf": orm.KpointsData(),
                },
                "prepare_wannier90_runtime_inputs": {
                    "nscf_kpoints": _create_explicit_kpoints([2, 2, 2]),
                    "wannier90_kpoints": _create_explicit_kpoints([2, 2, 2]),
                    "parameters": orm.Dict({"bands_plot": True, "mp_grid": [2, 2, 2]}),
                },
            }
        }
    )
    manager.ctx._task_results["generate_reciprocal_points"][
        "kpoints_scf"
    ].set_kpoints_mesh([2, 2, 2])
    manager.ctx._task_results["generate_reciprocal_points"]["qpoints"].set_kpoints_mesh(
        [1, 1, 1]
    )
    manager.ctx._task_results["generate_reciprocal_points"][
        "kpoints_nscf"
    ].set_kpoints_mesh([2, 2, 2])

    runtime_inputs = manager.get_inputs("w90_bands")["kwargs"]
    assert runtime_inputs["scf"]["pw"]["metadata"]["options"]["account"] == "elph"
    assert runtime_inputs["nscf"]["pw"]["metadata"]["options"]["account"] == "elph"
    assert (
        runtime_inputs["pw2wannier90"]["pw2wannier90"]["metadata"]["options"]["account"]
        == "elph"
    )
    assert (
        runtime_inputs["wannier90"]["wannier90"]["metadata"]["options"]["account"]
        == "elph"
    )
    assert len(runtime_inputs["nscf"]["kpoints"].get_kpoints()) == 8
    assert len(runtime_inputs["wannier90"]["wannier90"]["kpoints"].get_kpoints()) == 8
    json.dumps(wg.to_widget_value())


def test_high_level_workgraph_task_keeps_nested_metadata_at_runtime(fixture_code):
    """A high-level workchain task should preserve nested scheduler metadata when submitted."""
    from aiida.engine.utils import instantiate_process
    from aiida.manage.manager import get_manager

    nested_task = task(NestedMetadataWorkChain)
    code = fixture_code("arithmetic.add")
    scf_inputs = {
        "code": code,
        "x": orm.Int(1),
        "y": orm.Int(2),
        "metadata": {
            "options": {
                "resources": {"num_machines": 1, "num_mpiprocs_per_machine": 1},
                "max_wallclock_seconds": 3600,
                "withmpi": False,
                "account": "elph",
                "queue_name": "debug",
            }
        },
    }

    direct_process = instantiate_process(
        get_manager().get_runner(), NestedMetadataWorkChain, scf=scf_inputs
    )
    assert direct_process.inputs.scf.metadata.options.account == "elph"
    assert direct_process.inputs.scf.metadata.options.queue_name == "debug"
    assert (
        direct_process.node.base.attributes.get("metadata_inputs")["scf"]["metadata"][
            "options"
        ]["account"]
        == "elph"
    )
    assert (
        direct_process.node.base.attributes.get("metadata_inputs")["scf"]["metadata"][
            "options"
        ]["queue_name"]
        == "debug"
    )
    direct_process.close()

    with WorkGraph(name="nested_runtime_metadata") as wg:
        task_node = nested_task(
            metadata={"store_provenance": True},
            scf=scf_inputs,
        )

    wg.run()

    process_node = wg.tasks[task_node._task.name].process
    assert process_node is not None
    metadata_inputs = process_node.base.attributes.get("metadata_inputs")
    assert metadata_inputs["scf"]["metadata"]["options"]["account"] == "elph"
    assert metadata_inputs["scf"]["metadata"]["options"]["queue_name"] == "debug"


def test_high_level_workgraph_task_keeps_double_nested_metadata_at_runtime(
    fixture_code,
):
    """A high-level workchain task should preserve ``scf.pw.metadata`` style inputs."""
    from aiida.engine.utils import instantiate_process
    from aiida.manage.manager import get_manager

    nested_task = task(DoubleNestedMetadataWorkChain)
    code = fixture_code("arithmetic.add")
    scf_inputs = {
        "pw": {
            "code": code,
            "x": orm.Int(1),
            "y": orm.Int(2),
            "metadata": {
                "options": {
                    "resources": {"num_machines": 1, "num_mpiprocs_per_machine": 1},
                    "max_wallclock_seconds": 3600,
                    "withmpi": False,
                    "account": "elph",
                    "queue_name": "debug",
                }
            },
        }
    }

    direct_process = instantiate_process(
        get_manager().get_runner(),
        DoubleNestedMetadataWorkChain,
        scf=scf_inputs,
    )
    direct_meta = direct_process.node.base.attributes.get("metadata_inputs")
    assert direct_meta["scf"]["pw"]["metadata"]["options"]["account"] == "elph"
    assert direct_meta["scf"]["pw"]["metadata"]["options"]["queue_name"] == "debug"
    direct_process.close()

    with WorkGraph(name="double_nested_runtime_metadata") as wg:
        task_node = nested_task(
            metadata={"store_provenance": True},
            scf=scf_inputs,
        )

    wg.run()

    process_node = wg.tasks[task_node._task.name].process
    assert process_node is not None
    metadata_inputs = process_node.base.attributes.get("metadata_inputs")
    assert metadata_inputs["scf"]["pw"]["metadata"]["options"]["account"] == "elph"
    assert metadata_inputs["scf"]["pw"]["metadata"]["options"]["queue_name"] == "debug"


def test_workgraph_to_nested_workchains_preserves_account_to_calcjob(fixture_code):
    """Nested workchains should keep ``scf.pw.metadata.options.account`` all the way to the CalcJob."""
    nested_task = task(OuterSubmitInnerWorkChain)
    code = fixture_code("arithmetic.add")
    scf_inputs = {
        "pw": {
            "code": code,
            "x": orm.Int(1),
            "y": orm.Int(2),
            "metadata": {
                "options": {
                    "resources": {"num_machines": 1, "num_mpiprocs_per_machine": 1},
                    "max_wallclock_seconds": 3600,
                    "withmpi": False,
                    "account": "elph",
                    "queue_name": "debug",
                }
            },
        }
    }

    with WorkGraph(name="nested_submit_chain_metadata") as wg:
        task_node = nested_task(
            metadata={"store_provenance": True},
            scf=scf_inputs,
        )

    wg.run()

    outer_node = wg.tasks[task_node._task.name].process
    assert outer_node is not None
    outer_meta = outer_node.base.attributes.get("metadata_inputs")
    assert outer_meta["scf"]["pw"]["metadata"]["options"]["account"] == "elph"
    assert outer_meta["scf"]["pw"]["metadata"]["options"]["queue_name"] == "debug"

    inner_links = outer_node.base.links.get_outgoing(link_type=LinkType.CALL_WORK).all()
    assert len(inner_links) == 1
    inner_node = inner_links[0].node
    inner_meta = inner_node.base.attributes.get("metadata_inputs")
    assert inner_meta["pw"]["metadata"]["options"]["account"] == "elph"
    assert inner_meta["pw"]["metadata"]["options"]["queue_name"] == "debug"

    calc_links = inner_node.base.links.get_outgoing(link_type=LinkType.CALL_CALC).all()
    assert len(calc_links) == 1
    calc_node = calc_links[0].node
    assert calc_node.get_option("account") == "elph"
    assert calc_node.get_option("queue_name") == "debug"
