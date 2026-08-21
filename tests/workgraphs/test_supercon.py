"""Tests for ``aiida_epw.workgraphs.supercon``."""

from types import SimpleNamespace
from importlib import import_module

import pytest
from aiida import orm
from aiida.common import AttributeDict

supercon_module = import_module("aiida_epw.workgraphs.supercon")


def _metadata_dict(
    *,
    account: str = "elph",
    queue_name: str = "debug",
    num_mpiprocs_per_machine: int = 32,
    max_wallclock_seconds: int = 1800,
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


def _fake_epw_builder(code):
    """Return a minimal EPW builder-like namespace."""
    return AttributeDict(
        {
            "code": code,
            "parameters": orm.Dict({"INPUTEPW": {"eliashberg": True}}),
            "options": orm.Dict(_metadata_dict()["options"]),
            "settings": orm.Dict({"CMDLINE": ["-nk", "2"]}),
            "parallelization": orm.Dict({"npool": 2}),
            "clean_workdir": orm.Bool(False),
            "max_iterations": orm.Int(2),
        }
    )


def test_supercon_workgraph_builds_with_fake_builders(
    fixture_code,
    generate_kpoints_mesh,
    generate_remote_data,
    generate_structure,
    fixture_localhost,
    monkeypatch,
):
    """The supercon graph should build and wire the protocol-derived EPW inputs."""
    code = fixture_code("epw.epw")
    parent_folder_epw = generate_remote_data(fixture_localhost, "/remote/epw")
    structure = generate_structure()
    kpoints = generate_kpoints_mesh([6, 6, 6])
    qpoints = generate_kpoints_mesh([3, 3, 3])
    parent_epw = SimpleNamespace(
        process_label="EpwBaseWorkChain",
        inputs=SimpleNamespace(
            structure=structure,
            kpoints=kpoints,
            qpoints=qpoints,
        ),
    )

    monkeypatch.setattr(
        supercon_module,
        "get_protocol_inputs",
        lambda protocol=None, overrides=None: {
            "interpolation_distance": [0.2, 0.1],
            "kfpoints_factor": 3,
            "convergence_threshold": 0.05,
            "always_run_final": True,
            "epw_interp": {
                "settings": {"CMDLINE": ["-nk", "2"]},
            },
            "epw_final_iso": {
                "settings": {"CMDLINE": ["-nk", "2"]},
            },
            "epw_final_aniso": {
                "settings": {"CMDLINE": ["-nk", "2"]},
            },
        },
    )
    monkeypatch.setattr(
        supercon_module.EpwBaseWorkChain,
        "get_builder_from_protocol",
        lambda code, **kwargs: _fake_epw_builder(code),
    )

    wg = supercon_module.supercon.build(
        code=code,
        parent_epw=parent_epw,
        parent_folder_epw=parent_folder_epw,
        protocol="fast",
        overrides={},
    )

    epw_tasks = [task for task in wg.tasks if task.name.startswith("EpwBaseWorkChain")]
    assert len(epw_tasks) == 4

    interp_01 = epw_tasks[0]
    interp_02 = epw_tasks[1]
    final_iso = epw_tasks[2]
    final_aniso = epw_tasks[3]

    assert interp_01.inputs.code.value.uuid == code.uuid
    assert interp_02.inputs.code.value.uuid == code.uuid
    assert final_iso.inputs.code.value.uuid == code.uuid
    assert final_aniso.inputs.code.value.uuid == code.uuid
    assert interp_01.inputs.structure.value.uuid == structure.uuid
    assert interp_01.inputs.parent_folder_epw.value.uuid == parent_folder_epw.uuid
    assert interp_01.inputs.kpoints.value.get_kpoints_mesh()[0] == [6, 6, 6]
    assert interp_01.inputs.qpoints.value.get_kpoints_mesh()[0] == [3, 3, 3]
    assert interp_01.inputs.kfpoints_factor.value == 3
    assert interp_01.inputs.qfpoints_distance.value == 0.1
    assert interp_02.inputs.qfpoints_distance.value == 0.2


def test_supercon_workgraph_uses_prep_graph_stash_output_as_restart_parent(
    fixture_code,
    generate_kpoints_mesh,
    generate_remote_data,
    generate_structure,
    fixture_localhost,
    monkeypatch,
):
    """A prep workgraph parent should contribute `epw_stash` as the EPW restart folder."""
    code = fixture_code("epw.epw")
    epw_stash = generate_remote_data(fixture_localhost, "/remote/epw-stash")
    structure = generate_structure()
    kpoints = generate_kpoints_mesh([6, 6, 6])
    qpoints = generate_kpoints_mesh([3, 3, 3])
    parent_prep_graph = SimpleNamespace(
        process_label="WorkGraph<prep>",
        inputs=SimpleNamespace(structure=structure),
        outputs=SimpleNamespace(
            epw_stash=epw_stash,
        ),
        base=SimpleNamespace(),
    )

    monkeypatch.setattr(
        supercon_module,
        "get_protocol_inputs",
        lambda protocol=None, overrides=None: {
            "interpolation_distance": [0.1],
            "kfpoints_factor": 2,
            "always_run_final": False,
            "epw_interp": {},
            "epw_final_iso": {},
            "epw_final_aniso": {},
        },
    )
    monkeypatch.setattr(
        supercon_module.EpwBaseWorkChain,
        "get_builder_from_protocol",
        lambda code, **kwargs: _fake_epw_builder(code),
    )
    monkeypatch.setattr(
        supercon_module,
        "_get_prep_reciprocal_points",
        lambda _parent: AttributeDict({"kpoints": kpoints, "qpoints": qpoints}),
    )
    monkeypatch.setattr(
        supercon_module,
        "_get_prep_structure",
        lambda _parent: structure,
    )

    wg = supercon_module.supercon.build(
        code=code,
        parent_epw=parent_prep_graph,
        protocol="fast",
        overrides={},
    )

    epw_tasks = [task for task in wg.tasks if task.name.startswith("EpwBaseWorkChain")]
    assert len(epw_tasks) >= 1
    assert epw_tasks[0].inputs.parent_folder_epw.value.uuid == epw_stash.uuid
    assert epw_tasks[0].inputs.kpoints.value.get_kpoints_mesh()[0] == [6, 6, 6]
    assert epw_tasks[0].inputs.qpoints.value.get_kpoints_mesh()[0] == [3, 3, 3]


def test_supercon_workgraph_falls_back_to_prep_epw_folder_when_stash_missing(
    fixture_code,
    generate_kpoints_mesh,
    generate_remote_data,
    generate_structure,
    fixture_localhost,
    monkeypatch,
):
    """A prep workgraph parent should fall back to `epw_folder` when `epw_stash` is unavailable."""
    code = fixture_code("epw.epw")
    epw_folder = generate_remote_data(fixture_localhost, "/remote/epw-folder")
    structure = generate_structure()
    kpoints = generate_kpoints_mesh([6, 6, 6])
    qpoints = generate_kpoints_mesh([3, 3, 3])
    parent_prep_graph = SimpleNamespace(
        process_label="WorkGraph<prep>",
        outputs=SimpleNamespace(epw_folder=epw_folder),
        base=SimpleNamespace(),
    )

    monkeypatch.setattr(
        supercon_module,
        "get_protocol_inputs",
        lambda protocol=None, overrides=None: {
            "interpolation_distance": [0.1],
            "kfpoints_factor": 2,
            "always_run_final": False,
            "epw_interp": {},
            "epw_final_iso": {},
            "epw_final_aniso": {},
        },
    )
    monkeypatch.setattr(
        supercon_module.EpwBaseWorkChain,
        "get_builder_from_protocol",
        lambda code, **kwargs: _fake_epw_builder(code),
    )
    monkeypatch.setattr(
        supercon_module,
        "_get_prep_reciprocal_points",
        lambda _parent: AttributeDict({"kpoints": kpoints, "qpoints": qpoints}),
    )
    monkeypatch.setattr(
        supercon_module,
        "_get_prep_structure",
        lambda _parent: structure,
    )
    monkeypatch.setattr(
        supercon_module,
        "_get_prep_epw_base",
        lambda _parent: SimpleNamespace(
            inputs=SimpleNamespace(clean_workdir=orm.Bool(False))
        ),
    )

    wg = supercon_module.supercon.build(
        code=code,
        parent_epw=parent_prep_graph,
        protocol="fast",
        overrides={},
    )

    epw_tasks = [task for task in wg.tasks if task.name.startswith("EpwBaseWorkChain")]
    assert len(epw_tasks) >= 1
    assert epw_tasks[0].inputs.parent_folder_epw.value.uuid == epw_folder.uuid


def test_prep_epw_folder_fallback_rejects_cleaned_remote_folder(
    generate_remote_data, fixture_localhost
):
    """The fallback to `epw_folder` should fail if the prep EPW task cleaned its remote folder."""
    epw_folder = generate_remote_data(fixture_localhost, "/remote/epw-folder")
    parent_prep_graph = SimpleNamespace(
        outputs=SimpleNamespace(epw_folder=epw_folder),
        base=SimpleNamespace(),
    )

    clean_epw_base = SimpleNamespace(
        inputs=SimpleNamespace(clean_workdir=orm.Bool(True))
    )

    original_getter = supercon_module._get_prep_epw_base
    try:
        supercon_module._get_prep_epw_base = lambda _parent: clean_epw_base
        with pytest.raises(ValueError, match="clean_workdir=True"):
            supercon_module._get_prep_restart_parent_folder(parent_prep_graph)
    finally:
        supercon_module._get_prep_epw_base = original_getter
