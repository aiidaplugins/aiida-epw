from pathlib import Path

import pytest

from aiida import orm
from aiida.common.datastructures import StashMode
from aiida.common import exceptions
from aiida.common.warnings import AiidaDeprecationWarning
from aiida_quantumespresso.calculations.ph import PhCalculation
from aiida_quantumespresso.calculations.pw import PwCalculation

from aiida_epw.calculations.epw import EpwCalculation


def generate_kpoints_mesh(mesh):
    """Return a `KpointsData` with the provided mesh."""
    kpoints = orm.KpointsData()
    kpoints.set_kpoints_mesh(mesh)
    return kpoints


def generate_kpoints_list(points):
    """Return a `KpointsData` with the provided explicit point list."""
    kpoints = orm.KpointsData()
    kpoints.set_kpoints(points)
    return kpoints


@pytest.fixture
def generate_inputs_epw(fixture_code):
    """Return basic inputs for `EpwCalculation`."""

    def _generate_inputs(parameters=None, **overrides):
        inputs = {
            "code": fixture_code("epw.epw"),
            "parameters": orm.Dict(parameters or {"INPUTEPW": {}}),
            "kpoints": generate_kpoints_mesh([2, 2, 2]),
            "qpoints": generate_kpoints_mesh([2, 2, 1]),
            "kfpoints": generate_kpoints_mesh([4, 4, 4]),
            "qfpoints": generate_kpoints_mesh([4, 4, 2]),
            "metadata": {
                "options": {
                    "resources": {
                        "num_machines": 1,
                        "num_mpiprocs_per_machine": 1,
                    },
                    "max_wallclock_seconds": 1800,
                }
            },
        }
        inputs.update(overrides)
        return inputs

    return _generate_inputs


def test_epw_default_mesh_inputs(
    fixture_sandbox, generate_calc_job, generate_inputs_epw
):
    """Test that plugin-managed meshes are written to the EPW input file."""
    calc_info = generate_calc_job(fixture_sandbox, "epw.epw", generate_inputs_epw())

    input_contents = Path(fixture_sandbox.abspath, "aiida.in").read_text()

    assert "outdir = './out/'" in input_contents
    assert "dvscf_dir = 'save'" in input_contents
    assert "prefix = 'aiida'" in input_contents
    assert "nk1 = 2" in input_contents
    assert "nq3 = 1" in input_contents
    assert "nkf3 = 4" in input_contents
    assert "nqf2 = 4" in input_contents
    assert calc_info.retrieve_list == ["aiida.out"]
    assert calc_info.retrieve_temporary_list == []
    assert calc_info.retrieve_singlefile_list == []
    assert calc_info.codes_info[0].stdout_name == "aiida.out"


def test_epw_writes_explicit_fine_point_files(
    fixture_sandbox, generate_calc_job, generate_inputs_epw
):
    """Test that explicit fine grids are written to separate point-list files."""
    inputs = generate_inputs_epw(
        kfpoints=generate_kpoints_list([[0.0, 0.0, 0.0], [0.5, 0.5, 0.0]]),
        qfpoints=generate_kpoints_list([[0.0, 0.0, 0.0], [0.0, 0.5, 0.5]]),
    )

    generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    input_contents = Path(fixture_sandbox.abspath, "aiida.in").read_text()
    kfpoints_contents = Path(fixture_sandbox.abspath, "kfpoints.kpt").read_text()
    qfpoints_contents = Path(fixture_sandbox.abspath, "qfpoints.kpt").read_text()

    assert "filkf = 'kfpoints.kpt'" in input_contents
    assert "filqf = 'qfpoints.kpt'" in input_contents
    assert "nkf1" not in input_contents
    assert "nqf1" not in input_contents
    assert kfpoints_contents.splitlines()[0] == "2 crystal"
    assert qfpoints_contents.splitlines()[0] == "2 crystal"


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("outdir", "./custom-out"),
        ("nk1", 8),
    ],
)
def test_epw_rejects_plugin_managed_keywords(
    fixture_sandbox, generate_calc_job, generate_inputs_epw, key, value
):
    """Test that users cannot override keywords managed by the plugin."""
    inputs = generate_inputs_epw(parameters={"INPUTEPW": {key: value}})

    with pytest.raises(ValueError, match=rf"parameters\.INPUTEPW\.{key}"):
        generate_calc_job(fixture_sandbox, "epw.epw", inputs)


def test_epw_accepts_user_filkf_filqf(
    fixture_sandbox, generate_calc_job, generate_inputs_epw
):
    """Test that users can specify custom filkf and filqf names and the grid files are written accordingly."""
    inputs = generate_inputs_epw(
        kfpoints=generate_kpoints_list([[0.0, 0.0, 0.0], [0.5, 0.5, 0.0]]),
        qfpoints=generate_kpoints_list([[0.0, 0.0, 0.0], [0.0, 0.5, 0.5]]),
        parameters={
            "INPUTEPW": {
                "filkf": "user_kfpoints.kpt",
                "filqf": "user_qfpoints.kpt",
            }
        },
    )

    generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    input_contents = Path(fixture_sandbox.abspath, "aiida.in").read_text()
    kfpoints_contents = Path(fixture_sandbox.abspath, "user_kfpoints.kpt").read_text()
    qfpoints_contents = Path(fixture_sandbox.abspath, "user_qfpoints.kpt").read_text()

    assert "filkf = 'user_kfpoints.kpt'" in input_contents
    assert "filqf = 'user_qfpoints.kpt'" in input_contents
    assert kfpoints_contents.splitlines()[0] == "2 crystal"
    assert qfpoints_contents.splitlines()[0] == "2 crystal"


def test_epw_allows_user_amass_parameter(
    fixture_sandbox, generate_calc_job, generate_inputs_epw
):
    """Test that `amass` remains user-configurable."""
    inputs = generate_inputs_epw(parameters={"INPUTEPW": {"amass": 28.085}})

    generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    input_contents = Path(fixture_sandbox.abspath, "aiida.in").read_text()
    assert "amass" in input_contents


def test_epw_parallelization_flags_are_added_to_cmdline(
    fixture_sandbox, generate_calc_job, generate_inputs_epw
):
    """Test that the explicit `parallelization` input is translated to cmdline flags."""
    inputs = generate_inputs_epw(parallelization=orm.Dict({"npool": 2, "nimage": 4}))

    calc_info = generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    assert calc_info.codes_info[0].cmdline_params == [
        "-nimage",
        "4",
        "-npool",
        "2",
        "-in",
        "aiida.in",
    ]


def test_epw_accepts_parser_options_setting(
    fixture_sandbox, generate_calc_job, generate_inputs_epw
):
    """Test that parser settings are ignored instead of failing submission validation."""
    inputs = generate_inputs_epw(
        settings=orm.Dict({"parser_options": {"include_xml": False}})
    )

    calc_info = generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    assert calc_info.retrieve_list == ["aiida.out"]


def test_epw_accepts_manual_proj_for_wannierize(
    fixture_sandbox,
    fixture_localhost,
    generate_calc_job,
    generate_inputs_epw,
    generate_remote_data,
):
    """Direct EPW Wannierization should accept manual `proj` lists."""
    inputs = generate_inputs_epw(
        parameters={"INPUTEPW": {"wannierize": True, "proj": ["Si:s", "Si:p"]}},
        parent_folder_nscf=generate_remote_data(fixture_localhost, "/remote/nscf"),
    )

    generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    input_contents = Path(fixture_sandbox.abspath, "aiida.in").read_text()
    assert "wannierize = .true." in input_contents
    assert "proj(1) = 'Si:s'" in input_contents
    assert "proj(2) = 'Si:p'" in input_contents


def test_epw_requires_nscf_parent_for_wannierize(
    fixture_sandbox, generate_calc_job, generate_inputs_epw
):
    """Direct EPW Wannierization should always stage an NSCF parent."""
    inputs = generate_inputs_epw(
        parameters={"INPUTEPW": {"wannierize": True, "proj": ["Si:s"]}}
    )

    with pytest.raises(ValueError, match="parent_folder_nscf"):
        generate_calc_job(fixture_sandbox, "epw.epw", inputs)


def test_epw_rejects_auto_projections_for_wannierize(
    fixture_sandbox,
    fixture_localhost,
    generate_calc_job,
    generate_inputs_epw,
    generate_remote_data,
):
    """Only manual `proj` entries are supported for EPW Wannierization."""
    inputs = generate_inputs_epw(
        parameters={
            "INPUTEPW": {"wannierize": True, "auto_projections": True, "proj": ["Si:s"]}
        },
        parent_folder_nscf=generate_remote_data(fixture_localhost, "/remote/nscf"),
    )

    with pytest.raises(ValueError, match="auto_projections"):
        generate_calc_job(fixture_sandbox, "epw.epw", inputs)


def test_epw_requires_manual_proj_for_wannierize(
    fixture_sandbox,
    fixture_localhost,
    generate_calc_job,
    generate_inputs_epw,
    generate_remote_data,
):
    """Direct EPW Wannierization should require explicit manual projections."""
    inputs = generate_inputs_epw(
        parameters={"INPUTEPW": {"wannierize": True}},
        parent_folder_nscf=generate_remote_data(fixture_localhost, "/remote/nscf"),
    )

    with pytest.raises(ValueError, match="Manual `proj` entries"):
        generate_calc_job(fixture_sandbox, "epw.epw", inputs)


def test_epw_additional_retrieve_list_emits_deprecation_warning(
    fixture_sandbox, generate_calc_job, generate_inputs_epw
):
    """Test that legacy additional retrieve settings match QE namelist behavior."""
    inputs = generate_inputs_epw(
        settings=orm.Dict({"ADDITIONAL_RETRIEVE_LIST": ["custom.dat"]})
    )

    with pytest.warns(AiidaDeprecationWarning) as captured_warnings:
        calc_info = generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    assert calc_info.retrieve_list == ["aiida.out", "custom.dat"]
    assert any(
        "ADDITIONAL_RETRIEVE_LIST" in str(warning.message)
        for warning in captured_warnings.list
    )


def test_epw_rejects_invalid_parallelization_flag(
    fixture_sandbox, generate_calc_job, generate_inputs_epw
):
    """Test that unknown parallelization flags are rejected at validation time."""
    inputs = generate_inputs_epw(parallelization=orm.Dict({"unknown": 2}))

    with pytest.raises(ValueError, match="Unknown flags in `parallelization`"):
        generate_calc_job(fixture_sandbox, "epw.epw", inputs)


def test_epw_rejects_parallelization_conflict_with_cmdline(
    fixture_sandbox, generate_calc_job, generate_inputs_epw
):
    """Test that `parallelization` and raw cmdline flags cannot specify the same option twice."""
    inputs = generate_inputs_epw(
        parallelization=orm.Dict({"npool": 2}),
        settings=orm.Dict({"CMDLINE": ["-nk", "2"]}),
    )

    with pytest.raises(exceptions.InputValidationError, match="conflicts"):
        generate_calc_job(fixture_sandbox, "epw.epw", inputs)


@pytest.mark.parametrize("flag_name", ["npool", "nk", "nimage"])
def test_epw_parallelization_cmdline_deprecation_warning(
    fixture_sandbox, generate_calc_job, generate_inputs_epw, flag_name
):
    """Test that manual parallelization flags in CMDLINE emit the QE-style deprecation warning."""
    extra_cmdline_args = [f"-{flag_name}", "2"]
    inputs = generate_inputs_epw(settings=orm.Dict({"CMDLINE": extra_cmdline_args}))

    with pytest.warns(AiidaDeprecationWarning) as captured_warnings:
        calc_info = generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    assert calc_info.codes_info[0].cmdline_params == extra_cmdline_args + [
        "-in",
        "aiida.in",
    ]
    assert any(
        "parallelization flags" in str(warning.message)
        for warning in captured_warnings.list
    )


def test_epw_rejects_duplicate_parallelization_aliases_in_cmdline(
    fixture_sandbox, generate_calc_job, generate_inputs_epw
):
    """Test that duplicate aliases to the same parallelization flag are rejected."""
    inputs = generate_inputs_epw(
        settings=orm.Dict({"CMDLINE": ["-nk", "2", "-npools", "2"]})
    )

    with pytest.raises(exceptions.InputValidationError, match="Conflicting"):
        generate_calc_job(fixture_sandbox, "epw.epw", inputs)


@pytest.mark.parametrize("input_name", ["parent_folder_chk", "parent_folder_epw"])
def test_epw_rejects_wannierize_with_restart_parent(
    fixture_sandbox,
    fixture_localhost,
    generate_calc_job,
    generate_inputs_epw,
    generate_remote_data,
    input_name,
):
    """Test that wannierization cannot be mixed with restart parents."""
    inputs = generate_inputs_epw(parameters={"INPUTEPW": {"wannierize": True}})
    inputs[input_name] = generate_remote_data(
        fixture_localhost, fixture_sandbox.abspath
    )

    with pytest.raises(ValueError, match="wannierize"):
        generate_calc_job(fixture_sandbox, "epw.epw", inputs)


def test_epw_rejects_generic_parent_folder(
    fixture_sandbox,
    fixture_localhost,
    generate_calc_job,
    generate_inputs_epw,
    generate_remote_data,
):
    """Test that the inherited generic parent-folder port is not accepted."""
    inputs = generate_inputs_epw(
        parent_folder=generate_remote_data(fixture_localhost, fixture_sandbox.abspath)
    )

    with pytest.raises(ValueError, match="parent_folder"):
        generate_calc_job(fixture_sandbox, "epw.epw", inputs)


def test_epw_stages_nscf_parent_output_folder(
    fixture_sandbox,
    fixture_localhost,
    generate_calc_job,
    generate_inputs_epw,
    generate_remote_data,
):
    """Test that the NSCF parent contributes the QE output directory by copy."""
    parent_folder = generate_remote_data(
        fixture_localhost, "/remote/nscf", "quantumespresso.pw"
    )
    inputs = generate_inputs_epw(parent_folder_nscf=parent_folder)

    calc_info = generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    assert (
        parent_folder.computer.uuid,
        Path(
            parent_folder.get_remote_path(), PwCalculation._OUTPUT_SUBFOLDER
        ).as_posix(),
        EpwCalculation._OUTPUT_SUBFOLDER,
    ) in calc_info.remote_copy_list


def test_epw_stages_chk_parent_into_requested_transport_list(
    fixture_sandbox,
    fixture_localhost,
    generate_calc_job,
    generate_inputs_epw,
    generate_remote_data,
):
    """Test that Wannier checkpoint files follow the selected copy/symlink mode."""
    parent_folder = generate_remote_data(fixture_localhost, "/remote/chk")
    inputs = generate_inputs_epw(
        parent_folder_chk=parent_folder,
        settings=orm.Dict({"PARENT_FOLDER_SYMLINK": True}),
    )

    calc_info = generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    expected = {
        (
            parent_folder.computer.uuid,
            Path(parent_folder.get_remote_path(), "aiida.chk").as_posix(),
            "aiida.chk",
        ),
        (
            parent_folder.computer.uuid,
            Path(parent_folder.get_remote_path(), "aiida.bvec").as_posix(),
            "aiida.bvec",
        ),
        (
            parent_folder.computer.uuid,
            Path(parent_folder.get_remote_path(), "aiida.mmn").as_posix(),
            "aiida.wannier90.mmn",
        ),
    }

    assert expected.issubset(set(calc_info.remote_symlink_list))
    assert not expected.intersection(set(calc_info.remote_copy_list))


def test_epw_adds_w90_chk_to_ukk_script_to_prepend_text(
    fixture_sandbox,
    fixture_localhost,
    generate_calc_job,
    generate_inputs_epw,
    generate_remote_data,
):
    """The chk-to-ukk conversion command should be injected by the calcjob itself."""
    parent_folder = generate_remote_data(fixture_localhost, "/remote/chk")
    script = orm.RemoteData(
        computer=fixture_localhost,
        remote_path="/remote/bin/w90_chk2ukk.jl",
    ).store()
    inputs = generate_inputs_epw(
        parent_folder_chk=parent_folder,
        w90_chk_to_ukk_script=script,
    )

    calc_info = generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    assert "/remote/bin/w90_chk2ukk.jl" in calc_info.prepend_text
    assert "aiida.chk" in calc_info.prepend_text
    assert "aiida.ukk" in calc_info.prepend_text


def test_epw_stages_epw_restart_files_without_copying_epmatwp(
    fixture_sandbox,
    fixture_localhost,
    generate_calc_job,
    generate_inputs_epw,
    generate_remote_data,
):
    """Test that EPW restart staging links the large `epmatwp` file and copies metadata files."""
    parent_folder = generate_remote_data(fixture_localhost, "/remote/epw")
    inputs = generate_inputs_epw(
        parameters={"INPUTEPW": {"epwread": True, "elph": True}},
        parent_folder_epw=parent_folder,
    )

    calc_info = generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    assert (
        parent_folder.computer.uuid,
        Path(
            parent_folder.get_remote_path(),
            f"{EpwCalculation._OUTPUT_SUBFOLDER}/{EpwCalculation._PREFIX}.epmatwp",
        ).as_posix(),
        Path(
            f"{EpwCalculation._OUTPUT_SUBFOLDER}/{EpwCalculation._PREFIX}.epmatwp"
        ).as_posix(),
    ) in calc_info.remote_symlink_list

    expected_copied = {
        "crystal.fmt",
        "epwdata.fmt",
        "vmedata.fmt",
        "dmedata.fmt",
        "aiida.kgmap",
        "aiida.kmap",
        "aiida.ukk",
        "aiida.mmn",
        "aiida.bvec",
    }
    copied_targets = {entry[2] for entry in calc_info.remote_copy_list}
    assert expected_copied.issubset(copied_targets)


def test_epw_stages_ph_stash_folder_by_target_basepath(
    fixture_sandbox,
    fixture_localhost,
    generate_calc_job,
    generate_inputs_epw,
):
    """Test that stashed PH folders use their target base path instead of `get_remote_path()`."""
    parent_folder = orm.RemoteStashFolderData(
        stash_mode=StashMode.COPY,
        target_basepath="/stash/ph",
        source_list=["out", "DYN_MAT"],
    )
    parent_folder.computer = fixture_localhost

    inputs = generate_inputs_epw(
        parent_folder_ph=parent_folder,
        settings=orm.Dict({"NUMBER_OF_QPOINTS": 1}),
    )

    calc_info = generate_calc_job(fixture_sandbox, "epw.epw", inputs)

    assert (
        parent_folder.computer.uuid,
        Path(
            "/stash/ph", PhCalculation._OUTPUT_SUBFOLDER, "_ph0", "aiida.phsave"
        ).as_posix(),
        "save",
    ) in calc_info.remote_copy_list
