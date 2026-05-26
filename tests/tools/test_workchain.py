from types import SimpleNamespace

import pytest

from aiida_epw.tools.workchain import (
    find_related_calculation,
    get_parent_folder_calculation,
    get_parent_ph_pw_calculation,
    get_parent_ph_qpoints,
    structures_match,
    validate_parent_ph_inputs,
)


def test_get_parent_folder_calculation_returns_direct_creator():
    """Test that direct parent-folder creators are returned unchanged."""
    creator = SimpleNamespace(process_label="PhCalculation")
    parent_folder = SimpleNamespace(creator=creator)

    assert get_parent_folder_calculation(parent_folder) is creator


def test_get_parent_folder_calculation_unwraps_move_stash():
    """Test that stashed parent folders resolve to the original calculation."""
    creator = SimpleNamespace(process_label="PhCalculation")
    move_stash = SimpleNamespace(
        process_label="move_stash",
        inputs=SimpleNamespace(stash_data=SimpleNamespace(creator=creator)),
    )
    parent_folder = SimpleNamespace(creator=move_stash)

    assert get_parent_folder_calculation(parent_folder) is creator


def test_get_parent_folder_calculation_requires_creator():
    """Test that helper fails clearly for folders without provenance."""
    parent_folder = SimpleNamespace(creator=None)

    with pytest.raises(ValueError, match="does not have a creator"):
        get_parent_folder_calculation(parent_folder)


def test_find_related_calculation_rejects_non_epw_creator():
    """Test that EPW-specific helper still validates the resolved process label."""
    creator = SimpleNamespace(process_label="PhCalculation")
    parent_folder = SimpleNamespace(creator=creator)

    with pytest.raises(ValueError, match="not a valid epw calculation"):
        find_related_calculation(parent_folder)


def test_structures_match_distinguishes_equal_and_different_structures(
    generate_structure,
):
    """Structure comparison should accept equivalent inputs and reject changed cells."""
    from aiida import orm

    left = generate_structure()
    right = generate_structure()
    different = orm.StructureData(
        cell=[[5.5, 0.0, 0.0], [0.0, 5.5, 0.0], [0.0, 0.0, 5.5]]
    )
    different.append_atom(position=(0.0, 0.0, 0.0), symbols="Si")
    different.append_atom(position=(2.75, 2.75, 2.75), symbols="Si")

    assert structures_match(left, right)
    assert not structures_match(left, different)


def test_get_parent_ph_qpoints_requires_ph_creator():
    """The phonon helper should reject parent folders from unrelated calculations."""
    parent_folder = SimpleNamespace(
        creator=SimpleNamespace(process_label="PwCalculation")
    )

    with pytest.raises(ValueError, match="must be created by a `PhCalculation`"):
        get_parent_ph_qpoints(parent_folder)


def test_validate_parent_ph_inputs_checks_parent_pw_structure(generate_structure):
    """The phonon validation helper should compare the reused PW structure."""
    from aiida import orm

    qpoints = SimpleNamespace()
    matching_structure = generate_structure()
    target_structure = generate_structure()
    ph_parent_folder = SimpleNamespace(
        creator=SimpleNamespace(
            process_label="PhCalculation",
            inputs=SimpleNamespace(
                qpoints=qpoints,
                parent_folder=SimpleNamespace(
                    creator=SimpleNamespace(
                        process_label="PwCalculation",
                        inputs=SimpleNamespace(structure=matching_structure),
                    )
                ),
            ),
        )
    )

    assert validate_parent_ph_inputs(ph_parent_folder, target_structure) is qpoints

    mismatched_structure = orm.StructureData(
        cell=[
            [6.0, 0.0, 0.0],
            [0.0, 6.0, 0.0],
            [0.0, 0.0, 6.0],
        ]
    )
    mismatched_structure.append_atom(position=(0.0, 0.0, 0.0), symbols="Si")
    mismatched_structure.append_atom(position=(3.0, 3.0, 3.0), symbols="Si")

    with pytest.raises(ValueError, match="mismatched fields: structure"):
        validate_parent_ph_inputs(ph_parent_folder, mismatched_structure)


def test_validate_parent_ph_inputs_checks_parent_pw_runtime_details(generate_structure):
    """The phonon validation helper should compare SCF kpoints, parameters, and pseudos."""
    from aiida import orm

    qpoints = orm.KpointsData()
    qpoints.set_kpoints_mesh([2, 2, 2])
    scf_kpoints = orm.KpointsData()
    scf_kpoints.set_kpoints_mesh([4, 4, 4])
    parameters = orm.Dict({"SYSTEM": {"ecutwfc": 50}})
    pseudo = SimpleNamespace(
        base=SimpleNamespace(repository=SimpleNamespace(hash=lambda: "pseudo-hash"))
    )
    ph_parent_folder = SimpleNamespace(
        creator=SimpleNamespace(
            process_label="PhCalculation",
            inputs=SimpleNamespace(
                qpoints=qpoints,
                parent_folder=SimpleNamespace(
                    creator=SimpleNamespace(
                        process_label="PwCalculation",
                        inputs=SimpleNamespace(
                            structure=generate_structure(),
                            kpoints=scf_kpoints,
                            parameters=parameters,
                            pseudos={"Si": pseudo},
                        ),
                    )
                ),
            ),
        )
    )

    assert (
        validate_parent_ph_inputs(
            ph_parent_folder,
            generate_structure(),
            scf_kpoints=scf_kpoints,
            scf_parameters=parameters,
            scf_pseudos={"Si": pseudo},
        )
        is qpoints
    )

    wrong_kpoints = orm.KpointsData()
    wrong_kpoints.set_kpoints_mesh([6, 6, 6])

    with pytest.raises(ValueError, match="SCF kpoints"):
        validate_parent_ph_inputs(
            ph_parent_folder,
            generate_structure(),
            scf_kpoints=wrong_kpoints,
            scf_parameters=parameters,
            scf_pseudos={"Si": pseudo},
        )


def test_get_parent_ph_pw_calculation_walks_restart_chain():
    """The phonon helper should skip intermediate restarted `PhCalculation` parents."""
    original_pw = SimpleNamespace(
        process_label="PwCalculation", inputs=SimpleNamespace()
    )
    restarted_ph = SimpleNamespace(
        process_label="PhCalculation",
        inputs=SimpleNamespace(parent_folder=SimpleNamespace(creator=original_pw)),
    )
    latest_ph_parent = SimpleNamespace(
        creator=SimpleNamespace(
            process_label="PhCalculation",
            inputs=SimpleNamespace(
                qpoints=SimpleNamespace(),
                parent_folder=SimpleNamespace(creator=restarted_ph),
            ),
        )
    )

    assert get_parent_ph_pw_calculation(latest_ph_parent) is original_pw
