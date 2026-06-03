"""Tests for the ``DosData`` datatype."""

import pytest
from aiida.common import exceptions
from aiida.plugins import DataFactory

from aiida_epw.data import DosData, PDosData, PhDosData


def test_dos_data_roundtrip():
    """Test storing and retrieving a DOS dataset."""
    node = DosData()
    node.set_dos_data(
        energy=[10.0, 11.0, 12.0],
        dos=[0.5, 1.2, 0.8],
        integrated_dos=[0.05, 0.17, 0.25],
    )

    assert node.get_energy().tolist() == [10.0, 11.0, 12.0]
    assert node.get_dos().tolist() == [0.5, 1.2, 0.8]
    assert node.get_integrated_dos().tolist() == [0.05, 0.17, 0.25]


def test_dos_data_optional_integrated_dos():
    """Test storing and retrieving a DOS dataset without integrated DOS."""
    node = DosData()
    node.set_dos_data(
        energy=[10.0, 11.0, 12.0],
        dos=[0.5, 1.2, 0.8],
    )

    assert node.get_energy().tolist() == [10.0, 11.0, 12.0]
    assert node.get_dos().tolist() == [0.5, 1.2, 0.8]
    assert node.get_integrated_dos() is None

    # Verify the optional array is absent when not stored.
    with pytest.raises(KeyError):
        node.get_array("integrated_dos")


def test_dos_data_validates_shape_contract():
    """Test that invalid DOS payloads are rejected."""
    node = DosData()

    # Mismatched lengths
    with pytest.raises(exceptions.ValidationError):
        node.set_dos_data(
            energy=[10.0, 11.0],
            dos=[0.5, 1.2, 0.8],
        )

    # Inconsistent integrated_dos length
    with pytest.raises(exceptions.ValidationError):
        node.set_dos_data(
            energy=[10.0, 11.0, 12.0],
            dos=[0.5, 1.2, 0.8],
            integrated_dos=[0.05, 0.17],
        )

    # Multi-dimensional arrays
    with pytest.raises(exceptions.ValidationError):
        node.set_dos_data(
            energy=[[10.0]],
            dos=[0.5],
        )


def test_dos_data_entry_point():
    """Test the datatype is registered through ``aiida.data``."""
    assert DataFactory("epw.dos") is DosData


def test_dos_serialization_factories(files_path):
    """Test DosData.from_file and from_string classmethods."""
    dos_file = files_path / "tools" / "parsers" / "a2f" / "aiida.dos"

    # Test from_file
    node_file = DosData.from_file(dos_file)
    assert isinstance(node_file, DosData)
    assert node_file.get_energy().shape == (160,)
    assert node_file.get_dos().shape == (160,)
    assert node_file.get_integrated_dos().shape == (160,)
    assert node_file.get_energy()[0] == pytest.approx(10.871190406)

    # Test from_string
    content = dos_file.read_text(encoding="utf-8")
    node_str = DosData.from_string(content)
    assert isinstance(node_str, DosData)
    assert node_str.get_energy().tolist() == node_file.get_energy().tolist()


def test_phdos_data_roundtrip():
    """Test storing and retrieving a phonon DOS dataset."""
    node = PhDosData()
    node.set_phdos_data(
        frequency=[0.1, 0.2],
        phdos=[[1.0, 2.0], [3.0, 4.0]],
        num_smearings=2,
    )

    assert node.get_frequency().tolist() == [0.1, 0.2]
    assert node.get_phdos().tolist() == [[1.0, 2.0], [3.0, 4.0]]
    assert node.num_smearings == 2
    assert node.get_array("Frequency").tolist() == [0.1, 0.2]
    assert node.get_array("PHDOS").tolist() == [[1.0, 2.0], [3.0, 4.0]]


def test_phdos_data_validates_shape_contract():
    """Test invalid PhDosData payloads are rejected."""
    node = PhDosData()

    with pytest.raises(exceptions.ValidationError):
        node.set_phdos_data(
            frequency=[0.1, 0.2],
            phdos=[1.0, 2.0],
        )

    with pytest.raises(exceptions.ValidationError):
        node.set_phdos_data(
            frequency=[0.1, 0.2],
            phdos=[[1.0, 2.0]],
        )

    with pytest.raises(exceptions.ValidationError):
        node.set_phdos_data(
            frequency=[0.1, 0.2],
            phdos=[[1.0, 2.0], [3.0, 4.0]],
            num_smearings=3,
        )


def test_phdos_data_entry_point():
    """Test that PhDosData is registered under entry points."""
    assert DataFactory("epw.phdos") is PhDosData


def test_phdos_data_serialization_factories(files_path):
    """Test PhDosData.from_file and from_string classmethods."""
    phdos_file = files_path / "tools" / "parsers" / "a2f" / "aiida.phdos"

    node_file = PhDosData.from_file(phdos_file)
    assert isinstance(node_file, PhDosData)
    assert node_file.get_frequency().shape == (500,)
    assert node_file.get_phdos().shape == (500, 10)
    assert node_file.num_smearings == 10

    content = phdos_file.read_text(encoding="utf-8")
    node_str = PhDosData.from_string(content)
    assert isinstance(node_str, PhDosData)
    assert node_str.get_frequency().tolist() == node_file.get_frequency().tolist()


def test_pdos_data_roundtrip():
    """Test storing and retrieving projected DOS data."""
    node = PDosData()
    node.set_pdos_data(
        frequency=[0.1, 0.2],
        phdos=[1.0, 2.0],
        projected_phdos=[[0.1, 0.2], [0.3, 0.4]],
    )

    assert node.get_frequency().tolist() == [0.1, 0.2]
    assert node.get_phdos().tolist() == [1.0, 2.0]
    assert node.get_projected_phdos().tolist() == [[0.1, 0.2], [0.3, 0.4]]


def test_pdos_data_validates_shape_contract():
    """Test invalid PDosData shapes are rejected."""
    node = PDosData()

    with pytest.raises(exceptions.ValidationError):
        node.set_pdos_data(
            frequency=[0.1, 0.2],
            phdos=[1.0],  # Mismatched length
            projected_phdos=[[0.1, 0.2], [0.3, 0.4]],
        )

    with pytest.raises(exceptions.ValidationError):
        node.set_pdos_data(
            frequency=[0.1, 0.2],
            phdos=[1.0, 2.0],
            projected_phdos=[[0.1], [0.2], [0.3]],  # Mismatched length and shape
        )


def test_pdos_data_entry_point():
    """Test that PDosData is registered under entry points."""
    assert DataFactory("epw.pdos") is PDosData


def test_pdos_data_serialization_factories(files_path):
    """Test PDosData.from_file and from_string classmethods."""
    phdos_proj_file = files_path / "tools" / "parsers" / "a2f" / "aiida.phdos_proj"

    # Test from_file
    node_file = PDosData.from_file(phdos_proj_file)
    assert isinstance(node_file, PDosData)
    assert node_file.get_frequency().shape == (500,)
    assert node_file.get_phdos().shape == (500,)
    assert node_file.get_projected_phdos().shape == (500, 3)

    # Test from_string
    content = phdos_proj_file.read_text(encoding="utf-8")
    node_str = PDosData.from_string(content)
    assert isinstance(node_str, PDosData)
    assert node_str.get_frequency().tolist() == node_file.get_frequency().tolist()
