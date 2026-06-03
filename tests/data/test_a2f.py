"""Tests for the ``A2fData`` datatype."""

import numpy
import pytest
from aiida.common import exceptions
from aiida.plugins import DataFactory

from aiida_epw.data import A2fData, PA2fData


def test_a2f_data_roundtrip():
    """Test storing and retrieving an a2F spectrum."""
    node = A2fData()
    node.set_a2f_data(
        frequency=[0.1, 0.2],
        spectrum=[[1.0, 2.0], [3.0, 4.0]],
        lambda_values=[0.5, 0.6],
        phonon_smearing=[0.01, 0.02],
        electron_smearing=0.05,
        fermi_window=0.8,
        summed_elph_coupling=1.2,
    )

    assert node.get_frequency().tolist() == [0.1, 0.2]
    assert node.get_spectrum().tolist() == [[1.0, 2.0], [3.0, 4.0]]
    assert node.get_cumulative_lambda() is None
    assert node.get_lambda().tolist() == [0.5, 0.6]
    assert node.get_phonon_smearing().tolist() == [0.01, 0.02]
    assert node.get_array("degaussq").tolist() == [0.01, 0.02]
    assert node.electron_smearing == pytest.approx(0.05)
    assert node.fermi_window == pytest.approx(0.8)
    assert node.summed_elph_coupling == pytest.approx(1.2)


def test_a2f_data_splits_legacy_combined_columns():
    """Test the datatype can split EPW's combined `a2f` and cumulative lambda columns."""
    node = A2fData()
    node.set_a2f_data(
        frequency=[0.1, 0.2],
        spectrum=[
            [1.0, 2.0, 0.1, 0.2],
            [3.0, 4.0, 0.3, 0.4],
        ],
        lambda_values=[0.5, 0.6],
        phonon_smearing=[0.01, 0.02],
    )

    assert node.get_array("a2f").tolist() == [
        [1.0, 2.0, 0.1, 0.2],
        [3.0, 4.0, 0.3, 0.4],
    ]
    assert node.get_spectrum().tolist() == [[1.0, 2.0], [3.0, 4.0]]
    assert node.get_cumulative_lambda().tolist() == [[0.1, 0.2], [0.3, 0.4]]


def test_a2f_data_validates_shape_contract():
    """Test inconsistent a2F grids are rejected."""
    node = A2fData()

    with pytest.raises(exceptions.ValidationError):
        node.set_a2f_data(
            frequency=numpy.array([0.1, 0.2]),
            spectrum=numpy.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]),
            lambda_values=numpy.array([0.5]),
            phonon_smearing=numpy.array([0.01]),
        )


def test_a2f_data_entry_point():
    """Test the datatype is registered through ``aiida.data``."""
    assert DataFactory("epw.a2f") is A2fData


def test_a2f_data_serialization_factories(files_path):
    """Test A2fData.from_file and from_string classmethods."""
    a2f_file = files_path / "tools" / "parsers" / "a2f" / "aiida.a2f"

    # Test from_file
    node_file = A2fData.from_file(a2f_file)
    assert isinstance(node_file, A2fData)
    assert node_file.get_frequency().shape == (500,)
    assert node_file.get_spectrum().shape == (500, 10)
    assert node_file.get_cumulative_lambda().shape == (500, 10)
    assert node_file.get_lambda().shape == (10,)
    assert node_file.get_phonon_smearing().shape == (10,)
    assert node_file.electron_smearing == pytest.approx(0.05)
    assert node_file.fermi_window == pytest.approx(0.8)
    assert node_file.summed_elph_coupling == pytest.approx(1.9853134)

    # Test from_string
    content = a2f_file.read_text(encoding="utf-8")
    node_str = A2fData.from_string(content)
    assert isinstance(node_str, A2fData)
    assert node_str.get_frequency().tolist() == node_file.get_frequency().tolist()


def test_pa2f_data_roundtrip():
    """Test storing and retrieving projected a2f data."""
    node = PA2fData()
    node.set_pa2f_data(
        frequency=[0.1, 0.2],
        a2f=[1.0, 2.0],
        projected_a2f=[[0.1, 0.2], [0.3, 0.4]],
        lambda_int=1.99,
        lambda_sum=1.98,
    )

    assert node.get_frequency().tolist() == [0.1, 0.2]
    assert node.get_a2f().tolist() == [1.0, 2.0]
    assert node.get_projected_a2f().tolist() == [[0.1, 0.2], [0.3, 0.4]]
    assert node.lambda_int == pytest.approx(1.99)
    assert node.lambda_sum == pytest.approx(1.98)


def test_pa2f_data_validates_shape_contract():
    """Test invalid PA2fData shapes are rejected."""
    node = PA2fData()

    with pytest.raises(exceptions.ValidationError):
        node.set_pa2f_data(
            frequency=[0.1, 0.2],
            a2f=[1.0],  # Mismatched length
            projected_a2f=[[0.1, 0.2], [0.3, 0.4]],
        )

    with pytest.raises(exceptions.ValidationError):
        node.set_pa2f_data(
            frequency=[0.1, 0.2],
            a2f=[1.0, 2.0],
            projected_a2f=[[0.1], [0.2], [0.3]],  # Mismatched length and shape
        )


def test_pa2f_data_entry_point():
    """Test that PA2fData is registered under entry points."""
    assert DataFactory("epw.pa2f") is PA2fData


def test_pa2f_data_serialization_factories(files_path):
    """Test PA2fData.from_file and from_string classmethods."""
    a2f_proj_file = files_path / "tools" / "parsers" / "a2f" / "aiida.a2f_proj"

    # Test from_file
    node_file = PA2fData.from_file(a2f_proj_file)
    assert isinstance(node_file, PA2fData)
    assert node_file.get_frequency().shape == (500,)
    assert node_file.get_a2f().shape == (500,)
    assert node_file.get_projected_a2f().shape == (500, 3)
    assert node_file.lambda_int == pytest.approx(1.9917789)
    assert node_file.lambda_sum == pytest.approx(1.9853134)

    # Test from_string
    content = a2f_proj_file.read_text(encoding="utf-8")
    node_str = PA2fData.from_string(content)
    assert isinstance(node_str, PA2fData)
    assert node_str.get_frequency().tolist() == node_file.get_frequency().tolist()
