"""Tests for the ``A2fData`` datatype."""

import numpy
import pytest
from aiida.common import exceptions
from aiida.plugins import DataFactory

from aiida_epw.data import A2fData


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
