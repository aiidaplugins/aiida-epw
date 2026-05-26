"""Tests for the ``LambdaFSData`` datatype."""

import numpy
import pytest
from aiida.common import exceptions
from aiida.plugins import DataFactory

from aiida_epw.data import LambdaFSData


def test_lambda_fs_roundtrip():
    """Test storing and retrieving a lambda_FS table."""
    node = LambdaFSData()
    node.set_lambda_fs(
        kpoints=[[0.0, 0.1, 0.2], [0.5, 0.6, 0.7]],
        bands=[1, 2],
        energies=[0.3, 0.4],
        couplings=[0.9, 1.1],
    )

    assert node.energy_units == "eV"
    assert node.get_kpoints().tolist() == [[0.0, 0.1, 0.2], [0.5, 0.6, 0.7]]
    assert node.get_bands().tolist() == [1.0, 2.0]
    assert node.get_energies().tolist() == [0.3, 0.4]
    assert node.get_lambda().tolist() == [0.9, 1.1]
    assert node.get_array("Enk").tolist() == [0.3, 0.4]


def test_lambda_fs_validates_shape_contract():
    """Test invalid lambda_FS payloads are rejected."""
    node = LambdaFSData()

    with pytest.raises(exceptions.ValidationError):
        node.set_lambda_fs(
            kpoints=numpy.array([0.0, 0.1, 0.2]),
            bands=[1],
            energies=[0.3],
            couplings=[0.9],
        )

    with pytest.raises(exceptions.ValidationError):
        node.set_lambda_fs(
            kpoints=[[0.0, 0.1, 0.2], [0.5, 0.6, 0.7]],
            bands=[1],
            energies=[0.3, 0.4],
            couplings=[0.9, 1.1],
        )


def test_lambda_fs_entry_point():
    """Test the datatype is registered through ``aiida.data``."""
    assert DataFactory("epw.lambda_fs") is LambdaFSData
