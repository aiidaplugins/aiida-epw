"""Tests for the ``LambdaKPairsData`` datatype."""

import pytest
from aiida.common import exceptions
from aiida.plugins import DataFactory

from aiida_epw.data import LambdaKPairsData


def test_lambda_k_pairs_roundtrip():
    """Test storing and retrieving a lambda_k_pairs distribution."""
    node = LambdaKPairsData()
    node.set_lambda_k_pairs(lambda_nk=[0.1, 0.2], rho=[1.0, 1.5])

    assert node.get_lambda_nk().tolist() == [0.1, 0.2]
    assert node.get_rho().tolist() == [1.0, 1.5]
    assert node.get_array("lambda_nk").tolist() == [0.1, 0.2]
    assert node.get_array("rho").tolist() == [1.0, 1.5]


def test_lambda_k_pairs_validates_shape_contract():
    """Test invalid lambda_k_pairs payloads are rejected."""
    node = LambdaKPairsData()

    with pytest.raises(exceptions.ValidationError):
        node.set_lambda_k_pairs(lambda_nk=[[0.1]], rho=[1.0])

    with pytest.raises(exceptions.ValidationError):
        node.set_lambda_k_pairs(lambda_nk=[0.1, 0.2], rho=[1.0])


def test_lambda_k_pairs_entry_point():
    """Test the datatype is registered through ``aiida.data``."""
    assert DataFactory("epw.lambda_k_pairs") is LambdaKPairsData
