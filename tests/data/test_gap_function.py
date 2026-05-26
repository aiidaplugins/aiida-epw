"""Tests for the ``GapFunctionData`` datatype."""

import numpy
import pytest
from aiida.common import exceptions
from aiida.plugins import DataFactory

from aiida_epw.data import GapFunctionData


def test_gap_function_data_roundtrip():
    """Test storing and retrieving gap-function tables by temperature."""
    node = GapFunctionData()
    node.set_gap_functions(
        {
            5.0: [[0.4, 1.0], [0.8, 0.7]],
            3.0: [[0.2, 1.3], [0.6, 0.9]],
        },
        kind="iso",
    )

    assert node.kind == "iso"
    assert node.get_temperatures().tolist() == [3.0, 5.0]
    assert node.get_gap_function(3.0).tolist() == [[0.2, 1.3], [0.6, 0.9]]
    assert list(node.get_gap_functions()) == [3.0, 5.0]
    assert [
        (temperature, table.tolist())
        for temperature, table in node.get_itergap_functions()
    ] == [
        (3.0, [[0.2, 1.3], [0.6, 0.9]]),
        (5.0, [[0.4, 1.0], [0.8, 0.7]]),
    ]


def test_gap_function_data_replaces_previous_tables():
    """Test resetting gap functions drops stale arrays from a previous payload."""
    node = GapFunctionData()
    node.set_gap_functions({3.0: [[0.1, 0.2]]}, kind="iso")
    node.set_gap_functions({4.0: [[0.3, 0.4], [0.5, 0.6]]}, kind="aniso")

    assert node.kind == "aniso"
    assert node.get_temperatures().tolist() == [4.0]
    assert node.get_gap_function(4.0).tolist() == [[0.3, 0.4], [0.5, 0.6]]
    assert node.get_arraynames() == ["gap_function_000"]


def test_gap_function_data_validates_shape_contract():
    """Test invalid gap-function payloads are rejected."""
    node = GapFunctionData()

    with pytest.raises(exceptions.ValidationError):
        node.set_gap_functions({})

    with pytest.raises(exceptions.ValidationError):
        node.set_gap_functions({3.0: numpy.array([0.1, 0.2])})


def test_gap_function_data_entry_point():
    """Test the datatype is registered through ``aiida.data``."""
    assert DataFactory("epw.gap_function") is GapFunctionData
