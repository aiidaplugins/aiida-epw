"""Tests for the ``ProjectedSpectrumData`` datatype."""

import numpy
import pytest
from aiida.common import exceptions
from aiida.plugins import DataFactory

from aiida_epw.data import ProjectedSpectrumData


def test_projected_spectrum_roundtrip():
    """Test storing and retrieving a projected spectrum."""
    node = ProjectedSpectrumData()
    node.set_projected_spectrum(
        grid=[0.1, 0.2],
        series=[[1.0, 0.7, 0.3], [2.0, 1.4, 0.6]],
        kind="phdos_proj",
        grid_name="frequency",
        series_name="phdos_proj",
        total_label="phdos",
        projected_label="phdos_modeproj",
        legacy_grid_name="Frequency",
        legacy_series_name="PHDOS_proj",
    )

    assert node.kind == "phdos_proj"
    assert node.grid_name == "frequency"
    assert node.series_name == "phdos_proj"
    assert node.total_label == "phdos"
    assert node.projected_label == "phdos_modeproj"
    assert node.get_grid().tolist() == [0.1, 0.2]
    assert node.get_total().tolist() == [1.0, 2.0]
    assert node.get_projected().tolist() == [[0.7, 0.3], [1.4, 0.6]]
    assert node.get_series().tolist() == [[1.0, 0.7, 0.3], [2.0, 1.4, 0.6]]
    assert node.get_array("Frequency").tolist() == [0.1, 0.2]
    assert node.get_array("PHDOS_proj").tolist() == [[1.0, 0.7, 0.3], [2.0, 1.4, 0.6]]


def test_projected_spectrum_without_explicit_projected_block():
    """Test single-column spectra do not expose a projected block."""
    node = ProjectedSpectrumData()
    node.set_projected_spectrum(grid=[0.1, 0.2], series=[[1.0], [2.0]], kind="test")

    assert node.get_total().tolist() == [1.0, 2.0]
    assert node.get_projected() is None


def test_projected_spectrum_validates_shape_contract():
    """Test invalid projected spectrum payloads are rejected."""
    node = ProjectedSpectrumData()

    with pytest.raises(exceptions.ValidationError):
        node.set_projected_spectrum(grid=[[0.1]], series=[[1.0]], kind="broken")

    with pytest.raises(exceptions.ValidationError):
        node.set_projected_spectrum(
            grid=[0.1, 0.2], series=numpy.array([1.0, 2.0]), kind="broken"
        )


def test_projected_spectrum_entry_point():
    """Test the datatype is registered through ``aiida.data``."""
    assert DataFactory("epw.projected_spectrum") is ProjectedSpectrumData
