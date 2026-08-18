"""Tests for the additive typed gap-data classes."""

import pytest
from aiida.plugins import DataFactory

from aiida_epw.data import AnisoGap0Data, IsoGapData
from aiida_epw.tools.parsers import parse_epw_iso_gap_files


def test_iso_gap_data_keeps_imaginary_and_pade_sources_separate():
    """The same temperature can hold distinct imaginary-axis and Pade tables."""
    node = IsoGapData()
    node.set_gap_data(
        {
            ("imag", 3.0): {"omega": [0.1, 0.2], "deltaw": [0.01, 0.02]},
            ("pade", 3.0): {"omega": [0.1], "deltaw_real": [0.01]},
        }
    )

    assert node.sources == ["imag", "pade"]
    assert node.get_data(3.0, source="imag")["deltaw"].tolist() == [0.01, 0.02]
    with pytest.raises(KeyError, match="Multiple gap data"):
        node.get_data(3.0)


def test_aniso_gap_data_and_entry_points_are_available():
    """Anisotropic data remains an additive, separately registered type."""
    node = AnisoGap0Data()
    node.set_gap_data(
        {
            ("imag", 4.0): {
                "T_dist_scaled": [4.0],
                "delta_nk": [1.0],
                "T": [4.0],
                "dist_scaled": [0.1],
                "dist_not_scaled": [10.0],
            }
        }
    )

    assert node.get_temperatures().tolist() == [4.0]
    assert DataFactory("epw.iso_gap") is IsoGapData
    assert DataFactory("epw.aniso_gap0") is AnisoGap0Data


def test_gap_data_stores_one_table_per_source_and_temperature():
    """Columns are kept together, while ragged tables remain supported."""
    node = IsoGapData()
    node.set_gap_data({("imag", 4.0): {"omega": [0.1], "deltaw": [0.02]}})

    assert node.get_table(4.0, source="imag").tolist() == [[0.1, 0.02]]
    assert sorted(node.get_arraynames()) == ["imag_004_00", "temperatures"]


def test_gap_data_exposes_plotting_series():
    """Typed data can be converted into plain temperature/gap series."""
    iso = IsoGapData()
    iso.set_gap_data({("imag", 3.0): {"omega": [0.1], "deltaw": [0.002]}})
    assert iso.get_gap_fs() == {
        "T": [3.0],
        "gap": [2.0],
        "unit": "meV",
        "source": "imag",
    }
    assert iso.to_dict()["imag"][3.0]["columns"] == ["omega", "deltaw"]


def test_aniso_gap_data_exposes_averaged_gap_series():
    """A flat distribution produces one representative gap value."""
    aniso = AnisoGap0Data()
    aniso.set_gap_data(
        {
            ("imag", 3.0): {
                "T_dist_scaled": [3.0, 3.0, 3.0],
                "delta_nk": [1.0, 2.0, 3.0],
                "T": [3.0, 3.0, 3.0],
                "dist_scaled": [0.1, 0.1, 0.1],
                "dist_not_scaled": [1.0, 1.0, 1.0],
            }
        }
    )
    assert aniso.get_averaged_gap()["gap"] == [[2.0]]


def test_parse_pade_iso_gap_file():
    """Pade file names and their five-column layout are supported."""
    parsed = parse_epw_iso_gap_files(
        {"aiida.pade_iso_003.00": "w ReZ ImZ ReD ImD\n1.0 2.0 0.2 0.01 0.001\n"}
    )

    assert parsed[("pade", 3.0)]["deltaw_imag"].tolist() == [0.001]
