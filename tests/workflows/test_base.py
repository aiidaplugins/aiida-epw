"""Tests for the ``EpwBaseWorkChain``."""

import pytest
from aiida import orm

from aiida_epw.workflows.base import EpwBaseWorkChain


def test_get_builder_from_protocol_eliashberg_ports(fixture_code, generate_structure):
    """Test that the protocol builder forwards the explicit Eliashberg ports."""
    builder = EpwBaseWorkChain.get_builder_from_protocol(
        code=fixture_code("epw.epw"),
        structure=generate_structure(),
        protocol="fast",
        momentum_dependence=True,
        full_bandwidth=False,
        real_axis=False,
        analytical_continuation="pade",
    )

    assert builder.momentum_dependence.value is True
    assert builder.full_bandwidth.value is False
    assert builder.real_axis.value is False
    assert builder.analytical_continuation.value == "pade"


@pytest.mark.parametrize(
    ("inputs", "message"),
    (
        (
            {"analytical_continuation": "invalid"},
            "Invalid `analytical_continuation`",
        ),
        (
            {
                "real_axis": True,
                "parameters": {"INPUTEPW": {"tc_linear": True}},
            },
            "cannot be used with real_axis=True",
        ),
        (
            {
                "momentum_dependence": True,
                "real_axis": True,
            },
            "only implemented for the isotropic case",
        ),
        (
            {
                "analytical_continuation": "acon",
                "full_bandwidth": True,
            },
            "not implemented when full_bandwidth is True",
        ),
    ),
)
def test_validate_eliashberg_ports(inputs, message):
    """Test incompatible combinations of explicit Eliashberg ports."""
    inputs = {
        key: (
            orm.Str(value)
            if key == "analytical_continuation"
            else orm.Dict(value)
            if key == "parameters"
            else orm.Bool(value)
        )
        for key, value in inputs.items()
    }
    inputs.update(
        {
            "kfpoints_factor": orm.Int(2),
            "qfpoints_distance": orm.Float(0.1),
        }
    )

    assert message in EpwBaseWorkChain.spec().inputs.validator(inputs)


def test_validate_real_axis_without_continuation():
    """Test that real-axis solving accepts explicitly disabled continuation."""
    inputs = {
        "kfpoints_factor": orm.Int(2),
        "qfpoints_distance": orm.Float(0.1),
        "real_axis": orm.Bool(True),
        "analytical_continuation": orm.Str("none"),
    }

    assert EpwBaseWorkChain.spec().inputs.validator(inputs) is None
