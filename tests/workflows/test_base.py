"""Tests for the `EpwBaseWorkChain` class and its error handlers."""

from unittest.mock import MagicMock
from aiida import orm

try:
    from aiida_epw.common.types import RestartType

    HAS_RESTART_TYPE = True
except ImportError:
    HAS_RESTART_TYPE = False
from aiida_epw.workflows.base import EpwBaseWorkChain


def test_handle_pade_approximants(aiida_localhost):
    """Test the handle_pade_approximants error handler with successful/failed temperatures and nsiw reduction."""

    class MockWorkChain:
        _MAX_NSIW = EpwBaseWorkChain._MAX_NSIW
        _MIN_NSIW = EpwBaseWorkChain._MIN_NSIW
        exit_codes = EpwBaseWorkChain.exit_codes

        def __init__(self):
            self.ctx = MagicMock()
            self.report_messages = []

        def report(self, msg):
            self.report_messages.append(msg)

        def report_error_handled(self, calculation, action):
            self.report(f"Calculation failed: {action}")

        handle_pade_approximants = EpwBaseWorkChain.handle_pade_approximants

    workchain = MockWorkChain()

    # Mock calculation
    calc = MagicMock()
    calc.outputs = MagicMock()
    calc.outputs.remote_folder = orm.RemoteData(
        computer=aiida_localhost, remote_path="/tmp"
    )

    # Mock outputs: 2.0 succeeded, 1.0 failed
    calc.outputs.output_parameters = orm.Dict(
        dict={
            "isotropic_eliashberg": {
                "2.0": {
                    "nsiw": 400,
                    "iterations": {"ethr": [1e-8], "znormi": [1.2], "deltai": [2.5]},
                    "pade": {"delta": 2.4, "znorm": 1.2},
                },
                "1.0": {
                    "nsiw": 800,
                    "iterations": {"ethr": [None], "znormi": [None], "deltai": [None]},
                    "pade": {"delta": None, "znorm": None},
                },
            }
        }
    )

    # Setup ctx inputs
    initial_params = {"INPUTEPW": {"temps": [1.0, 2.0], "nstemp": 2}}
    workchain.ctx.inputs = MagicMock()
    workchain.ctx.inputs.parameters = orm.Dict(dict=initial_params)

    # Run handler
    report = workchain.handle_pade_approximants.__wrapped__(calc)

    # Assertions
    assert report.do_break is True
    assert report.exit_code.status == 0

    updated_params = workchain.ctx.inputs.parameters.get_dict()
    input_epw = updated_params["INPUTEPW"]

    # Successfully calculated temperature 2.0 should be popped
    assert input_epw["temps"] == [1.0]
    assert input_epw["nstemp"] == 1

    # nsiw for failed temp was 800 (> _MAX_NSIW of 200), so should be reduced to 200
    assert input_epw["nsiw"] == 200

    # Restart settings should be set
    if HAS_RESTART_TYPE:
        assert workchain.ctx.inputs.restart_type == RestartType.EPHREAD
    else:
        assert input_epw.get("epwread") is True
    assert workchain.ctx.inputs.parent_folder_epw == calc.outputs.remote_folder


def test_handle_pade_approximants_lower_bound(aiida_localhost):
    """Test that nsiw reduction respects the lower bound threshold (min limit)."""

    class MockWorkChain:
        _MAX_NSIW = EpwBaseWorkChain._MAX_NSIW
        _MIN_NSIW = EpwBaseWorkChain._MIN_NSIW
        exit_codes = EpwBaseWorkChain.exit_codes

        def __init__(self):
            self.ctx = MagicMock()
            self.report_messages = []

        def report(self, msg):
            self.report_messages.append(msg)

        def report_error_handled(self, calculation, action):
            self.report(f"Calculation failed: {action}")

        handle_pade_approximants = EpwBaseWorkChain.handle_pade_approximants

    workchain = MockWorkChain()

    # Mock calculation
    calc = MagicMock()
    calc.outputs = MagicMock()
    calc.outputs.remote_folder = orm.RemoteData(
        computer=aiida_localhost, remote_path="/tmp"
    )

    # Mock output where nsiw is already small
    calc.outputs.output_parameters = orm.Dict(
        dict={
            "isotropic_eliashberg": {
                "1.0": {
                    "nsiw": 30,
                    "iterations": {"ethr": [None], "znormi": [None], "deltai": [None]},
                }
            }
        }
    )

    # Setup ctx inputs
    initial_params = {"INPUTEPW": {"temps": [1.0], "nstemp": 1}}
    workchain.ctx.inputs = MagicMock()
    workchain.ctx.inputs.parameters = orm.Dict(dict=initial_params)

    # Run handler
    report = workchain.handle_pade_approximants.__wrapped__(calc)

    # Assertions
    assert report.do_break is True
    assert report.exit_code.status == 0

    updated_params = workchain.ctx.inputs.parameters.get_dict()
    # 30 * 0.5 = 15, which is below min limit of 20, so should be capped at 20
    assert updated_params["INPUTEPW"]["nsiw"] == 20


def test_handle_pade_approximants_string_temps(aiida_localhost):
    """Test that the handler works correctly when temps is a space-separated string."""

    class MockWorkChain:
        _MAX_NSIW = EpwBaseWorkChain._MAX_NSIW
        _MIN_NSIW = EpwBaseWorkChain._MIN_NSIW
        exit_codes = EpwBaseWorkChain.exit_codes

        def __init__(self):
            self.ctx = MagicMock()
            self.report_messages = []

        def report(self, msg):
            self.report_messages.append(msg)

        def report_error_handled(self, calculation, action):
            self.report(f"Calculation failed: {action}")

        handle_pade_approximants = EpwBaseWorkChain.handle_pade_approximants

    workchain = MockWorkChain()

    # Mock calculation
    calc = MagicMock()
    calc.outputs = MagicMock()
    calc.outputs.remote_folder = orm.RemoteData(
        computer=aiida_localhost, remote_path="/tmp"
    )

    # Mock outputs: 2.0 succeeded, 1.0 failed
    calc.outputs.output_parameters = orm.Dict(
        dict={
            "isotropic_eliashberg": {
                "2.0": {
                    "nsiw": 400,
                    "iterations": {"ethr": [1e-8], "znormi": [1.2], "deltai": [2.5]},
                    "pade": {"delta": 2.4, "znorm": 1.2},
                },
                "1.0": {
                    "nsiw": 800,
                    "iterations": {"ethr": [None], "znormi": [None], "deltai": [None]},
                    "pade": {"delta": None, "znorm": None},
                },
            }
        }
    )

    # Setup ctx inputs with space-separated string
    initial_params = {"INPUTEPW": {"temps": "1.0 2.0", "nstemp": 2}}
    workchain.ctx.inputs = MagicMock()
    workchain.ctx.inputs.parameters = orm.Dict(dict=initial_params)

    # Run handler
    report = workchain.handle_pade_approximants.__wrapped__(calc)

    # Assertions
    assert report.do_break is True
    assert report.exit_code.status == 0

    updated_params = workchain.ctx.inputs.parameters.get_dict()
    input_epw = updated_params["INPUTEPW"]

    # Successfully calculated temperature 2.0 should be popped
    # And since the original input was a string, the result should also be a string
    assert input_epw["temps"] == "1.0"
    assert input_epw["nstemp"] == 1
