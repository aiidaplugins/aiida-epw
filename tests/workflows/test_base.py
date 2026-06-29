"""Tests for the `EpwBaseWorkChain` class and its error handlers."""

from unittest.mock import MagicMock
from aiida import orm
from aiida_epw.workflows.base import EpwBaseWorkChain


def test_handle_temperature_out_of_range(aiida_localhost):
    """Test the handle_temperature_out_of_range error handler."""

    class MockWorkChain:
        exit_codes = EpwBaseWorkChain.exit_codes

        def __init__(self):
            self.ctx = MagicMock()
            self.ctx.is_finished = False
            self.report_messages = []

        def report(self, msg):
            self.report_messages.append(msg)

        def report_error_handled(self, calculation, action):
            self.report(f"Calculation failed: {action}")

        handle_temperature_out_of_range = (
            EpwBaseWorkChain.handle_temperature_out_of_range
        )

    workchain = MockWorkChain()

    # Mock calculation
    calc = MagicMock()
    calc.outputs = MagicMock()
    calc.outputs.remote_folder = orm.RemoteData(
        computer=aiida_localhost, remote_path="/tmp"
    )

    # Run handler
    report = workchain.handle_temperature_out_of_range.__wrapped__(calc)

    # Assertions
    assert report.do_break is True
    assert report.exit_code.status == 0
    assert workchain.ctx.is_finished is True
    assert any("phase transition" in msg for msg in workchain.report_messages)
