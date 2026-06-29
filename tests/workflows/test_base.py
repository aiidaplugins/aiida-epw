from unittest.mock import MagicMock
from aiida import orm
from aiida_epw.workflows.base import EpwBaseWorkChain


def test_handle_out_of_walltime(aiida_localhost):
    """Test that handle_out_of_walltime works correctly when Eliashberg and EPHWRITE are configured."""

    class MockWorkChain:
        exit_codes = EpwBaseWorkChain.exit_codes

        def __init__(self):
            self.ctx = MagicMock()
            self.report_messages = []

        def report(self, msg):
            self.report_messages.append(msg)

        def report_error_handled(self, calculation, action):
            self.report(f"Calculation failed: {action}")

        handle_out_of_walltime = EpwBaseWorkChain.handle_out_of_walltime

    workchain = MockWorkChain()

    # Mock calculation
    calc = MagicMock()
    calc.outputs = MagicMock()
    calc.outputs.remote_folder = orm.RemoteData(
        computer=aiida_localhost, remote_path="/tmp"
    )

    # Setup ctx inputs for Eliashberg + EPHWRITE
    initial_params = {
        "INPUTEPW": {
            "eliashberg": True,
            "epwread": True,
            "ephwrite": True,
        }
    }
    workchain.ctx.inputs = MagicMock()
    workchain.ctx.inputs.parameters = orm.Dict(dict=initial_params)

    # Run handler
    report = workchain.handle_out_of_walltime.__wrapped__(calc)

    # Assertions
    assert report.do_break is True
    assert report.exit_code.status == 0
    assert workchain.ctx.inputs.parent_folder_epw == calc.outputs.remote_folder

    updated_params = workchain.ctx.inputs.parameters.get_dict()
    assert updated_params["INPUTEPW"]["restart"] is True


def test_handle_out_of_walltime_unsupported(aiida_localhost):
    """Test that handle_out_of_walltime aborts for unsupported calculation/restart configurations."""

    class MockWorkChain:
        exit_codes = EpwBaseWorkChain.exit_codes

        def __init__(self):
            self.ctx = MagicMock()
            self.report_messages = []

        def report(self, msg):
            self.report_messages.append(msg)

        def report_error_handled(self, calculation, action):
            self.report(f"Calculation failed: {action}")

        handle_out_of_walltime = EpwBaseWorkChain.handle_out_of_walltime

    workchain = MockWorkChain()

    # Mock calculation
    calc = MagicMock()
    calc.outputs = MagicMock()
    calc.outputs.remote_folder = orm.RemoteData(
        computer=aiida_localhost, remote_path="/tmp"
    )

    # Setup ctx inputs for transport (not Eliashberg)
    initial_params = {
        "INPUTEPW": {
            "eliashberg": False,
            "scattering": True,
            "epwread": False,
            "ephwrite": True,
        }
    }
    workchain.ctx.inputs = MagicMock()
    workchain.ctx.inputs.parameters = orm.Dict(dict=initial_params)

    # Run handler
    report = workchain.handle_out_of_walltime.__wrapped__(calc)

    # Assertions
    assert report.do_break is True
    assert (
        report.exit_code.status
        == EpwBaseWorkChain.exit_codes.ERROR_KNOWN_UNRECOVERABLE_FAILURE.status
    )
