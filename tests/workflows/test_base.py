from unittest.mock import MagicMock
from aiida import orm
from aiida_epw.workflows.base import EpwBaseWorkChain
import enum

try:
    from aiida_epw.common.types import CalculationTypes, RestartType

    HAS_RESTART_TYPE = True
except ImportError:
    # Fallback for testing when types module is not fully defined on this branch
    class CalculationTypes(enum.Enum):
        ELIASHBERG = "eliashberg"
        TRANSPORT = "transport"
        POLARON = "polaron"

    try:
        from aiida_epw.common.types import RestartType

        HAS_RESTART_TYPE = True
    except ImportError:

        class RestartType(enum.Enum):
            WANNIERIZE = "wannierize"
            EPHWRITE = "ephwrite"
            EPHREAD = "ephread"
            EPHWRITE_RESTART = "ephwrite_restart"

        HAS_RESTART_TYPE = False


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

    if HAS_RESTART_TYPE:
        calc_type_mock = MagicMock()
        calc_type_mock.get_member.return_value = CalculationTypes.ELIASHBERG
        restart_type_mock = MagicMock()
        restart_type_mock.get_member.return_value = RestartType.EPHWRITE

        workchain.ctx.inputs.calculation_type = calc_type_mock
        workchain.ctx.inputs.restart_type = restart_type_mock
        workchain.ctx.inputs.__contains__.side_effect = lambda key: key in (
            "calculation_type",
            "restart_type",
            "parameters",
        )

    # Run handler
    report = workchain.handle_out_of_walltime.__wrapped__(calc)

    # Assertions
    assert report.do_break is True
    assert report.exit_code.status == 0
    assert workchain.ctx.inputs.parent_folder_epw == calc.outputs.remote_folder
    if HAS_RESTART_TYPE:
        assert workchain.ctx.inputs.restart_type == RestartType.EPHWRITE_RESTART
    else:
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


def test_handle_out_of_walltime_ephread_success(aiida_localhost):
    """Test that handle_out_of_walltime EPHREAD mode correctly pops succeeded temperatures and restarts."""

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

    # Mock output parameters: 2.0 succeeded, 1.0 failed
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

    # Setup ctx inputs for Eliashberg + EPHREAD
    initial_params = {
        "INPUTEPW": {
            "eliashberg": True,
            "epwread": True,
            "ephwrite": False,
            "temps": [1.0, 2.0],
            "nstemp": 2,
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
    # Successfully calculated temperature 2.0 should be popped, 1.0 remains
    assert updated_params["INPUTEPW"]["temps"] == [1.0]
    assert updated_params["INPUTEPW"]["nstemp"] == 1


def test_handle_out_of_walltime_ephread_failure(aiida_localhost):
    """Test that handle_out_of_walltime EPHREAD mode aborts if no temperatures succeeded."""

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

    # Mock output parameters: no temperatures succeeded
    calc.outputs.output_parameters = orm.Dict(
        dict={
            "isotropic_eliashberg": {
                "1.0": {
                    "nsiw": 800,
                    "iterations": {"ethr": [None], "znormi": [None], "deltai": [None]},
                    "pade": {"delta": None, "znorm": None},
                },
            }
        }
    )

    # Setup ctx inputs for Eliashberg + EPHREAD
    initial_params = {
        "INPUTEPW": {
            "eliashberg": True,
            "epwread": True,
            "ephwrite": False,
            "temps": [1.0],
            "nstemp": 1,
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


def test_handle_out_of_walltime_wannierize(aiida_localhost):
    """Test that handle_out_of_walltime Wannierize mode prints a message and aborts."""

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

    # Setup ctx inputs for Eliashberg + WANNIERIZE
    initial_params = {
        "INPUTEPW": {
            "eliashberg": True,
            "wannierize": True,
            "epwread": False,
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
    assert any(
        "Resuming Wannierization via epbread is not implemented yet" in msg
        for msg in workchain.report_messages
    )
