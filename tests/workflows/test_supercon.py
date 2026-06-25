"""Tests for SuperConWorkChain."""

from unittest.mock import MagicMock
import pytest
from aiida import orm


class MockBuilder(dict):
    def __getattr__(self, key):
        if key == "get_dict":
            return lambda: {}
        if key == "get_list":
            return lambda: []
        return self.setdefault(key, MockBuilder())

    def __setattr__(self, key, value):
        self[key] = value

    def __call__(self, *args, **kwargs):
        return self


def mock_get_builder(*args, **kwargs):
    return MockBuilder()


def test_supercon_get_builder_from_protocol_prep(
    fixture_code,
    generate_structure,
    generate_remote_data,
    fixture_localhost,
    monkeypatch,
):
    """Test get_builder_from_protocol for SuperConWorkChain with EpwPrepWorkChain parent."""
    from aiida_epw.workflows.supercon import SuperConWorkChain
    from aiida_epw.workflows.base import EpwBaseWorkChain
    from plumpy.ports import Port, PortNamespace

    monkeypatch.setattr(Port, "validate", lambda *a, **k: None)
    monkeypatch.setattr(PortNamespace, "validate", lambda *a, **k: None)
    monkeypatch.setattr(EpwBaseWorkChain, "get_builder_from_protocol", mock_get_builder)

    epw_code = fixture_code("epw.epw")
    structure = generate_structure()
    remote_stash = generate_remote_data(fixture_localhost, "/tmp/remote_stash")

    # Mock the EpwPrepWorkChain parent node
    parent_epw = MagicMock()
    parent_epw.process_label = "EpwPrepWorkChain"
    parent_epw.inputs = MagicMock()
    parent_epw.inputs.structure = structure

    # Mock base.links.get_outgoing
    epw_source = MagicMock()
    epw_source.inputs = MagicMock()
    epw_source.inputs.code = epw_code
    epw_source.inputs.kpoints = orm.KpointsData()
    epw_source.inputs.qpoints = orm.KpointsData()
    epw_source.outputs = MagicMock()
    epw_source.outputs.remote_stash = remote_stash

    parent_epw.base.links.get_outgoing.return_value.first.return_value.node = epw_source

    builder = SuperConWorkChain.get_builder_from_protocol(
        epw_code=epw_code,
        parent_epw=parent_epw,
        protocol="fast",
    )

    assert builder.structure is structure
    assert builder.parent_folder_epw is remote_stash


def test_supercon_get_builder_from_protocol_base_success(
    fixture_code,
    generate_structure,
    generate_remote_data,
    fixture_localhost,
    monkeypatch,
):
    """Test get_builder_from_protocol for SuperConWorkChain with EpwBaseWorkChain parent having structure."""
    from aiida_epw.workflows.supercon import SuperConWorkChain
    from aiida_epw.workflows.base import EpwBaseWorkChain
    from plumpy.ports import Port, PortNamespace

    monkeypatch.setattr(Port, "validate", lambda *a, **k: None)
    monkeypatch.setattr(PortNamespace, "validate", lambda *a, **k: None)
    monkeypatch.setattr(EpwBaseWorkChain, "get_builder_from_protocol", mock_get_builder)

    epw_code = fixture_code("epw.epw")
    structure = generate_structure()
    remote_stash = generate_remote_data(fixture_localhost, "/tmp/remote_stash")

    # Mock the EpwBaseWorkChain parent node
    parent_epw = MagicMock()
    parent_epw.process_label = "EpwBaseWorkChain"
    parent_epw.inputs = MagicMock()
    parent_epw.inputs.structure = structure
    parent_epw.inputs.code = epw_code
    parent_epw.inputs.kpoints = orm.KpointsData()
    parent_epw.inputs.qpoints = orm.KpointsData()
    parent_epw.outputs = MagicMock()
    parent_epw.outputs.remote_stash = remote_stash

    builder = SuperConWorkChain.get_builder_from_protocol(
        epw_code=epw_code,
        parent_epw=parent_epw,
        protocol="fast",
    )

    assert builder.structure is structure
    assert builder.parent_folder_epw is remote_stash


def test_supercon_get_builder_from_protocol_base_failure(
    fixture_code,
    generate_structure,
    generate_remote_data,
    fixture_localhost,
    monkeypatch,
):
    """Test get_builder_from_protocol for SuperConWorkChain with EpwBaseWorkChain parent missing structure raises ValueError."""
    from aiida_epw.workflows.supercon import SuperConWorkChain
    from aiida_epw.workflows.base import EpwBaseWorkChain
    from plumpy.ports import Port, PortNamespace

    monkeypatch.setattr(Port, "validate", lambda *a, **k: None)
    monkeypatch.setattr(PortNamespace, "validate", lambda *a, **k: None)
    monkeypatch.setattr(EpwBaseWorkChain, "get_builder_from_protocol", mock_get_builder)

    epw_code = fixture_code("epw.epw")
    remote_stash = generate_remote_data(fixture_localhost, "/tmp/remote_stash")

    # Mock the EpwBaseWorkChain parent node without structure
    parent_epw = MagicMock()
    parent_epw.process_label = "EpwBaseWorkChain"
    parent_epw.inputs = MagicMock()
    # Delete structure attribute to raise AttributeError when accessed
    del parent_epw.inputs.structure
    parent_epw.inputs.code = epw_code
    parent_epw.inputs.kpoints = orm.KpointsData()
    parent_epw.inputs.qpoints = orm.KpointsData()
    parent_epw.outputs = MagicMock()
    parent_epw.outputs.remote_stash = remote_stash

    with pytest.raises(ValueError, match="does not contain `structure` in its inputs"):
        SuperConWorkChain.get_builder_from_protocol(
            epw_code=epw_code,
            parent_epw=parent_epw,
            protocol="fast",
        )


def test_supercon_should_run_final():
    """Test should_run_final with different states of epw_interp and convergence."""
    from aiida_epw.workflows.supercon import SuperConWorkChain
    from types import SimpleNamespace
    from aiida.common.extendeddicts import AttributeDict

    # Mock exit codes
    class FakeExitCodes:
        ERROR_SUB_PROCESS_EPW_INTERP = "ERROR_SUB_PROCESS_EPW_INTERP"
        ERROR_ALLEN_DYNES_NOT_CONVERGED = "ERROR_ALLEN_DYNES_NOT_CONVERGED"

    # Define fake workchain helper
    def make_fake_workchain(epw_interp_list, is_converged, always_run_final):
        reports = []
        ctx = SimpleNamespace(
            epw_interp=epw_interp_list,
            is_converged=is_converged,
        )
        inputs = AttributeDict(
            {
                "always_run_final": SimpleNamespace(value=always_run_final),
            }
        )
        return SimpleNamespace(
            inputs=inputs,
            ctx=ctx,
            exit_codes=FakeExitCodes(),
            report=reports.append,
            reports=reports,
        )

    # 1. epw_interp is empty -> should return ERROR_SUB_PROCESS_EPW_INTERP
    wc1 = make_fake_workchain(
        epw_interp_list=[], is_converged=True, always_run_final=True
    )
    assert SuperConWorkChain.should_run_final(wc1) == "ERROR_SUB_PROCESS_EPW_INTERP"
    assert "empty" in wc1.reports[0]

    # 2. epw_interp is not empty, converged -> should return True
    wc2 = make_fake_workchain(
        epw_interp_list=[object()], is_converged=True, always_run_final=False
    )
    assert SuperConWorkChain.should_run_final(wc2) is True

    # 3. epw_interp is not empty, not converged but always_run_final is True -> should return True
    wc3 = make_fake_workchain(
        epw_interp_list=[object()], is_converged=False, always_run_final=True
    )
    assert SuperConWorkChain.should_run_final(wc3) is True

    # 4. epw_interp is not empty, not converged, always_run_final is False -> should return ERROR_ALLEN_DYNES_NOT_CONVERGED
    wc4 = make_fake_workchain(
        epw_interp_list=[object()], is_converged=False, always_run_final=False
    )
    assert SuperConWorkChain.should_run_final(wc4) == "ERROR_ALLEN_DYNES_NOT_CONVERGED"


def test_supercon_get_builder_from_protocol_default(
    fixture_code,
    generate_structure,
    generate_remote_data,
    fixture_localhost,
):
    """Test get_builder_from_protocol for SuperConWorkChain builds all namespaces with hardcoded defaults."""
    from aiida_epw.workflows.supercon import SuperConWorkChain

    epw_code = fixture_code("epw.epw")
    structure = generate_structure()
    remote_stash = generate_remote_data(fixture_localhost, "/tmp/remote_stash")

    # Mock the EpwBaseWorkChain parent node
    parent_epw = MagicMock()
    parent_epw.process_label = "EpwBaseWorkChain"
    parent_epw.inputs = MagicMock()
    parent_epw.inputs.structure = structure
    parent_epw.inputs.code = epw_code
    parent_epw.inputs.kpoints = orm.KpointsData()
    parent_epw.inputs.qpoints = orm.KpointsData()
    parent_epw.outputs = MagicMock()
    parent_epw.outputs.remote_stash = remote_stash

    builder = SuperConWorkChain.get_builder_from_protocol(
        epw_code=epw_code,
        parent_epw=parent_epw,
        protocol="fast",
    )
    assert "code" in builder.epw_interp
    assert "code" in builder.epw_final_iso
    assert "code" in builder.epw_final_aniso
    assert not builder.epw_final_iso.momentum_dependence.value
    assert not builder.epw_final_iso.real_axis.value
    assert builder.epw_final_iso.parameters.get_dict()["INPUTEPW"]["tc_linear"] is True
    assert builder.epw_final_aniso.momentum_dependence.value
    assert not builder.epw_final_aniso.full_bandwidth.value
    assert not builder.epw_final_aniso.real_axis.value


def test_epw_base_eliashberg_params(fixture_code, generate_structure):
    """Test that EpwBaseWorkChain correctly resolves Eliashberg parameters in get_builder_from_protocol, setup, and validate_inputs."""
    from aiida_epw.workflows.base import EpwBaseWorkChain
    from aiida.common import AttributeDict

    epw_code = fixture_code("epw.epw")
    structure = generate_structure()

    # 1. Test builder creation with new flags
    builder = EpwBaseWorkChain.get_builder_from_protocol(
        code=epw_code,
        structure=structure,
        protocol="fast",
        momentum_dependence=True,
        full_bandwidth=False,
        real_axis=False,
        analytical_continuation="pade",
    )
    assert builder.momentum_dependence.value
    assert not builder.full_bandwidth.value
    assert not builder.real_axis.value
    assert builder.analytical_continuation.value == "pade"

    # 2. Test input validation checks
    # - validate incorrect analytical_continuation value
    inputs_invalid_ac = {
        "kfpoints_factor": orm.Int(2),
        "qfpoints_distance": orm.Float(0.1),
        "analytical_continuation": orm.Str("invalid_method"),
    }
    assert (
        "Invalid `analytical_continuation`"
        in EpwBaseWorkChain.spec().inputs.validator(inputs_invalid_ac)
    )

    # - validate linearized Eliashberg constraints (tc_linear=True with real_axis=True)
    inputs_tc_real = {
        "kfpoints_factor": orm.Int(2),
        "qfpoints_distance": orm.Float(0.1),
        "real_axis": orm.Bool(True),
        "parameters": orm.Dict(dict={"INPUTEPW": {"tc_linear": True}}),
    }
    assert (
        "Linearized Eliashberg (tc_linear=True) cannot be used with real_axis=True"
        in EpwBaseWorkChain.spec().inputs.validator(inputs_tc_real)
    )

    # - validate linearized Eliashberg constraints (tc_linear=True with momentum_dependence=True)
    inputs_tc_aniso = {
        "kfpoints_factor": orm.Int(2),
        "qfpoints_distance": orm.Float(0.1),
        "momentum_dependence": orm.Bool(True),
        "parameters": orm.Dict(dict={"INPUTEPW": {"tc_linear": True}}),
    }
    assert (
        "Linearized Eliashberg (tc_linear=True) cannot be used with momentum_dependence=True"
        in EpwBaseWorkChain.spec().inputs.validator(inputs_tc_aniso)
    )

    # - validate real_axis anisotropic solver constraint (real_axis=True with momentum_dependence=True)
    inputs_real_aniso = {
        "kfpoints_factor": orm.Int(2),
        "qfpoints_distance": orm.Float(0.1),
        "real_axis": orm.Bool(True),
        "momentum_dependence": orm.Bool(True),
    }
    assert (
        "Real axis solver (real_axis=True) is only implemented for the isotropic case"
        in EpwBaseWorkChain.spec().inputs.validator(inputs_real_aniso)
    )

    # - validate continuation and real_axis mutual exclusivity
    inputs_real_ac = {
        "kfpoints_factor": orm.Int(2),
        "qfpoints_distance": orm.Float(0.1),
        "real_axis": orm.Bool(True),
        "analytical_continuation": orm.Str("pade"),
    }
    assert (
        "Analytical continuation (analytical_continuation) cannot be used when solving on the real axis"
        in EpwBaseWorkChain.spec().inputs.validator(inputs_real_ac)
    )

    # - validate that 'none' continuation and real_axis are allowed together
    inputs_real_ac_none = {
        "kfpoints_factor": orm.Int(2),
        "qfpoints_distance": orm.Float(0.1),
        "real_axis": orm.Bool(True),
        "analytical_continuation": orm.Str("none"),
    }
    assert EpwBaseWorkChain.spec().inputs.validator(inputs_real_ac_none) is None

    # - validate full_bandwidth and ACON mutual exclusivity
    inputs_fbw_acon = {
        "kfpoints_factor": orm.Int(2),
        "qfpoints_distance": orm.Float(0.1),
        "full_bandwidth": orm.Bool(True),
        "analytical_continuation": orm.Str("acon"),
    }
    assert (
        "Analytic continuation method 'acon' is not implemented when full_bandwidth is True"
        in EpwBaseWorkChain.spec().inputs.validator(inputs_fbw_acon)
    )

    # 3. Test setup method logic by executing a mock workchain setup
    class MockWorkChain(EpwBaseWorkChain):
        @property
        def inputs(self):
            return self._inputs_dict

        @inputs.setter
        def inputs(self, value):
            self._inputs_dict = value

        @property
        def ctx(self):
            return self._ctx_dict

        @ctx.setter
        def ctx(self, value):
            self._ctx_dict = value

        def setup(self):
            # Call the actual setup logic from EpwBaseWorkChain
            EpwBaseWorkChain.setup(self)

    # Mock BaseRestartWorkChain.setup which is super().setup()
    import unittest.mock

    with unittest.mock.patch(
        "aiida.engine.BaseRestartWorkChain.setup", return_value=None
    ):
        # Test isotropic imaginary axis with Pade continuation
        inputs_iso = {
            "code": epw_code,
            "structure": structure,
            "momentum_dependence": orm.Bool(False),
            "real_axis": orm.Bool(False),
            "analytical_continuation": orm.Str("pade"),
            "options": orm.Dict(dict={"resources": {"num_machines": 1}}),
            "parameters": orm.Dict(dict={"INPUTEPW": {}}),
        }
        wc_iso = MockWorkChain.__new__(MockWorkChain)
        wc_iso.inputs = AttributeDict(inputs_iso)
        wc_iso.ctx = AttributeDict()
        wc_iso.exposed_inputs = lambda *args, **kwargs: AttributeDict(inputs_iso)
        wc_iso.setup()

        assert not wc_iso.ctx.inputs.momentum_dependence.value
        assert not wc_iso.ctx.inputs.real_axis.value
        assert wc_iso.ctx.inputs.analytical_continuation.value == "pade"
        assert (
            "aiida.imag_iso_*"
            in wc_iso.ctx.inputs.metadata["options"]["additional_retrieve_list"]
        )

        # Test anisotropic imaginary axis (FSR-like)
        inputs_fsr = {
            "code": epw_code,
            "structure": structure,
            "momentum_dependence": orm.Bool(True),
            "full_bandwidth": orm.Bool(False),
            "real_axis": orm.Bool(False),
            "options": orm.Dict(dict={"resources": {"num_machines": 1}}),
            "parameters": orm.Dict(dict={"INPUTEPW": {}}),
        }
        wc_fsr = MockWorkChain.__new__(MockWorkChain)
        wc_fsr.inputs = AttributeDict(inputs_fsr)
        wc_fsr.ctx = AttributeDict()
        wc_fsr.exposed_inputs = lambda *args, **kwargs: AttributeDict(inputs_fsr)
        wc_fsr.setup()

        assert wc_fsr.ctx.inputs.momentum_dependence.value
        assert not wc_fsr.ctx.inputs.full_bandwidth.value
        assert not wc_fsr.ctx.inputs.real_axis.value
        assert (
            "aiida.imag_aniso*"
            in wc_fsr.ctx.inputs.metadata["options"]["additional_retrieve_list"]
        )
        assert (
            "aiida.lambda_FS"
            in wc_fsr.ctx.inputs.metadata["options"]["additional_retrieve_list"]
        )
        assert (
            "aiida.lambda_k_pairs"
            in wc_fsr.ctx.inputs.metadata["options"]["additional_retrieve_list"]
        )
