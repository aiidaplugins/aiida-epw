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
    from aiida_epw.common import EliashbergType

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
    assert builder.epw_final_iso.eliashberg_type == EliashbergType.LINEARIZED
    assert builder.epw_final_aniso.eliashberg_type == EliashbergType.FSR


def test_epw_base_eliashberg_types(fixture_code, generate_structure):
    """Test that EpwBaseWorkChain correctly resolves eliashberg_type in get_builder_from_protocol and setup."""
    from aiida_epw.workflows.base import EpwBaseWorkChain
    from aiida.common import AttributeDict
    from aiida_epw.common import EliashbergType

    epw_code = fixture_code("epw.epw")
    structure = generate_structure()

    # 1. Test builder creation
    builder = EpwBaseWorkChain.get_builder_from_protocol(
        code=epw_code,
        structure=structure,
        protocol="fast",
        eliashberg_type="fsr",
    )
    assert builder.eliashberg_type == EliashbergType.FSR

    # 2. Test setup method logic by executing a mock workchain setup
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
        # Test isotropic
        inputs_iso = {
            "code": epw_code,
            "structure": structure,
            "eliashberg_type": orm.EnumData(EliashbergType.ISOTROPIC),
            "options": orm.Dict(dict={"resources": {"num_machines": 1}}),
            "parameters": orm.Dict(dict={"INPUTEPW": {}}),
        }
        wc_iso = MockWorkChain.__new__(MockWorkChain)
        wc_iso.inputs = AttributeDict(inputs_iso)
        wc_iso.ctx = AttributeDict()
        wc_iso.exposed_inputs = lambda *args, **kwargs: {
            "parameters": orm.Dict(inputs_iso.get("parameters", {}))
        }
        wc_iso.setup()

        params_iso = wc_iso.ctx.inputs.parameters.get_dict()["INPUTEPW"]
        assert params_iso["liso"] is True
        assert params_iso["laniso"] is False
        assert params_iso["tc_linear"] is False
        assert params_iso["fbw"] is False
        assert (
            "aiida.imag_iso_*"
            in wc_iso.ctx.inputs.metadata["options"]["additional_retrieve_list"]
        )

        # Test fsr
        inputs_fsr = {
            "code": epw_code,
            "structure": structure,
            "eliashberg_type": orm.EnumData(EliashbergType.FSR),
            "options": orm.Dict(dict={"resources": {"num_machines": 1}}),
            "parameters": orm.Dict(dict={"INPUTEPW": {}}),
        }
        wc_fsr = MockWorkChain.__new__(MockWorkChain)
        wc_fsr.inputs = AttributeDict(inputs_fsr)
        wc_fsr.ctx = AttributeDict()
        wc_fsr.exposed_inputs = lambda *args, **kwargs: {
            "parameters": orm.Dict(inputs_fsr.get("parameters", {}))
        }
        wc_fsr.setup()

        params_fsr = wc_fsr.ctx.inputs.parameters.get_dict()["INPUTEPW"]
        assert params_fsr["liso"] is False
        assert params_fsr["laniso"] is True
        assert params_fsr["tc_linear"] is False
        assert params_fsr["fbw"] is False
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
