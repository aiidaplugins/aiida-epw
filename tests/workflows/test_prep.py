"""Tests for focused ``EpwPrepWorkChain`` helper logic."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from aiida import orm


class Namespace(dict):
    """Small attribute-access dict for lightweight workflow tests."""

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc

    def __setattr__(self, key, value):
        self[key] = value


class TestManualWannierization:
    """Tests for manual Wannier90 projection/window handling."""

    def test_apply_manual_wannierization_moves_projections_to_input_port(self):
        """Manual projections are written through the projections input."""
        from aiida_epw.workflows.prep import apply_manual_wannierization

        w90_bands = Namespace(
            wannier90=Namespace(
                shift_energy_windows=orm.Bool(True),
                auto_energy_windows=orm.Bool(True),
                wannier90=Namespace(
                    parameters=orm.Dict(
                        dict={
                            "auto_projections": True,
                            "num_wann": 8,
                            "projections": ["Si:sp3"],
                            "dis_win_max": 12.0,
                        }
                    )
                ),
            )
        )

        applied = apply_manual_wannierization(
            w90_bands,
            {
                "parameters": {
                    "num_wann": 4,
                    "num_bands": 8,
                    "dis_win_max": 19.2,
                    "dis_froz_max": 16.6,
                },
                "shift_energy_windows": False,
                "auto_energy_windows": False,
            },
        )

        params = w90_bands.wannier90.wannier90.parameters.get_dict()

        assert applied is True
        assert w90_bands.wannier90.wannier90.projections.get_list() == ["Si:sp3"]
        assert "projections" not in params
        assert "auto_projections" not in params
        assert params["num_wann"] == 4
        assert params["num_bands"] == 8
        assert params["dis_win_max"] == 19.2
        assert params["dis_froz_max"] == 16.6
        assert w90_bands.wannier90.shift_energy_windows.value is False
        assert w90_bands.wannier90.auto_energy_windows.value is False

    def test_conflicting_manual_projections_raise(self):
        """Different projection sources should fail instead of guessing."""
        from aiida_epw.workflows.prep import apply_manual_wannierization

        w90_bands = Namespace(
            wannier90=Namespace(
                wannier90=Namespace(
                    parameters=orm.Dict(
                        dict={
                            "projections": ["Si:sp3"],
                        }
                    )
                ),
            )
        )

        with pytest.raises(ValueError, match="Conflicting manual Wannier"):
            apply_manual_wannierization(
                w90_bands,
                {
                    "projections": ["Si:p"],
                },
            )


class TestPrepRestartLogic:
    """Tests for orthogonal Wannier90/phonon restart selection."""

    @staticmethod
    def _fake_workchain(inputs):
        reports = []
        return SimpleNamespace(
            inputs=Namespace(inputs),
            ctx=Namespace(),
            report=reports.append,
            reports=reports,
        )

    @pytest.mark.parametrize(
        ("inputs", "run_w90", "run_ph"),
        [
            ({}, True, True),
            ({"parent_folder_ph": object()}, True, False),
            (
                {
                    "parent_folder_nscf": object(),
                    "parent_folder_chk": object(),
                },
                False,
                True,
            ),
            (
                {
                    "parent_folder_ph": object(),
                    "parent_folder_nscf": object(),
                    "parent_folder_chk": object(),
                },
                False,
                False,
            ),
        ],
    )
    def test_restart_skip_conditions_are_orthogonal(self, inputs, run_w90, run_ph):
        """Phonon and Wannier90 parent folders should skip only their own step."""
        from aiida_epw.workflows.prep import EpwPrepWorkChain

        workchain = self._fake_workchain(inputs)

        assert EpwPrepWorkChain.should_run_wannier90(workchain) is run_w90
        assert EpwPrepWorkChain.should_run_ph(workchain) is run_ph

    def test_epw_restart_uses_provided_lower_level_parent_folders(self):
        """EPW receives provided phonon, NSCF, and chk folders directly."""
        from aiida_epw.workflows.prep import EpwPrepWorkChain

        ph_folder = object()
        chk_folder = object()
        nscf_folder = object()
        workchain = self._fake_workchain(
            {
                "parent_folder_ph": ph_folder,
                "parent_folder_nscf": nscf_folder,
                "parent_folder_chk": chk_folder,
            }
        )

        assert EpwPrepWorkChain._get_ph_parent_folder(workchain) is ph_folder
        assert EpwPrepWorkChain._get_w90_nscf_and_chk_folders(workchain) == (
            nscf_folder,
            chk_folder,
        )

    def test_ph_restart_can_use_nscf_parent_scf_folder(self):
        """An NSCF parent can supply SCF context when phonon must still run."""
        from aiida_epw.workflows.prep import EpwPrepWorkChain

        scf_folder = object()
        nscf_folder = SimpleNamespace(
            creator=SimpleNamespace(
                inputs=SimpleNamespace(parent_folder=scf_folder),
            )
        )
        workchain = self._fake_workchain({"parent_folder_nscf": nscf_folder})

        assert EpwPrepWorkChain._get_w90_scf_parent_folder(workchain) is scf_folder

    def test_ph_restart_without_w90_or_nscf_context_returns_none(self):
        """Fresh phonon requires W90 provenance or an NSCF parent with SCF context."""
        from aiida_epw.workflows.prep import EpwPrepWorkChain

        workchain = self._fake_workchain(
            {
                "parent_folder_nscf": object(),
                "parent_folder_chk": object(),
            }
        )

        assert EpwPrepWorkChain._get_w90_scf_parent_folder(workchain) is None


class TestBandplotRunPhInputs:
    """Test prebuilt PhononBandsWorkChain inputs used by bandplot mode."""

    @staticmethod
    def _fake_workchain(inputs):
        reports = []
        return SimpleNamespace(
            inputs=Namespace(inputs),
            ctx=Namespace(qpoints="QPOINTS"),
            exit_codes=SimpleNamespace(ERROR_SUB_PROCESS_FAILED_PHONON="PH_ERROR"),
            report=reports.append,
            reports=reports,
        )

    def test_non_bandplot_ph_base_config_comes_from_ph_bands(self):
        """Non-bandplot phonons use ph_bands.dynamical_matrix.ph_base."""
        from aiida_epw.workflows.prep import EpwPrepWorkChain

        config = {
            "ph_bands": {
                "dynamical_matrix": {
                    "ph_base": {
                        "parallelize_qpoints": True,
                        "ph": {
                            "settings": {"PREPARE_FOR_EPW": True},
                            "parameters": {"INPUTPH": {"epsil": True}},
                        },
                    }
                }
            }
        }

        assert EpwPrepWorkChain._get_ph_base_config(config) == {
            "ph": {
                "settings": {"PREPARE_FOR_EPW": True},
                "parameters": {"INPUTPH": {"epsil": True}},
            }
        }

    def test_bandplot_requires_optional_phonon_bands_dependency(self, monkeypatch):
        """Bandplot mode should report a clear error without the optional package."""
        import aiida_epw.workflows.prep as prep_module

        workchain = self._fake_workchain({"bandplot": SimpleNamespace(value=1)})

        monkeypatch.setattr(prep_module, "PhononBandsWorkChain", None)

        assert prep_module.EpwPrepWorkChain.run_ph(workchain) == "PH_ERROR"
        assert "aiida_quantumespresso_ph is not installed" in workchain.reports[0]

    def test_bandplot_requires_prebuilt_ph_bands_inputs(self):
        """Bandplot should not build or guess PhononBandsWorkChain inputs in run_ph."""
        import aiida_epw.workflows.prep as prep_module

        if prep_module.PhononBandsWorkChain is None:
            pytest.skip("aiida_quantumespresso_ph is not installed")

        workchain = self._fake_workchain({"bandplot": SimpleNamespace(value=1)})

        assert prep_module.EpwPrepWorkChain.run_ph(workchain) == "PH_ERROR"
        assert "no 'ph_bands' inputs" in workchain.reports[0]

    def test_bandplot_run_ph_only_injects_runtime_wiring(self):
        """run_ph should only add parent_folder, qpoints, and call label."""
        import aiida_epw.workflows.prep as prep_module

        if prep_module.PhononBandsWorkChain is None:
            pytest.skip("aiida_quantumespresso_ph is not installed")

        captured = {}
        ph_bands_inputs = Namespace(
            {
                "dynamical_matrix": Namespace({"ph_main": Namespace()}),
                "metadata": Namespace(),
            }
        )
        workchain = self._fake_workchain(
            {
                "bandplot": SimpleNamespace(value=1),
                "ph_bands": Namespace(),
            }
        )
        workchain._get_w90_scf_parent_folder = lambda: "SCF_PARENT"
        workchain.exposed_inputs = lambda *_args, **_kwargs: ph_bands_inputs

        def fake_submit(process_class, **inputs):
            captured["process_class"] = process_class
            captured["inputs"] = inputs
            return SimpleNamespace(pk=123)

        workchain.submit = fake_submit

        prep_module.EpwPrepWorkChain.run_ph(workchain)

        assert captured["process_class"] is prep_module.PhononBandsWorkChain
        assert captured["inputs"]["dynamical_matrix"]["parent_folder"] == "SCF_PARENT"
        assert captured["inputs"]["dynamical_matrix"]["ph_main"]["qpoints"] == "QPOINTS"
        assert captured["inputs"]["metadata"]["call_link_label"] == "ph_bands"


class TestQuadrupoleFileMaterialization:
    """Tests for converting quadrupole remote folders to uploadable files."""

    @staticmethod
    def _fake_workchain():
        reports = []
        return SimpleNamespace(
            report=reports.append,
            reports=reports,
        )

    def test_fetch_quadrupole_file_prefers_creator_output(self):
        """Stored creator output avoids transport access."""
        from aiida_epw.workflows.prep import EpwPrepWorkChain

        quadrupole_file = SimpleNamespace(pk=456)
        quadrupole_dir = SimpleNamespace(
            creator=SimpleNamespace(
                outputs=SimpleNamespace(quadrupole_fmt=quadrupole_file),
                process_label="QuadrupoleWorkChain",
                pk=123,
            )
        )

        workchain = self._fake_workchain()

        assert (
            EpwPrepWorkChain._fetch_quadrupole_file(workchain, quadrupole_dir)
            is quadrupole_file
        )
        assert "Using quadrupole_fmt" in workchain.reports[0]


class TestWannier90Selection:
    """Tests for selecting the correct Wannier90 child workflow."""

    def test_manual_projections_use_bands_workchain(self):
        """Manual projection runs should use the non-optimizing bands workflow."""
        from aiida_epw.workflows.prep import EpwPrepWorkChain

        dummy = MagicMock()
        dummy.inputs = Namespace(
            w90_bands=Namespace(
                wannier90=Namespace(
                    wannier90=Namespace(projections=orm.List(list=["Si:sp3"]))
                )
            )
        )

        assert EpwPrepWorkChain._uses_wannier90_bands_workchain(dummy) is True


def test_get_builder_from_protocol_w90_script(
    fixture_localhost,
    fixture_code,
    generate_structure,
    generate_remote_data,
    monkeypatch,
):
    """Test get_builder_from_protocol explicitly handles and propagates w90_chk_to_ukk_script."""
    from aiida_epw.workflows.prep import EpwPrepWorkChain, WannierProjectionType
    from aiida_wannier90_workflows.workflows import (
        Wannier90OptimizeWorkChain,
        Wannier90BandsWorkChain,
    )
    from aiida_quantumespresso.workflows.ph.base import PhBaseWorkChain
    from aiida_epw.workflows.base import EpwBaseWorkChain

    from plumpy.ports import Port, PortNamespace

    monkeypatch.setattr(Port, "validate", lambda *a, **k: None)
    monkeypatch.setattr(PortNamespace, "validate", lambda *a, **k: None)

    # Mock subprocess builders to isolate the test from heavy DB lookups/pseudos
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

        def pop(self, key, default=None):
            return super().pop(key, default)

    # We need to return an instance that can behave like a dict/builder
    def mock_get_builder(*args, **kwargs):
        builder = MockBuilder()
        if "w90_chk_to_ukk_script" in kwargs:
            builder.w90_chk_to_ukk_script = kwargs["w90_chk_to_ukk_script"]
        return builder

    monkeypatch.setattr(
        Wannier90OptimizeWorkChain, "get_builder_from_protocol", mock_get_builder
    )
    monkeypatch.setattr(
        Wannier90BandsWorkChain, "get_builder_from_protocol", mock_get_builder
    )
    monkeypatch.setattr(PhBaseWorkChain, "get_builder_from_protocol", mock_get_builder)
    monkeypatch.setattr(EpwBaseWorkChain, "get_builder_from_protocol", mock_get_builder)
    monkeypatch.setattr(
        "aiida_epw.tools.band_analysis.detect_bandgap_from_bands", lambda *a, **k: None
    )

    # Set up mock codes
    pw_code = fixture_code("quantumespresso.pw")
    epw_code = fixture_code("epw.epw")
    ph_code = fixture_code("quantumespresso.ph")
    q2r_code = fixture_code("quantumespresso.q2r")
    matdyn_code = fixture_code("quantumespresso.matdyn")
    wannier_code = fixture_code("wannier90.wannier90")
    pw2wannier_code = fixture_code("quantumespresso.pw2wannier90")

    codes = {
        "pw": pw_code,
        "epw": epw_code,
        "ph": ph_code,
        "q2r": q2r_code,
        "matdyn": matdyn_code,
        "wannier90": wannier_code,
        "pw2wannier90": pw2wannier_code,
    }

    structure = generate_structure()
    w90_script = generate_remote_data(fixture_localhost, "/tmp/w90_script.jl")

    # Mock the reference bands input
    reference_bands = orm.BandsData()
    reference_bands.set_kpoints([[0.0, 0.0, 0.0]])
    reference_bands.set_bands([[0.0]])

    # Call get_builder_from_protocol with w90_chk_to_ukk_script
    builder = EpwPrepWorkChain.get_builder_from_protocol(
        structure=structure,
        codes=codes,
        protocol="fast",
        w90_chk_to_ukk_script=w90_script,
        wannier_projection_type=WannierProjectionType.ATOMIC_PROJECTORS_QE,
        reference_bands=reference_bands,
    )

    # Check that it is also propagated to epw_base builder
    assert builder.epw_base.w90_chk_to_ukk_script == w90_script


class TestGetBuilderFromProtocol:
    """Tests for `EpwPrepWorkChain.get_builder_from_protocol`."""

    def test_stash_mode_copy_is_injected(self, fixture_code, generate_structure):
        """Test that `stash_mode` = "copy" is injected when target_base is auto-injected."""
        from unittest.mock import patch
        from aiida_epw.workflows.prep import EpwPrepWorkChain
        from aiida_wannier90_workflows.common.types import WannierProjectionType
        from aiida.plugins import DataFactory
        import io

        UpfData = DataFactory("pseudo.upf")
        dummy_upf = UpfData(
            io.BytesIO(b'element="Si" z_valence="4.0"'), filename="Si.upf"
        )
        dummy_upf.store()

        codes = {
            "epw": fixture_code("epw.epw"),
            "ph": fixture_code("quantumespresso.ph"),
            "pw": fixture_code("quantumespresso.pw"),
            "wannier90": fixture_code("wannier90.wannier90"),
            "pw2wannier90": fixture_code("wannier90.pw2wannier90"),
        }

        from aiida_pseudo.groups.family import PseudoDojoFamily

        family = PseudoDojoFamily(label="PseudoDojo/0.5/PBE/SR/standard/upf")
        family.store()
        family.add_nodes([dummy_upf])

        structure = generate_structure()

        with (
            patch(
                "aiida_wannier90_workflows.utils.pseudo.get_pseudo_and_cutoff"
            ) as mock_get_pseudo,
            patch(
                "aiida_wannier90_workflows.utils.pseudo.get_wannier_number_of_bands"
            ) as mock_get_bands,
            patch(
                "aiida_wannier90_workflows.utils.pseudo.get_number_of_projections"
            ) as mock_get_projs,
            patch(
                "aiida_wannier90_workflows.utils.pseudo.get_pseudo_orbitals"
            ) as mock_get_orbitals,
            patch.object(PseudoDojoFamily, "get_recommended_cutoffs") as mock_cutoffs,
            patch.object(PseudoDojoFamily, "get_pseudos") as mock_pseudos,
        ):
            mock_get_pseudo.return_value = ({"Si": dummy_upf}, 30.0, 120.0)
            mock_get_bands.return_value = 8
            mock_get_projs.return_value = 4
            mock_get_orbitals.return_value = {"Si": {"pswfcs": [], "semicores": []}}
            mock_cutoffs.return_value = (30.0, 120.0)
            mock_pseudos.return_value = {"Si": dummy_upf}
            builder = EpwPrepWorkChain.get_builder_from_protocol(
                codes=codes,
                structure=structure,
                wannier_projection_type=WannierProjectionType.ANALYTIC,
            )

        # Verify that stash options have stash_mode = "copy" and target_basepath set
        epw_base_options = builder.epw_base.options
        assert "stash" in epw_base_options
        assert epw_base_options["stash"]["stash_mode"] == "copy"
        assert "target_base" in epw_base_options["stash"]
