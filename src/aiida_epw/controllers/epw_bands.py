"""Submission controller for a band structure `EpwCalculation`."""

import copy

from aiida import orm

from aiida_quantumespresso.workflows.protocols.utils import recursive_merge

from aiida_epw.calculations.epw import EpwCalculation
from aiida_submission_controller import FromGroupSubmissionController
from aiida_wannier90_workflows.workflows.bands import Wannier90BandsWorkChain

class EpwBandsCalculationController(FromGroupSubmissionController):
    """Submission controller for a band structure `EpwCalculation`."""

    epw_code: str
    overrides: dict | None = None

    def get_inputs_and_processclass_from_extras(self, extras_values, _):
        """Return inputs and process class for the submission of this specific process."""
        parent_node = self.get_parent_node_from_extras(extras_values)

        overrides = copy.deepcopy(self.overrides)

        wannier = parent_node.get_outgoing(Wannier90BandsWorkChain).one().node

        epw_source = parent_node.base.links.get_outgoing(link_label_filter="epw").first().node

        builder = EpwCalculation.get_builder()
        builder.code = orm.load_code(self.epw_code)

        parameters = {
            "INPUTEPW": {
                "band_plot": True,
                "elph": True,
                "epbread": False,
                "epbwrite": False,
                "epwread": True,
                "epwwrite": False,
                "fsthick": 100,
                "wannierize": False,
                "vme": "dipole",
            }
        }
        parameters["INPUTEPW"]["nbndsub"] = epw_source.inputs.parameters["INPUTEPW"]["nbndsub"]
        parameters["INPUTEPW"]["use_ws"] = epw_source.inputs.parameters["INPUTEPW"].get("use_ws", False)
        if "bands_skipped" in epw_source.inputs.parameters["INPUTEPW"]:
            parameters["INPUTEPW"]["bands_skipped"] = epw_source.inputs.parameters["INPUTEPW"].get("bands_skipped")

        parameters = recursive_merge(parameters, overrides.get("parameters", {}))

        epw_folder = parent_node.outputs.epw_folder

        builder.parameters = orm.Dict(parameters)

        kpoints_path = orm.KpointsData()
        kpoints_path.set_kpoints(wannier.outputs.seekpath_parameters.get_dict()["explicit_kpoints_rel"])
        builder.kpoints = epw_source.inputs.kpoints
        builder.qpoints = epw_source.inputs.qpoints
        builder.kfpoints = kpoints_path
        builder.qfpoints = kpoints_path
        builder.parent_folder_epw = epw_folder
        builder.settings = orm.Dict(overrides.get("settings", {}))
        builder.metadata = overrides.get("metadata", {})

        return builder
