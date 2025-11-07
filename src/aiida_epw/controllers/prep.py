"""Submission controller for the `EpwPrepWorkChain`."""
from __future__ import annotations

import copy

from aiida import orm

from aiida_quantumespresso.workflows.pw.relax import PwRelaxWorkChain
from aiida_quantumespresso.common.types import ElectronicType, SpinType
from aiida_wannier90_workflows.common.types import WannierProjectionType
from aiida_submission_controller.from_group import FromGroupSubmissionController
from aiida_epw.workflows.prep import EpwPrepWorkChain


class EpwPrepWorkChainController(FromGroupSubmissionController):
    """Submission controller for the `EpwPrepWorkChain`."""

    pw_code: str
    ph_code: str
    projwfc_code: str
    pw2wannier90_code: str
    wannier90_code: str
    epw_code: str
    chk2ukk_path: str
    protocol: str = "moderate"
    overrides: dict | None = None
    electronic_type: ElectronicType = ElectronicType.METAL
    spin_type: SpinType = SpinType.NONE
    wannier_projection_type: WannierProjectionType = WannierProjectionType.SCDM
    bands_qe_group: str | None = None

    def get_inputs_and_processclass_from_extras(self, extras_values, dry_run=False):
        """Return inputs and process class for the submission of this specific process."""
        parent_node = self.get_parent_node_from_extras(extras_values)

        # Depending on the type of node in the parent class, grab the right inputs
        if isinstance(parent_node, orm.StructureData):
            structure = parent_node
        elif parent_node.process_class == PwRelaxWorkChain:
            structure = parent_node.outputs.output_structure
        else:
            raise TypeError(f"Node {parent_node} from parent group is of incorrect type: {type(parent_node)}.")

        codes = {
            "pw": self.pw_code,
            "ph": self.ph_code,
            "projwfc": self.projwfc_code,
            "pw2wannier90": self.pw2wannier90_code,
            "wannier90": self.wannier90_code,
            "epw": self.epw_code,
        }
        codes = {key: orm.load_code(code_label) for key, code_label in codes.items()}

        overrides = copy.deepcopy(self.overrides)

        inputs = {
            "codes": codes,
            "structure": structure,
            "protocol": self.protocol,
            "overrides": overrides,
            "electronic_type": self.electronic_type,
            "spin_type": self.spin_type,
            "wannier_projection_type": self.wannier_projection_type,
        }

        if self.wannier_projection_type == WannierProjectionType.ATOMIC_PROJECTORS_QE:
            raise ValueError('Atomic projectors not yet supported!')

        builder = EpwPrepWorkChain.get_builder_from_protocol(**inputs)
        w90_script = orm.RemoteData(remote_path=self.chk2ukk_path, computer=codes["pw"].computer)
        builder.w90_chk_to_ukk_script = w90_script

        return builder
