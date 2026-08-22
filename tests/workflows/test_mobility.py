"""Tests for the mobility workflow protocol."""


def test_mobility_protocol_omits_plugin_managed_parameters():
    """Mobility protocols omit plugin-managed EPW keywords."""
    from aiida_epw.calculations.epw import EpwCalculation
    from aiida_epw.workflows.mobility import MobilityWorkChain

    inputs = MobilityWorkChain.get_protocol_inputs("fast")
    parameters = inputs["epw_mobility"]["parameters"]["INPUTEPW"]
    managed = {
        key
        for namelist, key in EpwCalculation._blocked_keywords
        if namelist == "INPUTEPW"
    }

    assert managed.isdisjoint(parameters)
