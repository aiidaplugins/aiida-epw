"""Fixtures and configuration for the `aiida-epw` package."""

import pytest

from pathlib import Path

from aiida import orm
from aiida.common import LinkType
from aiida.plugins.entry_point import (
    format_entry_point_string,
    get_entry_point_string_from_class,
)
from aiida.parsers.parser import Parser


pytest_plugins = "aiida.tools.pytest_fixtures"


@pytest.fixture
def fixture_sandbox():
    """Return a `SandboxFolder`."""
    from aiida.common.folders import SandboxFolder

    with SandboxFolder() as folder:
        yield folder


@pytest.fixture
def fixture_localhost(aiida_localhost):
    """Return a localhost `Computer` configured for tests."""
    localhost = aiida_localhost
    localhost.set_default_mpiprocs_per_machine(1)
    return localhost


@pytest.fixture
def fixture_code(aiida_code_installed):
    """Return an installed code for the requested calculation entry point."""

    def _fixture_code(entry_point_name):
        return aiida_code_installed(
            label=f"test.{entry_point_name}",
            default_calc_job_plugin=entry_point_name,
        )

    return _fixture_code


@pytest.fixture
def generate_workchain():
    """Instantiate a work chain for direct method testing."""

    def _generate_workchain(entry_point_name, inputs=None):
        from aiida.engine.utils import instantiate_process
        from aiida.manage.manager import get_manager
        from aiida.plugins import WorkflowFactory

        manager = get_manager()
        runner = manager.get_runner()

        process_class = WorkflowFactory(entry_point_name)
        return instantiate_process(runner, process_class, **(inputs or {}))

    return _generate_workchain


@pytest.fixture
def generate_calc_job():
    """Instantiate a calcjob and call `prepare_for_submission`."""

    def _generate_calc_job(folder, entry_point_name, inputs=None):
        from aiida.engine.utils import instantiate_process
        from aiida.manage.manager import get_manager
        from aiida.plugins import CalculationFactory

        manager = get_manager()
        runner = manager.get_runner()

        process_class = CalculationFactory(entry_point_name)
        process = instantiate_process(runner, process_class, **inputs)

        return process.prepare_for_submission(folder)

    return _generate_calc_job


@pytest.fixture
def serialize_builder():
    """Serialize a process builder into plain Python types for protocol tests."""

    def serialize_data(data):
        from aiida.orm import (
            AbstractCode,
            BaseType,
            Data,
            Dict,
            KpointsData,
            List,
            RemoteData,
            SinglefileData,
        )
        from aiida.plugins import DataFactory

        StructureData = DataFactory("core.structure")

        if isinstance(data, dict):
            return {key: serialize_data(value) for key, value in data.items()}

        if isinstance(data, BaseType):
            return data.value

        if isinstance(data, AbstractCode):
            return data.full_label

        if isinstance(data, Dict):
            return data.get_dict()

        if isinstance(data, List):
            return data.get_list()

        if isinstance(data, StructureData):
            return data.get_formula()

        if isinstance(data, RemoteData):
            return data.base.repository.hash()

        if isinstance(data, KpointsData):
            try:
                return data.get_kpoints()
            except AttributeError:
                return data.get_kpoints_mesh()

        if isinstance(data, SinglefileData):
            return data.get_content()

        if isinstance(data, Data):
            return data.base.caching._get_hash()

        return data

    def _serialize_builder(builder):
        return serialize_data(builder._inputs(prune=True))

    return _serialize_builder


@pytest.fixture
def generate_remote_data():
    """Return a `RemoteData` node."""

    def _generate_remote_data(computer, remote_path, entry_point_name=None):
        from aiida.plugins.entry_point import format_entry_point_string

        remote = orm.RemoteData(remote_path=remote_path)
        remote.computer = computer

        if entry_point_name is not None:
            creator = orm.CalcJobNode(
                computer=computer,
                process_type=format_entry_point_string(
                    "aiida.calculations", entry_point_name
                ),
            )
            creator.set_option(
                "resources", {"num_machines": 1, "num_mpiprocs_per_machine": 1}
            )
            remote.base.links.add_incoming(
                creator, link_type=LinkType.CREATE, link_label="remote_folder"
            )
            creator.store()

        return remote

    return _generate_remote_data


@pytest.fixture
def generate_structure():
    """Return a minimal silicon structure for workflow tests."""

    def _generate_structure():
        structure = orm.StructureData(
            cell=[
                [0.0, 2.715, 2.715],
                [2.715, 0.0, 2.715],
                [2.715, 2.715, 0.0],
            ]
        )
        structure.append_atom(position=(0.0, 0.0, 0.0), symbols="Si")
        structure.append_atom(position=(1.3575, 1.3575, 1.3575), symbols="Si")
        return structure

    return _generate_structure


@pytest.fixture
def generate_kpoints_mesh():
    """Return a `KpointsData` node with the provided mesh."""

    def _generate_kpoints_mesh(mesh):
        kpoints = orm.KpointsData()
        kpoints.set_kpoints_mesh(mesh)
        return kpoints

    return _generate_kpoints_mesh


@pytest.fixture
def files_path():
    """Path to the data files used for the tests."""
    return Path(__file__).parent / "files"


@pytest.fixture
def parse_from_files(aiida_localhost, files_path):
    """Return a function that parses the files from a corresponding test name."""

    def factory(parser_class: Parser, test_name: str):
        """Parse the files from the corresponding test name using the parser class.

        :param parser_class: parser class used for the parsing.
        :param test_name: name of the directory in which the test files are stored.
            Resolves to `tests/files/parsers/<parser_entry_point.split('.')[-1]>/test_name`
        :return: Tuple of parsed results and the `CalcFunctionNode` representing the process of parsing
        """
        parser_entry_point = get_entry_point_string_from_class(
            class_module=parser_class.__module__, class_name=parser_class.__name__
        )
        calc_entry_point = format_entry_point_string(
            group="aiida.calculations", name=parser_entry_point.split(":")[1]
        )
        node = orm.CalcJobNode(computer=aiida_localhost, process_type=calc_entry_point)
        node.base.attributes.set("output_filename", "aiida.out")

        directory_path = (
            files_path / "parsers" / parser_entry_point.split(".")[-1] / test_name
        )

        node.store()

        retrieved = orm.FolderData()
        retrieved.base.repository.put_object_from_tree(directory_path.as_posix())

        retrieved.base.links.add_incoming(
            node, link_type=LinkType.CREATE, link_label="retrieved"
        )
        retrieved.store()

        return parser_class.parse_from_node(node, store_provenance=False)

    return factory
