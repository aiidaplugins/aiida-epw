"""Fixtures and configuration for the `aiida-epw` package."""

import pytest

from pathlib import Path

from aiida import orm
from aiida.common import LinkType
from aiida.plugins.entry_point import format_entry_point_string, get_entry_point_string_from_class
from aiida.parsers.parser import Parser


pytest_plugins = 'aiida.tools.pytest_fixtures'


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
            group='aiida.calculations', name=parser_entry_point.split(':')[1]
        )
        node = orm.CalcJobNode(computer=aiida_localhost, process_type=calc_entry_point)
        node.base.attributes.set('output_filename', 'aiida.out')

        directory_path = files_path / 'parsers' / parser_entry_point.split('.')[-1] / test_name

        node.store()

        retrieved = orm.FolderData()
        retrieved.base.repository.put_object_from_tree(directory_path.as_posix())

        retrieved.base.links.add_incoming(node, link_type=LinkType.CREATE, link_label='retrieved')
        retrieved.store()

        return parser_class.parse_from_node(node, store_provenance=False)

    return factory
