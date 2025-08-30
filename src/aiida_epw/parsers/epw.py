import re

from aiida import orm
import numpy

from aiida_quantumespresso.calculations.epw import EpwCalculation
from aiida_quantumespresso.parsers.base import BaseParser
from aiida_quantumespresso.utils.mapping import get_logging_container


class EpwParser(BaseParser):
    """``Parser`` implementation for the ``EpwCalculation`` calculation job."""

    success_string='EPW.bib'

    def parse(self, **kwargs):
        """Parse the retrieved files of a completed ``EpwCalculation`` into output nodes."""
        logs = get_logging_container()

        stdout, parsed_data, logs = self.parse_stdout_from_retrieved(logs)

        base_exit_code = self.check_base_errors(logs)
        if base_exit_code:
            return self.exit(base_exit_code, logs)

        parsed_epw, logs = self.parse_stdout(stdout, logs)
        parsed_data.update(parsed_epw)

        if EpwCalculation._output_elbands_file in self.retrieved.base.repository.list_object_names():
            elbands_contents = self.retrieved.base.repository.get_object_content(EpwCalculation._output_elbands_file)
            self.out('el_band_structure', self.parse_bands(elbands_contents))

        if EpwCalculation._output_phbands_file in self.retrieved.base.repository.list_object_names():
            phbands_contents = self.retrieved.base.repository.get_object_content(EpwCalculation._output_phbands_file)
            self.out('ph_band_structure', self.parse_bands(phbands_contents))

        if EpwCalculation._OUTPUT_A2F_FILE in self.retrieved.base.repository.list_object_names():
            a2f_contents = self.retrieved.base.repository.get_object_content(EpwCalculation._OUTPUT_A2F_FILE)
            a2f_xydata, parsed_a2f = self.parse_a2f(a2f_contents)
            self.out('a2f', a2f_xydata)
            parsed_data.update(parsed_a2f)

        if 'max_eigenvalue' in parsed_data:
            self.out('max_eigenvalue', parsed_data.pop('max_eigenvalue'))

        self.out('output_parameters', orm.Dict(parsed_data))

        if 'ERROR_OUTPUT_STDOUT_INCOMPLETE' in logs.error:
            return self.exit(self.exit_codes.get('ERROR_OUTPUT_STDOUT_INCOMPLETE'), logs)

        return self.exit(logs=logs)

    @staticmethod
    def parse_stdout(stdout, logs):
        """Parse the ``stdout``."""

        def parse_max_eigenvalue(stdout_block):
            re_pattern = re.compile(r'\s+([\d\.]+)\s+([\d\.-]+)\s+\d+\s+[\d\.]+\s+\d+\n')
            parsing_block = stdout_block.split('Finish: Solving (isotropic) linearized Eliashberg')[0]
            max_eigenvalue_array = orm.XyData()
            max_eigenvalue_array.set_array(
                'max_eigenvalue', numpy.array(re_pattern.findall(parsing_block), dtype=float)
            )
            return max_eigenvalue_array

        data_type_regex = (
            ('allen_dynes', float, re.compile(r'\s+Estimated Allen-Dynes Tc =\s+([\d\.]+) K')),
            ('fermi_energy_coarse', float, re.compile(r'\s+Fermi energy coarse grid =\s+([\d\.-]+)\seV')),
        )
        data_block_marker_parser = (
            ('max_eigenvalue', 'Superconducting transition temp. Tc', parse_max_eigenvalue),
        )
        parsed_data = {}
        stdout_lines = stdout.split('\n')

        for line_number, line in enumerate(stdout_lines):
            for data_key, type, re_pattern in data_type_regex:
                match = re.search(re_pattern, line)
                if match:
                    parsed_data[data_key] = type(match.group(1))

            for data_key, data_marker, block_parser in data_block_marker_parser:
                if data_marker in line:
                    parsed_data[data_key] = block_parser(stdout[line_number:])

        return parsed_data, logs

    @staticmethod
    def parse_a2f(content):
        """Parse the contents of the `.a2f` file."""
        a2f_array = numpy.array([line.split() for line in content.splitlines()[1:501]], dtype=float)

        a2f_xydata = orm.XyData()
        a2f_xydata.set_array(
            'frequency', a2f_array[:, 0]
        )
        a2f_xydata.set_array(
            'a2f', a2f_array[:, 1:]
        )
        a2f_xydata.set_array(
            'lambda', numpy.array([
                value for value in re.search(r'Integrated el-ph coupling\n\s+\#\s+([\d\.\s]+)', content).groups()[0].split()
            ], dtype=float
        ))
        a2f_xydata.set_array(
            'degaussq', numpy.array([
                value for value in re.search(r'Phonon smearing \(meV\)\n\s+\#\s+([\d\.\s]+)', content).groups()[0].split()
            ], dtype=float
        ))
        parsed_data = {
            'degaussw': float(re.search(r'Electron smearing \(eV\)\s+([\d\.]+)', content).groups()[0]),
            'fsthick': float(re.search(r'Fermi window \(eV\)\s+([\d\.]+)', content).groups()[0])
        }
        return a2f_xydata, parsed_data

    @staticmethod
    def parse_bands(content):
        """Parse the contents of a band structure file."""
        nbnd, nks = (
            int(v) for v in re.search(
                r'&plot nbnd=\s+(\d+), nks=\s+(\d+)', content
            ).groups()
        )
        kpt_pattern = re.compile(r'\s([\s-][\d\.]+)' * 3)
        band_pattern = re.compile(r'\s+([-\d\.]+)' * nbnd)

        kpts = []
        bands = []

        for number, line in enumerate(content.splitlines()):
            match_kpt = re.search(kpt_pattern, line)
            if match_kpt and number % 2 == 1:
                kpts.append(list(match_kpt.groups()))

            match_band = re.search(band_pattern, line)
            if match_band and number % 2 == 0:
                bands.append(list(match_band.groups()))

        kpoints_data = orm.KpointsData()
        kpoints_data.set_kpoints(numpy.array(kpts, dtype=float))
        bands = numpy.array(bands, dtype=float)

        bands_data = orm.BandsData()
        bands_data.set_kpointsdata(kpoints_data)
        bands_data.set_bands(bands, units='meV')

        return bands_data
