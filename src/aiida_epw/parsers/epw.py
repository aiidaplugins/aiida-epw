import re

from aiida import orm
import numpy

from aiida_epw.calculations.epw import EpwCalculation
from aiida_epw.data import A2fData
from aiida_quantumespresso.parsers.base import BaseParser
from aiida_quantumespresso.utils.mapping import get_logging_container


class EpwParser(BaseParser):
    """``Parser`` implementation for the ``EpwCalculation`` calculation job."""

    success_string = "EPW.bib"

    @staticmethod
    def get_parser_settings_key():
        """Return the settings key reserved for parser-specific options."""
        return "parser_options"

    def parse(self, **kwargs):
        """Parse the retrieved files of a completed ``EpwCalculation`` into output nodes."""
        logs = get_logging_container()

        stdout, parsed_data, logs = self.parse_stdout_from_retrieved(logs)

        base_exit_code = self.check_base_errors(logs)
        if base_exit_code:
            return self.exit(base_exit_code, logs)

        parsed_epw, logs = self.parse_stdout(stdout, logs)
        parsed_data.update(parsed_epw)

        if (
            EpwCalculation._output_elbands_file
            in self.retrieved.base.repository.list_object_names()
        ):
            elbands_contents = self.retrieved.base.repository.get_object_content(
                EpwCalculation._output_elbands_file
            )
            self.out("el_band_structure", self.parse_bands(elbands_contents))

        if (
            EpwCalculation._output_phbands_file
            in self.retrieved.base.repository.list_object_names()
        ):
            phbands_contents = self.retrieved.base.repository.get_object_content(
                EpwCalculation._output_phbands_file
            )
            self.out("ph_band_structure", self.parse_bands(phbands_contents))

        if (
            EpwCalculation._OUTPUT_A2F_FILE
            in self.retrieved.base.repository.list_object_names()
        ):
            a2f_contents = self.retrieved.base.repository.get_object_content(
                EpwCalculation._OUTPUT_A2F_FILE
            )
            a2f_xydata, parsed_a2f = self.parse_a2f(a2f_contents)
            self.out("a2f", a2f_xydata)
            parsed_data.update(parsed_a2f)

        if "max_eigenvalue" in parsed_data:
            self.out("max_eigenvalue", parsed_data.pop("max_eigenvalue"))

        self.out("output_parameters", orm.Dict(parsed_data))

        if "ERROR_OUTPUT_STDOUT_INCOMPLETE" in logs.error:
            return self.exit(
                self.exit_codes.get("ERROR_OUTPUT_STDOUT_INCOMPLETE"), logs
            )

        return self.exit(logs=logs)

    @staticmethod
    def parse_stdout(stdout, logs):
        """Parse the ``stdout``."""

        def parse_max_eigenvalue(stdout_block):
            re_pattern = re.compile(
                r"\s+([\d\.]+)\s+([\d\.-]+)\s+\d+\s+[\d\.]+\s+\d+\n"
            )
            parsing_block = stdout_block.split(
                "Finish: Solving (isotropic) linearized Eliashberg"
            )[0]
            max_eigenvalue_array = orm.XyData()
            max_eigenvalue_array.set_array(
                "max_eigenvalue",
                numpy.array(re_pattern.findall(parsing_block), dtype=float),
            )
            return max_eigenvalue_array

        def parse_transport_matrices(block):
            """Parse transport tensor matrices from a text block."""
            parsed = {}

            def extract_matrix_pair(header_pattern, text):
                start_match = re.search(header_pattern, text)
                if not start_match:
                    return None, None

                lines_start = start_match.end()
                lines = text[lines_start:].strip().split("\n")
                # Take up to 3 lines
                lines = lines[:3]

                m1 = []
                m2 = []
                try:
                    for line in lines:
                        # Format " val1 val2 val3 | val4 val5 val6 " (approx)
                        parts = line.split("|")
                        if len(parts) < 2:
                            continue

                        # Handle Fortran D notation
                        row1 = [
                            float(x.replace("D", "E").replace("d", "e"))
                            for x in parts[0].split()
                        ]
                        row2 = [
                            float(x.replace("D", "E").replace("d", "e"))
                            for x in parts[1].split()
                        ]

                        if len(row1) == 3 and len(row2) == 3:
                            m1.append(row1)
                            m2.append(row2)
                except ValueError:
                    pass

                if len(m1) == 3 and len(m2) == 3:
                    return m1, m2
                return None, None

            def extract_single_matrix(header_pattern, text):
                start_match = re.search(header_pattern, text)
                if not start_match:
                    return None

                lines_start = start_match.end()
                lines = text[lines_start:].strip().split("\n")[:3]
                m = []
                try:
                    for line in lines:
                        # Just one set of 3 values
                        row = [
                            float(x.replace("D", "E").replace("d", "e"))
                            for x in line.split()
                        ]
                        if len(row) == 3:
                            m.append(row)
                except ValueError:
                    pass

                if len(m) == 3:
                    return m
                return None

            # 1. Conductivity
            cond, cond_B = extract_matrix_pair(
                r"Conductivity tensor without magnetic field\s*\|\s*with magnetic field \[Siemens/m\]",
                block,
            )
            if cond:
                parsed["conductivity"] = cond
                parsed["conductivity_with_B"] = cond_B

            # 2. Mobility
            mob, hall_mob = extract_matrix_pair(
                r"Mobility tensor without magnetic field\s*\|\s*(?:Hall mobility|with magnetic field) \[cm\^2/Vs\]",
                block,
            )
            if mob:
                parsed["mobility"] = mob
                parsed["hall_mobility"] = hall_mob

            # 3. Hall Factor
            hall_fac = extract_single_matrix(r"Hall factor", block)
            if hall_fac:
                parsed["hall_factor"] = hall_fac

            return parsed

        data_type_regex = (
            (
                "allen_dynes",
                float,
                re.compile(r"\s+Estimated Allen-Dynes Tc =\s+([\d\.]+) K"),
            ),
            (
                "fermi_energy_coarse",
                float,
                re.compile(r"\s+Fermi energy coarse grid =\s+([\d\.-]+)\seV"),
            ),
        )
        data_block_marker_parser = (
            (
                "max_eigenvalue",
                "Superconducting transition temp. Tc",
                parse_max_eigenvalue,
            ),
        )
        parsed_data = {}
        stdout_lines = stdout.split("\n")

        for line_number, line in enumerate(stdout_lines):
            for data_key, type, re_pattern in data_type_regex:
                match = re.search(re_pattern, line)
                if match:
                    parsed_data[data_key] = type(match.group(1))

            for data_key, data_marker, block_parser in data_block_marker_parser:
                if data_marker in line:
                    parsed_data[data_key] = block_parser(stdout[line_number:])

        # Parse carrier mobility matrices (SERTA and iBTE)
        # Identify SERTA block
        serta_match = re.search(
            r"BTE in the self-energy relaxation time approximation \(SERTA\)", stdout
        )
        # Identify BTE block (looking for standalone BTE header)
        bte_match = re.search(r"\n\s+BTE\s*\n", stdout)

        serta_idx = serta_match.start() if serta_match else -1
        bte_idx = bte_match.start() if bte_match else -1

        if serta_idx != -1:
            end_serta = bte_idx if bte_idx > serta_idx else len(stdout)
            serta_block = stdout[serta_idx:end_serta]
            serta_data = parse_transport_matrices(serta_block)

            for k, v in serta_data.items():
                parsed_data[f"serta_{k}"] = v

            # Maintain backward compatibility for mobility scalar
            if "mobility" in serta_data:
                parsed_data["mobility_SERTA"] = (
                    numpy.trace(numpy.array(serta_data["mobility"])) / 3.0
                )

        if bte_idx != -1:
            ibte_block = stdout[bte_idx:]
            ibte_data = parse_transport_matrices(ibte_block)

            for k, v in ibte_data.items():
                parsed_data[f"ibte_{k}"] = v

            # Maintain backward compatibility for mobility scalar
            if "mobility" in ibte_data:
                parsed_data["mobility_iBTE"] = (
                    numpy.trace(numpy.array(ibte_data["mobility"])) / 3.0
                )

        return parsed_data, logs

    @staticmethod
    def parse_a2f(content):
        """Parse the contents of the `.a2f` file."""
        a2f_array = numpy.array(
            [line.split() for line in content.splitlines()[1:501]], dtype=float
        )

        a2f_data = A2fData()
        a2f_data.set_a2f_data(
            frequency=a2f_array[:, 0],
            spectrum=a2f_array[:, 1:],
            lambda_values=numpy.array(
                [
                    value
                    for value in re.search(
                        r"Integrated el-ph coupling\n\s+\#\s+([\d\.\s]+)", content
                    )
                    .groups()[0]
                    .split()
                ],
                dtype=float,
            ),
            phonon_smearing=numpy.array(
                [
                    value
                    for value in re.search(
                        r"Phonon smearing \(meV\)\n\s+\#\s+([\d\.\s]+)", content
                    )
                    .groups()[0]
                    .split()
                ],
                dtype=float,
            ),
            electron_smearing=float(
                re.search(r"Electron smearing \(eV\)\s+([\d\.]+)", content).groups()[0]
            ),
            fermi_window=float(
                re.search(r"Fermi window \(eV\)\s+([\d\.]+)", content).groups()[0]
            ),
        )
        parsed_data = {
            "degaussw": float(
                re.search(r"Electron smearing \(eV\)\s+([\d\.]+)", content).groups()[0]
            ),
            "fsthick": float(
                re.search(r"Fermi window \(eV\)\s+([\d\.]+)", content).groups()[0]
            ),
        }
        return a2f_data, parsed_data

    @staticmethod
    def parse_bands(content):
        """Parse the contents of a band structure file."""
        nbnd, nks = (
            int(v)
            for v in re.search(r"&plot nbnd=\s+(\d+), nks=\s+(\d+)", content).groups()
        )
        kpt_pattern = re.compile(r"\s([\s-][\d\.]+)" * 3)
        band_pattern = re.compile(r"\s+([-\d\.]+)" * nbnd)

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
        bands_data.set_bands(bands, units="meV")

        return bands_data
