"""Parser for the EPW calculations."""

import re
from pathlib import Path

from aiida import orm
from aiida_quantumespresso.parsers.base import BaseParser
from aiida_quantumespresso.utils.mapping import get_logging_container
from packaging.version import Version

from aiida_epw.calculations.epw import EpwCalculation
from aiida_epw.data import (
    A2fData,
    DosData,
    GapFunctionData,
    LambdaFSData,
    PA2fData,
    PDosData,
    PhDosData,
)
from aiida_epw.parsers.schemas import (
    REGEX_PATTERNS_LEGACY,
    REGEX_PATTERNS_MODERN,
)


class EpwParser(BaseParser):
    """``Parser`` implementation for the ``EpwCalculation`` calculation job."""

    success_string = "EPW.bib"

    class_error_map = {
        "Size of required memory exceeds max_memlt": "ERROR_MEMORY_EXCEEDS_MAX_MEMLT",
        "internal error, cannot bracket Ef": "ERROR_CANNOT_BRACKET_EF",
    }

    @staticmethod
    def is_scheduler_out_of_memory(stderr):
        """Return whether scheduler stderr indicates an out-of-memory kill."""
        if not stderr:
            return False

        stderr_lower = stderr.lower()
        return any(
            marker in stderr_lower
            for marker in (
                "oom_kill",
                "out of memory",
                "oom killed",
                "exceeded memory limit",
            )
        )

    @staticmethod
    def get_parser_settings_key():
        """Return the settings key reserved for parser-specific options."""
        return "parser_options"

    def get_retrieved_content(self, *filenames):
        """Return the content of the first retrieved file that exists."""
        for filename in filenames:
            try:
                return self.retrieved.base.repository.get_object_content(filename)
            except FileNotFoundError:
                continue

        return None

    def get_retrieved_contents_matching(self, pattern):
        """Return retrieved file contents whose names match a compiled regex pattern."""
        return {
            filename: self.retrieved.base.repository.get_object_content(filename)
            for filename in self.retrieved.base.repository.list_object_names()
            if pattern.match(filename)
        }

    def parse(self, **kwargs):
        """Parse the retrieved files of a completed ``EpwCalculation`` into output nodes."""
        logs = get_logging_container()

        stdout, parsed_data, logs = self.parse_stdout_from_retrieved(logs)
        scheduler_stderr = self.node.get_scheduler_stderr()

        # Preserve scheduler walltime failures instead of overriding them with parser-side stdout errors.
        if (
            self.node.exit_status
            == self.exit_codes.ERROR_SCHEDULER_OUT_OF_WALLTIME.status
        ):
            return self.exit(logs=logs)

        if (
            self.node.exit_status
            == self.exit_codes.ERROR_SCHEDULER_OUT_OF_MEMORY.status
        ):
            return self.exit(logs=logs)

        if self.is_scheduler_out_of_memory(scheduler_stderr):
            return self.exit(self.exit_codes.ERROR_SCHEDULER_OUT_OF_MEMORY, logs)

        base_exit_code = self.check_base_errors(logs)
        if base_exit_code:
            return self.exit(base_exit_code, logs)

        parsed_epw, logs = self.parse_stdout(
            stdout, logs, code_version=Version(parsed_data["code_version"])
        )
        parsed_data.update(parsed_epw)

        elbands_contents = self.get_retrieved_content(
            EpwCalculation._output_elbands_file
        )
        if elbands_contents is not None:
            self.out(
                "el_band_structure",
                self.parse_bands(
                    elbands_contents, getattr(self.node.inputs, "kfpoints", None), "eV"
                ),
            )

        phbands_contents = self.get_retrieved_content(
            EpwCalculation._output_phbands_file
        )
        if phbands_contents is not None:
            self.out(
                "ph_band_structure",
                self.parse_bands(
                    phbands_contents, getattr(self.node.inputs, "qfpoints", None), "meV"
                ),
            )

        a2f_contents = self.get_retrieved_content(EpwCalculation._OUTPUT_A2F_FILE)
        if a2f_contents is not None:
            a2f_data = A2fData.from_string(a2f_contents)
            self.out("a2f", a2f_data)
            parsed_data.update(
                {
                    "degaussw": a2f_data.electron_smearing,
                    "fsthick": a2f_data.fermi_window,
                }
            )

        # Declarative specification for standard retrieved array output files
        standard_outputs = [
            (
                [
                    EpwCalculation._OUTPUT_DOS_FILE,
                    Path(
                        EpwCalculation._OUTPUT_SUBFOLDER,
                        EpwCalculation._OUTPUT_DOS_FILE,
                    ).as_posix(),
                ],
                "dos",
                DosData.from_string,
            ),
            ([EpwCalculation._OUTPUT_PHDOS_FILE], "phdos", PhDosData.from_string),
            (
                [EpwCalculation._OUTPUT_PHDOS_PROJ_FILE],
                "phdos_proj",
                PDosData.from_string,
            ),
            ([EpwCalculation._OUTPUT_A2F_PROJ_FILE], "a2f_proj", PA2fData.from_string),
            (
                [EpwCalculation._OUTPUT_LAMBDA_FS_FILE],
                "lambda_FS",
                LambdaFSData.from_string,
            ),
            (
                [EpwCalculation._OUTPUT_LAMBDA_K_PAIRS_FILE],
                "lambda_k_pairs",
                self.parse_lambda_k_pairs,
            ),
        ]

        for paths, link_label, parser_func in standard_outputs:
            contents = self.get_retrieved_content(*paths)
            if contents is not None:
                self.out(link_label, parser_func(contents))

        iso_gap_filecontents = self.get_retrieved_contents_matching(
            re.compile(rf"{EpwCalculation._PREFIX}\.imag_iso_\d+\.\d+$")
        )
        if iso_gap_filecontents:
            self.out(
                "iso_gap_functions",
                GapFunctionData.from_files(
                    iso_gap_filecontents, prefix=EpwCalculation._PREFIX, kind="iso"
                ),
            )

        aniso_gap_filecontents = self.get_retrieved_contents_matching(
            re.compile(rf"{EpwCalculation._PREFIX}\.imag_aniso_gap0_\d+\.\d+$")
        )
        if aniso_gap_filecontents:
            self.out(
                "aniso_gap_functions",
                GapFunctionData.from_files(
                    aniso_gap_filecontents, prefix=EpwCalculation._PREFIX, kind="aniso"
                ),
            )

        if "max_eigenvalue" in parsed_data:
            self.out("max_eigenvalue", parsed_data.pop("max_eigenvalue"))

        if "Allen_Dynes_Tc" in parsed_data:
            parsed_data.setdefault("allen_dynes", parsed_data["Allen_Dynes_Tc"])

        self.out("output_parameters", orm.Dict(parsed_data))

        for exit_code in list(self.get_error_map().values()):
            if exit_code in logs.error:
                return self.exit(self.exit_codes.get(exit_code), logs)

        if "ERROR_OUTPUT_STDOUT_INCOMPLETE" in logs.error:
            return self.exit(
                self.exit_codes.get("ERROR_OUTPUT_STDOUT_INCOMPLETE"), logs
            )

        return self.exit(logs=logs)

    @staticmethod
    def parse_stdout(stdout, logs, code_version):
        """Parse the ``stdout``."""

        def parse_max_eigenvalue(stdout_block):
            from aiida_epw.tools.parsers import parse_epw_max_eigenvalue

            parsed_max_ev = parse_epw_max_eigenvalue(stdout_block)
            max_eigenvalue_array = orm.XyData()
            max_eigenvalue_array.set_array(
                "max_eigenvalue",
                parsed_max_ev["max_eigenvalue"],
            )
            return max_eigenvalue_array

        patterns = (
            REGEX_PATTERNS_LEGACY
            if code_version < Version("5.9")
            else REGEX_PATTERNS_MODERN
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
            for key, type_func, pattern in patterns:
                match = pattern.search(line)
                if match:
                    parsed_data[key] = type_func(match.group(1))

            for (
                data_key,
                data_marker,
                block_parser,
            ) in data_block_marker_parser:
                if data_marker in line:
                    parsed_data[data_key] = block_parser(
                        "\n".join(stdout_lines[line_number:])
                    )

        # Parse carrier mobility matrices (SERTA and iBTE)
        from aiida_epw.tools.parsers import parse_transport_matrices
        import numpy

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

        # Parse Eliashberg temperature blocks
        from aiida_epw.tools.parsers import parse_stdout_eliashberg

        parsed_data.update(parse_stdout_eliashberg(stdout))

        return parsed_data, logs

    @staticmethod
    def parse_bands(content, kpoints_data, units):
        """Parse the contents of a band structure file."""
        from aiida_epw.tools.parsers import parse_epw_bands

        parsed = parse_epw_bands(content)

        if kpoints_data is None:
            kpoints_data = orm.KpointsData()
            kpoints_data.set_kpoints(parsed["kpoints"])

        bands_data = orm.BandsData()
        # We should use the KpointsData from the inputs.
        bands_data.set_kpointsdata(kpoints_data)
        bands_data.set_bands(parsed["bands"], units=units)

        return bands_data

    @staticmethod
    def parse_lambda_k_pairs(content):
        """Parse ``.lambda_k_pairs`` content into a generic DOS-style dataset."""
        from aiida_epw.tools.parsers import parse_epw_lambda_k_pairs

        return DosData.from_parsed(parse_epw_lambda_k_pairs(content))
