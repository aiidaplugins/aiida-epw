import re
from pathlib import Path

import numpy
from aiida import orm
from aiida_quantumespresso.parsers.base import BaseParser
from aiida_quantumespresso.utils.mapping import get_logging_container

from aiida_epw.calculations.epw import EpwCalculation
from aiida_epw.data import (
    A2fData,
    PA2fData,
    DosData,
    PDosData,
    PhDosData,
    GapFunctionData,
    LambdaFSData,
)
from aiida_epw.tools.parsers import (
    parse_epw_a2f,
    parse_epw_a2f_proj,
    parse_epw_bands,
    parse_epw_dos,
    parse_epw_imag_aniso_gap0,
    parse_epw_imag_iso,
    parse_epw_lambda_fs,
    parse_epw_lambda_k_pairs,
    parse_epw_max_eigenvalue,
    parse_epw_phdos,
    parse_epw_phdos_proj,
    parse_aniso_FS,
    parse_aniso,
)


class EpwParser(BaseParser):
    """``Parser`` implementation for the ``EpwCalculation`` calculation job."""

    success_string = "EPW.bib"

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

        base_exit_code = self.check_base_errors(logs)
        if base_exit_code:
            return self.exit(base_exit_code, logs)

        parsed_epw, logs = self.parse_stdout(stdout, logs)
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

        # Determine whether Eliashberg is enabled and which files to parse
        eliashberg_enabled = None
        momentum_dependence = None
        real_axis = None
        analytical_continuation = None

        # 1. Try to read from direct inputs
        if any(
            hasattr(self.node.inputs, f)
            for f in (
                "momentum_dependence",
                "full_bandwidth",
                "real_axis",
                "analytical_continuation",
            )
        ):
            eliashberg_enabled = True

            momentum_dependence_input = getattr(
                self.node.inputs, "momentum_dependence", None
            )
            momentum_dependence = (
                momentum_dependence_input.value
                if momentum_dependence_input is not None
                else False
            )

            real_axis_input = getattr(self.node.inputs, "real_axis", None)
            real_axis = real_axis_input.value if real_axis_input is not None else False

            analytical_continuation_input = getattr(
                self.node.inputs, "analytical_continuation", None
            )
            analytical_continuation = (
                analytical_continuation_input.value
                if analytical_continuation_input is not None
                else None
            )
        else:
            # 2. Try to read from parameters
            parameters = getattr(self.node.inputs, "parameters", None)
            if parameters is not None:
                inputepw = parameters.get_dict().get("INPUTEPW", {})
                eliashberg_enabled = inputepw.get("eliashberg", False)
                momentum_dependence = inputepw.get("laniso", False)
                real_axis = inputepw.get("lreal", False)
                if inputepw.get("lacon", False):
                    analytical_continuation = "acon"
                elif inputepw.get("lpade", False):
                    analytical_continuation = "pade"

        # 3. Fallback: If we still don't know (e.g. mock node without inputs in unit tests),
        # check the retrieved folder for the existence of key files!
        if eliashberg_enabled is None:
            retrieved_files = self.retrieved.base.repository.list_object_names()
            has_iso_files = any("iso" in name for name in retrieved_files)
            has_aniso_files = any(
                "aniso" in name or "lambda_FS" in name or "lambda_k_pairs" in name
                for name in retrieved_files
            )
            has_a2f_files = any("a2f" in name for name in retrieved_files)

            if has_iso_files or has_aniso_files or has_a2f_files:
                eliashberg_enabled = True
                momentum_dependence = has_aniso_files

                # Check for real_axis based on retrieved files
                has_real_files = any("real" in name for name in retrieved_files)
                real_axis = has_real_files
            else:
                eliashberg_enabled = False

        if eliashberg_enabled:
            # Determine allowed prefixes based on inputs
            allowed_prefixes = []
            if real_axis is True:
                allowed_prefixes.append("real")
            elif real_axis is False:
                allowed_prefixes.append("imag")
                if analytical_continuation == "pade":
                    allowed_prefixes.append("pade")
                elif analytical_continuation == "acon":
                    allowed_prefixes.append("acon")
                else:
                    allowed_prefixes.extend(["pade", "acon"])
            else:
                allowed_prefixes.extend(["imag", "real", "pade", "acon"])

            prefix_pattern = "|".join(allowed_prefixes)

            a2f_contents = self.get_retrieved_content(EpwCalculation._OUTPUT_A2F_FILE)
            if a2f_contents is not None:
                a2f_data, parsed_a2f = self.parse_a2f(a2f_contents)
                self.out("a2f", a2f_data)
                parsed_data.update(parsed_a2f)

            dos_contents = self.get_retrieved_content(
                EpwCalculation._OUTPUT_DOS_FILE,
                Path(
                    EpwCalculation._OUTPUT_SUBFOLDER, EpwCalculation._OUTPUT_DOS_FILE
                ).as_posix(),
            )
            if dos_contents is not None:
                self.out("dos", self.parse_dos(dos_contents))

            phdos_contents = self.get_retrieved_content(
                EpwCalculation._OUTPUT_PHDOS_FILE
            )
            if phdos_contents is not None:
                self.out("phdos", self.parse_phdos(phdos_contents))

            phdos_proj_contents = self.get_retrieved_content(
                EpwCalculation._OUTPUT_PHDOS_PROJ_FILE
            )
            if phdos_proj_contents is not None:
                self.out("phdos_proj", self.parse_phdos_proj(phdos_proj_contents))

            a2f_proj_contents = self.get_retrieved_content(
                EpwCalculation._OUTPUT_A2F_PROJ_FILE
            )
            if a2f_proj_contents is not None:
                self.out("a2f_proj", self.parse_a2f_proj(a2f_proj_contents))

            if momentum_dependence:
                lambda_FS_contents = self.get_retrieved_content(
                    EpwCalculation._OUTPUT_LAMBDA_FS_FILE
                )
                if lambda_FS_contents is not None:
                    self.out("lambda_FS", self.parse_lambda_FS(lambda_FS_contents))

                lambda_k_pairs_contents = self.get_retrieved_content(
                    EpwCalculation._OUTPUT_LAMBDA_K_PAIRS_FILE
                )
                if lambda_k_pairs_contents is not None:
                    self.out(
                        "lambda_k_pairs",
                        self.parse_lambda_k_pairs(lambda_k_pairs_contents),
                    )

                aniso_gap_filecontents = self.get_retrieved_contents_matching(
                    re.compile(
                        rf"{EpwCalculation._PREFIX}\.(?:{prefix_pattern})_aniso_gap0_\d+\.\d+$"
                    )
                )
                if aniso_gap_filecontents:
                    self.out(
                        "aniso_gap_functions",
                        self.parse_aniso_gap_functions(aniso_gap_filecontents),
                    )

                for prefix in allowed_prefixes:
                    aniso_gap_fs_contents = self.get_retrieved_content(
                        f"{EpwCalculation._PREFIX}.{prefix}_aniso_gap_FS"
                    )
                    if aniso_gap_fs_contents is not None:
                        self.out(
                            "aniso_gap_FS",
                            self.parse_aniso_gap_fs(aniso_gap_fs_contents),
                        )
                        break

                for prefix in allowed_prefixes:
                    aniso_gap_imag_contents = self.get_retrieved_content(
                        f"{EpwCalculation._PREFIX}.{prefix}_aniso"
                    )
                    if aniso_gap_imag_contents is not None:
                        self.out(
                            "aniso_gap_imag",
                            self.parse_aniso_imag(aniso_gap_imag_contents),
                        )
                        break
            else:
                iso_gap_filecontents = self.get_retrieved_contents_matching(
                    re.compile(
                        rf"{EpwCalculation._PREFIX}\.(?:{prefix_pattern})_iso_\d+\.\d+$"
                    )
                )
                if iso_gap_filecontents:
                    self.out(
                        "iso_gap_functions",
                        self.parse_iso_gap_functions(iso_gap_filecontents),
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
    def parse_stdout(stdout, logs):
        """Parse the ``stdout``."""

        def parse_max_eigenvalue(stdout_block):
            parsed_max_ev = parse_epw_max_eigenvalue(stdout_block)
            max_eigenvalue_array = orm.XyData()
            max_eigenvalue_array.set_array(
                "max_eigenvalue",
                parsed_max_ev["max_eigenvalue"],
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
                match = re_pattern.search(line)
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
        parsed_a2f = parse_epw_a2f(content)

        a2f_data = A2fData()
        a2f_data.set_a2f_data(
            frequency=parsed_a2f["frequency"],
            spectrum=parsed_a2f["a2f"],
            lambda_values=parsed_a2f["lambda"],
            phonon_smearing=parsed_a2f["phonon_smearing"],
            cumulative_lambda=parsed_a2f.get("cumulative_lambda"),
            electron_smearing=parsed_a2f.get("electron_smearing"),
            fermi_window=parsed_a2f.get("fermi_window"),
            summed_elph_coupling=parsed_a2f.get("summed_elph_coupling"),
        )

        parsed_data = {
            "degaussw": parsed_a2f["electron_smearing"],
            "fsthick": parsed_a2f["fermi_window"],
        }
        return a2f_data, parsed_data

    @staticmethod
    def parse_iso_gap_functions(file_contents):
        """Parse isotropic gap-function files into a typed datatype."""
        gap_functions = parse_epw_imag_iso(file_contents, prefix=EpwCalculation._PREFIX)
        gap_function_data = GapFunctionData()
        gap_function_data.set_gap_functions(gap_functions, kind="iso")
        return gap_function_data

    @staticmethod
    def parse_aniso_gap_functions(file_contents):
        """Parse anisotropic gap-function files into a typed datatype."""
        gap_functions = parse_epw_imag_aniso_gap0(
            file_contents, prefix=EpwCalculation._PREFIX
        )
        gap_function_data = GapFunctionData()
        gap_function_data.set_gap_functions(gap_functions, kind="aniso")
        return gap_function_data

    @staticmethod
    def parse_aniso_gap_fs(content):
        """Parse the imag_aniso_gap_FS file contents into an ArrayData node."""
        parsed = parse_aniso_FS(content)
        array_data = orm.ArrayData()
        bands = [int(k) for k in parsed.keys() if isinstance(k, int)]
        for band in sorted(bands):
            band_data = parsed[band]
            array_data.set_array(f"band_{band}_kpoints", band_data["kpoints"])
            array_data.set_array(f"band_{band}_energy", band_data["energy"])
            array_data.set_array(f"band_{band}_delta", band_data["delta"])
        array_data.base.attributes.set("bands", sorted(bands))
        if "units" in parsed:
            array_data.base.attributes.set("units", parsed["units"])
        return array_data

    @staticmethod
    def parse_aniso_imag(content):
        """Parse the imag_aniso file contents into an ArrayData node."""
        parsed = parse_aniso(content)
        array_data = orm.ArrayData()
        frequencies = sorted([float(k) for k in parsed.keys() if isinstance(k, float)])
        for index, w in enumerate(frequencies):
            w_data = parsed[w]
            array_data.set_array(f"freq_{index}_energy", w_data["energy"])
            array_data.set_array(f"freq_{index}_znorm", w_data["znorm"])
            array_data.set_array(f"freq_{index}_delta", w_data["delta"])
            array_data.set_array(f"freq_{index}_shift", w_data["shift"])
        array_data.base.attributes.set("frequencies", frequencies)
        if "units" in parsed:
            array_data.base.attributes.set("units", parsed["units"])
        return array_data

    @staticmethod
    def parse_a2f_proj(content):
        """Parse the contents of the `.a2f_proj` file."""
        parsed = parse_epw_a2f_proj(content)
        pa2f_data = PA2fData()
        pa2f_data.set_pa2f_data(
            frequency=parsed["frequency"],
            a2f=parsed["a2f"],
            projected_a2f=parsed["projected_a2f"],
            lambda_int=parsed.get("lambda_int"),
            lambda_sum=parsed.get("lambda_sum"),
        )
        return pa2f_data

    @staticmethod
    def parse_bands(content, kpoints_data, units):
        """Parse the contents of a band structure file."""
        parsed_bands = parse_epw_bands(content)
        kpts = parsed_bands["kpoints"]
        bands = parsed_bands["bands"]

        if kpoints_data is None:
            nbnd, nks = (
                int(v)
                for v in re.search(
                    r"&plot nbnd=\s+(\d+), nks=\s+(\d+)", content
                ).groups()
            )
            if len(kpts) != nks:
                raise ValueError(
                    "Could not reconstruct the band k-points from the retrieved EPW file."
                )

            kpoints_data = orm.KpointsData()
            kpoints_data.set_kpoints(kpts)

        bands_data = orm.BandsData()
        # We should use the KpointsData from the inputs.
        bands_data.set_kpointsdata(kpoints_data)
        bands_data.set_bands(bands, units=units)

        return bands_data

    @staticmethod
    def parse_dos(content):
        """Parse the contents of the `.dos` file."""
        parsed_dos = parse_epw_dos(content)
        dos_data = DosData()
        dos_data.set_dos_data(
            energy=parsed_dos["energy"],
            dos=parsed_dos["dos"],
            integrated_dos=parsed_dos.get("integrated_dos"),
        )
        return dos_data

    @staticmethod
    def parse_phdos(content):
        """Parse the contents of the `.phdos` file."""
        parsed_phdos = parse_epw_phdos(content)
        phdos_data = PhDosData()
        phdos_data.set_phdos_data(
            frequency=parsed_phdos["frequency"],
            phdos=parsed_phdos["phdos"],
        )
        return phdos_data

    @staticmethod
    def parse_phdos_proj(content):
        """Parse the contents of the `.phdos_proj` file."""
        parsed = parse_epw_phdos_proj(content)
        pdos_data = PDosData()
        pdos_data.set_pdos_data(
            frequency=parsed["frequency"],
            phdos=parsed["phdos"],
            projected_phdos=parsed["projected_phdos"],
        )
        return pdos_data

    @staticmethod
    def parse_lambda_FS(content):
        """Parse the contents of the `.lambda_FS` file."""
        parsed_lambda_fs = parse_epw_lambda_fs(content)
        lambda_fs_data = LambdaFSData()
        lambda_fs_data.set_lambda_fs(
            kpoints=parsed_lambda_fs["kpoints"],
            bands=parsed_lambda_fs["band"],
            energies=parsed_lambda_fs["energy"],
            couplings=parsed_lambda_fs["lambda"],
            energy_units=parsed_lambda_fs["energy_units"],
        )
        return lambda_fs_data

    @staticmethod
    def parse_lambda_k_pairs(content):
        """Parse the contents of the `.lambda_k_pairs` file."""
        parsed = parse_epw_lambda_k_pairs(content)
        dos_data = DosData()
        dos_data.set_dos_data(
            energy=parsed["energy"],
            dos=parsed["dos"],
            integrated_dos=parsed.get("integrated_dos"),
        )
        return dos_data

    @staticmethod
    def parse_gap_function(content, skiprows=0):
        """Parse the contents of the `gap_function.dat` file."""
        import io

        gap_function = numpy.loadtxt(
            io.StringIO(content), dtype=float, comments="#", skiprows=skiprows
        )

        return gap_function
