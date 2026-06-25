"""Manual parsing functions for post-processing."""

import io
import re

import numpy

Ry2eV = 13.605662285137


def parse_epw_bands(file_content):
    """Parse the contents of a `band.eig`-style EPW bands file."""
    header_match = re.search(r"&plot nbnd=\s+(\d+), nks=\s+(\d+)", file_content)
    if not header_match:
        raise ValueError(
            "Malformed bands file: Could not parse '&plot nbnd=..., nks=...' header."
        )
    nbnd, _ = (int(value) for value in header_match.groups())

    kpt_pattern = re.compile(r"^\s*([-\d\.]+)\s+([-\d\.]+)\s+([-\d\.]+)\s*$")
    band_pattern = re.compile(r"\s+([-\d\.]+)" * nbnd)

    parsed_data = {"kpoints": [], "bands": []}

    for line in file_content.splitlines():
        match_kpt = re.search(kpt_pattern, line)
        if match_kpt:
            parsed_data["kpoints"].append(match_kpt.groups())
            continue

        match_band = re.search(band_pattern, line)
        if match_band:
            parsed_data["bands"].append(match_band.groups())

    if not parsed_data["kpoints"]:
        raise ValueError("Malformed bands file: No k-points parsed.")
    if not parsed_data["bands"]:
        raise ValueError("Malformed bands file: No band energies parsed.")

    parsed_data["kpoints"] = numpy.array(parsed_data["kpoints"], dtype=float)
    parsed_data["bands"] = numpy.array(parsed_data["bands"], dtype=float)

    return parsed_data


def parse_epw_a2f(file_content):
    """Parse the contents of the `.a2f` file."""
    parsed_data = {}

    lines = file_content.splitlines()
    if not lines:
        raise ValueError("Malformed .a2f file: File is empty.")

    first_line = lines[0]
    smearing_match = re.search(r"for\s+(\d+)\s+smearing", first_line)
    if not smearing_match:
        raise ValueError(
            "Malformed .a2f file: Could not parse the number of smearing values from the header."
        )
    num_smearings = int(smearing_match.group(1))

    if "\n Integrated el-ph coupling" not in file_content:
        raise ValueError(
            "Malformed .a2f file: Could not find 'Integrated el-ph coupling' section."
        )
    a2f, footer = file_content.split("\n Integrated el-ph coupling", maxsplit=1)

    a2f_lines = [line.split() for line in a2f.splitlines()[1:] if line.strip()]
    if not a2f_lines:
        raise ValueError(
            "Malformed .a2f file: The a2F spectrum table is empty or missing."
        )
    try:
        a2f_array = numpy.array(a2f_lines, dtype=float)
    except ValueError as exc:
        raise ValueError(
            f"Malformed .a2f file: Could not convert a2F spectrum to float: {exc}"
        ) from exc

    expected_cols_with_cumulative = 1 + 2 * num_smearings
    expected_cols_without_cumulative = 1 + num_smearings

    if a2f_array.shape[1] >= expected_cols_with_cumulative:
        parsed_data["frequency"] = a2f_array[:, 0]
        parsed_data["a2f"] = a2f_array[:, 1 : num_smearings + 1]
        parsed_data["cumulative_lambda"] = a2f_array[
            :, num_smearings + 1 : 2 * num_smearings + 1
        ]
    elif a2f_array.shape[1] >= expected_cols_without_cumulative:
        parsed_data["frequency"] = a2f_array[:, 0]
        parsed_data["a2f"] = a2f_array[:, 1 : num_smearings + 1]
        parsed_data["cumulative_lambda"] = None
    else:
        raise ValueError(
            f"Malformed .a2f file: Expected at least {expected_cols_without_cumulative} columns for {num_smearings} smearing values, got {a2f_array.shape[1]}."
        )

    footer = footer.split("\n")
    if len(footer) < 4:
        raise ValueError("Malformed .a2f file: The footer section is incomplete.")
    try:
        parsed_data["lambda"] = numpy.array(footer[1].strip("# ").split(), dtype=float)
        parsed_data["phonon_smearing"] = numpy.array(
            footer[3].strip("# ").split(), dtype=float
        )
    except Exception as exc:
        raise ValueError(
            f"Malformed .a2f file: Failed to parse integrated lambda or phonon smearing values: {exc}"
        ) from exc

    key_property_dict = {
        "Electron smearing (eV)": "electron_smearing",
        "Fermi window (eV)": "fermi_window",
        "Summed el-ph coupling": "summed_elph_coupling",
    }
    for line in footer:
        for key, property_name in key_property_dict.items():
            if key in line:
                try:
                    parsed_data[property_name] = float(line.split()[-1])
                except (IndexError, ValueError) as exc:
                    raise ValueError(
                        f"Malformed .a2f file: Failed to parse {key}: {exc}"
                    ) from exc

    return parsed_data


def parse_epw_max_eigenvalue(file_content):
    """Parse the max_eigenvalue part of the `stdout` file when solving the linearized Eliashberg equation."""
    parsed_data = {}
    re_pattern = re.compile(r"\s+([\d\.]+)\s+([\d\.-]+)\s+\d+\s+[\d\.]+\s+\d+\n")
    if "Finish: Solving (isotropic) linearized Eliashberg" not in file_content:
        raise ValueError(
            "Malformed stdout: 'Finish: Solving (isotropic) linearized Eliashberg' section not found."
        )
    parsing_block = file_content.split(
        "Finish: Solving (isotropic) linearized Eliashberg"
    )[0]

    matches = re_pattern.findall(parsing_block)
    if not matches:
        raise ValueError(
            "Malformed stdout: Could not parse any max_eigenvalue records."
        )
    parsed_data["max_eigenvalue"] = numpy.array(matches, dtype=float)
    return parsed_data


def parse_epw_dos(file_content):
    """Parse the contents of the electronic DOS file produced by EPW."""
    try:
        dos = numpy.loadtxt(io.StringIO(file_content), dtype=float, comments="#")
    except Exception as exc:
        raise ValueError(
            f"Malformed electronic DOS file: Failed to load tabular data: {exc}"
        ) from exc
    if dos.ndim == 0 or dos.size == 0:
        raise ValueError("Malformed electronic DOS file: The file is empty.")
    if dos.ndim == 1:
        dos = dos[numpy.newaxis, :]
    if dos.shape[1] < 2:
        raise ValueError(
            "Malformed electronic DOS file: Expected at least 2 columns (Energy, DOS)."
        )
    return {
        "energy": dos[:, 0],
        "dos": dos[:, 1],
        "integrated_dos": dos[:, 2] if dos.shape[1] > 2 else None,
    }


def parse_epw_phdos(file_content):
    """Parse the contents of the phonon DOS file produced by EPW."""
    lines = file_content.splitlines()
    if not lines:
        raise ValueError("Malformed phonon DOS file: The file is empty.")

    smearing_match = re.search(r"for\s+(\d+)\s+smearing", lines[0])
    if not smearing_match:
        raise ValueError(
            "Malformed phonon DOS file: Could not parse the number of smearing values from the header."
        )
    num_smearings = int(smearing_match.group(1))

    try:
        phdos = numpy.loadtxt(io.StringIO(file_content), dtype=float, skiprows=1)
    except Exception as exc:
        raise ValueError(
            f"Malformed phonon DOS file: Failed to load tabular data: {exc}"
        ) from exc
    if phdos.ndim == 0 or phdos.size == 0:
        raise ValueError("Malformed phonon DOS file: The file is empty.")
    if phdos.ndim == 1:
        phdos = phdos[numpy.newaxis, :]
    if phdos.shape[1] < 2:
        raise ValueError(
            "Malformed phonon DOS file: Expected at least 2 columns (Frequency, PHDOS)."
        )
    expected_num_columns = 1 + num_smearings
    if phdos.shape[1] != expected_num_columns:
        raise ValueError(
            "Malformed phonon DOS file: "
            f"Expected {expected_num_columns} columns for {num_smearings} smearing values, got {phdos.shape[1]}."
        )
    return {
        "frequency": phdos[:, 0],
        "phdos": phdos[:, 1:],
        "num_smearings": num_smearings,
    }


def parse_epw_a2f_proj(file_content):
    """Parse the contents of the projected `.a2f_proj` file."""
    lines = [line.strip() for line in file_content.splitlines() if line.strip()]
    if not lines:
        raise ValueError("Projected a2F spectrum file is empty.")

    # Check and extract footer
    last_line = lines[-1]
    lambda_int = None
    lambda_sum = None
    if "lambda_int" in last_line:
        match = re.search(
            r"lambda_int\s*=\s*([+-]?[\d\.]+)\s+lambda_sum\s*=\s*([+-]?[\d\.]+)",
            last_line,
        )
        if match:
            lambda_int = float(match.group(1))
            lambda_sum = float(match.group(2))
        lines = lines[:-1]

    # Middle data lines (skipping header at lines[0])
    data_lines = [line for line in lines[1:] if _is_numeric_table_row(line)]
    if not data_lines:
        raise ValueError(
            "Malformed projected a2F spectrum: No numeric table rows found."
        )

    table = _load_numeric_table("\n".join(data_lines))
    if table.shape[1] < 2:
        raise ValueError(
            "Malformed projected a2F spectrum: Expected at least 2 columns."
        )

    return {
        "frequency": table[:, 0],
        "a2f": table[:, 1],
        "projected_a2f": table[:, 2:],
        "lambda_int": lambda_int,
        "lambda_sum": lambda_sum,
    }


def parse_epw_phdos_proj(file_content):
    """Parse the contents of the projected `.phdos_proj` file."""
    lines = [line.strip() for line in file_content.splitlines() if line.strip()]
    if not lines:
        raise ValueError("Projected phdos file is empty.")

    data_lines = [line for line in lines[1:] if _is_numeric_table_row(line)]
    if not data_lines:
        raise ValueError("Malformed projected phdos: No numeric table rows found.")

    table = _load_numeric_table("\n".join(data_lines))
    if table.shape[1] < 2:
        raise ValueError("Malformed projected phdos: Expected at least 2 columns.")

    return {
        "frequency": table[:, 0],
        "phdos": table[:, 1],
        "projected_phdos": table[:, 2:],
    }


def parse_epw_lambda_fs(file_content):
    """Parse the contents of the `.lambda_FS` file."""
    try:
        lambda_fs = _load_numeric_table(file_content, comments="#")
    except Exception as exc:
        raise ValueError(
            f"Malformed .lambda_FS file: Failed to parse numeric table: {exc}"
        ) from exc
    if lambda_fs.shape[1] < 6:
        raise ValueError(
            f"Malformed .lambda_FS file: Expected at least 6 columns, got {lambda_fs.shape[1]}."
        )
    return {
        "kpoints": lambda_fs[:, :3],
        "band": lambda_fs[:, 3],
        "energy": lambda_fs[:, 4],
        "lambda": lambda_fs[:, 5],
        "energy_units": "eV",
    }


def parse_epw_lambda_k_pairs(file_content):
    """Parse the contents of the `.lambda_k_pairs` file."""
    return _parse_epw_lambda_distribution(
        file_content, ".lambda_k_pairs", x_key="energy", y_key="dos"
    )


def parse_epw_lambda_pairs(file_content):
    """Parse the contents of the `.lambda_pairs` file."""
    return _parse_epw_lambda_distribution(
        file_content, ".lambda_pairs", x_key="energy", y_key="dos"
    )


def _parse_epw_lambda_distribution(file_content, file_label, *, x_key, y_key):
    """Parse lambda-distribution tables into a DOS-like two-column mapping."""
    try:
        table = _load_numeric_table(file_content, comments="#")
    except Exception as exc:
        raise ValueError(
            f"Malformed {file_label} file: Failed to parse numeric table: {exc}"
        ) from exc
    if table.shape[1] < 2:
        raise ValueError(
            f"Malformed {file_label} file: Expected at least 2 columns, got {table.shape[1]}."
        )
    return {
        x_key: table[:, 0],
        y_key: table[:, 1],
        "integrated_dos": None,
    }


def parse_epw_imag_iso(file_contents, prefix="aiida"):
    """Parse the isotropic gap functions from EPW isotropic Eliashberg equation calculation.

    :param file_contents: mapping of file names to file contents.
    :param prefix: the prefix of the `imag_iso` files.
    :returns: dictionary containing the isotropic gap functions keyed by temperature.
    """
    if not file_contents:
        raise ValueError("No gap-function file contents provided.")
    parsed_data = {}
    pattern_iso = re.compile(rf"^{prefix}\.imag_iso_(\d{{3}}\.\d{{2}})$")

    for filename, file_content in file_contents.items():
        match = pattern_iso.match(filename)
        if match:
            temperature = float(match.group(1))
            try:
                gap_function = numpy.loadtxt(
                    io.StringIO(file_content), dtype=float, comments="#", skiprows=1
                )
            except Exception as exc:
                raise ValueError(
                    f"Failed to parse gap function file {filename}: {exc}"
                ) from exc
            parsed_data[temperature] = gap_function

    if not parsed_data:
        raise ValueError(
            f"No files matching the template '{prefix}.imag_iso_XXX.XX' were parsed successfully."
        )
    return parsed_data


def parse_epw_imag_aniso_gap0(file_contents, prefix="aiida"):
    """Parse the anisotropic gap functions from EPW anisotropic Eliashberg equation calculation.

    :param file_contents: mapping of file names to file contents.
    :param prefix: the prefix of the `imag_aniso_gap0` files.
    :returns: dictionary containing the anisotropic gap functions keyed by temperature.
    """
    if not file_contents:
        raise ValueError("No gap-function file contents provided.")
    parsed_data = {}
    pattern_aniso_gap0 = re.compile(rf"^{prefix}\.imag_aniso_gap0_(\d{{3}}\.\d{{2}})$")

    for filename, file_content in file_contents.items():
        match = pattern_aniso_gap0.match(filename)
        if match:
            temperature = float(match.group(1))
            try:
                gap_function = numpy.loadtxt(
                    io.StringIO(file_content), dtype=float, comments="#", skiprows=1
                )
            except Exception as exc:
                raise ValueError(
                    f"Failed to parse gap function file {filename}: {exc}"
                ) from exc
            parsed_data[temperature] = gap_function

    if not parsed_data:
        raise ValueError(
            f"No files matching the template '{prefix}.imag_aniso_gap0_XXX.XX' were parsed successfully."
        )
    return parsed_data


def parse_aniso_FS(file_content):
    """Parse the contents of the `imag_aniso_gap_FS` file.

    :param file_content: the string content of the `imag_aniso_gap_FS` file.
    :returns: dictionary containing arrays classified by the 4th column 'Band', and their units.
    """
    try:
        data = _load_numeric_table(file_content, comments="#")
    except Exception as exc:
        raise ValueError(
            f"Malformed imag_aniso_gap_FS file: Failed to parse numeric table: {exc}"
        ) from exc

    if data.shape[1] < 6:
        raise ValueError(
            f"Malformed imag_aniso_gap_FS file: Expected at least 6 columns, got {data.shape[1]}."
        )

    parsed_data = {}
    bands = data[:, 3].astype(int)
    unique_bands = numpy.unique(bands)

    for band in unique_bands:
        band_mask = bands == band
        band_data = data[band_mask]
        parsed_data[int(band)] = {
            "kpoints": band_data[:, :3],
            "energy": band_data[:, 4],
            "delta": band_data[:, 5],
        }

    parsed_data["units"] = {
        "kpoints": "crystal",
        "energy": "eV",
        "delta": "meV",
    }
    return parsed_data


def parse_aniso(file_content):
    """Parse the contents of the `imag_aniso` file.

    :param file_content: the string content of the `imag_aniso` file.
    :returns: dictionary containing arrays classified by the 1st column 'w', and their units.
    """
    try:
        data = _load_numeric_table(file_content, comments="#")
    except Exception as exc:
        raise ValueError(
            f"Malformed imag_aniso file: Failed to parse numeric table: {exc}"
        ) from exc

    if data.shape[1] < 5:
        raise ValueError(
            f"Malformed imag_aniso file: Expected at least 5 columns, got {data.shape[1]}."
        )

    parsed_data = {}
    frequencies = data[:, 0]
    unique_frequencies = numpy.unique(frequencies)

    for w in unique_frequencies:
        mask = frequencies == w
        subset = data[mask]
        parsed_data[float(w)] = {
            "energy": subset[:, 1],
            "znorm": subset[:, 2],
            "delta": subset[:, 3],
            "shift": subset[:, 4],
        }

    parsed_data["units"] = {
        "frequency": "eV",
        "energy": "eV",
        "znorm": "",
        "delta": "eV",
        "shift": "eV",
    }
    return parsed_data


def _load_numeric_table(file_content, **kwargs):
    """Load a numeric table from in-memory text and preserve 2D shape for single-row tables."""
    table = numpy.loadtxt(io.StringIO(file_content), dtype=float, **kwargs)
    if table.ndim == 1:
        table = table[numpy.newaxis, :]

    return table


def _is_numeric_table_row(line):
    """Return whether a line begins with numeric tabular data."""
    try:
        float(line.split()[0])
    except (IndexError, ValueError):
        return False

    return True


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
                    float(x.replace("D", "E").replace("d", "e")) for x in line.split()
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


def parse_stdout_eliashberg(stdout: str) -> dict:
    """Parse isotropic and anisotropic Eliashberg temperature blocks from stdout."""
    parsed_data = {}

    def parse_fortran_float(value: str) -> float:
        """Parse a Fortran-style double float (e.g. 1.23D-04) into a Python float."""
        val = value.replace("D", "E").replace("d", "E")
        val = re.sub(r"([0-9\.]+)([\+\-]\d+)$", r"\1E\2", val)
        return float(val)

    # Parse isotropic Eliashberg temperature blocks
    eliashberg_marker = "isotropic eliashberg equations"
    eliashberg_idx = stdout.lower().find(eliashberg_marker)
    if eliashberg_idx != -1:
        eliashberg_content = stdout[eliashberg_idx:]
        temp_pattern = re.compile(r"temp\(\s*\d+\s*\)\s*=\s*([\d\.]+)\s*K")
        matches = list(temp_pattern.finditer(eliashberg_content))

        blocks = []
        for i, match in enumerate(matches):
            start = match.start()
            end = (
                matches[i + 1].start()
                if i + 1 < len(matches)
                else len(eliashberg_content)
            )

            unfolding_idx = eliashberg_content.find(
                "Unfolding on the coarse grid", start, end
            )
            if unfolding_idx != -1:
                end = unfolding_idx

            block_text = eliashberg_content[start:end]
            temp = float(match.group(1))

            nsiw_match = re.search(
                r"Total number of frequency points nsiw\(\s*\d+\s*\)\s*=\s*(\d+)",
                block_text,
            )
            wscut_match = re.search(
                r"Cutoff frequency wscut\s*=\s*([\d\.]+)\s*eV", block_text
            )
            broyden_match = re.search(
                r"broyden mixing factor\s*=\s*([\d\.]+)", block_text
            )
            nsiter_match = re.search(
                r"Convergence was reached in nsiter\s*=\s*(\d+)", block_text
            )
            free_energy_match = re.search(
                r"Free energy\s*=\s*([\d\.-]+)\s*meV", block_text
            )

            block_data = {"temp": temp}
            if nsiw_match:
                block_data["nsiw"] = int(nsiw_match.group(1))
            if wscut_match:
                block_data["wscut"] = float(wscut_match.group(1))
            if broyden_match:
                block_data["broyden_mixing_factor"] = float(broyden_match.group(1))
            if nsiter_match:
                block_data["nsiter"] = int(nsiter_match.group(1))
            if free_energy_match:
                block_data["free_energy"] = float(free_energy_match.group(1))

            iw_match = re.search(
                r"startiw\s*=\s*(\d+),\s*lastiw\s*=\s*(\d+),\s*nsiw\(itemp\)\s*=\s*(\d+)",
                block_text,
            )
            if iw_match:
                block_data["startiw"] = int(iw_match.group(1))
                block_data["lastiw"] = int(iw_match.group(2))
                block_data["nsiw_itemp"] = int(iw_match.group(3))

            iter_header = re.search(
                r"iter\s+ethr\s+znormi\s+deltai\s+\[meV\]", block_text
            )
            if iter_header:
                header_end = iter_header.end()
                remaining_text = block_text[header_end:]
                iterations = []
                row_pattern = re.compile(
                    r"^\s*(\d+)\s+(\S+)\s+(\S+)\s+(\S+)(?:\s+(\S+))?(?:\s+(\S+))?\s*$"
                )
                for line in remaining_text.split("\n"):
                    row_match = row_pattern.match(line)
                    if row_match:
                        iter_data = {
                            "iter": int(row_match.group(1)),
                            "ethr": parse_fortran_float(row_match.group(2)),
                            "znormi": parse_fortran_float(row_match.group(3)),
                            "deltai": parse_fortran_float(row_match.group(4)),
                        }
                        if row_match.group(5) is not None:
                            iter_data["shifti"] = parse_fortran_float(
                                row_match.group(5)
                            )
                        if row_match.group(6) is not None:
                            iter_data["mu"] = parse_fortran_float(row_match.group(6))
                        iterations.append(iter_data)
                    elif iterations:
                        break
                if iterations:
                    block_data["iterations"] = iterations

            blocks.append(block_data)

        if blocks:
            parsed_data["isotropic_eliashberg"] = blocks

    # Parse anisotropic Eliashberg temperature blocks
    anisotropic_marker = "anisotropic eliashberg equations"
    anisotropic_idx = stdout.lower().find(anisotropic_marker)
    if anisotropic_idx != -1:
        anisotropic_content = stdout[anisotropic_idx:]
        temp_pattern = re.compile(r"temp\(\s*\d+\s*\)\s*=\s*([\d\.]+)\s*K")
        matches = list(temp_pattern.finditer(anisotropic_content))

        blocks = []
        for i, match in enumerate(matches):
            start = match.start()
            end = (
                matches[i + 1].start()
                if i + 1 < len(matches)
                else len(anisotropic_content)
            )

            unfolding_idx = anisotropic_content.find(
                "Unfolding on the coarse grid", start, end
            )
            if unfolding_idx != -1:
                end = unfolding_idx

            block_text = anisotropic_content[start:end]
            temp = float(match.group(1))

            nsiw_match = re.search(
                r"Total number of frequency points nsiw\(\s*\d+\s*\)\s*=\s*(\d+)",
                block_text,
            )
            wscut_match = re.search(
                r"Cutoff frequency wscut\s*=\s*([\d\.]+)\s*eV", block_text
            )
            broyden_match = re.search(
                r"broyden mixing factor\s*=\s*([\d\.]+)", block_text
            )
            nsiter_match = re.search(
                r"Convergence was reached in nsiter\s*=\s*(\d+)", block_text
            )
            free_energy_match = re.search(
                r"Free energy\s*=\s*([\d\.-]+)\s*meV", block_text
            )

            block_data = {"temp": temp}
            if nsiw_match:
                block_data["nsiw"] = int(nsiw_match.group(1))
            if wscut_match:
                block_data["wscut"] = float(wscut_match.group(1))
            if broyden_match:
                block_data["broyden_mixing_factor"] = float(broyden_match.group(1))
            if nsiter_match:
                block_data["nsiter"] = int(nsiter_match.group(1))
            if free_energy_match:
                block_data["free_energy"] = float(free_energy_match.group(1))

            iw_match = re.search(
                r"startiw\s*=\s*(\d+),\s*lastiw\s*=\s*(\d+),\s*nsiw\(itemp\)\s*=\s*(\d+)",
                block_text,
            )
            if iw_match:
                block_data["startiw"] = int(iw_match.group(1))
                block_data["lastiw"] = int(iw_match.group(2))
                block_data["nsiw_itemp"] = int(iw_match.group(3))

            gap_match = re.search(
                r"Min\.\s*/\s*Max\.\s*values\s*of\s*superconducting\s*gap\s*=\s*([\d\.-]+)\s+([\d\.-]+)\s*meV",
                block_text,
            )
            if gap_match:
                block_data["gap_min"] = float(gap_match.group(1))
                block_data["gap_max"] = float(gap_match.group(2))

            iter_header = re.search(
                r"iter\s+ethr\s+znormi\s+deltai\s+\[meV\]", block_text
            )
            if iter_header:
                header_end = iter_header.end()
                remaining_text = block_text[header_end:]
                iterations = []
                row_pattern = re.compile(
                    r"^\s*(\d+)\s+(\S+)\s+(\S+)\s+(\S+)(?:\s+(\S+))?(?:\s+(\S+))?\s*$"
                )
                for line in remaining_text.split("\n"):
                    row_match = row_pattern.match(line)
                    if row_match:
                        iter_data = {
                            "iter": int(row_match.group(1)),
                            "ethr": parse_fortran_float(row_match.group(2)),
                            "znormi": parse_fortran_float(row_match.group(3)),
                            "deltai": parse_fortran_float(row_match.group(4)),
                        }
                        if row_match.group(5) is not None:
                            iter_data["shifti"] = parse_fortran_float(
                                row_match.group(5)
                            )
                        if row_match.group(6) is not None:
                            iter_data["mu"] = parse_fortran_float(row_match.group(6))
                        iterations.append(iter_data)
                    elif iterations:
                        break
                if iterations:
                    block_data["iterations"] = iterations

            blocks.append(block_data)

        if blocks:
            parsed_data["anisotropic_eliashberg"] = blocks

    return parsed_data
