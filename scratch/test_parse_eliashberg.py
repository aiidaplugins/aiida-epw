import re


def parse_fortran_float(value: str) -> float:
    value = value.replace("D", "E").replace("d", "E")
    value = re.sub(r"([0-9\.]+)([\+\-]\d+)$", r"\1E\2", value)
    return float(value)


def test_parse(path):
    with open(path, "r") as f:
        stdout = f.read()

    idx = stdout.find("anisotropic Eliashberg equations")
    if idx == -1:
        print("Marker not found")
        return

    content_to_parse = stdout[idx:]
    temp_pattern = re.compile(r"temp\(\s*\d+\s*\)\s*=\s*([\d\.]+)\s*K")
    matches = list(temp_pattern.finditer(content_to_parse))

    blocks = []
    for i, match in enumerate(matches):
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(content_to_parse)

        unfolding_idx = content_to_parse.find(
            "Unfolding on the coarse grid", start, end
        )
        if unfolding_idx != -1:
            end = unfolding_idx

        block_text = content_to_parse[start:end]
        temp = float(match.group(1))

        # Parse basic fields
        nsiw_match = re.search(
            r"Total number of frequency points nsiw\(\s*\d+\s*\)\s*=\s*(\d+)",
            block_text,
        )
        wscut_match = re.search(
            r"Cutoff frequency wscut\s*=\s*([\d\.]+)\s*eV", block_text
        )
        broyden_match = re.search(r"broyden mixing factor\s*=\s*([\d\.]+)", block_text)
        nsiter_match = re.search(
            r"Convergence was reached in nsiter\s*=\s*(\d+)", block_text
        )
        free_energy_match = re.search(r"Free energy\s*=\s*([\d\.-]+)\s*meV", block_text)

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

        gap_match = re.search(
            r"Min\.\s*/\s*Max\.\s*values\s*of\s*superconducting\s*gap\s*=\s*([\d\.-]+)\s+([\d\.-]+)\s*meV",
            block_text,
        )
        if gap_match:
            block_data["gap_min"] = float(gap_match.group(1))
            block_data["gap_max"] = float(gap_match.group(2))

        # Parse iter, ethr, znormi, deltai, shifti, mu
        iter_header = re.search(r"iter\s+ethr\s+znormi\s+deltai\s+\[meV\]", block_text)
        if iter_header:
            header_end = iter_header.end()
            remaining_text = block_text[header_end:]
            iterations = []
            row_pattern = re.compile(
                r"^\s*(\d+)\s+([\d\.\+\-EeDd]+)\s+([\d\.\+\-EeDd]+)\s+([\d\.\+\-EeDd]+)(?:\s+([\d\.\+\-EeDd]+)\s+([\d\.\+\-EeDd]+))?\s*$"
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
                        iter_data["shifti"] = parse_fortran_float(row_match.group(5))
                    if row_match.group(6) is not None:
                        iter_data["mu"] = parse_fortran_float(row_match.group(6))
                    iterations.append(iter_data)
                elif iterations:
                    break
            if iterations:
                block_data["iterations"] = iterations

        blocks.append(block_data)

    print(f"Parsed {len(blocks)} blocks from {path}:")
    print("First iteration of first block:")
    print(blocks[0]["iterations"][0])


if __name__ == "__main__":
    test_parse(
        "/Users/yimingzhang/Developer/aiida-plugins/aiida-epw/tests/files/parsers/epw/anisotropic_eliashberg_fsr/aiida.out"
    )
    test_parse(
        "/Users/yimingzhang/Developer/aiida-plugins/aiida-epw/tests/files/parsers/epw/anisotropic_eliashberg_fbw/aiida.out"
    )
