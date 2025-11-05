"""Manual parsing functions for post-processing."""

import numpy

Ry2eV = 13.605662285137


def parse_epw_a2f(file_content):
    parsed_data = {}

    a2f, footer = file_content.split("\n Integrated el-ph coupling")

    a2f_array = numpy.array([line.split() for line in a2f.split("\n")], dtype=float)
    parsed_data["frequency"] = a2f_array[:, 0]
    parsed_data["a2f"] = a2f_array[:, 1:]

    footer = footer.split("\n")
    parsed_data["lambda"] = numpy.array(footer[1].strip("# ").split(), dtype=float)
    parsed_data["phonon_smearing"] = numpy.array(
        footer[3].strip("# ").split(), dtype=float
    )

    key_property_dict = {
        "Electron smearing (eV)": "electron_smearing",
        "Fermi window (eV)": "fermi_window",
        "Summed el-ph coupling": "summed_elph_coupling",
    }
    for line in footer:
        for key, property in key_property_dict.items():
            if key in line:
                parsed_data[property] = float(line.split()[-1])

    return parsed_data
