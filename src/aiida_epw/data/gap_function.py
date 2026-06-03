"""Domain-specific data type for EPW gap-function tables."""

import numpy
from aiida import orm
from aiida.common import exceptions


class GapFunctionData(orm.ArrayData):
    """Store a temperature-indexed collection of EPW gap-function tables."""

    ATTRIBUTE_TEMPERATURES = "temperatures"
    ATTRIBUTE_ARRAY_NAMES = "array_names"
    ATTRIBUTE_KIND = "kind"
    ARRAY_NAME_TEMPLATE = "gap_function_{index:03d}"

    def set_gap_functions(self, gap_functions, *, kind=None):
        """Store gap-function tables keyed by temperature."""
        if not gap_functions:
            raise exceptions.ValidationError("`gap_functions` cannot be empty.")

        normalized = []
        for temperature, gap_function in sorted(
            gap_functions.items(), key=lambda item: float(item[0])
        ):
            table = numpy.array(gap_function, dtype=float)
            if table.ndim != 2:
                raise exceptions.ValidationError(
                    "Each gap-function entry must be a two-dimensional array."
                )
            normalized.append((float(temperature), table))

        self._delete_gap_function_arrays()

        temperatures = []
        array_names = []
        for index, (temperature, table) in enumerate(normalized):
            array_name = self.ARRAY_NAME_TEMPLATE.format(index=index)
            self.set_array(array_name, table)
            temperatures.append(temperature)
            array_names.append(array_name)

        self.base.attributes.set(self.ATTRIBUTE_TEMPERATURES, temperatures)
        self.base.attributes.set(self.ATTRIBUTE_ARRAY_NAMES, array_names)
        self._set_optional_attribute(self.ATTRIBUTE_KIND, kind)

    def get_temperatures(self):
        """Return the stored temperatures in Kelvin."""
        return numpy.array(
            self.base.attributes.get(self.ATTRIBUTE_TEMPERATURES, []), dtype=float
        )

    def get_gap_functions(self):
        """Return all stored gap-function tables keyed by temperature."""
        return {
            temperature: self.get_array(array_name)
            for temperature, array_name in self._get_temperature_array_pairs()
        }

    def get_gap_function(self, temperature, *, atol=1e-8):
        """Return the gap-function table for a specific temperature."""
        target = float(temperature)

        for stored_temperature, array_name in self._get_temperature_array_pairs():
            if numpy.isclose(stored_temperature, target, atol=atol, rtol=0.0):
                return self.get_array(array_name)

        raise KeyError(f"No gap function stored for temperature {target}.")

    def get_itergap_functions(self):
        """Yield `(temperature, table)` pairs in ascending temperature order."""
        for temperature, array_name in self._get_temperature_array_pairs():
            yield temperature, self.get_array(array_name)

    @property
    def kind(self):
        """Return the optional gap-function kind, e.g. `iso` or `aniso`."""
        return self.base.attributes.get(self.ATTRIBUTE_KIND, None)

    def _get_temperature_array_pairs(self):
        """Return the stored temperature-to-array mapping."""
        temperatures = self.base.attributes.get(self.ATTRIBUTE_TEMPERATURES, [])
        array_names = self.base.attributes.get(self.ATTRIBUTE_ARRAY_NAMES, [])
        return list(zip(temperatures, array_names, strict=True))

    def _delete_gap_function_arrays(self):
        """Delete previously stored gap-function tables."""
        for array_name in self.base.attributes.get(self.ATTRIBUTE_ARRAY_NAMES, []):
            if array_name in self.get_arraynames():
                self.delete_array(array_name)

    def _set_optional_attribute(self, key, value):
        """Set or clear an optional scalar attribute."""
        if value is None:
            try:
                self.base.attributes.delete(key)
            except AttributeError:
                pass
            return

        self.base.attributes.set(key, value)

    @classmethod
    def from_files(cls, file_contents_or_paths, prefix="aiida", kind="iso"):
        """Instantiate and populate a `GapFunctionData` node from multiple files.

        :param file_contents_or_paths: list of filepaths or a dict mapping filenames to string contents.
        :param prefix: prefix of the files (e.g. 'aiida').
        :param kind: kind of gap function ('iso' or 'aniso').
        """
        from aiida_epw.tools.parsers import (
            parse_epw_imag_iso,
            parse_epw_imag_aniso_gap0,
        )
        from pathlib import Path

        file_contents = {}
        if isinstance(file_contents_or_paths, dict):
            file_contents = file_contents_or_paths
        else:
            for filepath in file_contents_or_paths:
                path = Path(filepath)
                file_contents[path.name] = path.read_text(encoding="utf-8")

        if kind == "iso":
            gap_functions = parse_epw_imag_iso(file_contents, prefix=prefix)
        elif kind == "aniso":
            gap_functions = parse_epw_imag_aniso_gap0(file_contents, prefix=prefix)
        else:
            raise ValueError(f"Unknown kind '{kind}': Must be either 'iso' or 'aniso'.")

        node = cls()
        node.set_gap_functions(gap_functions, kind=kind)
        return node

    @classmethod
    def from_directory(cls, dirpath, prefix="aiida", kind="iso"):
        """Instantiate and populate a `GapFunctionData` node from gap files in a directory."""
        from pathlib import Path

        path = Path(dirpath)
        pattern = (
            f"{prefix}.imag_iso_*" if kind == "iso" else f"{prefix}.imag_aniso_gap0_*"
        )
        filepaths = list(path.glob(pattern))
        if not filepaths:
            raise FileNotFoundError(
                f"No files matching '{pattern}' in directory '{dirpath}'"
            )

        return cls.from_files(filepaths, prefix=prefix, kind=kind)
