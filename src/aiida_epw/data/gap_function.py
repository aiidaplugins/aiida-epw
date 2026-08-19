"""Domain-specific data types for typed EPW gap data."""

from pathlib import Path

import numpy
from aiida import orm
from aiida.common import exceptions


def _temperature_label(temperature):
    """Return a stable array-name fragment for a temperature value."""
    return f"{float(temperature):06.2f}".replace(".", "_")


def _read_file_contents(file_contents_or_paths):
    """Return a filename-to-content mapping from paths or an existing mapping."""
    if isinstance(file_contents_or_paths, dict):
        return file_contents_or_paths

    contents = {}
    for filepath in file_contents_or_paths:
        path = Path(filepath)
        contents[path.name] = path.read_text(encoding="utf-8")
    return contents


class _RaggedGapData(orm.ArrayData):
    """Store gap-data tables without requiring equal temperature grids."""

    ATTRIBUTE_ENTRIES = "entries"
    ARRAY_TEMPERATURES = "temperatures"

    def set_gap_data(self, gap_data):
        """Store a mapping keyed by ``(source, temperature)``."""
        if not gap_data:
            raise exceptions.ValidationError("`gap_data` cannot be empty.")

        self._delete_gap_arrays()
        entries = []
        temperatures = []
        for (source, temperature), columns in sorted(
            gap_data.items(), key=lambda item: (item[0][0], float(item[0][1]))
        ):
            if not columns:
                raise exceptions.ValidationError(
                    "Gap-data column mappings cannot be empty."
                )

            source = str(source)
            temperature = float(temperature)
            label = _temperature_label(temperature)
            row_count = None
            column_names = []
            column_arrays = []
            for column_name, values in columns.items():
                array = numpy.array(values, dtype=float)
                if array.ndim != 1:
                    raise exceptions.ValidationError(
                        f"Column `{column_name}` for {source}_{label} must be one-dimensional."
                    )
                if row_count is None:
                    row_count = array.shape[0]
                elif array.shape[0] != row_count:
                    raise exceptions.ValidationError(
                        f"Columns for {source}_{label} must have the same length."
                    )

                column_names.append(column_name)
                column_arrays.append(array)

            self.set_array(f"{source}_{label}", numpy.column_stack(column_arrays))

            entries.append(
                {
                    "source": source,
                    "temperature": temperature,
                    "array_name": f"{source}_{label}",
                    "columns": column_names,
                }
            )
            temperatures.append(temperature)

        self.set_array(self.ARRAY_TEMPERATURES, numpy.array(temperatures, dtype=float))
        self.base.attributes.set(self.ATTRIBUTE_ENTRIES, entries)

    def get_temperatures(self, source=None):
        """Return temperatures, optionally restricted to one source."""
        return numpy.array(
            [entry["temperature"] for entry in self._get_entries(source)], dtype=float
        )

    def get_data(self, temperature, *, source=None, atol=1.0e-8):
        """Return named arrays for one temperature/source entry."""
        entry = self._find_entry(temperature, source=source, atol=atol)
        table = self.get_array(entry["array_name"])
        return {
            column: table[:, index] for index, column in enumerate(entry["columns"])
        }

    def get_table(self, temperature, *, source=None, atol=1.0e-8):
        """Return the stored two-dimensional table for one entry."""
        entry = self._find_entry(temperature, source=source, atol=atol)
        return self.get_array(entry["array_name"])

    def get_iterdata(self, source=None):
        """Yield ``(source, temperature, columns)`` in stored order."""
        for entry in self._get_entries(source):
            yield (
                entry["source"],
                entry["temperature"],
                self.get_data(entry["temperature"], source=entry["source"]),
            )

    def to_dict(self, source=None):
        """Return entries as plain dictionaries suitable for plotting utilities."""
        data = {}
        for entry in self._get_entries(source):
            table = self.get_array(entry["array_name"])
            columns = list(entry["columns"])
            data.setdefault(entry["source"], {})[entry["temperature"]] = {
                "columns": columns,
                "table": table,
                "data": {
                    column: table[:, index] for index, column in enumerate(columns)
                },
            }
        return data

    @property
    def sources(self):
        """Return available source labels, such as ``imag`` and ``pade``."""
        return sorted({entry["source"] for entry in self._get_entries()})

    def _get_entries(self, source=None):
        entries = self.base.attributes.get(self.ATTRIBUTE_ENTRIES, [])
        return [
            entry for entry in entries if source is None or entry["source"] == source
        ]

    def _find_entry(self, temperature, *, source=None, atol=1.0e-8):
        matches = [
            entry
            for entry in self._get_entries(source)
            if numpy.isclose(
                entry["temperature"], float(temperature), atol=atol, rtol=0.0
            )
        ]
        if not matches:
            raise KeyError(f"No gap data stored for temperature {float(temperature)}.")
        if len(matches) > 1:
            raise KeyError("Multiple gap data entries found; pass `source`.")
        return matches[0]

    def _delete_gap_arrays(self):
        for entry in self._get_entries():
            array_name = entry.get(
                "array_name",
                f"{entry['source']}_{_temperature_label(entry['temperature'])}",
            )
            if array_name in self.get_arraynames():
                self.delete_array(array_name)
        if self.ARRAY_TEMPERATURES in self.get_arraynames():
            self.delete_array(self.ARRAY_TEMPERATURES)


class IsoGapData(_RaggedGapData):
    """Typed data for isotropic imaginary-axis and Pade gap functions."""

    def get_gap_fs(self, source="imag", component=None, unit="meV", drop_nan=True):
        """Return the first-frequency gap as a temperature series."""
        factor = {"eV": 1.0, "meV": 1000.0}[unit]
        temperatures = []
        gaps = []
        for _, temperature, columns in self.get_iterdata(source=source):
            column = component
            if column is None:
                column = "deltaw" if "deltaw" in columns else "deltaw_real"
            gap = float(columns[column][0]) * factor
            if drop_nan and numpy.isnan(gap):
                continue
            temperatures.append(float(temperature))
            gaps.append(gap)
        return {"T": temperatures, "gap": gaps, "unit": unit, "source": source}

    @classmethod
    def from_files(cls, file_contents_or_paths, prefix="aiida"):
        """Build a node from ``imag_iso`` and ``pade_iso`` files."""
        from aiida_epw.tools.parsers import parse_epw_iso_gap_files

        node = cls()
        node.set_gap_data(
            parse_epw_iso_gap_files(
                _read_file_contents(file_contents_or_paths), prefix=prefix
            )
        )
        return node


class AnisoGap0Data(_RaggedGapData):
    """Typed data for anisotropic gap-zero distributions."""

    def get_averaged_gap(self, source="imag", bandwidth_factor=1.5):
        """Return representative anisotropic gap values for each temperature."""
        temperatures = []
        gaps = []
        for _, temperature, _ in self.get_iterdata(source=source):
            temperatures.append(float(temperature))
            gaps.append(
                self._find_averaged_gap(
                    self.get_table(temperature, source=source), bandwidth_factor
                )
            )
        return {"T": temperatures, "gap": gaps, "source": source}

    @staticmethod
    def _find_averaged_gap(data, bandwidth_factor):
        """Find gap peaks from a smoothed distribution."""
        from scipy.signal import find_peaks

        data = numpy.array(data, dtype=float)
        gaps = data[:, 1]
        signal = data[:, 0] - numpy.min(data[:, 0])
        maximum = numpy.max(signal)
        if maximum == 0:
            return [float(numpy.mean(gaps))]

        window = max(3, int(len(signal) * 0.03 * bandwidth_factor))
        if window % 2 == 0:
            window += 1
        density = numpy.convolve(
            signal / maximum, numpy.ones(window) / window, mode="same"
        )
        peaks, _ = find_peaks(density, prominence=numpy.max(density) * 0.1)
        if len(peaks) == 0:
            return [float(gaps[numpy.argmax(density)])]
        return sorted(float(gaps[index]) for index in peaks)

    @classmethod
    def from_files(cls, file_contents_or_paths, prefix="aiida"):
        """Build a node from ``imag_aniso_gap0`` and Pade equivalents."""
        from aiida_epw.tools.parsers import parse_epw_aniso_gap0_files

        node = cls()
        node.set_gap_data(
            parse_epw_aniso_gap0_files(
                _read_file_contents(file_contents_or_paths), prefix=prefix
            )
        )
        return node
