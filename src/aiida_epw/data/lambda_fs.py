"""Domain-specific data type for EPW Fermi-surface electron-phonon couplings."""

import numpy
from aiida import orm
from aiida.common import exceptions


class LambdaFSData(orm.ArrayData):
    """Store the EPW ``lambda_FS`` table with explicit semantic getters."""

    ARRAY_KPOINTS = "kpoints"
    ARRAY_BANDS = "band"
    ARRAY_ENERGIES = "energy"
    ARRAY_LAMBDA = "lambda"
    LEGACY_ARRAY_ENERGIES = "Enk"

    ATTRIBUTE_ENERGY_UNITS = "energy_units"

    def set_lambda_fs(self, kpoints, bands, energies, couplings, *, energy_units="eV"):
        """Store the Fermi-surface coupling table."""
        kpoints = numpy.array(kpoints, dtype=float)
        bands = numpy.array(bands, dtype=float)
        energies = numpy.array(energies, dtype=float)
        couplings = numpy.array(couplings, dtype=float)

        if kpoints.ndim != 2 or kpoints.shape[1] != 3:
            raise exceptions.ValidationError(
                "`kpoints` must be a two-dimensional array with shape (N, 3)."
            )
        for name, array in (
            ("bands", bands),
            ("energies", energies),
            ("couplings", couplings),
        ):
            if array.ndim != 1:
                raise exceptions.ValidationError(
                    f"`{name}` must be a one-dimensional array."
                )
            if array.shape[0] != kpoints.shape[0]:
                raise exceptions.ValidationError(
                    f"`{name}` must have the same length as `kpoints`."
                )

        self.set_array(self.ARRAY_KPOINTS, kpoints)
        self.set_array(self.ARRAY_BANDS, bands)
        self.set_array(self.ARRAY_ENERGIES, energies)
        self.set_array(self.LEGACY_ARRAY_ENERGIES, energies)
        self.set_array(self.ARRAY_LAMBDA, couplings)
        self.base.attributes.set(self.ATTRIBUTE_ENERGY_UNITS, energy_units)

    def get_kpoints(self):
        """Return the k-points."""
        return self.get_array(self.ARRAY_KPOINTS)

    def get_bands(self):
        """Return the band indices."""
        return self.get_array(self.ARRAY_BANDS)

    def get_energies(self):
        """Return the band energies."""
        return self.get_array(self.ARRAY_ENERGIES)

    def get_lambda(self):
        """Return the electron-phonon couplings."""
        return self.get_array(self.ARRAY_LAMBDA)

    @property
    def energy_units(self):
        """Return the energy units."""
        return self.base.attributes.get(self.ATTRIBUTE_ENERGY_UNITS)

    @classmethod
    def from_string(cls, content, energy_units="eV"):
        """Instantiate and populate a `LambdaFSData` node directly from `.lambda_FS` string content."""
        from aiida_epw.tools.parsers import parse_epw_lambda_fs

        parsed = parse_epw_lambda_fs(content)
        node = cls()
        node.set_lambda_fs(
            kpoints=parsed["kpoints"],
            bands=parsed["band"],
            energies=parsed["energy"],
            couplings=parsed["lambda"],
            energy_units=energy_units,
        )
        return node

    @classmethod
    def from_file(cls, filepath, energy_units="eV"):
        """Instantiate and populate a `LambdaFSData` node directly from a `.lambda_FS` file."""
        from pathlib import Path

        content = Path(filepath).read_text(encoding="utf-8")
        return cls.from_string(content, energy_units=energy_units)
