"""Domain-specific data type for EPW electronic Density of States (DOS)."""

import numpy
from aiida import orm
from aiida.common import exceptions


class DosData(orm.ArrayData):
    """Store the EPW electronic DOS table with explicit semantic getters."""

    ARRAY_ENERGY = "energy"
    ARRAY_DOS = "dos"
    ARRAY_INTEGRATED_DOS = "integrated_dos"

    def set_dos_data(self, energy, dos, integrated_dos=None):
        """Store the electronic DOS arrays."""
        energy = numpy.array(energy, dtype=float)
        dos = numpy.array(dos, dtype=float)

        if energy.ndim != 1:
            raise exceptions.ValidationError(
                "`energy` must be a one-dimensional array."
            )
        if dos.ndim != 1:
            raise exceptions.ValidationError("`dos` must be a one-dimensional array.")
        if energy.shape[0] != dos.shape[0]:
            raise exceptions.ValidationError(
                "`energy` and `dos` must have the same length."
            )

        self.set_array(self.ARRAY_ENERGY, energy)
        self.set_array(self.ARRAY_DOS, dos)

        if integrated_dos is not None:
            integrated_dos = numpy.array(integrated_dos, dtype=float)
            if integrated_dos.ndim != 1:
                raise exceptions.ValidationError(
                    "`integrated_dos` must be a one-dimensional array."
                )
            if integrated_dos.shape[0] != energy.shape[0]:
                raise exceptions.ValidationError(
                    "`integrated_dos` must have the same length as `energy`."
                )
            self.set_array(self.ARRAY_INTEGRATED_DOS, integrated_dos)

    def get_energy(self):
        """Return the energy array."""
        return self.get_array(self.ARRAY_ENERGY)

    def get_dos(self):
        """Return the electronic DOS array."""
        return self.get_array(self.ARRAY_DOS)

    def get_integrated_dos(self):
        """Return the integrated electronic DOS array, or None if not set."""
        try:
            return self.get_array(self.ARRAY_INTEGRATED_DOS)
        except KeyError:
            return None

    @classmethod
    def from_string(cls, content):
        """Instantiate and populate a `DosData` node directly from `.dos` string content."""
        from aiida_epw.tools.parsers import parse_epw_dos

        parsed = parse_epw_dos(content)
        return cls.from_parsed(parsed)

    @classmethod
    def from_parsed(cls, parsed):
        """Instantiate and populate a `DosData` node from a parsed mapping."""
        node = cls()
        node.set_dos_data(
            energy=parsed["energy"],
            dos=parsed["dos"],
            integrated_dos=parsed.get("integrated_dos"),
        )
        return node

    @classmethod
    def from_file(cls, filepath):
        """Instantiate and populate a `DosData` node directly from a `.dos` file."""
        from pathlib import Path

        content = Path(filepath).read_text(encoding="utf-8")
        return cls.from_string(content)


class PhDosData(orm.ArrayData):
    """Store the EPW phonon DOS table together with the number of smearings."""

    ARRAY_FREQUENCY = "frequency"
    ARRAY_PHDOS = "phdos"
    ATTRIBUTE_NUM_SMEARINGS = "num_smearings"

    # Legacy array keys for compatibility with generic XyData usages
    LEGACY_ARRAY_FREQUENCY = "Frequency"
    LEGACY_ARRAY_PHDOS = "PHDOS"

    def set_phdos_data(self, frequency, phdos, num_smearings=None):
        """Store the phonon DOS arrays."""
        frequency = numpy.array(frequency, dtype=float)
        phdos = numpy.array(phdos, dtype=float)

        if frequency.ndim != 1:
            raise exceptions.ValidationError(
                "`frequency` must be a one-dimensional array."
            )
        if phdos.ndim != 2:
            raise exceptions.ValidationError("`phdos` must be a two-dimensional array.")
        if frequency.shape[0] != phdos.shape[0]:
            raise exceptions.ValidationError(
                "The first phdos dimension must match the frequency grid length."
            )

        inferred_num_smearings = phdos.shape[1]
        if num_smearings is None:
            num_smearings = inferred_num_smearings
        elif int(num_smearings) != inferred_num_smearings:
            raise exceptions.ValidationError(
                "`num_smearings` must match the number of phdos columns."
            )

        self.set_array(self.ARRAY_FREQUENCY, frequency)
        self.set_array(self.LEGACY_ARRAY_FREQUENCY, frequency)
        self.set_array(self.ARRAY_PHDOS, phdos)
        self.set_array(self.LEGACY_ARRAY_PHDOS, phdos)
        self.base.attributes.set(self.ATTRIBUTE_NUM_SMEARINGS, int(num_smearings))

    def get_frequency(self):
        """Return the frequency array."""
        return self.get_array(self.ARRAY_FREQUENCY)

    def get_phdos(self):
        """Return the phonon DOS array for all smearings."""
        return self.get_array(self.ARRAY_PHDOS)

    @property
    def num_smearings(self):
        """Return the number of smearing values encoded in the file header."""
        return self.base.attributes.get(self.ATTRIBUTE_NUM_SMEARINGS)

    @classmethod
    def from_string(cls, content):
        """Instantiate and populate a `PhDosData` node directly from `.phdos` string content."""
        from aiida_epw.tools.parsers import parse_epw_phdos

        parsed = parse_epw_phdos(content)
        node = cls()
        node.set_phdos_data(
            frequency=parsed["frequency"],
            phdos=parsed["phdos"],
            num_smearings=parsed["num_smearings"],
        )
        return node

    @classmethod
    def from_file(cls, filepath):
        """Instantiate and populate a `PhDosData` node directly from a `.phdos` file."""
        from pathlib import Path

        content = Path(filepath).read_text(encoding="utf-8")
        return cls.from_string(content)


class PDosData(orm.ArrayData):
    """Store the EPW projected DOS table with semantic getters."""

    ARRAY_FREQUENCY = "frequency"
    ARRAY_PHDOS = "phdos"
    ARRAY_PROJECTED_PHDOS = "projected_phdos"

    def set_pdos_data(self, frequency, phdos, projected_phdos):
        """Store the projected DOS arrays."""
        frequency = numpy.array(frequency, dtype=float)
        phdos = numpy.array(phdos, dtype=float)
        projected_phdos = numpy.array(projected_phdos, dtype=float)

        if frequency.ndim != 1:
            raise exceptions.ValidationError(
                "`frequency` must be a one-dimensional array."
            )
        if phdos.ndim != 1:
            raise exceptions.ValidationError("`phdos` must be a one-dimensional array.")
        if projected_phdos.ndim != 2:
            raise exceptions.ValidationError(
                "`projected_phdos` must be a two-dimensional array."
            )
        if (
            frequency.shape[0] != phdos.shape[0]
            or frequency.shape[0] != projected_phdos.shape[0]
        ):
            raise exceptions.ValidationError(
                "Arrays `frequency`, `phdos`, and `projected_phdos` must have the same length."
            )

        self.set_array(self.ARRAY_FREQUENCY, frequency)
        self.set_array(self.ARRAY_PHDOS, phdos)
        self.set_array(self.ARRAY_PROJECTED_PHDOS, projected_phdos)

    def get_frequency(self):
        """Return the frequency array."""
        return self.get_array(self.ARRAY_FREQUENCY)

    def get_phdos(self):
        """Return the total phonon DOS array."""
        return self.get_array(self.ARRAY_PHDOS)

    def get_projected_phdos(self):
        """Return the projected phonon DOS array."""
        return self.get_array(self.ARRAY_PROJECTED_PHDOS)

    @classmethod
    def from_string(cls, content):
        """Instantiate a `PDosData` node from `.phdos_proj` string content."""
        from aiida_epw.tools.parsers import parse_epw_phdos_proj

        parsed = parse_epw_phdos_proj(content)
        node = cls()
        node.set_pdos_data(
            frequency=parsed["frequency"],
            phdos=parsed["phdos"],
            projected_phdos=parsed["projected_phdos"],
        )
        return node

    @classmethod
    def from_file(cls, filepath):
        """Instantiate a `PDosData` node from a `.phdos_proj` file."""
        from pathlib import Path

        content = Path(filepath).read_text(encoding="utf-8")
        return cls.from_string(content)
