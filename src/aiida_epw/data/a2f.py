"""Domain-specific data type for EPW ``.a2f`` spectra."""

import numpy
from aiida import orm
from aiida.common import exceptions


class A2fData(orm.ArrayData):
    """Store an EPW a2F spectrum together with its smearing grids and metadata."""

    ARRAY_FREQUENCY = "frequency"
    ARRAY_RAW_SPECTRUM = "a2f"
    ARRAY_SPECTRUM = "a2f_spectrum"
    ARRAY_CUMULATIVE_LAMBDA = "lambda_cumulative"
    ARRAY_LAMBDA = "lambda"
    ARRAY_PHONON_SMEARING = "degaussq"

    ATTRIBUTE_ELECTRON_SMEARING = "electron_smearing"
    ATTRIBUTE_FERMI_WINDOW = "fermi_window"
    ATTRIBUTE_SUMMED_ELPH_COUPLING = "summed_elph_coupling"

    def set_a2f_data(
        self,
        frequency,
        spectrum,
        lambda_values,
        phonon_smearing,
        *,
        cumulative_lambda=None,
        electron_smearing=None,
        fermi_window=None,
        summed_elph_coupling=None,
    ):
        """Store the EPW a2F spectrum and validate the expected array shapes."""
        frequency = numpy.array(frequency, dtype=float)
        spectrum = numpy.array(spectrum, dtype=float)
        lambda_values = numpy.array(lambda_values, dtype=float)
        phonon_smearing = numpy.array(phonon_smearing, dtype=float)
        if cumulative_lambda is not None:
            cumulative_lambda = numpy.array(cumulative_lambda, dtype=float)

        if frequency.ndim != 1:
            raise exceptions.ValidationError(
                "`frequency` must be a one-dimensional array."
            )
        if spectrum.ndim != 2:
            raise exceptions.ValidationError(
                "`spectrum` must be a two-dimensional array."
            )
        if lambda_values.ndim != 1:
            raise exceptions.ValidationError(
                "`lambda_values` must be a one-dimensional array."
            )
        if phonon_smearing.ndim != 1:
            raise exceptions.ValidationError(
                "`phonon_smearing` must be a one-dimensional array."
            )
        if spectrum.shape[0] != frequency.shape[0]:
            raise exceptions.ValidationError(
                "The first spectrum dimension must match the frequency grid length."
            )
        if lambda_values.shape != phonon_smearing.shape:
            raise exceptions.ValidationError(
                "`lambda_values` and `phonon_smearing` must have the same shape."
            )

        num_smearings = lambda_values.shape[0]

        if cumulative_lambda is not None:
            if cumulative_lambda.ndim != 2:
                raise exceptions.ValidationError(
                    "`cumulative_lambda` must be a two-dimensional array."
                )
            if cumulative_lambda.shape != spectrum.shape:
                raise exceptions.ValidationError(
                    "`cumulative_lambda` must have the same shape as `spectrum`."
                )
            raw_spectrum = numpy.concatenate([spectrum, cumulative_lambda], axis=1)
            spectrum_data = spectrum
        elif spectrum.shape[1] == num_smearings:
            raw_spectrum = spectrum
            spectrum_data = spectrum
            cumulative_lambda = None
        elif spectrum.shape[1] == 2 * num_smearings:
            spectrum_data = spectrum[:, :num_smearings]
            cumulative_lambda = spectrum[:, num_smearings:]
            raw_spectrum = spectrum
        else:
            raise exceptions.ValidationError(
                "The spectrum must have either one or two blocks of columns per smearing value."
            )

        self.set_array(self.ARRAY_FREQUENCY, frequency)
        self.set_array(self.ARRAY_RAW_SPECTRUM, raw_spectrum)
        self.set_array(self.ARRAY_SPECTRUM, spectrum_data)
        self.set_array(self.ARRAY_LAMBDA, lambda_values)
        self.set_array(self.ARRAY_PHONON_SMEARING, phonon_smearing)
        if cumulative_lambda is None:
            self._delete_optional_array(self.ARRAY_CUMULATIVE_LAMBDA)
        else:
            self.set_array(self.ARRAY_CUMULATIVE_LAMBDA, cumulative_lambda)

        self._set_optional_attribute(
            self.ATTRIBUTE_ELECTRON_SMEARING, electron_smearing
        )
        self._set_optional_attribute(self.ATTRIBUTE_FERMI_WINDOW, fermi_window)
        self._set_optional_attribute(
            self.ATTRIBUTE_SUMMED_ELPH_COUPLING, summed_elph_coupling
        )

    def get_frequency(self):
        """Return the frequency grid in meV."""
        return self.get_array(self.ARRAY_FREQUENCY)

    def get_spectrum(self):
        """Return the a2F spectrum values."""
        return self.get_array(self.ARRAY_SPECTRUM)

    def get_cumulative_lambda(self):
        """Return the cumulative integrated lambda spectrum if available."""
        if self.ARRAY_CUMULATIVE_LAMBDA not in self.get_arraynames():
            return None

        return self.get_array(self.ARRAY_CUMULATIVE_LAMBDA)

    def get_lambda(self):
        """Return the integrated electron-phonon coupling values."""
        return self.get_array(self.ARRAY_LAMBDA)

    def get_phonon_smearing(self):
        """Return the phonon smearing grid in meV."""
        return self.get_array(self.ARRAY_PHONON_SMEARING)

    @property
    def electron_smearing(self):
        """Return the electron smearing in eV if available."""
        return self.base.attributes.get(self.ATTRIBUTE_ELECTRON_SMEARING, None)

    @property
    def fermi_window(self):
        """Return the Fermi window in eV if available."""
        return self.base.attributes.get(self.ATTRIBUTE_FERMI_WINDOW, None)

    @property
    def summed_elph_coupling(self):
        """Return the summed electron-phonon coupling if available."""
        return self.base.attributes.get(self.ATTRIBUTE_SUMMED_ELPH_COUPLING, None)

    def _set_optional_attribute(self, key, value):
        """Set or clear an optional scalar attribute."""
        if value is None:
            try:
                self.base.attributes.delete(key)
            except AttributeError:
                pass
            return

        self.base.attributes.set(key, float(value))

    def _delete_optional_array(self, name):
        """Delete an optional array if it exists."""
        if name in self.get_arraynames():
            self.delete_array(name)
