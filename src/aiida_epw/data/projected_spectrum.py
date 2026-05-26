"""Domain-specific data type for EPW projected spectra."""

import numpy
from aiida import orm
from aiida.common import exceptions


class ProjectedSpectrumData(orm.ArrayData):
    """Store a projected spectrum with explicit grid and split total/projected series."""

    ARRAY_GRID = "grid"
    ARRAY_SERIES = "series"
    ARRAY_TOTAL = "total"
    ARRAY_PROJECTED = "projected"

    ATTRIBUTE_KIND = "kind"
    ATTRIBUTE_GRID_NAME = "grid_name"
    ATTRIBUTE_SERIES_NAME = "series_name"
    ATTRIBUTE_TOTAL_LABEL = "total_label"
    ATTRIBUTE_PROJECTED_LABEL = "projected_label"
    ATTRIBUTE_LEGACY_GRID_NAME = "legacy_grid_name"
    ATTRIBUTE_LEGACY_SERIES_NAME = "legacy_series_name"

    def set_projected_spectrum(
        self,
        grid,
        series,
        *,
        kind,
        grid_name=None,
        series_name=None,
        total_label=None,
        projected_label=None,
        legacy_grid_name=None,
        legacy_series_name=None,
    ):
        """Store a projected spectrum and split off the total series."""
        grid = numpy.array(grid, dtype=float)
        series = numpy.array(series, dtype=float)

        if grid.ndim != 1:
            raise exceptions.ValidationError("`grid` must be a one-dimensional array.")
        if series.ndim != 2:
            raise exceptions.ValidationError(
                "`series` must be a two-dimensional array."
            )
        if series.shape[0] != grid.shape[0]:
            raise exceptions.ValidationError(
                "The first spectrum dimension must match the grid length."
            )
        if series.shape[1] < 1:
            raise exceptions.ValidationError(
                "The spectrum must contain at least one series column."
            )

        self._delete_alias_array(
            self.base.attributes.get(self.ATTRIBUTE_LEGACY_GRID_NAME, None)
        )
        self._delete_alias_array(
            self.base.attributes.get(self.ATTRIBUTE_LEGACY_SERIES_NAME, None)
        )

        self.set_array(self.ARRAY_GRID, grid)
        self.set_array(self.ARRAY_SERIES, series)
        self.set_array(self.ARRAY_TOTAL, series[:, 0])
        if series.shape[1] == 1:
            self._delete_alias_array(self.ARRAY_PROJECTED)
        else:
            self.set_array(self.ARRAY_PROJECTED, series[:, 1:])

        if legacy_grid_name is not None:
            self.set_array(legacy_grid_name, grid)
        if legacy_series_name is not None:
            self.set_array(legacy_series_name, series)

        self.base.attributes.set(self.ATTRIBUTE_KIND, kind)
        self._set_optional_attribute(self.ATTRIBUTE_GRID_NAME, grid_name)
        self._set_optional_attribute(self.ATTRIBUTE_SERIES_NAME, series_name)
        self._set_optional_attribute(self.ATTRIBUTE_TOTAL_LABEL, total_label)
        self._set_optional_attribute(self.ATTRIBUTE_PROJECTED_LABEL, projected_label)
        self._set_optional_attribute(self.ATTRIBUTE_LEGACY_GRID_NAME, legacy_grid_name)
        self._set_optional_attribute(
            self.ATTRIBUTE_LEGACY_SERIES_NAME, legacy_series_name
        )

    def get_grid(self):
        """Return the spectrum grid."""
        return self.get_array(self.ARRAY_GRID)

    def get_series(self):
        """Return the combined total-plus-projected spectrum matrix."""
        return self.get_array(self.ARRAY_SERIES)

    def get_total(self):
        """Return the total spectrum."""
        return self.get_array(self.ARRAY_TOTAL)

    def get_projected(self):
        """Return the projected spectrum columns, if present."""
        if self.ARRAY_PROJECTED not in self.get_arraynames():
            return None

        return self.get_array(self.ARRAY_PROJECTED)

    @property
    def kind(self):
        """Return the spectrum kind, e.g. `a2f_proj` or `phdos_proj`."""
        return self.base.attributes.get(self.ATTRIBUTE_KIND)

    @property
    def grid_name(self):
        """Return the semantic name of the grid."""
        return self.base.attributes.get(self.ATTRIBUTE_GRID_NAME, None)

    @property
    def series_name(self):
        """Return the semantic name of the combined series block."""
        return self.base.attributes.get(self.ATTRIBUTE_SERIES_NAME, None)

    @property
    def total_label(self):
        """Return the label of the total spectrum column."""
        return self.base.attributes.get(self.ATTRIBUTE_TOTAL_LABEL, None)

    @property
    def projected_label(self):
        """Return the label of the projected spectrum block."""
        return self.base.attributes.get(self.ATTRIBUTE_PROJECTED_LABEL, None)

    def _delete_alias_array(self, name):
        """Delete an alias array if it exists."""
        if name and name in self.get_arraynames():
            self.delete_array(name)

    def _set_optional_attribute(self, key, value):
        """Set or clear an optional scalar attribute."""
        if value is None:
            try:
                self.base.attributes.delete(key)
            except AttributeError:
                pass
            return

        self.base.attributes.set(key, value)
