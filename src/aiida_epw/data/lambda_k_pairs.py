"""Domain-specific data type for EPW ``lambda_k_pairs`` spectra."""

import numpy
from aiida import orm
from aiida.common import exceptions


class LambdaKPairsData(orm.ArrayData):
    """Store the EPW ``lambda_k_pairs`` distribution with explicit getters."""

    ARRAY_LAMBDA_NK = "lambda_nk"
    ARRAY_RHO = "rho"

    def set_lambda_k_pairs(self, lambda_nk, rho):
        """Store the EPW ``lambda_k_pairs`` columns."""
        lambda_nk = numpy.array(lambda_nk, dtype=float)
        rho = numpy.array(rho, dtype=float)

        if lambda_nk.ndim != 1:
            raise exceptions.ValidationError(
                "`lambda_nk` must be a one-dimensional array."
            )
        if rho.ndim != 1:
            raise exceptions.ValidationError("`rho` must be a one-dimensional array.")
        if lambda_nk.shape != rho.shape:
            raise exceptions.ValidationError(
                "`lambda_nk` and `rho` must have the same shape."
            )

        self.set_array(self.ARRAY_LAMBDA_NK, lambda_nk)
        self.set_array(self.ARRAY_RHO, rho)

    def get_lambda_nk(self):
        """Return the lambda_nk grid."""
        return self.get_array(self.ARRAY_LAMBDA_NK)

    def get_rho(self):
        """Return the density distribution."""
        return self.get_array(self.ARRAY_RHO)
