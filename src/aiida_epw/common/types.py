# -*- coding: utf-8 -*-
"""Data types for EPW plugin."""

import enum


class CalculationTypes(enum.Enum):
    """Enumeration of EPW calculation types."""

    ELIASHBERG = "eliashberg"
    TRANSPORT = "transport"
    POLARON = "polaron"
