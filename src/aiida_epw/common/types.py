import enum


class RestartType(enum.Enum):
    """Enumeration of EPW run/restart modes."""

    WANNIERIZE = "wannierize"
    EPHWRITE = "ephwrite"
    EPHREAD = "ephread"
