import enum


class RestartType(enum.Enum):
    """Enumeration of EPW run/restart modes."""

    EPHWRITE = "ephwrite"
    EPHREAD = "ephread"
    EPHWRITE_RESTART = "ephwrite_restart"
