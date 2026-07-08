import enum


class RestartType(enum.Enum):
    """Enumeration of EPW run/restart modes."""

    NONE = "none"
    EPHWRITE = "ephwrite"
    EPHREAD = "ephread"
    EPHWRITE_RESTART = "ephwrite_restart"
    EPWREAD = "epwread"
