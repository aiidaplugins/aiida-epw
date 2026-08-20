import enum


class RestartType(enum.Enum):
    """Enumeration of EPW run/restart modes."""

    FROM_SCRATCH = "from_scratch"
    FROM_EPB = "from_epb"
    FROM_EPMATWP = "from_epmatwp"
    EPHWRITE = "ephwrite"
    FROM_EPH = "from_eph"


RESTART_TYPE_DEFAULTS = {
    RestartType.FROM_SCRATCH: {
        "epwread": False,
        "epwwrite": True,
        "epbwrite": True,
        "epbread": False,
    },
    RestartType.FROM_EPB: {
        "epbread": True,
        "epbwrite": False,
        "epwread": False,
        "epwwrite": True,
    },
    RestartType.FROM_EPMATWP: {
        "epwread": True,
        "epwwrite": False,
        "epbwrite": False,
        "epbread": False,
    },
    RestartType.EPHWRITE: {
        "epwread": True,
        "ep_coupling": True,
        "elph": True,
        "ephwrite": True,
        "restart": True,
    },
    RestartType.FROM_EPH: {
        "epwread": True,
        "ep_coupling": False,
        "elph": False,
        "ephwrite": False,
        "restart": False,
    },
}
