import enum


class EliashbergType(enum.Enum):
    """Enumeration of Eliashberg calculation types."""

    ISOTROPIC = "isotropic"
    LINEARIZED = "linearized"
    FSR = "fsr"
    FBW = "fbw"
