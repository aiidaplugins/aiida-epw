def check_kpoints_qpoints_compatibility(
    kpoints,
    qpoints,
) -> tuple[bool, str]:
    """Check if the kpoints and qpoints are compatible."""

    kpoints_mesh, kpoints_shift = kpoints.get_kpoints_mesh()
    qpoints_mesh, qpoints_shift = qpoints.get_kpoints_mesh()

    multiplicities = []
    remainder = []

    for k, q in zip(kpoints_mesh, qpoints_mesh):
        multiplicities.append(k // q)
        remainder.append(k % q)

    if kpoints_shift != [0.0, 0.0, 0.0] or qpoints_shift != [0.0, 0.0, 0.0]:
        return (False, "Shift grid is not supported.")
    else:
        if remainder == [0, 0, 0]:
            return (
                True,
                f"The kpoints and qpoints are compatible with multiplicities {multiplicities}.",
            )
        else:
            return (False, "The kpoints and qpoints are not compatible.")
