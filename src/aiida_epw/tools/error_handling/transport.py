"""Transport and mesh error-recovery decisions for EPW workflows."""


def prepare_mesh_refinement(
    stdout_content, scheduler_stderr, kfpoints_factor=None, qfpoints_distance=None
):
    """Return mesh updates and an action, or ``(None, None)`` if unrecoverable."""
    updates = {}
    if "LU factorization failed" in stdout_content:
        if kfpoints_factor == 1:
            updates["kfpoints_factor"] = 2
            action = (
                "Increased kfpoints_factor from 1 to 2 due to LU factorization failure."
            )
        elif qfpoints_distance is not None and qfpoints_distance * 0.8 >= 0.03:
            updates["qfpoints_distance"] = qfpoints_distance * 0.8
            action = (
                f"Decreased qfpoints_distance from {qfpoints_distance:.3f} to "
                f"{updates['qfpoints_distance']:.3f} due to LU factorization failure."
            )
        else:
            return None, None
    elif any(
        marker in scheduler_stderr.upper()
        for marker in ("KILLED", "SIGKILL", "OOM", "OUT OF MEMORY")
    ):
        if qfpoints_distance is not None and qfpoints_distance * 1.25 <= 0.15:
            updates["qfpoints_distance"] = qfpoints_distance * 1.25
            action = (
                f"Increased qfpoints_distance from {qfpoints_distance:.3f} to "
                f"{updates['qfpoints_distance']:.3f} due to OOM."
            )
        elif kfpoints_factor is not None and kfpoints_factor > 1:
            updates["kfpoints_factor"] = kfpoints_factor - 1
            action = (
                f"Decreased kfpoints_factor from {kfpoints_factor} to "
                f"{updates['kfpoints_factor']} due to OOM."
            )
        else:
            return None, None
    else:
        return None, None
    return updates, action
