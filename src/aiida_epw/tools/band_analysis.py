"""Utility functions for analyzing band structure data."""

import logging
import re
import numpy as np
from aiida import orm
from aiida.engine import calcfunction

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  Core band-edge detection (pure numpy, no AiiDA graph navigation)  #
# ------------------------------------------------------------------ #


def detect_bandgap_from_nscf(bands_array, num_electrons, exclude_bands=None):
    """Detect bandgap from a bands array using the electron count.

    This is the most robust method for insulators: it does **not** depend on
    the Fermi energy (which equals VBM for ``occupations='fixed'``) and
    instead uses the number of valence electrons to locate the VBM/CBM
    boundary.

    :param bands_array: numpy array of shape ``(nkpts, nbands)``.
    :param num_electrons: total number of electrons (float or int).
    :param exclude_bands: optional 1-indexed list of bands to exclude
        (e.g. bands excluded in Wannier90).
    :return: dict with ``vbm``, ``cbm``, ``bandgap``, ``num_val_bands``,
        or ``None`` if the system is metallic (gap < 0.01 eV).
    """
    num_val_bands = int(num_electrons / 2)  # spin-degenerate

    if exclude_bands:
        keep = [i for i in range(bands_array.shape[1]) if (i + 1) not in exclude_bands]
        bands_array = bands_array[:, keep]
        num_excluded_below = sum(1 for b in exclude_bands if b <= num_val_bands)
        num_val_bands -= num_excluded_below

    if num_val_bands <= 0 or num_val_bands >= bands_array.shape[1]:
        logger.warning(
            "Cannot detect bandgap: num_val_bands=%d, num_bands=%d",
            num_val_bands,
            bands_array.shape[1],
        )
        return None

    vbm = float(np.max(bands_array[:, num_val_bands - 1]))
    cbm = float(np.min(bands_array[:, num_val_bands]))
    bandgap = cbm - vbm

    if bandgap < 0.01:
        return None

    return {
        "vbm": vbm,
        "cbm": cbm,
        "bandgap": bandgap,
        "num_val_bands": num_val_bands,
    }


def detect_bandgap_largest_gap(bands_array):
    """Detect bandgap using the largest-gap heuristic.

    Finds the biggest energy gap among *all* eigenvalues.  This is
    Fermi-energy-independent and works as a last-resort fallback, but can
    be wrong when the true gap is not the largest gap in the eigenvalue
    spectrum (e.g. deep semi-core states).

    :param bands_array: numpy array of shape ``(nkpts, nbands)``.
    :return: dict with ``vbm``, ``cbm``, ``bandgap``, or ``None`` if the
        array is empty.
    """
    all_energies = np.sort(np.unique(bands_array.flatten()))
    energy_diffs = np.diff(all_energies)

    if len(energy_diffs) == 0:
        return None

    gap_idx = int(np.argmax(energy_diffs))
    vbm = float(all_energies[gap_idx])
    cbm = float(all_energies[gap_idx + 1])
    bandgap = float(energy_diffs[gap_idx])

    return {
        "vbm": vbm,
        "cbm": cbm,
        "bandgap": bandgap,
    }


def detect_band_edges_from_occupations(bands_data, threshold=0.005):
    """Detect VBM/CBM directly from the band occupations.

    This method does not assume a fixed spin degeneracy and is therefore robust
    for SOC/non-collinear calculations where electron-count heuristics can be
    ambiguous.

    :param bands_data: ``BandsData`` node containing ``bands`` and
        ``occupations`` arrays.
    :param threshold: Occupation threshold below which a state is treated as
        unoccupied.
    :return: dict with ``vbm``, ``cbm``, ``bandgap`` (and optional ``warning``),
        or ``None`` if occupations are unavailable/invalid.
    """
    try:
        occupations = bands_data.get_array("occupations")
    except KeyError:
        return None

    bands = _extract_bands_array(bands_data)

    if occupations.ndim == 3:
        occupations = occupations.reshape(-1, occupations.shape[-1])

    if occupations.shape != bands.shape:
        logger.warning(
            "Cannot detect band edges from occupations: bands shape %s "
            "!= occupations shape %s",
            bands.shape,
            occupations.shape,
        )
        return None

    occupied_mask = occupations > threshold
    if not np.any(occupied_mask):
        logger.warning("No occupied states found from occupations.")
        return None

    vbm = float(np.max(bands[occupied_mask]))

    unoccupied_mask = occupations < threshold
    if not np.any(unoccupied_mask):
        return {
            "vbm": vbm,
            "cbm": None,
            "bandgap": None,
            "warning": (
                "No unoccupied states found in NSCF occupations. "
                "Likely only occupied states were computed (nbnd too small)."
            ),
        }

    cbm = float(np.min(bands[unoccupied_mask]))
    bandgap = cbm - vbm

    result = {
        "vbm": vbm,
        "cbm": cbm,
        "bandgap": bandgap,
    }
    if bandgap < 0.01:
        result["warning"] = (
            f"Bandgap from occupations is very small ({bandgap:.6f} eV). "
            "The system may be metallic or strongly entangled."
        )
    return result


# ------------------------------------------------------------------ #
#  Wrappers that obtain electron count from different sources         #
# ------------------------------------------------------------------ #


def detect_bandgap_from_bands(
    reference_bands, structure, pseudo_family, exclude_bands=None
):
    """Detect the true bandgap from reference bands using electron count.

    Unlike ``get_homo_lumo`` which uses the Fermi energy (unreliable for
    ``occupations='fixed'`` where Ef = VBM), this function determines the
    VBM/CBM from the number of electrons and the band structure directly.

    :param reference_bands: ``BandsData`` from a PwBandsWorkChain.
    :param structure: ``StructureData`` of the material.
    :param pseudo_family: Pseudopotential family label string.
    :param exclude_bands: Optional 1-indexed list of bands excluded in W90.
    :return: dict with keys ``vbm``, ``cbm``, ``bandgap``, ``num_val_bands``.
             Returns None if the system is metallic.
    """
    from aiida_wannier90_workflows.utils.pseudo import (
        get_number_of_electrons,
        get_pseudo_and_cutoff,
    )

    pseudos, _, _ = get_pseudo_and_cutoff(pseudo_family, structure)
    num_electrons = get_number_of_electrons(structure, pseudos)
    bands_array = reference_bands.get_bands()  # (nk, nb)

    return detect_bandgap_from_nscf(bands_array, num_electrons, exclude_bands)


# ------------------------------------------------------------------ #
#  Band-exclusion helpers (semicore, energy cutoff, VBM-relative)    #
# ------------------------------------------------------------------ #


def _extract_bands_array(reference_bands):
    """Extract a 2-D (nkpts, nbands) numpy array from BandsData or ndarray.

    Handles spin-collinear shape ``(2, nkpts, nbands)`` by stacking the two
    spin channels along the k-point axis.
    """
    if isinstance(reference_bands, np.ndarray):
        bands = reference_bands
    else:
        bands = reference_bands.get_bands()  # type: ignore[attr-defined]

    if bands.ndim == 3:
        # spin-collinear: (2, nkpts, nbands) → (2*nkpts, nbands)
        bands = bands.reshape(-1, bands.shape[-1])

    return bands


def get_semicore_bands(
    structure,
    pseudo_family,
    spin_non_collinear=False,
    force_exclude=False,
    reference_bands=None,
):
    """Return 1-indexed band indices of semicore states identified from pseudo metadata.

    :param structure: ``StructureData`` of the material.
    :param pseudo_family: Pseudopotential family label string.
    :param spin_non_collinear: Set True for SOC or non-collinear spin calculations.
    :param force_exclude: If True, skip the hybridisation-overlap check and return
        the semicore list even when those bands are entangled with valence bands.
        Default False matches upstream ``Wannier90OptimizeWorkChain`` behaviour
        (returns ``[]`` when overlap is detected).
    :param reference_bands: ``BandsData`` from a PW calculation.  Required when
        ``force_exclude=False`` to check for band overlap.
    :return: Sorted list of 1-indexed band indices, e.g. ``[1, 2, 3, 4, 5]``.
    """
    from aiida_wannier90_workflows.utils.pseudo import (
        get_pseudo_and_cutoff,
        get_pseudo_orbitals,
        get_semicore_list,
    )
    from aiida_wannier90_workflows.utils.workflows.bands import has_overlapping_semicore

    pseudos, _, _ = get_pseudo_and_cutoff(pseudo_family, structure)
    pseudo_orbitals = get_pseudo_orbitals(pseudos)
    semicore_list = get_semicore_list(structure, pseudo_orbitals, spin_non_collinear)

    if not semicore_list:
        return []

    if not force_exclude:
        if reference_bands is None:
            raise ValueError(
                "reference_bands is required when force_exclude=False "
                "to check whether semicore bands overlap with valence bands."
            )
        if has_overlapping_semicore(reference_bands, semicore_list):
            logger.warning(
                "Semicore bands overlap with valence bands (gap < 0.01 eV). "
                "Returning [] to match upstream behaviour. "
                "Pass force_exclude=True to exclude anyway."
            )
            return []

    return sorted(set(semicore_list))


def get_exclude_bands_by_energy(reference_bands, energy_max_eV):
    """Return 1-indexed indices of bands whose global maximum energy is below a threshold.

    :param reference_bands: ``BandsData`` or numpy array of shape ``(nkpts, nbands)``.
    :param energy_max_eV: Float (eV).  All bands whose maximum energy across all
        k-points is strictly less than this value are excluded.
    :return: Sorted list of 1-indexed band indices.
    """
    bands = _extract_bands_array(reference_bands)
    band_maxima = np.max(bands, axis=0)
    return [i + 1 for i, emax in enumerate(band_maxima) if emax < energy_max_eV]


def get_exclude_bands_below_vbm(
    reference_bands,
    structure,
    pseudo_family,
    eV_below_vbm=None,
    n_bands=None,
    spin_non_collinear=False,
):
    """Return 1-indexed band indices to exclude based on proximity to the VBM.

    The VBM is located via the electron-count method (``detect_bandgap_from_nscf``),
    which is robust for insulators with ``occupations='fixed'``.

    Exactly one of ``eV_below_vbm`` or ``n_bands`` must be provided:

    * ``eV_below_vbm``: exclude every band whose maximum energy is more than
      ``eV_below_vbm`` eV below the VBM, i.e. bands with
      ``max_energy < VBM − abs(eV_below_vbm)``.
    * ``n_bands``: exclude the *n_bands* lowest-energy valence bands (by mean
      energy across k-points).

    :param reference_bands: ``BandsData`` or numpy array.
    :param structure: ``StructureData``.
    :param pseudo_family: Pseudopotential family label string.
    :param eV_below_vbm: Positive float.  Bands more than this many eV below VBM
        are excluded.
    :param n_bands: Integer.  Exclude the bottom *n_bands* valence bands.
    :param spin_non_collinear: For SOC / non-collinear calculations.
    :return: Sorted list of 1-indexed band indices.
    """
    from aiida_wannier90_workflows.utils.pseudo import (
        get_number_of_electrons,
        get_pseudo_and_cutoff,
    )

    if (eV_below_vbm is None) == (n_bands is None):
        raise ValueError("Exactly one of 'eV_below_vbm' or 'n_bands' must be set.")

    pseudos, _, _ = get_pseudo_and_cutoff(pseudo_family, structure)
    num_electrons = get_number_of_electrons(structure, pseudos)

    bands = _extract_bands_array(reference_bands)
    gap_info = detect_bandgap_from_nscf(bands, num_electrons)

    num_val = int(num_electrons / 2)
    if gap_info is not None:
        vbm = gap_info["vbm"]
        num_val = gap_info["num_val_bands"]
    else:
        logger.warning(
            "System appears metallic (no band gap detected). "
            "Using electron count to estimate VBM: band %d.",
            num_val,
        )
        vbm = float(np.max(bands[:, num_val - 1]))

    if eV_below_vbm is not None:
        threshold = vbm - abs(eV_below_vbm)
        return get_exclude_bands_by_energy(bands, threshold)

    # n_bands path
    if n_bands >= num_val:
        raise ValueError(
            f"n_bands={n_bands} must be less than the total number of "
            f"valence bands ({num_val})."
        )
    val_indices = list(range(num_val))
    mean_energy = np.mean(bands, axis=0)
    val_indices_sorted = sorted(val_indices, key=lambda i: mean_energy[i])
    return sorted(val_indices_sorted[i] + 1 for i in range(n_bands))


def compute_w90_exclusion_overrides(
    structure,
    pseudo_family,
    exclude_bands,
    num_bands_factor=2.0,
    spin_non_collinear=False,
    spin_orbit_coupling=False,
):
    """Build a Wannier90 overrides dict that excludes the specified DFT bands.

    The returned dict is ready to be passed as ``overrides={"w90_bands": result}``
    to ``EpwPrepWorkChain.get_builder_from_protocol``.  It:

    * Sets ``meta_parameters.exclude_semicore = False`` to prevent the upstream
      workflow from running its own semicore detection on top of the manual list.
    * Keeps ``num_wann`` at the full projection count (NOT reduced by excluded
      bands).  With ``ATOMIC_PROJECTORS_QE`` (auto_projections), pw2wannier90
      always generates AMN for **all** pswfc orbitals — the AMN column count is
      fixed and ``num_wann`` must match it exactly.
    * Reduces ``num_bands`` (Wannier window size) by the number of excluded bands.
    * Embeds the explicit ``exclude_bands`` list in the Wannier90 parameters.

    .. note::
        ``exclude_bands`` removes DFT energy bands from the W90 disentanglement
        **window** only.  It does **not** reduce the number of target WFs
        (``num_wann``).  This is the correct use-case for deep bands that would
        otherwise pollute the disentanglement but whose projector character is
        still needed for the WFs.

        If you want fewer WFs (e.g. valence-only Wannierization for a
        semiconductor), use ``ElectronicType.INSULATOR`` in the submit script
        instead of ``--exclude-bands``.

    :param structure: ``StructureData``.
    :param pseudo_family: Pseudopotential family label string.
    :param exclude_bands: Non-empty list of 1-indexed band indices to exclude.
    :param num_bands_factor: Factor used by ``get_wannier_number_of_bands`` to
        compute the Wannier energy window (default 2.0, the upstream moderate
        protocol value).
    :param spin_non_collinear: For SOC / non-collinear calculations.
    :param spin_orbit_coupling: For SOC calculations.
    :return: Nested dict of overrides for the ``w90_bands`` namespace.
    :raises ValueError: If ``exclude_bands`` is empty or the resulting
        ``num_bands`` < ``num_wann``.
    """
    from aiida_wannier90_workflows.utils.pseudo import (
        get_number_of_projections,
        get_pseudo_and_cutoff,
        get_wannier_number_of_bands,
    )

    exclude_bands = sorted(set(exclude_bands))
    if not exclude_bands:
        raise ValueError(
            "exclude_bands is empty. "
            "Passing an empty list would disable upstream semicore auto-exclusion "
            "without replacing it — this is almost certainly unintentional."
        )

    pseudos, _, _ = get_pseudo_and_cutoff(pseudo_family, structure)

    num_bands_base = get_wannier_number_of_bands(
        structure=structure,
        pseudos=pseudos,
        factor=num_bands_factor,
        only_valence=False,
        spin_non_collinear=spin_non_collinear,
        spin_orbit_coupling=spin_orbit_coupling,
    )
    num_wann_base = get_number_of_projections(
        structure=structure,
        pseudos=pseudos,
        spin_non_collinear=spin_non_collinear,
        spin_orbit_coupling=spin_orbit_coupling,
    )

    n_excl = len(exclude_bands)
    # num_wann stays at the full projection count: pw2wannier90 with auto_projections
    # always generates AMN for all pswfc orbitals; num_wann must match AMN columns.
    num_wann = num_wann_base
    num_bands = num_bands_base - n_excl

    if num_bands < num_wann:
        raise ValueError(
            f"After excluding {n_excl} bands, num_bands={num_bands} < num_wann={num_wann}. "
            "Increase num_bands_factor or reduce the number of excluded bands."
        )

    logger.info(
        "compute_w90_exclusion_overrides: excluding bands %s → "
        "num_wann=%d (unchanged), num_bands=%d (was %d)",
        exclude_bands,
        num_wann,
        num_bands,
        num_bands_base,
    )

    return {
        "meta_parameters": {"exclude_semicore": False},
        "wannier90": {
            "wannier90": {
                "parameters": {
                    "exclude_bands": exclude_bands,
                    "num_wann": num_wann,
                    "num_bands": num_bands,
                }
            }
        },
    }


# ------------------------------------------------------------------ #
#  AiiDA graph navigation helpers                                     #
# ------------------------------------------------------------------ #


def _find_w90_workchain(epw_prep_node):
    """Locate the Wannier90 workchain child of an EpwPrepWorkChain.

    Tries the ``w90_bands`` link label first, then falls back to scanning
    ``called`` children by process label. If neither works (e.g., when
    ``parent_folder_w90`` was provided to reuse previous W90 results), traces
    back through the ``parent_folder_w90`` RemoteData to find the original
    W90 workchain.

    :return: The Wannier90OptimizeWorkChain / Wannier90BandsWorkChain node,
        or ``None``.
    """
    # Try link label (reliable, set by prep.py)
    try:
        return (
            epw_prep_node.base.links.get_outgoing(link_label_filter="w90_bands")
            .first()
            .node
        )
    except (AttributeError, IndexError, ValueError):
        pass

    # Fallback: scan called children
    for called in epw_prep_node.called:
        if called.process_label in (
            "Wannier90OptimizeWorkChain",
            "Wannier90BandsWorkChain",
        ):
            return called

    # Fallback: trace back via parent_folder_w90 (when reusing previous W90)
    try:
        if "parent_folder_w90" in epw_prep_node.inputs:
            parent_w90_folder = epw_prep_node.inputs.parent_folder_w90
            logger.info(
                "No W90 child found, tracing back via parent_folder_w90 (PK %s)",
                parent_w90_folder.pk,
            )
            # Navigate backwards through incoming links to find the W90 workchain
            for link in parent_w90_folder.base.links.get_incoming().all():
                caller = link.node
                if caller.process_label in (
                    "Wannier90OptimizeWorkChain",
                    "Wannier90BandsWorkChain",
                ):
                    logger.info(
                        "Found original W90 workchain via parent_folder_w90: "
                        "%s (PK %s)",
                        caller.process_label,
                        caller.pk,
                    )
                    return caller
    except (AttributeError, KeyError):
        pass

    return None


def _get_nscf_data(w90_wc):
    """Extract nscf output_parameters dict and output_band BandsData.

    Tries the exposed ``nscf`` namespace first, then navigates via the
    ``nscf`` link label.

    :return: tuple ``(output_params_dict, bands_data)`` – either element
        may be ``None``.
    """
    nscf_params = None
    nscf_bands = None

    # Exposed outputs (clean path)
    try:
        nscf_params = w90_wc.outputs.nscf.output_parameters.get_dict()
        nscf_bands = w90_wc.outputs.nscf.output_band
    except AttributeError:
        pass

    # Fallback: navigate by link label
    if nscf_params is None or nscf_bands is None:
        try:
            nscf_base = (
                w90_wc.base.links.get_outgoing(link_label_filter="nscf").first().node
            )
            if nscf_params is None and "output_parameters" in nscf_base.outputs:
                nscf_params = nscf_base.outputs.output_parameters.get_dict()
            if nscf_bands is None and "output_band" in nscf_base.outputs:
                nscf_bands = nscf_base.outputs.output_band
        except (AttributeError, IndexError, ValueError):
            pass

    return nscf_params, nscf_bands


def _get_epw_coarse_bands(epw_prep_node):
    """Locate the EPW coarse-grid ``el_band_structure`` BandsData.

    Searches ``epw_bands`` child, direct outputs, then ``epw_base`` child.

    :return: BandsData or ``None``.
    """
    # epw_bands child
    try:
        epw_bands = (
            epw_prep_node.base.links.get_outgoing(link_label_filter="epw_bands")
            .first()
            .node
        )
        if "el_band_structure" in epw_bands.outputs:
            return epw_bands.outputs.el_band_structure
    except (AttributeError, IndexError):
        pass

    # Direct output
    if "el_band_structure" in epw_prep_node.outputs:
        return epw_prep_node.outputs.el_band_structure

    # epw_base child
    try:
        epw_base = (
            epw_prep_node.base.links.get_outgoing(link_label_filter="epw_base")
            .first()
            .node
        )
        if "el_band_structure" in epw_base.outputs:
            return epw_base.outputs.el_band_structure
    except (AttributeError, IndexError):
        pass

    return None


def extract_band_edges_from_epw_prep(epw_prep_node):
    """Extract VBM/CBM from an EpwPrepWorkChain node.

    **Primary method** – occupation-based edge detection via the W90 nscf step::

        EpwPrepWorkChain
          └─ [w90_bands] Wannier90OptimizeWorkChain
               └─ .outputs.nscf.output_parameters  → number_of_electrons
               └─ .outputs.nscf.output_band         → BandsData (nk, nb)

    If occupations are unavailable or incomplete, it falls back to the
    electron-count method. As a last resort it uses the largest-gap heuristic
    on EPW coarse-grid bands.

    :param epw_prep_node: a completed ``EpwPrepWorkChain`` node.
    :return: dict with ``vbm``, ``cbm``, ``bandgap``, ``method``
        (and optionally ``warning``), or ``None`` on failure.
    """
    # ── Primary: occupation-based (then electron-count) from NSCF ──
    w90_wc = _find_w90_workchain(epw_prep_node)
    partial_occupation_result = None
    if w90_wc is not None:
        nscf_params, nscf_bands = _get_nscf_data(w90_wc)
        if nscf_bands is not None:
            occ_result = detect_band_edges_from_occupations(nscf_bands)
            if occ_result is not None:
                occ_result["method"] = "occupation_from_nscf"
                if occ_result.get("cbm") is not None:
                    return occ_result
                partial_occupation_result = occ_result
                logger.warning(
                    "Occupation-based method found VBM but no CBM. "
                    "Will try electron-count and fallback methods for CBM."
                )

        if nscf_params is not None and nscf_bands is not None:
            num_electrons = nscf_params.get("number_of_electrons")
            if num_electrons is not None:
                bands_array = nscf_bands.get_bands()
                result = detect_bandgap_from_nscf(bands_array, num_electrons)
                if result is not None:
                    result["method"] = "electron_count_from_nscf"
                    return result
                logger.warning(
                    "Electron-count method returned None (metallic or "
                    "invalid band count)."
                )
            else:
                logger.warning(
                    "number_of_electrons not found in nscf output_parameters"
                )
        else:
            logger.warning(
                "Could not access nscf output_parameters or output_band "
                "from W90 workchain <%s>",
                w90_wc.pk,
            )
    else:
        logger.warning(
            "No Wannier90OptimizeWorkChain/Wannier90BandsWorkChain found "
            "under EpwPrepWorkChain <%s>",
            epw_prep_node.pk,
        )

    if partial_occupation_result is not None:
        return partial_occupation_result

    # ── Fallback: largest-gap heuristic on EPW coarse bands ──
    bands_data = _get_epw_coarse_bands(epw_prep_node)
    if bands_data is not None:
        result = detect_bandgap_largest_gap(bands_data.get_bands())
        if result is not None:
            result["method"] = "largest_gap_epw_coarse"
            result["warning"] = (
                "Band edges from EPW coarse grid (largest-gap heuristic) "
                "may be inaccurate. True band gap could be different."
            )
            return result

    return None


# ------------------------------------------------------------------ #
#  Fermi-energy-based helpers (kept for backward compatibility)       #
# ------------------------------------------------------------------ #


def get_vbm_cbm(bands, fermi_energy):
    """Extract VBM and CBM from band structure using fermi_energy as reference.

    .. warning::

       For insulators computed with ``occupations='fixed'`` the Fermi
       energy equals the VBM, which makes this function predict a zero
       band gap.  Prefer :func:`detect_bandgap_from_nscf` or
       :func:`detect_bandgap_from_bands` instead.

    :param bands: numpy array of shape ``(nkpts, nbands)``.
    :param fermi_energy: Fermi energy (float).
    :return: tuple ``(idx_vbm, vbm, idx_cbm, cbm)``.
    """
    occupied_mask = bands <= fermi_energy

    if not np.any(occupied_mask):
        raise ValueError("No occupied bands found, all bands are above Fermi energy")

    bands_occupied = np.where(occupied_mask, bands, np.min(bands) - 1000)
    idx_vbm = np.unravel_index(np.argmax(bands_occupied), bands_occupied.shape)
    vbm = bands[idx_vbm]

    unoccupied_mask = bands > fermi_energy

    if not np.any(unoccupied_mask):
        raise ValueError("No unoccupied bands found, all bands are below Fermi energy")

    bands_unoccupied = np.where(unoccupied_mask, bands, np.max(bands) + 1000)
    idx_cbm = np.unravel_index(np.argmin(bands_unoccupied), bands_unoccupied.shape)
    cbm = bands[idx_cbm]

    return idx_vbm, vbm, idx_cbm, cbm


@calcfunction
def extract_band_edges(
    bands_data: orm.BandsData, fermi_energy: orm.Float = None
) -> orm.Dict:
    """Extract CBM and VBM from BandsData using fermi_energy as reference.

    .. warning::

       For insulators with ``occupations='fixed'`` the Fermi energy equals
       VBM, making the detected gap ~0.  Use
       :func:`extract_band_edges_from_epw_prep` (electron-count method)
       instead when possible.
    """
    bands = bands_data.get_bands()
    kpoints = bands_data.get_kpoints()

    if fermi_energy is None:
        result = detect_bandgap_largest_gap(bands)
        if result is None:
            return orm.Dict(
                {"vbm": None, "cbm": None, "band_gap": None, "fermi_energy": None}
            )
        return orm.Dict(
            {
                "vbm": result["vbm"],
                "cbm": result["cbm"],
                "band_gap": result["bandgap"],
                "fermi_energy": None,
            }
        )

    try:
        idx_vbm, vbm, idx_cbm, cbm = get_vbm_cbm(bands, fermi_energy.value)
        band_gap = float(cbm - vbm)

        vbm_kpoint = kpoints[idx_vbm[0]].tolist() if idx_vbm[0] < len(kpoints) else None
        cbm_kpoint = kpoints[idx_cbm[0]].tolist() if idx_cbm[0] < len(kpoints) else None

        result = {
            "vbm": float(vbm),
            "cbm": float(cbm),
            "band_gap": band_gap,
            "vbm_kpoint": vbm_kpoint,
            "cbm_kpoint": cbm_kpoint,
            "fermi_energy": float(fermi_energy.value),
        }
    except ValueError as e:
        gap_result = detect_bandgap_largest_gap(bands)
        if gap_result is not None:
            result = {
                "vbm": gap_result["vbm"],
                "cbm": gap_result["cbm"],
                "band_gap": gap_result["bandgap"],
                "fermi_energy": float(fermi_energy.value),
                "warning": str(e),
            }
        else:
            result = {
                "vbm": None,
                "cbm": None,
                "band_gap": None,
                "fermi_energy": float(fermi_energy.value),
                "warning": str(e),
            }

    return orm.Dict(result)


@calcfunction
def set_fermi_energy_from_band_edges(
    band_edges: orm.Dict, carrier_type: orm.Str
) -> orm.Dict:
    """Set fermi energy based on carrier type and band edges.

    :param band_edges: Dict containing CBM, VBM, and other band edge information.
    :param carrier_type: ``'ele'`` for electrons, ``'hole'`` for holes.
    :return: Dict with updated fermi_energy value.
    """
    band_edges_dict = band_edges.get_dict()
    carrier_type_str = carrier_type.value.lower()

    vbm = band_edges_dict.get("vbm")
    cbm = band_edges_dict.get("cbm")

    if carrier_type_str == "ele":
        fermi_energy = cbm + 0.01
    elif carrier_type_str == "hole":
        fermi_energy = vbm - 0.01
    else:
        fermi_energy = (vbm + cbm) / 2.0

    return orm.Dict(
        {
            "fermi_energy": float(fermi_energy),
            "carrier_type": carrier_type_str,
            "vbm": vbm,
            "cbm": cbm,
            "band_gap": band_edges_dict.get("band_gap"),
        }
    )


def parse_vbm_cbm_from_epw_output(stdout_content):
    """Parse VBM and CBM from EPW stdout output.

    EPW prints the valence band maximum and conduction band minimum
    in the output file.

    :param stdout_content: string content of EPW stdout.
    :return: dict with ``vbm``, ``cbm``, ``band_gap`` (and ``warning``),
        or ``None`` if not found.
    """
    vbm_match = re.search(r"Valence band maximum\s+=\s+([\d\.]+)\s+eV", stdout_content)
    cbm_match = re.search(
        r"Conduction band minimum\s+=\s+([\d\.]+)\s+eV", stdout_content
    )

    if vbm_match and cbm_match:
        vbm = float(vbm_match.group(1))
        cbm = float(cbm_match.group(1))
        band_gap = cbm - vbm

        result = {"vbm": vbm, "cbm": cbm, "band_gap": band_gap}

        if band_gap < 0.08:
            result["warning"] = (
                f"Band gap ({band_gap:.4f} eV) is very small. "
                "The Fermi level may be crossing the top of valence band "
                "or bottom of conduction band. Verify the band edges are correct."
            )
        return result

    return None
