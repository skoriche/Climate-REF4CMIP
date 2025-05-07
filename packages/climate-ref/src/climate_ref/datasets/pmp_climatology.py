from __future__ import annotations

from climate_ref.datasets.obs4mips import Obs4MIPsDatasetAdapter
from climate_ref.models.dataset import PMPClimatologyDataset


class PMPClimatologyDatasetAdapter(Obs4MIPsDatasetAdapter):
    """
    Adapter for climatology datasets post-processed from obs4MIPs datasets by PMP.

    These data look like obs4MIPs datasets and are ingested in the same way, but
    are treated separately as they may have the same metadata as the obs4MIPs datasets.
    """

    dataset_cls = PMPClimatologyDataset
