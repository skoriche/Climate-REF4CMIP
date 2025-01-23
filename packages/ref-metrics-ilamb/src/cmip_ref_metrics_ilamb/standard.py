import importlib

import pandas as pd
import pooch
import xarray as xr
from ilamb3.analysis import bias_analysis  # type: ignore

from cmip_ref_core.datasets import DatasetCollection, FacetFilter, SourceDatasetType
from cmip_ref_core.metrics import (
    DataRequirement,
    Metric,
    MetricExecutionDefinition,
    MetricResult,
)

_ILAMB_DATA_VERSION = "0.1"  # we don't really have data versions for the collection :/


def _build_ilamb_data_registry(version: str) -> pooch.Pooch:
    registry = pooch.create(
        path=pooch.os_cache("cmip_ref_metrics_ilamb"),
        base_url="https://www.ilamb.org/ILAMB-Data/DATA",
        version=version,
        env="REF_METRICS_ILAMB_DATA_DIR",
    )
    registry.load_registry(importlib.resources.open_binary("cmip_ref_metrics_ilamb", "registry.txt"))
    return registry


def _build_ilamb_collection(version: str) -> DatasetCollection:
    ilamb_registry = _build_ilamb_data_registry(version)
    df = pd.DataFrame(
        [
            {
                "variable_id": key.split("/")[0],
                "source_id": key.split("/")[1].replace(".nc", ""),
                "path": ilamb_registry.fetch(key),
            }
            for key in ilamb_registry.registry.keys()
        ]
    )
    df["instance_id"] = df["variable_id"] + "_" + df["source_id"]
    collection = DatasetCollection(df, "instance_id")
    return collection


class ILAMBStandard(Metric):
    """
    Apply the standard ILAMB analysis with respect to a given reference dataset.
    """

    ilamb_data: DatasetCollection = _build_ilamb_collection(_ILAMB_DATA_VERSION)

    def __init__(self, variable_id: str, collection_key: str):
        # Programatically setup the metric
        self.variable_id = variable_id
        self.collection_key = collection_key
        self.name = f"ILAMB Standard {collection_key}"
        self.slug = self.name.lower().replace(" ", "_")
        self.data_requirements = (
            DataRequirement(
                source_type=SourceDatasetType.CMIP6,
                filters=(
                    FacetFilter(facets={"variable_id": self.variable_id}),
                    FacetFilter(facets={"frequency": "mon"}),
                    FacetFilter(facets={"experiment_id": ("historical", "land-hist")}),
                ),
                group_by=("experiment_id",),
            ),
        )

        # Check that the collection key exists and associate it with this class instance
        df = self.ilamb_data.datasets.set_index(self.ilamb_data.slug_column)
        self.reference_data = df.loc[collection_key].to_dict()

    def run(self, definition: MetricExecutionDefinition) -> MetricResult:
        """
        Run the ILAMB standard analysis.

        Note
        ----
        For the moment, this is implemented at a coarser granularity than
        ideal. For a given `experiment_id`, the group will pass in all
        (`source_id`, `member_id`, `grid_label`)s which have the target
        `variable_id`. This means that if any of those models change, the whole
        metric will have to recompute.

        Instead, it would be better if, as an output, a Metric could insert into
        a DataCollection which another Metric could depend on. I could then take
        the below and separate it out:

        MetricPhase1( model1, reference_data ) --> intermediate1
        MetricPhase1( model2, reference_data ) --> intermediate2
        MetricPhase1( model3, reference_data ) --> intermediate3

        MetricPhase2( intermediate1,intermediate2,intermediate3 ) --> result

        """
        reference_dataset = xr.open_dataset(self.reference_data["path"])
        analysis = bias_analysis(self.variable_id)

        # Phase 1: loop over each model in the group and run an analysis function
        df = []
        ds_com = {}
        for _, row in definition.metric_dataset[SourceDatasetType.CMIP6].datasets.iterrows():
            # Define what we will call the output artifacts
            source_name = "{source_id}_{member_id}_{grid_label}".format(**row.to_dict())
            csv_file = definition.to_output_path(f"{source_name}.csv")
            ref_file = definition.to_output_path("Reference.nc")
            com_file = definition.to_output_path(f"{source_name}.nc")

            # Run the analysis on the data/model pair
            com = xr.open_mfdataset(row["path"])
            df_scalars, ds_ref, ds_com[source_name] = analysis(reference_dataset, com)
            ds_ref = ds_ref.pint.dequantify()  # FIX this in ilamb3
            ds_com[source_name] = ds_com[source_name].pint.dequantify()
            df_scalars["source"] = df_scalars["source"].str.replace("Comparison", source_name)
            df += [df_scalars]

            # Write out the artifacts to the output directories
            df_scalars.to_csv(csv_file, index=False)
            if not ref_file.is_file():
                ds_ref.to_netcdf(ref_file)
            ds_com[source_name].to_netcdf(com_file)

        # Phase 2:
        return MetricResult.build_from_output_bundle(definition, {})
