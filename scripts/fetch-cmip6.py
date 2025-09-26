"""
Basic script for fetching a single ensemble member for each of the diagnostics of interest

This fetches about 660GB of datasets into the default location for intake esgf.
"""

import intake_esgf
import typer
from attrs import define


@define
class CMIP6Request:
    """
    A set of CMIP6 data that will be fetched from ESGF
    """

    id: str
    facets: dict[str, str | tuple[str, ...] | list[str]]

    def fetch(self, remove_ensembles: bool = True):
        """
        Fetch CMIP6 data from the ESGF catalog and return it as a DataFrame.

        Parameters
        ----------
        remove_ensembles : bool, default True
            Whether to remove ensemble members, keeping only one per model.
            If False, all ensemble members will be included.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the metadata for the CMIP6 datasets.
        """
        catalog = intake_esgf.ESGFCatalog()
        try:
            cmip6_data = catalog.search(
                project="CMIP6",
                frequency=["mon", "fx"],
                **self.facets,
            )
            if remove_ensembles:
                cmip6_data = cmip6_data.remove_ensembles()
            return cmip6_data.to_path_dict()
        except Exception as e:
            print(e)
        return []


@define
class Obs4MIPsRequest:
    """
    A set of Obs4MIPs data that will be fetched from ESGF
    """

    id: str
    facets: dict[str, str | tuple[str, ...] | list[str]]

    def fetch(self):
        """
        Fetch Obs4MIPs data from the ESGF catalog and return it as a DataFrame.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the metadata for the CMIP6 datasets.
        """
        catalog = intake_esgf.ESGFCatalog()
        try:
            obs_data = catalog.search(
                project="obs4MIPs",
                frequency="mon",
                **self.facets,
            )
            return obs_data.to_path_dict()
        except Exception as e:
            print(e)
        return []


requests = [
    CMIP6Request(
        id="esmvaltool-climate-at-global-warmings-levels",
        facets=dict(
            variable_id=["pr", "tas"],
            experiment_id=["ssp126", "ssp245", "ssp370", "ssp585", "historical"],
        ),
    ),
    # ESMValTool Cloud radiative effects
    CMIP6Request(
        id="esmvaltool-cloud-radiative-effects",
        facets=dict(
            variable_id=["rlut", "rlutcs", "rsut", "rsutcs"],
            experiment_id="historical",
        ),
    ),
    # ESMValTool cloud scatterplots
    CMIP6Request(
        id="esmvaltool-cloud-scatterplots-cmip6",
        facets=dict(
            variable_id=[
                "areacella",
                "cli",
                "clivi",
                "clt",
                "clwvi",
                "pr",
                "rlut",
                "rlutcs",
                "rsut",
                "rsutcs",
                "ta",
            ],
            experiment_id="historical",
        ),
    ),
    Obs4MIPsRequest(
        id="esmvaltool-cloud-scatterplots-obs4mips",
        facets=dict(
            source_id="ERA-5",
            variable_id="ta",
        ),
    ),
    # ESMValTool ECS data
    CMIP6Request(
        id="esmvaltool-ecs",
        facets=dict(
            variable_id=["rlut", "rsdt", "rsut", "tas"],
            experiment_id=["abrupt-4xCO2", "piControl"],
        ),
    ),
    # ESMValTool ENSO data
    CMIP6Request(
        id="esmvaltool-enso",
        facets=dict(
            variable_id=[
                "pr",
                "tos",
                "tauu",
            ],
            experiment_id=["historical"],
        ),
    ),
    # ESMValTool fire data
    CMIP6Request(
        id="esmvaltool-fire",
        facets=dict(
            variable_id=[
                "cVeg",
                "hurs",
                "pr",
                "sftlf",
                "tas",
                "tasmax",
                "treeFrac",
                "vegFrac",
            ],
            experiment_id=["historical"],
        ),
    ),
    # ESMValTool Historical data
    CMIP6Request(
        id="esmvaltool-historical-cmip6",
        facets=dict(
            variable_id=[
                "hus",
                "pr",
                "psl",
                "tas",
                "ua",
            ],
            experiment_id=["historical"],
        ),
    ),
    Obs4MIPsRequest(
        id="esmvaltool-historical-obs4mips",
        facets=dict(
            source_id="ERA-5",
            variable_id=[
                "psl",
                "tas",
                "ua",
            ],
        ),
    ),
    # ESMValTool TCR data
    CMIP6Request(
        id="esmvaltool-tcr",
        facets=dict(
            variable_id=["tas"],
            experiment_id=["1pctCO2", "piControl"],
        ),
    ),
    # ESMValTool TCRE data
    CMIP6Request(
        id="esmvaltool-tcre",
        facets=dict(
            variable_id=["fco2antt", "tas"],
            experiment_id=["esm-1pctCO2", "esm-piControl"],
        ),
    ),
    # ESMValTool ZEC data
    CMIP6Request(
        id="esmvaltool-zec",
        facets=dict(
            variable_id=["areacella", "tas"],
            experiment_id=["1pctCO2", "esm-1pct-brch-1000PgC"],
        ),
    ),
    # ESMValTool Sea Ice Area Seasonal Cycle data
    CMIP6Request(
        id="esmvaltool-sea-ice-area-seasonal-cycle",
        facets=dict(
            variable_id=["areacello", "siconc"],
            experiment_id=["historical"],
        ),
    ),
    # ILAMB data
    CMIP6Request(
        id="ilamb-data",
        facets=dict(
            variable_id=[
                "areacella",
                "sftlf",
                "gpp",
                "pr",
                "tas",
                "mrro",
                "mrsos",
                "cSoil",
                "lai",
                "areacella",
                "burntFractionAll",
                "snc",
                "nbp",
            ],
            experiment_id=["historical"],
        ),
    ),
    # IOMB data
    CMIP6Request(  # Already provided by the ESMValTool ENSO request.
        id="iomb-data",
        facets=dict(
            variable_id=["areacello", "tos"],
            experiment_id=["historical"],
        ),
    ),
    CMIP6Request(
        id="iomb-data-2",
        facets=dict(
            variable_id=["sftof", "sos", "msftmz", "volcello", "thetao", "ohc"],
            experiment_id=["historical"],
        ),
    ),
    # PMP modes of variability data
    CMIP6Request(
        id="pmp-modes-of-variability",
        facets=dict(
            variable_id=["areacella", "ts", "psl"],
            experiment_id=["historical", "hist-GHG"],
        ),
    ),
]


def run_request(request: CMIP6Request, remove_ensembles: bool = True):
    """
    Fetch and log the results of a request
    """
    print(f"Processing request: {request.id}")
    df = request.fetch(remove_ensembles=remove_ensembles)
    print(f"{len(df)} datasets")
    print("\n")


def main(
    request_id: str = typer.Option(
        None, help="ID of a specific request to run. If not provided, all requests will be run."
    ),
    remove_ensembles: bool = typer.Option(
        True,
        help=(
            "Remove ensemble members, keeping only one per model. "
            "Use --no-remove-ensembles to fetch all ensembles."
        ),
    ),
):
    """
    Fetch CMIP6 datasets from ESGF.

    This script can either run all predefined requests or a specific request by ID.
    By default, only one ensemble member per model is fetched, but this can be changed
    with the --no-remove-ensembles flag.
    """
    if request_id:
        # Find and run the specific request
        matching_requests = [req for req in requests if req.id == request_id]
        if not matching_requests:
            print(f"Error: No request found with ID '{request_id}'")
            print("Available request IDs:")
            for req in requests:
                print(f"  - {req.id}")
            raise typer.Exit(1)

        print(f"Running single request: {request_id}")
        if not remove_ensembles:
            print("Fetching all ensemble members")
        run_request(matching_requests[0], remove_ensembles=remove_ensembles)
    else:
        print("Running all requests...")
        if not remove_ensembles:
            print("Fetching all ensemble members")
        for request in requests:
            run_request(request, remove_ensembles=remove_ensembles)


# joblib.Parallel(n_jobs=2)(joblib.delayed(run_request)(request) for request in requests)

if __name__ == "__main__":
    typer.run(main)
