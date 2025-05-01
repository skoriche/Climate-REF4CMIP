"""
Solver to determine which metrics need to be calculated

This module provides a solver to determine which metrics need to be calculated.
"""

import itertools
import pathlib
import typing
from collections.abc import Sequence

import pandas as pd
from attrs import define, frozen
from loguru import logger

from climate_ref.config import Config
from climate_ref.database import Database
from climate_ref.datasets import get_dataset_adapter
from climate_ref.datasets.cmip6 import CMIP6DatasetAdapter
from climate_ref.datasets.obs4mips import Obs4MIPsDatasetAdapter
from climate_ref.datasets.pmp_climatology import PMPClimatologyDatasetAdapter
from climate_ref.models import Diagnostic as MetricModel
from climate_ref.models import ExecutionGroup as MetricExecutionGroupModel
from climate_ref.models import Provider as ProviderModel
from climate_ref.models.execution import Execution
from climate_ref.provider_registry import ProviderRegistry
from climate_ref_core.constraints import apply_constraint
from climate_ref_core.datasets import DatasetCollection, ExecutionDatasetCollection, SourceDatasetType
from climate_ref_core.diagnostics import DataRequirement, Diagnostic, ExecutionDefinition
from climate_ref_core.exceptions import InvalidDiagnosticException
from climate_ref_core.providers import DiagnosticProvider

SelectorKey = tuple[tuple[str, str], ...]
"""
Type describing the key used to identify a group of datasets

This is a tuple of tuples, where each inner tuple contains a metadata and dimension value
that was used to group the datasets together.

This SelectorKey type must be hashable, as it is used as a key in a dictionary.
"""


@frozen
class DiagnosticExecution:
    """
    Class to hold information about the execution of a diagnostic
    """

    provider: DiagnosticProvider
    metric: Diagnostic
    datasets: ExecutionDatasetCollection

    @property
    def dataset_key(self) -> str:
        """
        Key used to uniquely identify the execution group

        This key is unique to an execution group and uses unique set of metadata (selectors)
         that defines the group.
        This key is combines the selectors from each source dataset type into a single key
        and should be stable if new datasets are added or removed.
        """
        key_values = []

        for source_type in SourceDatasetType.ordered():
            # Ensure the selector is sorted using the dimension names
            # This will ensure a stable key even if the groupby order changes
            if source_type not in self.datasets:
                continue

            selector = self.datasets[source_type].selector
            selector_sorted = sorted(selector, key=lambda item: item[0])

            source_key = f"{source_type.value}_" + "_".join(value for _, value in selector_sorted)
            key_values.append(source_key)

        return "__".join(key_values)

    @property
    def selectors(self) -> dict[str, SelectorKey]:
        """
        Collection of selectors used to identify the datasets

        These are the key, value pairs that were selected during the initial group-by,
        for each data requirement.
        """
        # The "value" of SourceType is used here so this can be stored in the db
        s = {}
        for source_type in SourceDatasetType.ordered():
            if source_type not in self.datasets:
                continue
            s[source_type.value] = self.datasets[source_type].selector
        return s

    def build_execution_definition(self, output_root: pathlib.Path) -> ExecutionDefinition:
        """
        Build the diagnostic execution definition for the current diagnostic execution
        """
        # Ensure that the output root is always an absolute path
        output_root = output_root.resolve()

        # This is the desired path relative to the output directory
        fragment = pathlib.Path() / self.provider.slug / self.metric.slug / self.datasets.hash

        return ExecutionDefinition(
            root_directory=output_root,
            output_directory=output_root / fragment,
            dataset_key=self.dataset_key,
            metric_dataset=self.datasets,
        )


def extract_covered_datasets(
    data_catalog: pd.DataFrame, requirement: DataRequirement
) -> dict[SelectorKey, pd.DataFrame]:
    """
    Determine the different diagnostic executions that should be performed with the current data catalog
    """
    if len(data_catalog) == 0:
        logger.error(f"No datasets found in the data catalog: {requirement.source_type.value}")
        return {}

    subset = requirement.apply_filters(data_catalog)

    if len(subset) == 0:
        logger.debug(f"No datasets found for requirement {requirement}")
        return {}

    if requirement.group_by is None:
        # Use a single group
        groups = [((), subset)]
    else:
        groups = list(subset.groupby(list(requirement.group_by)))

    results = {}

    for name, group in groups:
        if requirement.group_by is None:
            assert len(groups) == 1  # noqa: S101
            group_keys: SelectorKey = ()
        else:
            group_keys = tuple(zip(requirement.group_by, name))
        constrained_group = _process_group_constraints(data_catalog, group, requirement)

        if constrained_group is not None:
            results[group_keys] = constrained_group

    return results


def _process_group_constraints(
    data_catalog: pd.DataFrame, group: pd.DataFrame, requirement: DataRequirement
) -> pd.DataFrame | None:
    for constraint in requirement.constraints or []:
        constrained_group = apply_constraint(group, constraint, data_catalog)
        if constrained_group is None:
            return None

        group = constrained_group
    return group


def solve_executions(
    data_catalog: dict[SourceDatasetType, pd.DataFrame], metric: Diagnostic, provider: DiagnosticProvider
) -> typing.Generator["DiagnosticExecution", None, None]:
    """
    Calculate the diagnostic executions that need to be performed for a given diagnostic

    Parameters
    ----------
    data_catalog
        Data catalogs for each source dataset type
    metric
        Diagnostic of interest
    provider
        Provider of the diagnostic

    Returns
    -------
    :
        A generator that yields the diagnostic executions that need to be performed

    """
    if not metric.data_requirements:
        raise ValueError(f"Diagnostic {metric.slug} has no data requirements")

    first_item = next(iter(metric.data_requirements))

    if isinstance(first_item, DataRequirement):
        # We have a single collection of data requirements
        yield from _solve_from_data_requirements(
            data_catalog,
            metric,
            typing.cast(Sequence[DataRequirement], metric.data_requirements),
            provider,
        )
    elif isinstance(first_item, Sequence):
        # We have a sequence of collections of data requirements
        for requirement_collection in metric.data_requirements:
            if not isinstance(requirement_collection, Sequence):
                raise TypeError(f"Expected a sequence of DataRequirement, got {type(requirement_collection)}")
            yield from _solve_from_data_requirements(data_catalog, metric, requirement_collection, provider)
    else:
        raise TypeError(f"Expected a DataRequirement, got {type(first_item)}")


def _solve_from_data_requirements(
    data_catalog: dict[SourceDatasetType, pd.DataFrame],
    metric: Diagnostic,
    data_requirements: Sequence[DataRequirement],
    provider: DiagnosticProvider,
) -> typing.Generator["DiagnosticExecution", None, None]:
    # Collect up the different data groups that can be used to calculate the diagnostic
    dataset_groups = {}

    for requirement in data_requirements:
        if not isinstance(requirement, DataRequirement):
            raise TypeError(f"Expected a DataRequirement, got {type(requirement)}")
        if requirement.source_type not in data_catalog:
            raise InvalidDiagnosticException(
                metric, f"No data catalog for source type {requirement.source_type}"
            )

        dataset_groups[requirement.source_type] = extract_covered_datasets(
            data_catalog[requirement.source_type], requirement
        )

    # Calculate the product across each of the source types
    for items in itertools.product(*dataset_groups.values()):
        yield DiagnosticExecution(
            provider=provider,
            metric=metric,
            datasets=ExecutionDatasetCollection(
                {
                    key: DatasetCollection(
                        datasets=dataset_groups[key][dataset_group_key],
                        slug_column=get_dataset_adapter(key.value).slug_column,
                        selector=dataset_group_key,
                    )
                    for key, dataset_group_key in zip(dataset_groups.keys(), items)
                }
            ),
        )


@define
class MetricSolver:
    """
    A solver to determine which metrics need to be calculated
    """

    provider_registry: ProviderRegistry
    data_catalog: dict[SourceDatasetType, pd.DataFrame]

    @staticmethod
    def build_from_db(config: Config, db: Database) -> "MetricSolver":
        """
        Initialise the solver using information from the database

        Parameters
        ----------
        db
            Database instance

        Returns
        -------
        :
            A new MetricSolver instance
        """
        return MetricSolver(
            provider_registry=ProviderRegistry.build_from_config(config, db),
            data_catalog={
                SourceDatasetType.CMIP6: CMIP6DatasetAdapter().load_catalog(db),
                SourceDatasetType.obs4MIPs: Obs4MIPsDatasetAdapter().load_catalog(db),
                SourceDatasetType.PMPClimatology: PMPClimatologyDatasetAdapter().load_catalog(db),
            },
        )

    def solve(self) -> typing.Generator[DiagnosticExecution, None, None]:
        """
        Solve which metrics need to be calculated for a dataset

        The solving scheme is iterative,
        for each iteration we find all metrics that can be solved and calculate them.
        After each iteration we check if there are any more metrics to solve.
        This may not be the most efficient way to solve the metrics, but it's a start.

        Yields
        ------
        DiagnosticExecution
            A class containing the information related to the execution of a diagnostic
        """
        for provider in self.provider_registry.providers:
            for metric in provider.metrics():
                yield from solve_executions(self.data_catalog, metric, provider)


def solve_metrics(
    db: Database,
    dry_run: bool = False,
    solver: MetricSolver | None = None,
    config: Config | None = None,
    timeout: int = 60,
) -> None:
    """
    Solve for metrics that require recalculation

    This may trigger a number of additional calculations depending on what data has been ingested
    since the last solve.

    Raises
    ------
    TimeoutError
        If the execution isn't completed within the specified timeout
    """
    if config is None:
        config = Config.default()
    if solver is None:
        solver = MetricSolver.build_from_db(config, db)

    logger.info("Solving for metrics that require recalculation...")

    executor = config.executor.build(config, db)

    for metric_execution in solver.solve():
        # The diagnostic output is first written to the scratch directory
        definition = metric_execution.build_execution_definition(output_root=config.paths.scratch)

        logger.debug(f"Identified candidate diagnostic execution {definition.dataset_key}")

        if dry_run:
            continue

        # Use a transaction to make sure that the models
        # are created correctly before potentially executing out of process
        with db.session.begin(nested=True):
            metric = (
                db.session.query(MetricModel)
                .join(MetricModel.provider)
                .filter(
                    ProviderModel.slug == metric_execution.provider.slug,
                    ProviderModel.version == metric_execution.provider.version,
                    MetricModel.slug == metric_execution.metric.slug,
                )
                .one()
            )
            metric_execution_group_model, created = db.get_or_create(
                MetricExecutionGroupModel,
                dataset_key=definition.dataset_key,
                metric_id=metric.id,
                defaults={
                    "selectors": metric_execution.selectors,
                    "dirty": True,
                },
            )

            if created:
                logger.info(f"Created diagnostic execution {definition.dataset_key}")
                db.session.flush()

            if metric_execution_group_model.should_run(definition.metric_dataset.hash):
                logger.info(
                    f"Running diagnostic "
                    f"{metric_execution.metric.slug}-{metric_execution_group_model.dataset_key}"
                )
                metric_execution_result = Execution(
                    metric_execution_group=metric_execution_group_model,
                    dataset_hash=definition.metric_dataset.hash,
                    output_fragment=str(definition.output_fragment()),
                )
                db.session.add(metric_execution_result)
                db.session.flush()

                # Add links to the datasets used in the execution
                metric_execution_result.register_datasets(db, definition.metric_dataset)

                executor.run(
                    provider=metric_execution.provider,
                    diagnostic=metric_execution.metric,
                    definition=definition,
                    execution=metric_execution_result,
                )
    if timeout > 0:
        executor.join(timeout=timeout)
