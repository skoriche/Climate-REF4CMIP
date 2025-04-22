"""
Solver to determine which metrics need to be calculated

This module provides a solver to determine which metrics need to be calculated.
"""

import itertools
import pathlib
import typing

import pandas as pd
from attrs import define, frozen
from loguru import logger

from cmip_ref.config import Config
from cmip_ref.database import Database
from cmip_ref.datasets import get_dataset_adapter
from cmip_ref.datasets.cmip6 import CMIP6DatasetAdapter
from cmip_ref.datasets.obs4mips import Obs4MIPsDatasetAdapter
from cmip_ref.datasets.pmp_climatology import PMPClimatologyDatasetAdapter
from cmip_ref.models import Metric as MetricModel
from cmip_ref.models import MetricExecutionGroup as MetricExecutionGroupModel
from cmip_ref.models import Provider as ProviderModel
from cmip_ref.models.metric_execution import MetricExecutionResult
from cmip_ref.provider_registry import ProviderRegistry
from cmip_ref_core.constraints import apply_constraint
from cmip_ref_core.datasets import DatasetCollection, MetricDataset, SourceDatasetType
from cmip_ref_core.exceptions import InvalidMetricException
from cmip_ref_core.metrics import DataRequirement, Metric, MetricExecutionDefinition
from cmip_ref_core.providers import MetricsProvider

SelectorKey = tuple[tuple[str, str], ...]
"""
Type describing the key used to identify a group of datasets

This is a tuple of tuples, where each inner tuple contains a metadata and dimension value
that was used to group the datasets together.

This SelectorKey type must be hashable, as it is used as a key in a dictionary.
"""


@frozen
class MetricExecution:
    """
    Class to hold information about the execution of a metric
    """

    provider: MetricsProvider
    metric: Metric
    metric_dataset: MetricDataset

    def _source_type_order(self) -> list[SourceDatasetType]:
        source_types = [requirement.source_type for requirement in self.metric.data_requirements]
        return sorted(source_types, key=lambda x: x.value)

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

        source_type_order = self._source_type_order()
        for source_type in source_type_order:
            # Ensure the selector is sorted using the dimension names
            # This will ensure a stable key even if the groupby order changes
            selector = self.metric_dataset[source_type].selector
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
        source_type_order = self._source_type_order()

        # The value of SourceType is used here so that this
        # result can be stored in the db
        return {
            source_type.value: self.metric_dataset[source_type].selector for source_type in source_type_order
        }

    def build_metric_execution_info(self, output_root: pathlib.Path) -> MetricExecutionDefinition:
        """
        Build the metric execution info for the current metric execution
        """
        # Ensure that the output root is always an absolute path
        output_root = output_root.resolve()

        # This is the desired path relative to the output directory
        fragment = pathlib.Path() / self.provider.slug / self.metric.slug / self.metric_dataset.hash

        return MetricExecutionDefinition(
            root_directory=output_root,
            output_directory=output_root / fragment,
            dataset_key=self.dataset_key,
            metric_dataset=self.metric_dataset,
        )


def extract_covered_datasets(
    data_catalog: pd.DataFrame, requirement: DataRequirement
) -> dict[SelectorKey, pd.DataFrame]:
    """
    Determine the different metric executions that should be performed with the current data catalog
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


def solve_metric_executions(
    data_catalog: dict[SourceDatasetType, pd.DataFrame], metric: Metric, provider: MetricsProvider
) -> typing.Generator["MetricExecution", None, None]:
    """
    Calculate the metric executions that need to be performed for a given metric

    Parameters
    ----------
    data_catalog
        Data catalogs for each source dataset type
    metric
        Metric of interest
    provider
        Provider of the metric

    Returns
    -------
    :
        A generator that yields the metric executions that need to be performed

    """
    # Collect up the different data groups that can be used to calculate the metric
    dataset_groups = {}

    for requirement in metric.data_requirements:
        if requirement.source_type not in data_catalog:
            raise InvalidMetricException(metric, f"No data catalog for source type {requirement.source_type}")

        dataset_groups[requirement.source_type] = extract_covered_datasets(
            data_catalog[requirement.source_type], requirement
        )

    # Calculate the product across each of the source types
    for items in itertools.product(*dataset_groups.values()):
        yield MetricExecution(
            provider=provider,
            metric=metric,
            metric_dataset=MetricDataset(
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

    def solve(self) -> typing.Generator[MetricExecution, None, None]:
        """
        Solve which metrics need to be calculated for a dataset

        The solving scheme is iterative,
        for each iteration we find all metrics that can be solved and calculate them.
        After each iteration we check if there are any more metrics to solve.
        This may not be the most efficient way to solve the metrics, but it's a start.

        Yields
        ------
        MetricExecution
            A class containing the information related to the execution of a metric
        """
        for provider in self.provider_registry.providers:
            for metric in provider.metrics():
                yield from solve_metric_executions(self.data_catalog, metric, provider)


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
        # The metric output is first written to the scratch directory
        definition = metric_execution.build_metric_execution_info(output_root=config.paths.scratch)

        logger.debug(f"Identified candidate metric execution {definition.dataset_key}")

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
                logger.info(f"Created metric execution {definition.dataset_key}")
                db.session.flush()

            if metric_execution_group_model.should_run(definition.metric_dataset.hash):
                logger.info(
                    f"Running metric "
                    f"{metric_execution.metric.slug}-{metric_execution_group_model.dataset_key}"
                )
                metric_execution_result = MetricExecutionResult(
                    metric_execution_group=metric_execution_group_model,
                    dataset_hash=definition.metric_dataset.hash,
                    output_fragment=str(definition.output_fragment()),
                )
                db.session.add(metric_execution_result)
                db.session.flush()

                # Add links to the datasets used in the execution
                metric_execution_result.register_datasets(db, definition.metric_dataset)

                executor.run_metric(
                    provider=metric_execution.provider,
                    metric=metric_execution.metric,
                    definition=definition,
                    metric_execution_result=metric_execution_result,
                )
    if timeout > 0:
        executor.join(timeout=timeout)
