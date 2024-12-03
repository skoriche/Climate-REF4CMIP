# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
#
# # Testing metric providers locally
# Metric providers can be run locally without requiring the rest of the REF infrastructure.
# This is useful for testing and debugging metrics.

# Running a metric locally requires that the target REF metrics package, e.g. `ref-metrics-example`,
# and its dependencies are installed in the current Python environment.
#
# This guide will walk you through how to run a metric provider locally.

# %% tags=["remove_input"]
import json
from pathlib import Path

import ref_metrics_example
from ref_core.datasets import MetricDataset
from ref_core.executor import run_metric
from ref_core.metrics import MetricExecutionDefinition

from ref.cli.config import load_config
from ref.database import Database
from ref.datasets import get_dataset_adapter

# %%
provider = ref_metrics_example.provider
provider

# %% tags=["hide_code"]
config = load_config()
db = Database.from_config(config)

# %%
# Load the data catalog
data_catalog = get_dataset_adapter("cmip6").load_catalog(db)

# %% [markdown]
# Each metric execution requires a `MetricExecutionDefinition` object.
# This object contains the information about where data should be stored
# and which datasets should be used for the metric calculation.
#
# This object is created by hand,
# but in the REF a `Solver` is used to determine the executions that are required
# given a set of requirements and the currently available dataset.

# %%
definition = MetricExecutionDefinition(
    slug="global_mean_timeseries",
    output_fragment=Path("global_mean_timeseries"),
    metric_dataset=MetricDataset({"cmip6": ["tas_Amon"]}),
)

# %% [markdown]
# ## Metric calculations
#
# Metric calculations are typically run using an [Executor](ref_core.executor.Executor)
# which provides an abstraction to enable metrics to be run in multiple different ways.
#
# The simplest executor is the `LocalExecutor`.
# This executor runs a given metric synchronously in the current process.
#
# The `LocalExecutor` is the default executor when using the  `ref_core.executor.run_metric` function.
# This can be overridden by specifying the `REF_EXECUTOR` environment variable.

# %%
result = run_metric("global_mean_timeseries", provider, definition=definition)
result

# %%
with open(result.output_bundle) as fh:
    # Load the output bundle and pretty print
    loaded_result = json.loads(fh.read())
    print(json.dumps(loaded_result, indent=2))

# %% [markdown]
# ### Running directly
#
# The local executor can be bypassed if you need access to running the metric calculation directly.
# This will not perform and validation/verification of the output results.

# %%
metric = provider.get("global_mean_timeseries")

direct_result = metric.run(definition=definition)
assert direct_result.successful

direct_result
