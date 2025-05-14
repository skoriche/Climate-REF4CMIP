# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
#
# # Testing diagnostic providers locally
# Diagnostic providers can be run locally without requiring the rest of the REF infrastructure.
# This is useful for testing and debugging diagnostics.
#
# Running a diagnostic locally requires that the target REF diagnostics package, e.g. `climate_ref_example`,
# and its dependencies are installed in the current Python environment.
#
# This guide will walk you through how to run a diagnostic provider locally.


# %% tags=["remove_input"]
import json
from pathlib import Path

import climate_ref_example
import pandas as pd
import prettyprinter

from climate_ref.config import Config
from climate_ref.database import Database
from climate_ref.datasets import get_dataset_adapter
from climate_ref.solver import solve_executions
from climate_ref_core.datasets import SourceDatasetType

prettyprinter.install_extras(["attrs"])

# %%
provider = climate_ref_example.provider
provider

# %% [markdown]
# We select a diagnostic which simply calculates the annual mean, global mean timeseries of a dataset.
#
# The data requirements of this diagnostic filter out all variables except `tas` and `rsut`.
# The `group_by` specification ensures that each execution has a unique combination of
# `source_id`, `variant_id`, `variable_id` and `experiment_id` values.

# %%
diagnostic = provider.get("global-mean-timeseries")

# %%
prettyprinter.pprint(diagnostic.data_requirements[0])

# %% tags=["hide_code"]
config = Config.default()
provider.configure(config)
db = Database.from_config(config)

# %% [markdown]
# Load the data catalog containing the CMIP6 datasets.
# This contains the datasets that have been ingested into the REF database.
# You could also use the `find_local_datasets` function to find local datasets on disk,
# thereby bypassing the need for a database.

# %%
# Load the data catalog containing the
data_catalog = get_dataset_adapter("cmip6").load_catalog(db)
data_catalog.head()

# %% [markdown]
# Below, the unique combinations of the metadata values that apply to the groupby are shown:

# %%
data_catalog[["source_id", "variant_label", "variable_id", "experiment_id"]].drop_duplicates()

# %% [markdown]
#
# ## Executions
#
# An execution is a combination of a diagnostic, a provider, and a group of datasets.
#
# The `DiagnosticSolver` is used to determine which executions are required given a set of requirements
# and the currently available datasets.
# This doesn't require the use of the REF database.

# %%
executions_generator = solve_executions(
    data_catalog={
        SourceDatasetType.CMIP6: data_catalog,
    },
    diagnostic=diagnostic,
    provider=provider,
)

# Convert from a generator to a list to inspect the complete set of executions
executions = list(executions_generator)
prettyprinter.pprint(executions)

# %% [markdown]
# We get multiple proposed executions.

# %%
pd.concat(execution.datasets["cmip6"] for execution in executions)[
    ["experiment_id", "variable_id"]
].drop_duplicates()

# %% [markdown]
# Each execution contains a single unique dataset because of the groupby definition.
# The data catalog for the diagnostic execution may contain more than one row
# as a dataset may contain multiple files.

# %%
executions[0].datasets["cmip6"].instance_id.unique().tolist()

# %%
executions[0].datasets["cmip6"]

# %% [markdown]
#
# ## Diagnostic Definitions
#
# Each diagnostic execution requires a `ExecutionDefinition` object.
# This object contains the information about where data should be stored
# and which datasets should be used for the diagnostic calculation.

# %%
output_directory = Path("../out")
definition = executions[0].build_execution_definition(output_directory)
prettyprinter.pprint(definition)


# %% [markdown]
# ### Running directly locally
#
# A diagnostic can be run directly if you want to run a calculation synchronously
# without any additional infrastructure.
#
# This will not perform and validation/verification of the output executions.

# %%
definition.output_directory.mkdir(parents=True, exist_ok=True)
direct_result = diagnostic.run(definition=definition)
assert direct_result.successful

prettyprinter.pprint(direct_result)

# %% [markdown]
# ## Diagnostic calculations
#
# Diagnostic calculations are typically run using an
# [Executor](../../api/climate_ref_core/executor/#climate_ref_core.executor.Executor)
# which provides an abstraction to enable diagnostics to be run in multiple different ways.
# These executors can run diagnostics locally, on a cluster, or on a remote service
#
# The default executor is the
# [LocalExecutor][climate_ref.executor.LocalExecutor]
# This executor runs executions in parallel using a process pool.
# Another option is the [SynchronousExecutor][climate_ref.executor.SynchronousExecutor].
# This executor runs the execution in the main process which can be useful for debugging purposes.
#
# The executor can be specified in the configuration, or
# using the `REF_EXECUTOR` environment variable which takes precedence.
# The [LocalExecutor][climate_ref.executor.LocalExecutor] is the default executor,
# if no other configuration is provided.

# %%
executor = config.executor.build(config=config, database=db)
diagnostic = provider.get("global-mean-timeseries")

executor.run(definition)
executor.join(timeout=30)

# %%
output_file = definition.to_output_path("output.json")
with open(output_file) as fh:
    # Load the output bundle and pretty print
    loaded_result = json.loads(fh.read())
    print(json.dumps(loaded_result, indent=2))


# %%
