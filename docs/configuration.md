# Configuration

## Environment Variables

Environment variables are used to control some aspects of the model.
The default values for these environment variables are generally suitable,
but if you require updating these values we recommend the use of a `.env` file
to make the changes easier to reproduce in future.

### `REF_EXECUTOR`

Executor to use for running the diagnostics.

Defaults to use the local executor ("climate_ref.executor.LocalExecutor")
which runs the executions locally, in-parallel using a process pool.


### `REF_RESULTS_ROOT`

Path to the root directory where data should be stored.
This has to be shared between any workers and the parent
process.


### `REF_DATASET_CACHE_DIR`

Path where any datasets that are fetched via the `ref datasets fetch-data` command are stored.
This is used to cache the datasets so that they are not downloaded multiple times.
It is not recommended to ingest datasets from this directory (see `--output-dir` argument for `ref datasets fetch-data`).
