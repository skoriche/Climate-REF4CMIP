# ⚠️ Package Has Been Renamed! ⚠️

**This package, `cmip_ref_celery`, is deprecated and no longer maintained.**

It has been renamed to **`climate-ref-celery`**.

---

## Please Update Your Dependencies

To continue receiving updates and ensure compatibility, please switch to the new package.

**Reason for rename:**
The rename was necessary to better reflect the purpose and scope of the package.

### How to Switch

1.  **Uninstall the old package:**
    ```bash
    pip uninstall cmip_ref_celery
    ```

2.  **Install the new package:**
    ```bash
    pip install climate-ref-celery
    ```

3.  **Update your code/requirements:**
    *   Change any import statements from `import cmip_ref_celery` to `import climate_ref_celery`.
    *   Update your `requirements.txt`, `pyproject.toml`, `setup.py`, or other dependency management files to list `climate-ref-celery` instead of `cmip_ref_celery`.

---

# ref-celery

This package provides celery task generation from Provider and Metric definitions.

## CLI tool

The `cmip_ref_celery` package provides a CLI tool to start a worker instance from a REF metrics provider.
This worker instance will listen for tasks related to a provider and execute them.
The compute engine worker will then collect the results of these tasks and store them in the database.
This allows for the REF to be run in a distributed manner,
with multiple workers running on different machines with a centrally managed database.

### Usage

For example, to start a worker instance for the `cmip_ref_metrics_example` package:

```bash
ref-celery start-worker --package cmip_ref_metrics_example
```

This requires the `cmip_ref_metrics_example` package to be installed in the current virtual environment.

If the `cmip_ref` package is also installed,
the celery CLI command is available via the `ref` CLI tool.
The equivalent command to the above is:

```bash
ref celery start-worker --package cmip_ref_metrics_example
```

### Configuration

Each worker instance may not share the same configuration as the orchestrator.
This is because the worker may be running on a different machine with different resources available or
directories.

Each worker instance requires access to a shared input data directory and the output directory.
If the worker is deployed as a docker container these directories can be mounted as volumes.


#### Environment Variables

TODO
