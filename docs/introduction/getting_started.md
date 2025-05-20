# Getting started

The REF is designed to be run in a variety of environments, including local execution, cloud-based execution, and execution on HPC systems.
The REF can be run as a standalone application, as a set of services, or as a set of docker containers.

After [installation](../installation.md), you can start using the REF via the CLI tool:

```bash
$ uv run ref

 Usage: ref [OPTIONS] COMMAND [ARGS]...

 climate_ref: A CLI for the Assessment Fast Track Rapid Evaluation Framework

 This CLI provides a number of commands for managing and executing diagnostics.

╭─ Options ────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --configuration-directory          PATH                        Configuration directory [default: None]               │
│ --verbose                  -v                                  Set the log level to DEBUG                            │
│ --quiet                    -q                                  Set the log level to WARNING                          │
│ --log-level                        [ERROR|WARNING|DEBUG|INFO]  Set the level of logging information to display       │
│                                                                [default: INFO]                                       │
│ --version                                                      Print the version and exit                            │
│ --install-completion                                           Install completion for the current shell.             │
│ --show-completion                                              Show completion for the current shell, to copy it or  │
│                                                                customize the installation.                           │
│ --help                                                         Show this message and exit.                           │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ solve        Solve for executions that require recalculation                                                         │
│ config       View and update the REF configuration                                                                   │
│ datasets     View and ingest input datasets                                                                          │
│ executions   View diagnostic executions                                                                              │
│ providers    Manage the REF providers.                                                                               │
│ celery       Managing remote execution workers                                                                       │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

This provides the ability to:

* **Ingest** new input datasets
* **Solve** for the unique metrics executions that are required
* **Execute** the metrics either locally or remotely

This CLI tool is the main interface to the REF and is used to manage the REF.

Before running the REF, you will need to [configure the REF](../configuration.md#configuration).
The REF can be configured using a configuration file or environment variables.

For HPC users, it is recommended to set the `REF_CONFIGURATION` and `REF_DATASET_CACHE_DIR` environment variables
to point to a location where the REF can store its outputs and cache any downloaded datasets.
These directories may be large, so it is recommended to set them to a scratch filesystem rather than a home filesystem.


---8<--- "README.md:getting-started"
