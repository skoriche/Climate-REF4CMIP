# Getting started

The REF is designed to be run in a variety of environments, including local execution, cloud-based execution, and execution on HPC systems.
The REF can be run as a standalone application, as a set of services, or as a set of docker containers.

After [installation](./installation.md), you can start using the REF via the CLI tool:

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


---8<--- "README.md:getting-started"
