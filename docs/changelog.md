# Changelog

Versions follow [Semantic Versioning](https://semver.org/) (`<major>.<minor>.<patch>`).

Backward incompatible (breaking) changes will only be introduced in major versions
with advance notice in the **Deprecations** section of releases.

<!--
You should *NOT* be adding new changelog entries to this file,
this file is managed by towncrier.
See `changelog/README.md`.

You *may* edit previous changelogs to fix problems like typo corrections or such.
To add a new changelog entry, please see
`changelog/README.md`
and https://pip.pypa.io/en/latest/development/contributing/#news-entries,
noting that we use the `changelog` directory instead of news,
markdown instead of restructured text and use slightly different categories
from the examples given in that link.
-->

<!-- towncrier release notes start -->

## cmip_ref 0.1.5 (2025-01-13)

### Trivial/Internal Changes

- [#56](https://github.com/CMIP-REF/cmip-ref/pulls/56)


## cmip_ref 0.1.4 (2025-01-13)

### Breaking Changes

- Adds an `ingest` CLI command to ingest a new set of data into the database.

  This breaks a previous migration as alembic's `render_as_batch` attribute should have been set
  to support targeting sqlite. ([#14](https://github.com/CMIP-REF/cmip-ref/pulls/14))
- Renames `ref ingest` to `ref datasets ingest` ([#30](https://github.com/CMIP-REF/cmip-ref/pulls/30))
- Prepend package names with `cmip_` to avoid conflicting with an existing `PyPI` package.

  This is a breaking change because it changes the package name and all imports.
  All package names will now begin with `cmip_ref`. ([#53](https://github.com/CMIP-REF/cmip-ref/pulls/53))

### Features

- Migrate to use UV workspaces to support multiple packages in the same repository.
  Adds a `ref-metrics-example` package that will be used to demonstrate the integration of a metric
  package into the REF. ([#2](https://github.com/CMIP-REF/cmip-ref/pulls/2))
- Adds the placeholder concept of `Executor`'s which are responsible for running metrics
  in different environments. ([#4](https://github.com/CMIP-REF/cmip-ref/pulls/4))
- Adds the concept of MetricProvider's and Metrics to the core.
  These represent the functionality that metric providers must implement in order to be part of the REF.
  The implementation is still a work in progress and will be expanding in follow-up PRs. ([#5](https://github.com/CMIP-REF/cmip-ref/pulls/5))
- Add a collection of ESGF data that is required for test suite.

  Package developers should run `make fetch-test-data` to download the required data for the test suite. ([#6](https://github.com/CMIP-REF/cmip-ref/pulls/6))
- Adds the `ref` package with a basic CLI interface that will allow for users to interact with the database of jobs. ([#8](https://github.com/CMIP-REF/cmip-ref/pulls/8))
- Add `SqlAlchemy` as an ORM for the database alongside `alembic` for managing database migrations. ([#11](https://github.com/CMIP-REF/cmip-ref/pulls/11))
- Added a `DataRequirement` class to declare the requirements for a metric.

  This provides the ability to:

  * filter a data catalog
  * group datasets together to be used in a metric calculation
  * declare constraints on the data that is required for a metric calculation

  ([#15](https://github.com/CMIP-REF/cmip-ref/pulls/15))
- Add a placeholder iterative metric solving scheme ([#16](https://github.com/CMIP-REF/cmip-ref/pulls/16))
- Extract a data catalog from the database to list the currently ingested datasets ([#24](https://github.com/CMIP-REF/cmip-ref/pulls/24))
- Translated selected groups of datasets into `MetricDataset`s.
  Each `MetricDataset` contains all of the dataset's needed for a given execution of a metric.

  Added a slug to the `MetricDataset` to uniquely identify the execution
  and make it easier to identify the execution in the logs. ([#29](https://github.com/CMIP-REF/cmip-ref/pulls/29))
- Adds `ref datasets list` command to list ingested datasets ([#30](https://github.com/CMIP-REF/cmip-ref/pulls/30))
- Add database structures to represent a metric execution.
  We record previous executions of a metric to minimise the number of times we need to run metrics. ([#31](https://github.com/CMIP-REF/cmip-ref/pulls/31))
- Added option to skip any datasets that fail validation and to specify the number of cores to
  use when ingesting datasets.
  This behaviour can be opted in using the `--skip-invalid` and `--n-jobs` options respectively. ([#36](https://github.com/CMIP-REF/cmip-ref/pulls/36))
- Track datasets that were used for different metric executions ([#39](https://github.com/CMIP-REF/cmip-ref/pulls/39))
- Added an example ESMValTool metric. ([#40](https://github.com/CMIP-REF/cmip-ref/pulls/40))
- Support the option for different assumptions about the root paths between executors and the ref CLI.

  Where possible path fragments are stored in the database instead of complete paths.
  This allows the ability to move the data folders without needing to update the database. ([#46](https://github.com/CMIP-REF/cmip-ref/pulls/46))

### Improvements

- Add a bump, release and deploy flow for automating the release procedures ([#20](https://github.com/CMIP-REF/cmip-ref/pulls/20))
- Migrate test data into standalone [CMIP-REF/ref-sample-data](https://github.com/CMIP-REF/ref-sample-data) repository.

  The sample data will be downloaded by the test suite automatically into `tests/test-data/sample-data`,
  or manually by running `make fetch-test-data`. ([#49](https://github.com/CMIP-REF/cmip-ref/pulls/49))

### Bug Fixes

- Adds `version` field to the `instance_id` field for CMIP6 datasets ([#35](https://github.com/CMIP-REF/cmip-ref/pulls/35))
- Handle missing branch times.
  Fixes [#38](https://github.com/CMIP-REF/cmip-ref/issues/38). ([#42](https://github.com/CMIP-REF/cmip-ref/pulls/42))
- Move alembic configuration and migrations to `cmip_ref` package so that they can be included in the distribution. ([#54](https://github.com/CMIP-REF/cmip-ref/pulls/54))

### Improved Documentation

- Deployed documentation to https://cmip-ref.readthedocs.io/en/latest/ ([#16](https://github.com/CMIP-REF/cmip-ref/pulls/16))
- General documentation cleanup.

  Added notebook describing the process of executing a notebook locally ([#19](https://github.com/CMIP-REF/cmip-ref/pulls/19))
- Add Apache licence to the codebase ([#21](https://github.com/CMIP-REF/cmip-ref/pulls/21))
- Improved developer documentation. ([#47](https://github.com/CMIP-REF/cmip-ref/pulls/47))

### Trivial/Internal Changes

- [#41](https://github.com/CMIP-REF/cmip-ref/pulls/41), [#44](https://github.com/CMIP-REF/cmip-ref/pulls/44), [#48](https://github.com/CMIP-REF/cmip-ref/pulls/48), [#52](https://github.com/CMIP-REF/cmip-ref/pulls/52), [#55](https://github.com/CMIP-REF/cmip-ref/pulls/55)
