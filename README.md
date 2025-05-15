# Rapid Evaluation Framework (REF)

<!--- --8<-- [start:description] -->

**Status**: This project is in active development. We expect to be ready for beta releases in Q2 2025.

The Rapid Evaluation Framework(REF) is a set of Python packages that provide the ability to manage the execution of calculations against climate datasets.
The aim is to be able to evaluate climate data against a set of reference data in near-real time as datasets are published,
and to update any produced data and figures as new datasets become available.
This is somewhat analogous to a CI/CD pipeline for climate data.

REF is a community project, and we welcome contributions from anyone.

## Deployments

The concept of the REF was proposed by the CMIP Model Benchmarking Task Team (MBTT),
to enable the near-realtime evaluation of CMIP7 Assessment Fast Track (AFT) datasets.
This particular deployment will feature a set of diagnostics selected by
the MBTT in consultation with the community (URL with the CMIP7 AFT configuration to come).

The individual components of the REF have been designed to be useful for
applications past the lifetime of the CMIP7 AFT.
These components can be extended to support evaluating other types of climate data.
The REF is agnostic to what types of data and metrics are to be performed.


[![CI](https://github.com/Climate-REF/climate-ref/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/Climate-REF/climate-ref/actions/workflows/ci.yaml)
[![Coverage](https://codecov.io/gh/Climate-REF/climate-ref/branch/main/graph/badge.svg)](https://codecov.io/gh/Climate-REF/climate-ref)
[![Docs](https://readthedocs.org/projects/climate-ref/badge/?version=latest)](https://climate-ref.readthedocs.io)

**PyPI :**
[![PyPI](https://img.shields.io/pypi/v/climate-ref.svg)](https://pypi.org/project/climate-ref/)
[![PyPI: Supported Python versions](https://img.shields.io/pypi/pyversions/climate-ref.svg)](https://pypi.org/project/climate-ref/)

**Other info :**
[![Licence](https://img.shields.io/github/license/Climate-REF/climate-ref.svg)](https://github.com/Climate-REF/climate-ref/blob/main/LICENCE)
[![SPEC 0 — Minimum Supported Dependencies](https://img.shields.io/badge/SPEC-0-green?labelColor=%23004811&color=%235CA038)](https://scientific-python.org/specs/spec-0000/)
[![Last Commit](https://img.shields.io/github/last-commit/Climate-REF/climate-ref.svg)](https://github.com/Climate-REF/climate-ref/commits/main)
[![Contributors](https://img.shields.io/github/contributors/Climate-REF/climate-ref.svg)](https://github.com/Climate-REF/climate-ref/graphs/contributors)

<!--- --8<-- [end:description] -->

## Installation

<!--- --8<-- [start:installation] -->

CMIP Rapid Evaluation Framework can be installed with pip,
alongside the providers selected for the Assessment Fast Track.
(mamba and conda releases are not yet implemented):

```bash
pip install climate-ref[aft-providers]
```

<!--- --8<-- [end:installation] -->

## Getting started
<!--- --8<-- [start:getting-started] -->

The REF is designed to enable Modelling Centers to quickly evaluate their data against a set of reference data.
The data under test here may not be published to ESGF yet,
but the REF can still be used to evaluate it.

The REF requires some reference data to be available to run the diagnostics.
Some of the reference datasets needed by the REF are available on ESGF yet.
The following command will download the reference datasets needed by the REF and store them in a local directory (`datasets/obs4ref`) as well as some sample CMIP6 datasets that we used in our test suite:

```bash
ref datasets fetch-data --registry obs4ref --output-dir datasets/obs4ref
ref datasets fetch-data --registry pmp-climatology --output directory data/pmp-climatology
ref datasets fetch-data --registry sample-data --output-dir datasets/sample-data
```

These datasets can then be ingested into the REF and the metrics solved using:

```bash
ref datasets ingest --source-type cmip6 datasets/sample-data/CMIP6/
ref datasets ingest --source-type obs4mips datasets/obs4ref
ref datasets ingest --source-type obs4mips datasets/pmp-climatology

ref solve
```

Ingesting a large set of datasets (e.g. the entire CMIP6 corpus) can take a long time.
The ingest command accepts multiple directories via a glob pattern to limit the number of datasets that are ingested.

For the AFT, we are only interested in monthly datasets (and the associated ancillary files).
Note that the globs should be unquoted so that they are expanded by the shell as the cli command expects a list of directories.

```bash
ref datasets ingest --source-type cmip6 path_to_archive/CMIP6/*/*/*/*/*/*mon path_to_archive/CMIP6/*/*/*/*/*/*fx
```

The executed metrics can then be viewed using the `ref executions list-groups` and `ref executions inspect` commands.
This will show the metrics that have been executed and the results that have been produced.

<!--- --8<-- [end:getting-started] -->

Full documentation can be found at:
[climate-ref.readthedocs.io](https://climate-ref.readthedocs.io/en/latest/).
We recommend reading the docs there because the internal documentation links
don't render correctly on GitHub's viewer.


### For contributors

<!--- sec-begin-installation-dev -->

For information on how to contribute see https://climate-ref.readthedocs.io/en/latest/development/.

<!--- sec-end-installation-dev -->
