# Installation

There are several ways to install and use the `climate-ref` package and associated CLI tool:

## PyPI

You can install `climate-ref` using `pip`:

```bash
pip install climate-ref
```

The PyPi package contains some additional extras that bundle some additional dependencies.
Depending on your use case you may want to install additional extras:

* `celery` - for running the REF in a distributed manner
* `providers` - metrics providers for CMIP7-FastTrack

These can be installed using:

```bash
pip install climate-ref[celery,providers]
```

/// admonition | Note

`pip install climate-ref[providers]` installs the metric providers packages which container the metrics definitions,
but not necessarily the additional packages required to run the metrics.
///

Some of the metric providers require additional dependencies to be installed.
For these providers, the REF will automatically create a new Conda environment
and install the required dependencies.
This uses a bundled version of the [micromamba](https://github.com/mamba-org/micromamba-releases)
to create and manage the environment so no additional dependencies are required.

## Conda

/// admonition | conda-forge
    type: warning

The conda-forge packages are a work in progress and are not yet available.
See [#80](https://github.com/Climate-REF/climate-ref/issues/80) for more information.
///

You can install `climate-ref` using `mamba` or `conda`:

```bash
mamba install -c conda-forge climate-ref
conda install -c conda-forge climate-ref
```

A modern alternative to using `conda` as package a manager is [Pixi](https://pixi.sh/dev/).
Pixi uses the same packages as `conda` but is faster and can create reproducible environments out of the box.


## Docker

You can run `climate-ref` using Docker. First, build the Docker container from the source code:

```bash
git clone https://github.com/Climate-REF/climate-ref.git
cd climate-ref
docker-compose build
```

Then, run the container:

```bash
docker-compose up
```

## From Source

To install `climate-ref` from the source code, clone the repository and install it:

```bash
git clone https://github.com/Climate-REF/climate-ref.git
cd climate-ref
make virtual-environment
```

See the [development documentation](development.md) for more information on how to contribute to the project.
