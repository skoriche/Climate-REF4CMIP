# Hackathon and Pre-Alpha Onboarding

**Hackathon at the Met Office, Exeter, UK, 10 - 14 May 2025**

This hybrid hackathon is being run by the Model Benchmarking Task Team and the REF delivery team.
We welcome attendance from technical and domain scientists from modelling centres involved in the CMIP7 Assessment Fast Track (AFT),
observation dataset providers, as well as ESGF nodes and developers.

During the hackathon, there will also be dedicated drop-ins for wider community interest:

* **10 March 14:00 UTC – 15:00 UTC**: Hackathon launch - providing a brief overview of the REF and a status update by the CMIP AFT delivery team
* **11 March 17:00 – 18:00 UTC**: Modelling Centres
* **13 March 08:00 – 09:00 UTC**: Modelling Centres
* **13 March 11:00 – Midday UTC**: Observation dataset providers

**Pre-Alpha Onboarding**

If you couldn't make it to the hackathon but still want to get started in REF development, this information will be useful as well.

## Technical Requirements

* A laptop with Python 3.11 or later installed
* A GitHub account
* Docker installed (Optional)
* [uv](https://docs.astral.sh/uv) installed

## Set up your development environment

Please clone the package and set up your local environment by following the [development installation](development.md#development-installation) instructions.

### Additional Data

We welcome the testing of ingesting additional local datasets into the REF.
We currently support ingesting CMIP6-like and obs4MIPs datasets,
but are interested in hearing about other datasets that you would like to see supported.

We have tested a subset of CMIP6 data,
but are particularly interested in any non-published datasets that you may have access to.
This will help us to ensure that the REF can support a wide range of datasets.

The metrics that are currently available in the REF are relatively limited so not all datasets will be useful for testing.

### Reading Material

It is recommended to read through the [developer documentation](development.md) to get an understanding of the REF and how we collaborate.

For those interested in learning more about the REF,
we recommend reading the [Introduction section](index.md) [Architecture design document](background/architecture.md).
This outlines the design of the REF and provides some background about the project.


## Deployment environments

The REF is designed to be run on a local machine,
but we are also working on support for running the REF on cloud-based systems and HPC systems.

The main interface with the REF application for local users,
is through the command line interface tool, `ref`.

```
$ ref --help
```

### Executors

An [executor][climate_ref_core.executor.Executor] is responsible for executing a diagnostic calculation in an aync manner.
The REF supports multiple executors,
each of which is responsible for running a metric calculation in a different way.

* [Local][climate_ref.executor.LocalExecutor] - Runs the metric calculation locally
* [Celery][climate_ref_celery.executor.CeleryExecutor] - Runs the metric calculation using Celery

One of the outcomes of this Hackathon will be to add support for running metrics on HPC systems.

## Finally

We are working towards a beta release of the REF in the coming months,
so this project is under heavy development.
There may be aspects that do not work as expected,
but welcome any possible involvement.
Please raise [issues](https://github.com/Climate-REF/climate-ref/issues)
if anything doesn’t work as expected or if you have any features that you would like to see implemented.
