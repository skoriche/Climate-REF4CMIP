This part of the project documentation
will focus on an **understanding-oriented** approach.
Here, we will describe the background of the project,
as well as reasoning about how it was implemented.

Points we will aim to cover:

- Context and background on the library
- Why it was created
- Help the reader make connections

We will aim to avoid writing instructions or technical descriptions here,
they belong elsewhere.


## Execution Environments

The REF aims to support the execution of metrics in a variety of environments.
This includes local execution, testing, cloud-based execution, and execution on HPC systems.

The currently supported execution environments are:

* Local

The following environments are planned to be supported in the future:

* Kubernetes (for cloud-based execution)
* Subprocess (for HPC systems)

The selected executor is defined using the `CMIP_REF_EXECUTOR` environment variable.
See the [Configuration](configuration.md) page for more information.
