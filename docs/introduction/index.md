# Overview

Climate-REF (Rapid Evaluation Framework) is a comprehensive framework for performing climate model evaluation and benchmarking.
This framework is designed to aggregate the results of various diagnostic providers into a single unified framework.
It allows users to easily compare and analyze the performance of different climate models against a set of reference datasets.

The Climate-REF is responsible for determining which diagnostics need to be executed based on the input datasets and the reference datasets.
It also manages the execution of these diagnostics, either locally or remotely, and provides a consistent interface for users to interact with the results, either via a CLI, web interface, or python API.


/// admonition | Note

The Python API has not been implemented yet.

///

## Key Features

* **Modular:** This is a community project.
    It should be easy for existing and future benchmarking packages to integrate with the framework.
    The developer experience for the benchmarking package providers should be paramount.
* **Scalable:** For CMIP7-FT we are focusing on a small subset of possible derived metrics and diagnostics.
    The metrics will scale over time as the science progresses in terms of data volume and complexity.
* **Reusable:** We are targeting multiple different deployment environments and a range of different potential users.
    Where possible, we should preserve the ability to reuse components of the REF in different contexts.
