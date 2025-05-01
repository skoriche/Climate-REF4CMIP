# ⚠️ Package Has Been Renamed! ⚠️

**This package, `cmip_ref`, is deprecated and no longer maintained.**

It has been renamed to **`climate-ref`**.

---

## Please Update Your Dependencies

To continue receiving updates and ensure compatibility, please switch to the new package.

**Reason for rename:**
The rename was necessary to better reflect the purpose and scope of the package.

### How to Switch

1.  **Uninstall the old package:**
    ```bash
    pip uninstall cmip_ref
    ```

2.  **Install the new package:**
    ```bash
    pip install climate-ref
    ```

3.  **Update your code/requirements:**
    *   Change any import statements from `import cmip_ref` to `import climate_ref`.
    *   Update your `requirements.txt`, `pyproject.toml`, `setup.py`, or other dependency management files to list `climate-ref` instead of `cmip_ref`.

---

## Project Links

*   **New Package on PyPI:** [https://pypi.org/project/climate-ref/](https://pypi.org/project/climate-ref/)

---


# REF (Rapid Evaluation Framework)

**Status**: This project is in active development. We expect to be ready for beta releases in Q2 2025.

The Rapid Evaluation Framework(REF) is a set of Python packages that provide the ability to manage the execution of calculations against climate datasets.
The aim is to be able to evaluate climate data against a set of reference data in near-real time as datasets are published,
and to update any produced data and figures as new datasets become available.
This is somewhat analogous to a CI/CD pipeline for climate data.

REF is a community project, and we welcome contributions from anyone.


## Usage

The `ref` package exposes a command line interface (CLI) that can be used to
interact with the

````
