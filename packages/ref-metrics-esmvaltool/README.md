# ⚠️ Package Has Been Renamed! ⚠️

**This package, `cmip_ref_metrics_esmvaltool`, is deprecated and no longer maintained.**

It has been renamed to **`climate-ref-esmvaltool`**.

---

## Please Update Your Dependencies

To continue receiving updates and ensure compatibility, please switch to the new package.

**Reason for rename:**
The rename was necessary to better reflect the purpose and scope of the package.

### How to Switch

1.  **Uninstall the old package:**
    ```bash
    pip uninstall cmip_ref_metrics_esmvaltool
    ```

2.  **Install the new package:**
    ```bash
    pip install climate-ref-esmvaltool
    ```

3.  **Update your code/requirements:**
    *   Change any import statements from `import cmip_ref_metrics_esmvaltool` to `import climate_ref_esmvaltool`.
    *   Update your `requirements.txt`, `pyproject.toml`, `setup.py`, or other dependency management files to list `climate-ref-esmvaltool` instead of `cmip_ref_metrics_esmvaltool`.

---

## Project Links

*   **New Package on PyPI:** [https://pypi.org/project/climate-ref-esmvaltool/](https://pypi.org/project/climate-ref-esmvaltool/)

---

# ref-metrics-esmvaltool

Use [ESMValTool](https://esmvaltool.org/) as a REF metrics provider.

To use this, install ESMValTool and then install the REF into the same conda
environment.

See [running-metrics-locally](https://cmip-ref.readthedocs.io/en/latest/how-to-guides/running-metrics-locally/) for usage instructions.
