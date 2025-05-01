# ⚠️ Package Has Been Renamed! ⚠️

**This package, `cmip_ref_metrics_pmp`, is deprecated and no longer maintained.**

It has been renamed to **`climate-ref-pmp`**.

---

## Please Update Your Dependencies

To continue receiving updates and ensure compatibility, please switch to the new package.

**Reason for rename:**
The rename was necessary to better reflect the purpose and scope of the package.

### How to Switch

1.  **Uninstall the old package:**
    ```bash
    pip uninstall cmip_ref_metrics_pmp
    ```

2.  **Install the new package:**
    ```bash
    pip install climate-ref-pmp
    ```

3.  **Update your code/requirements:**
    *   Change any import statements from `import cmip_ref_metrics_pmp` to `import climate_ref_pmp`.
    *   Update your `requirements.txt`, `pyproject.toml`, `setup.py`, or other dependency management files to list `climate-ref-pmp` instead of `cmip_ref_metrics_pmp`.

---


# ref-metrics-pmp

Use [PCMDI Metrics Package (PMP)](https://github.com/PCMDI/pcmdi_metrics) as a REF metrics provider. See [http://pcmdi.github.io/pcmdi_metrics/](http://pcmdi.github.io/pcmdi_metrics/) for more information on project goals and resources.

See [running-metrics-locally](https://cmip-ref.readthedocs.io/en/latest/how-to-guides/running-metrics-locally/) for usage instructions.
