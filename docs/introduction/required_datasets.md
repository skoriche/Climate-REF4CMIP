# Required Datasets

The REF requires some reference data to be available to run the diagnostics.
These datasets are used to evaluate the data under test.
The available reference datasets will determine which diagnostics can be run.


## Reference dataset requirements

The AFT REF requires that any reference datasets are public and usable via a permissive open license.
Where possible, [obs4MIPs](https://pcmdi.github.io/obs4MIPs/) datasets should be used as these datasets
have been made CMOR-compliant, are openly licensed, and are archived on ESGF so can be easily accessed.

During the REF development process a number of reference datasets used by the diagnostic providers have been
identified for inclusion in Obs4MIPs.
This is an on-going process and the REF will be updated to include these datasets as they are made available.


## Obtaining the AFT REF reference datasets

These datasets may require 10s of GB of disk space so may need to be stored on a scratch filesystem.
The location of the download cache for these data can be controlled via the [REF_DATASET_CACHE_DIR][#ref_dataset_cache_dir] environment variable.

### Obs4REF

The REF intends to migrate to use the datasets available on ESGF as the source of truth in the future.
Not all the datasets required by the REF are available on ESGF yet (as of May 2025) so as a temporary measure,
the REF is hosting some of the observation datasets until they are made available on ESGF.

These datasets can be fetched and copied to the `datasets/obs4ref` directory and then ingested using the following command:

```bash
ref datasets fetch-data --registry obs4ref ---output-directory datasets/obs4ref
ref datasets ingest --source-type obs4mips datasets/obs4ref
```

### PMP Climatology

The PMP climatology datasets are different from the datasets above as they are ingested into the REF
and used in diagnostics data requirements.

```bash
ref datasets fetch-data --registry pmp-climatology ---output-directory datasets/pmp-climatology
ref datasets ingest --source-type pmp-climatology datasets/pmp-climatology
```

### Provider-specific datasets

Some additional provider specific datasets are required to run the diagnostics.
These datasets are not used in data requirements but are used within the diagnostics themselves
so do not need to be ingested into the REF.
These commands will download the datasets and store them in a cache directory.

```bash
ref datasets fetch-data --registry ilamb
ref datasets fetch-data --registry iomb
ref datasets fetch-data --registry esmvaltool
```
