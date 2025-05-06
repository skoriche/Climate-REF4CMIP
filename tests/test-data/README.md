# Test data

This directory contains data used by the test suite (and documentation).
Not all test data are checked into this repository due to the size of the files.

The test suite will automatically download the required data when it is run.
Alternatively, these data can be downloaded manually by running:

```
make fetch-test-data
```

This also fetches the test data neeed by the ILAMB metric provider.
These data are stored in a pooch cache outside of the repository.

## Sample Data

A consistent set of [sample data](https://github.com/Climate-REF/ref-sample-data)
is used by the REF test suite.
This ensures that the tests are reproducible and that the test data are versioned.

These data are used in the REF tests and are not intended to be used for any other purpose
as they are decimated to reduce the size of the test dataset.

These data are fetched using pooch and then copied into the `sample-data` directory,
during the `make fetch-test-data` process.
These sample data are also used to generate the documentation.

## CMEC Output

Reference output from the CMEC model is stored in the `cmec-output` directory.


## Regression output

A set of output from execution the suite of available diagnostics.
These data can be used to test any post-processing of the outputs that is required to ingest these data into
the REF system.

The `execution_regression` fixture is responsible for collecting the output and copying it to the regression directory.
This fixture will replace any exisiting output for an execution if the `--force-regen` flag is set.
This flag is borrowed from the `pytest-regressions` package which is used elsewhere in the test suite.
