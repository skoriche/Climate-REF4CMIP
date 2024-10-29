# Configuration

## Environment Variables

Environment variables are used to control some aspects of the model.
The default values for these environment variables are generally suitable,
but if you require updating these values we recommend the use of a `.env` file
to make the changes easier to reproduce in future.

### `CMIP_REF_EXECUTOR`

Executor to use for running the metrics.

Defaults to use the local executor ("local").
