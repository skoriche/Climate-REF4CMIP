# Basic Concepts

The Climate-REF (Rapid Evaluation Framework) is a comprehensive framework for performing climate model evaluation and benchmarking.

The Climate-REF doesn't perform any calculations itself,
instead delegates these operations to external diagnostic providers.
These providers are responsible for translating a set of datasets

The operation of the Climate-REF is split into four main phases:

1. **Ingest**: Ingesting datasets into the REF
2. **Solve**: Solving for the unique metric executions that are required
3. **Execute**: Executing the diagnostics either locally or remotely
4. **Visualise**: Visualising the results of the metrics executions
