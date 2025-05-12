# Build the container for the ILAMB metrics package
# This docker container extends the base REF compute engine with ILAMB-specific configurations

FROM climate-ref:latest AS base

# ILAMB doesn't have a conda environment

ENV REF_EXECUTOR=climate_ref_celery.executor.CeleryExecutor

# Start the celery worker by default
ENTRYPOINT ["/app/.venv/bin/ref"]
CMD ["celery", "start-worker", "--package=climate-ref-ilamb"]
