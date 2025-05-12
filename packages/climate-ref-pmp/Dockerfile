# Build the container for the ESMValTool metrics package
# This docker container extends the base REF compute engine with ESMValTool-specific configurations

FROM climate-ref:latest AS base

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

USER app

# Set ESMValTool specific environment variables
ENV REF_SOFTWARE_ROOT=/app/cache
ENV REF_EXECUTOR=climate_ref_celery.executor.CeleryExecutor

# Install the ESMValTool metrics package
RUN /app/.venv/bin/ref providers create-env --provider pmp

# Start the celery worker by default
ENTRYPOINT ["/app/.venv/bin/ref"]
CMD ["celery", "start-worker", "--package=climate-ref-esmvaltool"]
