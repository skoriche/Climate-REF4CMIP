# Build the container for the ref-metrics-example worker
#
# ref-metrics-example is a relatively simple package so can be directly installed via pip.
# More complex packages may require a multi-stage build to build the package before installing it.

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

LABEL maintainer="Jared Lewis <jared.lewis@climate-resource.com>"
LABEL description="Docker image with the execution environment for the ref-metrics-example package"

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

WORKDIR /app

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-workspace --no-dev

# Allow celery to run as root
ENV C_FORCE_ROOT=false

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
ADD . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package ref-metrics-example
# Add the ref-celery package to the worker
RUN --mount=type=cache,target=/root/.cache/uv \
    uv add ref-celery

# Reset the entrypoint, don't invoke `uv`
ENTRYPOINT []
