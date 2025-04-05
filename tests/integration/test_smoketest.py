"""
Run a docker-based smoketest of the dockerised REF.

This smoketest creates the following services:
* postgres
* redis
* REF worker
* Example worker


Then we run a simple ingestion and solve to ensure that the REF works as expected.

"""

from pathlib import Path

from pytest_docker_tools import container, volume

from tests.integration.conftest import build_local

ROOT_DIR = Path(__file__).parents[2]

ref_volume = volume()

ref_image = build_local(
    context=str(ROOT_DIR),
    file=str(ROOT_DIR / "packages/ref/Dockerfile"),
    platform="linux/amd64",
)

ref_worker = container(
    image="{ref_image.id}",
    command="ref celery start-worker",
    environment={
        "REF_DATABASE_URL": "postgresql://postgres:example@postgres:5432/postgres",
        "REF_REDIS_URL": "redis://redis:6379/0",
        "REF_DATA_DIR": "/ref",
    },
    volumes={
        "{ref_volume.name}": {"bind": "/ref"},
    },
)


def test_smoketest_ingest(ref_worker, ref_image):
    ref_image
