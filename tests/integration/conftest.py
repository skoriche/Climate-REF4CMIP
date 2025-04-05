import time

import psycopg2
import redis
from loguru import logger
from pytest_docker_tools import container, fetch, wrappers

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "example"  # noqa: S105


class PostgresContainer(wrappers.Container):
    PORT_ID = "5432/tcp"

    def ready(self):
        if super().ready() and len(self.ports[self.PORT_ID]) > 0:
            port = self.ports[self.PORT_ID][0]

            try:
                conn = psycopg2.connect(
                    host="localhost",
                    port=port,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    dbname="postgres",
                )
                logger.info("Postgres is ready!")
                conn.close()
                return True
            except psycopg2.OperationalError as e:
                logger.info(str(e).strip())
                logger.info("Postgres isn't ready")
                time.sleep(3)

        return False

    def connection_url(self) -> str:
        port = self.ports[self.PORT_ID][0]
        return f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:{port}/postgres"


class RedisContainer(wrappers.Container):
    def ready(self):
        if super().ready() and len(self.ports["6379/tcp"]) > 0:
            port = self.ports["6379/tcp"][0]
            print(f"Redis using port: {port}")
            # Perform a simple ping to check if the server is ready
            r = redis.Redis(host="localhost", port=port)
            try:
                return r.ping()
            except redis.ConnectionError:
                print("Redis connection error, retrying...")
                time.sleep(2)  # Increase the sleep time
        return False

    def connection_url(self) -> str:
        port = self.ports["6379/tcp"][0]
        return f"redis://localhost:{port}/0"


postgres_image = fetch(repository="postgres:17")

postgres_container = container(
    image="{postgres_image.id}",
    ports={
        PostgresContainer.PORT_ID: None,
    },
    wrapper_class=PostgresContainer,
    environment={
        "POSTGRES_USER": POSTGRES_USER,
        "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
        "POSTGRES_DB": "postgres",
    },
)


redis_image = fetch(repository="redis:7")

redis_container = container(
    image="{redis_image.id}",
    ports={
        "6379/tcp": None,
    },
    wrapper_class=RedisContainer,
)
