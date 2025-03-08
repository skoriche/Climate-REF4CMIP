import redis
from pytest_docker_tools import wrappers  # type: ignore


class RedisContainer(wrappers.Container):  # type: ignore
    def ready(self) -> bool:  # pragma: no cover
        if super().ready() and len(self.ports["6379/tcp"]) > 0:
            print(f"Redis using port:{self.ports['6379/tcp'][0]}")
            # Perform a simple ping to check if the server is ready
            r = redis.Redis(host="localhost", port=self.ports["6379/tcp"][0])
            try:
                return r.ping()  # type: ignore
            except redis.ConnectionError:
                return False

        return False

    def connection_url(self) -> str:  # pragma: no cover
        port = self.ports["6379/tcp"][0]
        return f"redis://localhost:{port}/0"
