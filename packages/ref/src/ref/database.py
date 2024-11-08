from pathlib import Path

import alembic.command
import sqlalchemy
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from loguru import logger
from sqlalchemy.orm import Session

from ref.config import Config


class Database:
    """
    Manage the database connection and migrations

    The database migrations are optionally run after the connection to the database is established.
    """

    def __init__(self, url: str, run_migrations: bool = True) -> None:
        logger.info(f"Connecting to database at {url}")
        self.url = url
        self._engine = sqlalchemy.create_engine(self.url)
        self.session = Session(self._engine)
        if run_migrations:
            self._migrate()

    def _migrate(self):
        root_dir = Path(__file__).parents[4]

        alembic_config = AlembicConfig(root_dir / "alembic.ini")
        # alembic_config.set_main_option("script_location", str(root_dir / "packages" / "ref" / "alembic"))
        alembic_config.attributes["connection"] = self._engine

        script = ScriptDirectory.from_config(alembic_config)
        head = script.get_current_head()

        # Run migrations
        alembic.command.upgrade(alembic_config, head)

    @staticmethod
    def from_config(config: Config, run_migrations: bool = True) -> "Database":
        """
        Create a Database instance from a Config instance

        Parameters
        ----------
        config
            The Config instance that includes information about where the database is located
        run_migrations
            If true, run the migrations when the database is loaded

        Returns
        -------
        :
            A new Database instance
        """
        # TODO: move the database URL creation to the Config class
        config.paths.db.mkdir(parents=True, exist_ok=True)
        url = config.db.connection_url

        return Database(url, run_migrations=run_migrations)


if __name__ == "__main__":
    Database.from_config(Config(), run_migrations=True)
