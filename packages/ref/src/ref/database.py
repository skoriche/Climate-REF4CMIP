from pathlib import Path
from urllib import parse as urlparse

import alembic.command
import sqlalchemy
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from loguru import logger
from sqlalchemy.orm import Session

from ref.config import Config
from ref.env import env


def validate_database_url(database_url: str):
    """
    Validate a database URL

    We support sqlite databases, and we create the directory if it doesn't exist.

    Parameters
    ----------
    database_url
        The database URL to validate

        See [ref.config.Db.database_url](ref.config.Db.database_url) for more information
        on the format of the URL.

    Raises
    ------
    ValueError
        If the database scheme is not supported

    Returns
    -------
    :
        The validated database URL
    """
    split_url = urlparse.urlsplit(database_url)
    path = split_url.path[1:]

    if split_url.scheme == "sqlite":
        if path == ":memory:":
            logger.warning("Using an in-memory database")
        else:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
    elif split_url.scheme == "postgresql":
        # We don't need to do anything special for PostgreSQL
        logger.warning("PostgreSQL support is currently experimental and untested")
    else:
        raise ValueError(f"Unsupported database scheme: {split_url.scheme}")

    return database_url


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

        The `REF_DATABASE_URL` environment variable will take preference,
         and override the database URL specified in the config.

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
        database_url: str = env.str("REF_DATABASE_URL", default=config.db.database_url)

        database_url = validate_database_url(database_url)
        return Database(database_url, run_migrations=run_migrations)
