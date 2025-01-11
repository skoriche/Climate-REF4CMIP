"""Environment variable management"""

import os

import platformdirs
from environs import Env


def _set_defaults() -> None:
    os.environ.setdefault("REF_CONFIGURATION", str(platformdirs.user_config_path("cmip_ref")))


def get_env() -> Env:
    """
    Get the current environment

    Returns
    -------
    :
        The current environment including any environment variables loaded from the .env file
        and any defaults set by this application.
    """
    # Set the default values for the environment variables
    _set_defaults()

    env = Env(expand_vars=True)

    # Load the environment variables from the .env file
    # This will override any defaults set above
    env.read_env(verbose=True)

    return env


env = get_env()
