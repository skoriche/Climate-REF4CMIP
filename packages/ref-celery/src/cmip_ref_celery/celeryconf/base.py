"""
Base configuration for Celery.

Other environments can use these settings as a base and override them as needed.
"""

from cmip_ref_core.env import get_env

env = get_env()

broker_url = env.str("CELERY_BROKER_URL", "redis://localhost:6379/1")
result_backend = env.str("CELERY_RESULT_BACKEND", broker_url)
broker_connection_retry_on_startup = True

# Accept JSON and pickle as content
accept_content = ["json", "pickle"]
task_serializer = "pickle"
result_serializer = "pickle"
