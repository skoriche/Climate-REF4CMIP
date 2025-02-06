import pytest
from celery import Celery
from cmip_ref_celery.app import create_celery_app


def test_create_celery_app(monkeypatch):
    app = create_celery_app("test")

    assert isinstance(app, Celery)
    assert app.main == "test"

    assert app.configured is False
    assert app.conf["broker_url"] == "redis://localhost:6379/1"
    assert app.conf["task_serializer"] == "pickle"
    assert app.configured


def test_create_celery_app_invalid_config(monkeypatch):
    monkeypatch.setenv("CELERY_CONFIG_MODULE", "unknown")
    app = create_celery_app("test")

    with pytest.raises(ImportError):
        # Celery only loads the configuration when it is accessed
        app.conf["task_serializer"]
