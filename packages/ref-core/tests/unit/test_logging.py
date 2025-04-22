import logging as std_logging

import pytest
from loguru import logger

from cmip_ref_core.logging import capture_logging, redirect_logs
from cmip_ref_core.metrics import MetricExecutionDefinition


@pytest.fixture
def definition(mocker, tmp_path):
    d = mocker.MagicMock(spec=MetricExecutionDefinition)
    d.output_directory = tmp_path / "output"
    from cmip_ref_core.executor import EXECUTION_LOG_FILENAME

    return d, d.output_directory / EXECUTION_LOG_FILENAME


def test_intercept_logs(caplog):
    std_logger = std_logging.getLogger("cmip_ref_core")

    capture_logging()

    with caplog.at_level(std_logging.INFO):
        std_logger.error("this is an error log")
        std_logger.debug("this is an debug log")

    assert "this is an error log" in caplog.text
    assert "this is an debug log" not in caplog.text


def test_redirect_logs(definition, caplog):
    log_level = "DEBUG"
    text_outer = "Outer log message"
    text_inner = "Inner log message"
    text_post = "Post log message"

    definition, output_file = definition

    with caplog.at_level(log_level):
        logger.info(text_outer)

        with redirect_logs(definition, log_level):
            logger.info(text_inner)

        logger.info(text_post)

    assert text_outer in caplog.text
    assert text_inner not in caplog.text
    assert text_post in caplog.text

    assert text_inner in output_file.read_text()


def test_redirect_logs_without_handler(definition):
    definition, output_file = definition

    assert not hasattr(logger, "default_handler_id")
    with redirect_logs(definition, "INFO"):
        logger.info("inner")
    assert not hasattr(logger, "default_handler_id")

    assert "inner" in output_file.read_text()


def test_redirect_logs_stdlog_captured(definition):
    definition, output_file = definition

    with redirect_logs(definition, "INFO"):
        import logging

        stdlog = logging.getLogger("stdlog")
        stdlog.info("stdlog inner")
        stdlog.debug("stdlog debug")

    assert "stdlog inner" in output_file.read_text()
    assert "stdlog debug" not in output_file.read_text()


def test_redirect_logs_exception(definition, caplog):
    log_level = "DEBUG"

    orig_handler = logger.default_handler_id

    with pytest.raises(ValueError):
        with redirect_logs(definition[0], log_level):
            logger.debug("This will raise an exception")
            raise ValueError()

    assert (
        orig_handler != logger.default_handler_id
    ), "The default handler should have changed during the redirect and cleanup."
