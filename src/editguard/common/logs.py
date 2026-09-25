"""JSON logs via structlog. Never log secrets, usernames or diff text."""

import logging

import structlog


def configure_logging(service: str, level: str = "INFO") -> structlog.stdlib.BoundLogger:
    """Set up JSON logging and return a logger that tags every line with the service name."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger().bind(service=service)
