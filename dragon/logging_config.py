from __future__ import annotations

import logging


def configure_app_logging(app) -> None:
    app.logger.setLevel(logging.INFO)
    for handler in list(app.logger.handlers):
        handler.setLevel(logging.INFO)

