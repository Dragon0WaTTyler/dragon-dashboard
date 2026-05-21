from __future__ import annotations

import os

from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix

from .logging_config import configure_app_logging


def create_app(import_name: str, *, secret_key: str, is_production: bool) -> Flask:
    app = Flask(import_name)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    configure_app_logging(app)
    app.secret_key = secret_key
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=is_production,
        PREFERRED_URL_SCHEME="https" if is_production else "http",
    )
    if not is_production:
        os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")
    return app

