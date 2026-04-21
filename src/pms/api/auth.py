from __future__ import annotations

import logging
import secrets
from typing import NoReturn

from fastapi import HTTPException, Request, status

from pms.config import PMSSettings


logger = logging.getLogger(__name__)
AUTH_FAILURE_DETAIL = "Missing or invalid API token."
REDACTED_BEARER_TOKEN = "Bearer [REDACTED]"


def _redact_authorization_header(value: str | None) -> str | None:
    if value is None:
        return None

    scheme, _, _token = value.partition(" ")
    if scheme.lower() == "bearer":
        return REDACTED_BEARER_TOKEN
    return "[REDACTED]"


def _settings_from_request(request: Request) -> PMSSettings:
    settings = getattr(request.app.state, "settings", None)
    if isinstance(settings, PMSSettings):
        return settings

    msg = "Application settings are not configured on app.state.settings"
    raise RuntimeError(msg)


def _raise_unauthorized(request: Request) -> NoReturn:
    logger.warning(
        "Rejected API request due to missing or invalid bearer token: method=%s path=%s authorization=%s",
        request.method,
        request.url.path,
        _redact_authorization_header(request.headers.get("Authorization")),
    )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=AUTH_FAILURE_DETAIL,
        headers={"WWW-Authenticate": "Bearer"},
    )


def require_api_token(request: Request) -> None:
    settings = _settings_from_request(request)
    expected_token = settings.api_token
    if not expected_token:
        return

    authorization = request.headers.get("Authorization")
    if authorization is None:
        _raise_unauthorized(request)

    scheme, _, presented_token = authorization.partition(" ")
    if scheme.lower() != "bearer":
        _raise_unauthorized(request)

    stripped_token = presented_token.strip()
    if not stripped_token or not secrets.compare_digest(stripped_token, expected_token):
        _raise_unauthorized(request)
