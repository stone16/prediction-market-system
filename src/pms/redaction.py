from __future__ import annotations

import re
from collections.abc import Iterable


POLYMARKET_CREDENTIAL_REDACTION = "<redacted-polymarket-credential>"
_DATABASE_URL_RE = re.compile(
    r"\b(?:postgresql|postgres|postgresql\+asyncpg)://[^\s]+",
    flags=re.IGNORECASE,
)
_DISCORD_WEBHOOK_URL_RE = re.compile(
    r"\bhttps?://(?:ptb\.|canary\.)?discord(?:app)?\.com/api/[^\s'\"\])]+",
    flags=re.IGNORECASE,
)
_PASSWORD_ASSIGNMENT_RE = re.compile(r"(?i)(password=)[^\s]+")


def redact_database_error(message: str) -> str:
    redacted = _DATABASE_URL_RE.sub("<redacted-database-url>", message)
    redacted = _DISCORD_WEBHOOK_URL_RE.sub(
        "<redacted-discord-webhook-url>",
        redacted,
    )
    return _PASSWORD_ASSIGNMENT_RE.sub(r"\1<redacted>", redacted)


def redact_live_error_values(
    message: str,
    credential_values: Iterable[str | None],
) -> str:
    redacted = redact_database_error(message)
    for secret in _credential_redaction_values(credential_values):
        redacted = redacted.replace(secret, POLYMARKET_CREDENTIAL_REDACTION)
    return redacted


def _credential_redaction_values(
    credential_values: Iterable[str | None],
) -> tuple[str, ...]:
    values: list[str] = []
    seen: set[str] = set()
    for raw_secret in credential_values:
        if raw_secret is None:
            continue
        for secret in (raw_secret, raw_secret.strip()):
            if secret == "" or secret in seen:
                continue
            values.append(secret)
            seen.add(secret)
    return tuple(sorted(values, key=len, reverse=True))
