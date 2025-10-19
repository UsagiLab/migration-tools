from __future__ import annotations

import logging
from typing import Optional

_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def configure_logging(level: Optional[str] = None) -> None:
    """Configure root logging only once."""
    if getattr(
        configure_logging, "_configured", False
    ):  # pragma: no cover - guard branch
        return

    resolved_level = level or "INFO"
    logging.basicConfig(level=resolved_level, format=_LOG_FORMAT)
    configure_logging._configured = True  # type: ignore[attr-defined]


__all__ = ["configure_logging"]
