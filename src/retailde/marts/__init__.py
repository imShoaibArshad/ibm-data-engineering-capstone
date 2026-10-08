"""Business marts derived from the star schema."""

from __future__ import annotations

from .build import MARTS, build, rows

__all__ = ["MARTS", "build", "rows"]
