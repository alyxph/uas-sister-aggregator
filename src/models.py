from __future__ import annotations

from typing import Any

from pydantic import BaseModel, field_validator


class Event(BaseModel):
    """Core event model. Primary key for deduplication: (topic, event_id)."""

    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: dict[str, Any] = {}

    @field_validator("topic", "event_id", "source")
    @classmethod
    def not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        return v.strip()

    @field_validator("timestamp")
    @classmethod
    def valid_iso8601(cls, v: str) -> str:
        from datetime import datetime

        # Accept both Z-suffix and +00:00 variants
        normalised = v.replace("Z", "+00:00")
        try:
            datetime.fromisoformat(normalised)
        except ValueError:
            raise ValueError(f"timestamp '{v}' is not valid ISO 8601")
        return v


class PublishRequest(BaseModel):
    """Accepts a single event OR a batch via 'events' list."""

    # --- Batch path ---
    events: list[Event] | None = None

    # --- Single-event path (mirrors Event fields) ---
    topic: str | None = None
    event_id: str | None = None
    timestamp: str | None = None
    source: str | None = None
    payload: dict[str, Any] | None = None

    def to_event_list(self) -> list[Event]:
        """Return normalised list of Event objects regardless of input shape."""
        if self.events is not None:
            return self.events
        # Single-event path — all required fields must be present
        return [
            Event(
                topic=self.topic,  # type: ignore[arg-type]
                event_id=self.event_id,  # type: ignore[arg-type]
                timestamp=self.timestamp,  # type: ignore[arg-type]
                source=self.source,  # type: ignore[arg-type]
                payload=self.payload or {},
            )
        ]
