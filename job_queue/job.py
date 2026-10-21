"""The Job model: what gets put on the queue.

A job is just a type name (what kind of work it is, e.g. "send_email") plus a
payload dict with whatever that work needs. Jobs travel through Redis as JSON
strings, so everything in the payload has to be JSON-serializable.
"""

import json
import time
import uuid
from dataclasses import dataclass, field


@dataclass
class Job:
    job_type: str
    payload: dict = field(default_factory=dict)
    job_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    created_at: float = field(default_factory=time.time)

    def __post_init__(self):
        if not isinstance(self.job_type, str) or not self.job_type.strip():
            raise ValueError("job_type must be a non-empty string")
        if not isinstance(self.payload, dict):
            raise ValueError("payload must be a dict")
        # Check this now, at submit time. If a bad payload got onto the queue we
        # would only find out when a worker tried to read it back.
        try:
            json.dumps(self.payload)
        except (TypeError, ValueError) as error:
            raise ValueError(f"payload must be JSON-serializable: {error}") from error

    def to_json(self):
        return json.dumps(
            {
                "job_id": self.job_id,
                "job_type": self.job_type,
                "payload": self.payload,
                "created_at": self.created_at,
            }
        )

    @classmethod
    def from_json(cls, raw):
        # The redis client returns bytes unless decode_responses=True, so accept both.
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValueError(f"not valid job JSON: {error}") from error

        if not isinstance(data, dict):
            raise ValueError("job JSON must be an object")
        missing = [name for name in ("job_id", "job_type", "payload", "created_at") if name not in data]
        if missing:
            raise ValueError(f"job JSON is missing fields: {', '.join(missing)}")

        return cls(
            job_type=data["job_type"],
            payload=data["payload"],
            job_id=data["job_id"],
            created_at=data["created_at"],
        )
