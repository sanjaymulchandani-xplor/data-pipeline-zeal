from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid


@dataclass(frozen=True)
class UserActivityEvent:
    event_id: str
    user_id: str
    event_type: str
    timestamp: datetime
    session_id: str
    page_url: Optional[str] = None
    duration_ms: Optional[int] = None
    metadata: Optional[dict] = None

    @staticmethod
    def create(
        user_id: str,
        event_type: str,
        session_id: str,
        page_url: Optional[str] = None,
        duration_ms: Optional[int] = None,
        metadata: Optional[dict] = None,
    ) -> "UserActivityEvent":
        return UserActivityEvent(
            event_id=str(uuid.uuid4()),
            user_id=user_id,
            event_type=event_type,
            timestamp=datetime.utcnow(),
            session_id=session_id,
            page_url=page_url,
            duration_ms=duration_ms,
            metadata=metadata,
        )

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "user_id": self.user_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp.isoformat(),
            "session_id": self.session_id,
            "page_url": self.page_url,
            "duration_ms": self.duration_ms,
            "metadata": self.metadata,
        }

    @staticmethod
    def from_dict(data: dict) -> "UserActivityEvent":
        return UserActivityEvent(
            event_id=data["event_id"],
            user_id=data["user_id"],
            event_type=data["event_type"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            session_id=data["session_id"],
            page_url=data.get("page_url"),
            duration_ms=data.get("duration_ms"),
            metadata=data.get("metadata"),
        )

