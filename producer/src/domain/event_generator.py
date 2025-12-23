import random
import uuid
from typing import Iterator

from shared.domain.events import UserActivityEvent


EVENT_TYPES = [
    "page_view",
    "click",
    "scroll",
    "form_submit",
    "video_play",
    "video_pause",
    "purchase",
    "add_to_cart",
    "search",
    "logout",
]

PAGES = [
    "/home",
    "/products",
    "/products/123",
    "/cart",
    "/checkout",
    "/profile",
    "/search",
    "/help",
]


class EventGenerator:
    def __init__(self, user_pool_size: int = 100):
        self._user_pool_size = user_pool_size
        self._active_sessions: dict[str, str] = {}

    def generate(self) -> UserActivityEvent:
        user_id = f"user_{random.randint(1, self._user_pool_size)}"
        
        if user_id not in self._active_sessions or random.random() < 0.1:
            self._active_sessions[user_id] = str(uuid.uuid4())
        
        event_type = random.choice(EVENT_TYPES)
        page_url = random.choice(PAGES) if event_type in ("page_view", "click", "scroll") else None
        duration_ms = random.randint(100, 30000) if event_type in ("page_view", "video_play") else None
        
        metadata = None
        if event_type == "search":
            metadata = {"query": f"sample_query_{random.randint(1, 50)}"}
        elif event_type == "purchase":
            metadata = {"amount": round(random.uniform(10.0, 500.0), 2)}

        return UserActivityEvent.create(
            user_id=user_id,
            event_type=event_type,
            session_id=self._active_sessions[user_id],
            page_url=page_url,
            duration_ms=duration_ms,
            metadata=metadata,
        )

    def generate_batch(self, count: int) -> Iterator[UserActivityEvent]:
        for _ in range(count):
            yield self.generate()

