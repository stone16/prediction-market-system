from __future__ import annotations

from pms.core.models import Feedback
from pms.storage.feedback_store import FeedbackStore


async def list_feedback(
    store: FeedbackStore,
    *,
    resolved: bool | None = None,
) -> list[Feedback]:
    return await store.list(resolved=resolved)


async def resolve_feedback(
    store: FeedbackStore,
    feedback_id: str,
) -> Feedback | None:
    existing = await store.get(feedback_id)
    if existing is None:
        return None
    if not existing.resolved:
        await store.resolve(feedback_id)
    return await store.get(feedback_id)
