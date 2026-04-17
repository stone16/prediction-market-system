from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from pms.core.enums import FeedbackSource, FeedbackTarget
from pms.core.models import Feedback, OrderState
from pms.storage.feedback_store import FeedbackStore


@dataclass(frozen=True)
class ActuatorFeedback:
    store: FeedbackStore

    async def generate(self, order_state: OrderState, *, reason: str) -> Feedback:
        feedback = Feedback(
            feedback_id=f"actuator-{uuid4().hex}",
            target=FeedbackTarget.CONTROLLER.value,
            source=FeedbackSource.ACTUATOR.value,
            message=(
                f"Actuator could not execute decision {order_state.decision_id}: "
                f"{reason}"
            ),
            severity="warning",
            created_at=datetime.now(tz=UTC),
            category=reason,
            metadata={
                "order_id": order_state.order_id,
                "decision_id": order_state.decision_id,
                "market_id": order_state.market_id,
                "status": order_state.status,
            },
        )
        await self.store.append(feedback)
        return feedback
