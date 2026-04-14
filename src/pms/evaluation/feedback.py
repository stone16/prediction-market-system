from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from pms.config import RiskSettings
from pms.core.enums import FeedbackSource, FeedbackTarget
from pms.core.models import Feedback
from pms.evaluation.metrics import MetricsSnapshot
from pms.storage.feedback_store import FeedbackStore


@dataclass(frozen=True)
class EvaluatorFeedback:
    store: FeedbackStore
    risk: RiskSettings

    def generate(self, metrics: MetricsSnapshot) -> list[Feedback]:
        feedback: list[Feedback] = []

        for category, brier_score in metrics.brier_by_category.items():
            if (
                metrics.brier_samples.get(category, 0) >= 20
                and brier_score > self.risk.max_brier_score
            ):
                feedback.append(
                    self._build_feedback(
                        category=f"brier:{category}",
                        message=(
                            f"Brier score {brier_score:.4f} exceeded "
                            f"{self.risk.max_brier_score:.4f} for {category}"
                        ),
                    )
                )

        if metrics.slippage_bps > self.risk.slippage_threshold_bps:
            feedback.append(
                self._build_feedback(
                    category="slippage",
                    message=(
                        f"Slippage {metrics.slippage_bps:.2f} bps exceeded "
                        f"{self.risk.slippage_threshold_bps:.2f} bps"
                    ),
                )
            )

        if metrics.win_rate < self.risk.min_win_rate:
            feedback.append(
                self._build_feedback(
                    category="win_rate",
                    message=(
                        f"Win rate {metrics.win_rate:.4f} fell below "
                        f"{self.risk.min_win_rate:.4f}"
                    ),
                )
            )

        for item in feedback:
            self.store.append(item)
        return feedback

    def _build_feedback(self, *, category: str, message: str) -> Feedback:
        return Feedback(
            feedback_id=f"evaluator-{uuid4().hex}",
            target=FeedbackTarget.CONTROLLER.value,
            source=FeedbackSource.EVALUATOR.value,
            message=message,
            severity="warning",
            created_at=datetime.now(tz=UTC),
            category=category,
        )
