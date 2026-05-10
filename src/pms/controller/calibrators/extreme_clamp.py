from __future__ import annotations

from pms.strategies.projections import CalibrationContext, CalibrationSpec


class ExtremeProbClamp:
    def __init__(self, spec: CalibrationSpec) -> None:
        self._spec = spec

    def calibrate(
        self,
        prob: float,
        *,
        context: CalibrationContext,
    ) -> float | None:
        has_enough_samples = (
            context.resolved_sample_count >= self._spec.min_resolved_for_extreme
        )
        if has_enough_samples:
            return prob
        if prob < self._spec.extreme_clamp_low or prob > self._spec.extreme_clamp_high:
            return None
        return prob
