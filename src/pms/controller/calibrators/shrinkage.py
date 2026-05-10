from __future__ import annotations

import math

from pms.strategies.projections import CalibrationContext, CalibrationSpec


class LogitShrinkageCalibrator:
    def __init__(self, spec: CalibrationSpec) -> None:
        self._spec = spec

    def calibrate(
        self,
        prob: float,
        *,
        context: CalibrationContext,
    ) -> float | None:
        del context
        if not math.isfinite(prob):
            return 0.999 if prob > 0.0 else 0.001
        if prob <= 0.0:
            return 0.001
        if prob >= 1.0:
            return 0.999

        logit = math.log(prob / (1.0 - prob))
        shrunk_logit = self._spec.shrinkage_factor * logit + self._spec.shrinkage_bias
        return 1.0 / (1.0 + math.exp(-shrunk_logit))
