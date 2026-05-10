from __future__ import annotations

import inspect
from typing import get_type_hints

from pms.core.interfaces import IPreCalibrator
from pms.strategies.projections import CalibrationContext


def test_pre_calibrator_protocol_signature_matches_projection_context() -> None:
    signature = inspect.signature(IPreCalibrator.calibrate)
    parameters = list(signature.parameters.values())

    assert [parameter.name for parameter in parameters] == [
        "self",
        "prob",
        "context",
    ]
    assert parameters[1].annotation == "float"
    assert parameters[2].kind is inspect.Parameter.KEYWORD_ONLY

    hints = get_type_hints(IPreCalibrator.calibrate)

    assert hints == {
        "prob": float,
        "context": CalibrationContext,
        "return": float | None,
    }


def test_runtime_pre_calibrator_implementation_can_reject_sizing() -> None:
    class RejectingPreCalibrator:
        def calibrate(
            self,
            prob: float,
            *,
            context: CalibrationContext,
        ) -> float | None:
            del context
            if prob > 0.92:
                return None
            return prob

    calibrator: IPreCalibrator = RejectingPreCalibrator()

    assert calibrator.calibrate(
        0.95,
        context=CalibrationContext(resolved_sample_count=0, model_id="llm"),
    ) is None
    assert calibrator.calibrate(
        0.75,
        context=CalibrationContext(resolved_sample_count=0, model_id="llm"),
    ) == 0.75
