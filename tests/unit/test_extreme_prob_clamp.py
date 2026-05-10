from __future__ import annotations

import pytest

from pms.controller.calibrators.extreme_clamp import ExtremeProbClamp
from pms.core.interfaces import IPreCalibrator
from pms.strategies.projections import CalibrationContext, CalibrationSpec


def _context(sample_count: int) -> CalibrationContext:
    return CalibrationContext(resolved_sample_count=sample_count, model_id="llm")


def test_extreme_prob_clamp_passes_in_band_probabilities_below_threshold() -> None:
    clamp = ExtremeProbClamp(CalibrationSpec())

    assert clamp.calibrate(0.08, context=_context(0)) == 0.08
    assert clamp.calibrate(0.5, context=_context(0)) == 0.5
    assert clamp.calibrate(0.92, context=_context(0)) == 0.92


@pytest.mark.parametrize("probability", [0.01, 0.079999, 0.920001, 0.99])
def test_extreme_prob_clamp_rejects_out_of_band_probabilities_below_threshold(
    probability: float,
) -> None:
    clamp = ExtremeProbClamp(CalibrationSpec())

    assert clamp.calibrate(probability, context=_context(499)) is None


@pytest.mark.parametrize("probability", [0.01, 0.079999, 0.920001, 0.99])
def test_extreme_prob_clamp_passes_out_of_band_probabilities_at_threshold(
    probability: float,
) -> None:
    clamp = ExtremeProbClamp(CalibrationSpec())

    assert clamp.calibrate(probability, context=_context(500)) == probability


def test_extreme_prob_clamp_uses_configurable_band_and_threshold() -> None:
    clamp = ExtremeProbClamp(
        CalibrationSpec(
            extreme_clamp_low=0.2,
            extreme_clamp_high=0.8,
            min_resolved_for_extreme=20,
        )
    )

    assert clamp.calibrate(0.19, context=_context(19)) is None
    assert clamp.calibrate(0.2, context=_context(19)) == 0.2
    assert clamp.calibrate(0.8, context=_context(19)) == 0.8
    assert clamp.calibrate(0.81, context=_context(19)) is None
    assert clamp.calibrate(0.81, context=_context(20)) == 0.81


def test_extreme_prob_clamp_satisfies_pre_calibrator_protocol() -> None:
    calibrator: IPreCalibrator = ExtremeProbClamp(CalibrationSpec())

    assert calibrator.calibrate(0.5, context=_context(0)) == 0.5
