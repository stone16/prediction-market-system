from __future__ import annotations

import math

import pytest

from pms.controller.calibrators.shrinkage import LogitShrinkageCalibrator
from pms.core.interfaces import IPreCalibrator
from pms.strategies.projections import CalibrationContext, CalibrationSpec


def _context() -> CalibrationContext:
    return CalibrationContext(resolved_sample_count=0, model_id="llm")


def test_logit_shrinkage_compresses_extreme_default_probability() -> None:
    calibrator = LogitShrinkageCalibrator(CalibrationSpec())

    calibrated = calibrator.calibrate(0.95, context=_context())

    assert calibrated is not None
    assert calibrated <= 0.75
    assert calibrated == pytest.approx(0.737023, abs=1e-6)


def test_logit_shrinkage_keeps_midpoint_fixed_when_bias_is_zero() -> None:
    context = _context()

    for factor in (0.1, 0.35, 1.0, 2.0):
        calibrator = LogitShrinkageCalibrator(
            CalibrationSpec(shrinkage_factor=factor),
        )

        assert calibrator.calibrate(0.5, context=context) == 0.5


def test_logit_shrinkage_factor_controls_distance_from_midpoint() -> None:
    context = _context()
    raw_probability = 0.9

    weak = LogitShrinkageCalibrator(
        CalibrationSpec(shrinkage_factor=0.2),
    ).calibrate(raw_probability, context=context)
    strong = LogitShrinkageCalibrator(
        CalibrationSpec(shrinkage_factor=0.8),
    ).calibrate(raw_probability, context=context)

    assert weak is not None
    assert strong is not None
    assert 0.5 < weak < strong < raw_probability


@pytest.mark.parametrize(
    ("raw_probability", "expected"),
    [
        (0.0, 0.001),
        (-0.5, 0.001),
        (1.0, 0.999),
        (2.0, 0.999),
        (math.inf, 0.999),
        (-math.inf, 0.001),
        (math.nan, 0.001),
    ],
)
def test_logit_shrinkage_clamps_malformed_inputs(
    raw_probability: float,
    expected: float,
) -> None:
    calibrator = LogitShrinkageCalibrator(CalibrationSpec())

    assert calibrator.calibrate(raw_probability, context=_context()) == expected


def test_logit_shrinkage_bias_moves_probability_directionally() -> None:
    context = _context()
    downward = LogitShrinkageCalibrator(
        CalibrationSpec(shrinkage_bias=-0.4),
    ).calibrate(0.65, context=context)
    upward = LogitShrinkageCalibrator(
        CalibrationSpec(shrinkage_bias=0.4),
    ).calibrate(0.65, context=context)

    assert downward is not None
    assert upward is not None
    assert downward < upward


def test_logit_shrinkage_satisfies_pre_calibrator_protocol() -> None:
    calibrator: IPreCalibrator = LogitShrinkageCalibrator(CalibrationSpec())

    assert calibrator.calibrate(0.75, context=_context()) is not None
