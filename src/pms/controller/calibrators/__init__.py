"""Calibration implementations."""

from .extreme_clamp import ExtremeProbClamp
from .shrinkage import LogitShrinkageCalibrator

__all__ = ["ExtremeProbClamp", "LogitShrinkageCalibrator"]
