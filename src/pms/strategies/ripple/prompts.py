"""Static prompt text used by ripple fixture tests."""

from __future__ import annotations

from pms.strategies.intents import StrategyCandidate


RIPPLE_JUDGE_SYSTEM_PROMPT = (
    "Evaluate fixture evidence only. Return approval when evidence is sufficient, "
    "confidence is above threshold, and no contradiction references are present."
)


def render_fixture_judgement_prompt(candidate: StrategyCandidate) -> str:
    evidence = ", ".join(candidate.evidence_refs)
    return (
        f"candidate={candidate.candidate_id}\n"
        f"market={candidate.market_id}\n"
        f"thesis={candidate.thesis}\n"
        f"evidence={evidence}"
    )
