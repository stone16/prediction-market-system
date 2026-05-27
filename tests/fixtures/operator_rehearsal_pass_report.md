# Operator Approval Rehearsal - 2026-05-25

## Report Provenance

| Field | Value |
|---|---|
| generated_by | scripts/rehearse_first_order.py |
| generated_at | 2026-05-25T00:00:00+00:00 |
| artifact_mode | persisted |
| output_path | docs/live/operator-rehearsal-report.md |

## Operator Approval Rehearsal

**Decision:** PASS

| Check | Status | Detail |
|---|---|---|
| approval_denied | PASS | observed before approval file existed |
| approval_matched | PASS | approval JSON matched preview |
| approval_consumed | PASS | approval JSON and sidecar were unlinked |
| strict_sidecar_provenance | PASS | strict gate required sidecar approver_id, timestamp, and approval hash |
| fresh_approval_required | PASS | every-order mode denied the next submit after approval consume |
| unexpected_events | PASS | events=['approval_denied', 'approval_matched', 'approval_consumed', 'approval_denied'] |
| operator_id | PASS | rehearsal-operator |
