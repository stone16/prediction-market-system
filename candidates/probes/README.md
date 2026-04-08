# Candidate Probe Scripts

Each probe is a single source file that the harness's
`SubprocessRunnerFactory` runs *inside the candidate's isolated env*
during the survival gate. The probe imports the candidate tool, attempts
the smallest meaningful operation (typically "fetch one market"), prints
a one-line JSON summary on success, and exits with a stable code.

## Naming convention

```
candidates/probes/<safe_slug>_probe.<ext>
```

* `safe_slug` is `pms.tool_harness.subprocess_runner._safe_slug(candidate.name)` —
  lowercase letters, digits, and underscores only. Example:
  `py-clob-client` → `py_clob_client`.
* `ext` is `py` (Python), `ts` (TypeScript/Node), or `rs` (Rust).
* All probes for a given candidate share the same slug regardless of
  which module YAML refers to them. `pmxt_probe.ts` is shared by
  `pmxt_data_connector.yaml`, `pmxt_data_normalizer.yaml`, etc.

## Exit-code contract

| Code | Meaning |
|-----:|---------|
| `0`  | Success — the tool installed, the import worked, the operation returned data, and the probe printed a one-line JSON summary on stdout. |
| `2`  | **Missing credentials.** The tool is healthy but the probe could not run because required environment variables / API keys were absent. The harness reports this as a survival failure, but distinct from `1` so dashboards can recommend "set credentials" instead of "tool broken". |
| `1`  | Generic failure — network error, runtime exception, missing dependency, malformed response, anything else. |
| other| Any other non-zero code is treated like `1`. Avoid using `124` (the harness reserves it internally for timeouts) and `137` (SIGKILL). |

## What a probe MUST do

1. **Import the tool.** If the import fails, exit `1` with the
   exception message on stderr — the survival error string is the only
   diagnostic the runner surfaces.
2. **Check credentials early.** If the tool needs an API key/secret and
   the matching env var is missing, print a short message to stderr and
   call `sys.exit(2)`. Do **not** continue past missing creds — the
   exit code contract relies on credential failures being identifiable.
3. **Fetch one record.** Call the smallest available "fetch" method —
   one market, one trade, one symbol. The goal is to verify that the
   tool can talk to its remote API end-to-end, not to benchmark
   throughput.
4. **Print JSON to stdout on success.** A one-line summary like
   `{"ok": true, "market_id": "0xabc...", "fetched_at": "..."}`. The
   harness does not currently parse this, but P2-04's functional probes
   will, so keep it parseable.
5. **Exit 0 on success, 1 on any failure.**

## What a probe MUST NOT do

* Print credentials, signed headers, or wallet addresses to stdout/stderr
  (the harness captures both streams and includes them in error reports).
* Write files outside its temp env.
* Run for more than `survival_gate.<item>.timeout_seconds` from the
  benchmark (the harness will SIGKILL the child after that).
* Use `print()` for diagnostics on stderr — prefer `sys.stderr.write`
  or `logging` so the JSON-on-stdout contract stays clean.
* Catch broad exceptions that mask failures. Always re-raise or exit 1.

## Adding a new probe

1. Pick the canonical candidate name and run it through `_safe_slug` to
   get the filename stem.
2. Decide the language based on the candidate's `language:` field —
   the harness only looks for `.py` (python), `.ts` (typescript), and
   `.rs` (rust) extensions. Java/Go/etc. need a runner extension first.
3. Write the probe following the contract above.
4. (Optional) Add an `@pytest.mark.integration` test that runs the probe
   end-to-end via `SubprocessRunnerFactory`. See
   `tests/test_subprocess_runner.py::test_py_clob_client_probe_end_to_end`
   for the template.
