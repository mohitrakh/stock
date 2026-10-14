# Milestone 24 — Gateway and Trust Boundary

The specification is in `PROJECT_DIRECTION.md`, section "24. Gateway and Trust Boundary". This
folder holds everything else about the milestone: why it was chosen, the theory needed to read it,
what each part changed and why, every measurement with its raw output, the owner's decisions, and a
glossary. Learning is the main goal of this project, so these files are written to be studied cold,
months later, without the conversations that produced them.

## Reading order

1. `00-selection.md`: why this milestone was chosen over latency. The trust holes found in the code,
   and the latency analysis with every number's source and arithmetic.
2. `01-theory.md`: the background, from first principles, with worked examples:
   - trust boundaries; authentication and authorization;
   - why replay decides which checks are journaled;
   - reference data; rate-limiting algorithms;
   - bcrypt; Tokio's threads and blocking;
   - timing side channels;
   - open-loop measurement; `fsync` and drive caches; containers.
3. `decisions.md`: what the owner decided, when, and the options shown.
4. The parts, in order: `part-1-baseline.md`, then `part-2-...` and so on as they are written.
5. `performance/`: one file per measurement. `performance/results/` holds the raw output, and
   `performance/scripts/` the scripts that produced it.
6. `glossary.md`: every term, alphabetically, pointing back to the theory.

## Status

| Part | What | Status |
|---|---|---|
| 1 | Baseline on the milestone 23 gateway: HTTP latency, bcrypt cost, login stall, raw sync, login enumeration | complete, 2026-10-12 |
| 2 | Login and registration hardening | complete, 2026-10-12 |
| 3 | Journaled product list | dropped by the owner, 2026-10-14 |
| 4 | Deposits and risk limits by the operator only | dropped by the owner, 2026-10-14 |
| 5 | Per-user book share and rate limits | dropped by the owner, 2026-10-14 |
| 6 | Final measurement and live abuse tests | dropped by the owner, 2026-10-14 |

**The milestone closed on 2026-10-14 after Part 2.** This is a learning project, and protection
against abusive customers is not one of its goals. The reasoning is in `decisions.md`.

## Files

| File | What it holds |
|---|---|
| `00-selection.md` | The reasoning for choosing this milestone, in full. |
| `01-theory.md` | Twelve sections of background theory. |
| `decisions.md` | The selection, the measurement machine, and decisions 1–7, each dated, with its options. |
| `glossary.md` | Definitions. |
| `part-1-baseline.md` | The measurement tools built (`--http-bench`, `--sync-probe`, `--bcrypt-probe`), how and why, the tests, and how the runs were made. |
| `performance/01-http-vs-in-process.md` | Order latency through HTTP against the same orders in-process, alone and replicated, at 1,000 and 5,000 orders/s, with the `/health` control. |
| `performance/02-bcrypt-cost.md` | One bcrypt at cost 12, the doubling per cost step, and 1–8 threads at once. |
| `performance/03-login-stall.md` | Order latency while 0 to 16 clients log in back to back. |
| `performance/04-raw-sync.md` | The office SSD's flush latency, against tmpfs. |
| `performance/05-login-enumeration.md` | What a login's body and timing reveal about an account. |
| `part-2-login.md` | bcrypt off the worker threads behind a cap; one uniform failed-login answer with a dummy-hash verify; the JWT key read once; the placeholder route removed; the generator's busy-loop fix; tests and mutation checks. |
| `performance/06-login-after-part-2.md` | The enumeration check on both binaries: the body and the time no longer reveal an account. |
| `performance/07-login-stall-after-part-2.md` | The login stall on both binaries, the same hour: the stall is gone, and logins are capped and wait instead. |

## Conventions in these files

- Every number gives its source: this milestone's raw output (file and section), an earlier
  document (file and section), or an outside reference.
- Every derived figure shows its arithmetic.
- An estimate says it is an estimate.
- Units are explicit: µs and ms of time. "CPU time" means time a thread spent running, as opposed to
  waiting. Cycles are never used to mean time.
- Raw results are `.txt` files: `.gitignore` ignores `*.log`.
