# Milestone 25 — Latency

The specification is in `PROJECT_DIRECTION.md`, section "25. Latency". This folder holds everything
else: why the milestone was chosen, the theory, every part's implementation, every test and its
result, every measurement and optimization with its before-and-after numbers, the owner's
decisions, and a glossary. It is written to be studied cold.

## Reading order

1. `00-selection.md`: why latency, and why the parts come in this order, with the numbers known at
   the start.
2. `01-theory.md`:
   - stages and the critical path;
   - why means add up and medians do not;
   - group commit as a queue, and the inspection paradox;
   - the tail;
   - instrumentation cost;
   - pipelining, and what durability forbids it;
   - record formats.
3. `decisions.md`.
4. The parts: `part-1-stage-timing.md`, then the later parts as they are written.
5. `performance/`: one file per measurement. `performance/results/` holds the raw output, and
   `performance/scripts/` the scripts.
6. `glossary.md`.

## Status

| Part | What | Status |
|---|---|---|
| 1 | Where the time goes: stage-by-stage timing (`--features stage-timing`) | complete, 2026-10-14 |
| 2 | A pipelined journal sync | not started |
| 3 | A binary record format, only if Part 1 shows it matters | not started |
| 4 | Final measurement against the milestone 24 part 2 binary | not started |

## Files

| File | What it holds |
|---|---|
| `00-selection.md` | Why latency, the numbers known at the start, why this order of parts. |
| `01-theory.md` | Eight sections of theory, with worked examples. |
| `decisions.md` | The owner's decisions, and the design choices made inside them. |
| `glossary.md` | Definitions. |
| `part-1-stage-timing.md` | The `stage-timing` feature, every code change, every test with its result, the mutation check, the review. |
| `performance/01-instrumentation-overhead.md` | What the timing costs: nothing measurable at 5,000/s, about 3% at the maximum rate; a run-order effect of about 9.5%. |
| `performance/02-where-the-time-goes.md` | The stage breakdown in ten configurations, the means check, the tail, the maximum-rate cycle, and projections for Parts 2 and 3. |

## Conventions

The same as milestone 24, now written in the repository's `CLAUDE.md`:
- every number gives its source and unit;
- every derived figure shows its arithmetic;
- estimates and inferences say so;
- every test is recorded with its command and result;
- every optimization is recorded with its numbers before and after;
- raw results are `.txt` files.
