# Milestone 26 — Pushed Updates

The specification is in `PROJECT_DIRECTION.md`, section "26. Pushed Updates". This folder holds
everything else: why the milestone was chosen, the theory, every part's implementation, every test
and its result, every measurement with its numbers, the owner's decisions, and a glossary. It is
written to be studied cold.

## Reading order

1. `00-selection.md`: why pushed updates, and why the parts come in this order.
2. `01-theory.md`, in eight sections:
   - polling against pushing;
   - the WebSocket protocol;
   - snapshot plus deltas and sequence numbers;
   - the subscribe race;
   - fan-out, backpressure and slow consumers;
   - durable before visible for a feed;
   - privacy and long-lived authentication;
   - measuring push latency.
3. `decisions.md`.
4. The parts, as they are written.
5. `performance/`: one file per measurement, with raw output in `performance/results/` and the
   scripts in `performance/scripts/`.
6. `glossary.md`.
7. `notebook.md`: how the work actually went, dead ends included.

## Status

| Part | What | Status |
|---|---|---|
| — | The plan and decisions 1–6 | accepted by the owner as recommended (2026-10-19) |
| 1 | The public L2 feed, in the MDP | complete, 2026-10-19 |
| 2 | The private feed, in its own process | complete, 2026-10-20 |
| 3 | Measurement | complete, 2026-10-20 (`performance/01-feeds-measured.md`); the 10% criterion is not met, explained as CPU contention on a two-core machine, and the owner accepted the explanation |
| 4 | Final review and close | not started |

## Files

| File | What it holds |
|---|---|
| `00-selection.md` | Why this milestone, what is known at the start, why this order of parts. |
| `01-theory.md` | Eight sections of theory, with worked examples. |
| `decisions.md` | The owner's decisions, and the design choices made inside them. |
| `glossary.md` | Definitions. |
| `part-1-public-book-feed.md` | The WebSocket book feed: the protocol, every code change, the tests (an oracle at every sequence, durability, slow consumers, halt, silence, resubscription, the shipped executable), the mutation check, two review rounds. |
| `performance/scripts/m26p1-mutate.sh`, `performance/results/m26p1-mutate*.txt` | Part 1's mutation check and its runs. |
| `notebook.md` | The lab notebook: every problem, wrong guess, pilot failure, review finding and aborted run, in the order they happened, with why and what was done. |
| `part-2-private-feed.md` | The private feed: who is told what, the protocol, authentication, every code change, the tests (an oracle against the core with colliding ids and other users' cancellations, drift, restart, durability, the socket, the shipped executable), the mutation check, the review. |
| `performance/scripts/m26p2-mutate.sh`, `performance/results/m26p2-mutate*.txt` | Part 2's mutation check and its runs. |
| `part-3-measurement.md` | The measurement tool (`--feed-bench`, `--start-at` for both generators), how a message is linked to its order, the scripts, the plan of sections A–E, the problems found while building it. |
| `performance/scripts/m26p3-lib.sh`, `m26p3-measure.sh`, `m26p3-cpu.awk`, `m26p3-summarize.awk` | Part 3's measurement: containers and runs, the plan, each container's CPU, the summary. |
| `performance/01-feeds-measured.md` | Part 3's results: push latency against clients, keeping up at the maximum, the exchange with the feeds against without, pinned cores, slow consumers. |
| `performance/results/m26p3-*.txt` | Part 3's raw output: the full measurement and its summary, the pilots, the aborted first measurement. |

## Conventions

The repository's `CLAUDE.md`:
- every number gives its source and unit;
- every derived figure shows its arithmetic;
- estimates and inferences say so;
- every test is recorded with its command and result;
- every measurement is recorded with its numbers before and after;
- raw results are `.txt` files.
