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

## Status

| Part | What | Status |
|---|---|---|
| — | The plan and decisions 1–6 | accepted by the owner as recommended (2026-10-19) |
| 1 | The public L2 feed, in the MDP | complete, 2026-10-19 |
| 2 | The private feed, in its own process | not started |
| 3 | Measurement | not started |
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

## Conventions

The repository's `CLAUDE.md`:
- every number gives its source and unit;
- every derived figure shows its arithmetic;
- estimates and inferences say so;
- every test is recorded with its command and result;
- every measurement is recorded with its numbers before and after;
- raw results are `.txt` files.
