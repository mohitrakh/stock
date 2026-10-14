# Working on this repository

A learning stock exchange in Rust. Learning is the main goal: every change is written down in depth so
it can be studied later without the conversation that produced it.

## Before anything

1. Read `PROJECT_DIRECTION.md` (the canonical journal), then `EXCHANGE_PIPELINE_TODO.md`.
2. Read the current milestone's folder in `docs/milestones/`.
3. Verify code and test results before trusting old notes.
4. Discuss architecture before implementing. The owner confirms each milestone's plan and decisions.

## Mandatory: document every milestone in depth (owner's rule)

Every milestone has its own folder, `docs/milestones/NN-name/`. Nothing a milestone does may live only
in chat, in a commit message or in a reviewer's notes. The folder holds:

- `README.md`: the reading order, the status of each part, and a one-line summary of every file.
- `00-selection.md`: why this milestone, with every number's source and arithmetic.
- `01-theory.md`: the background needed to read the code cold, from first principles, with worked
  examples.
- `decisions.md`: every owner decision, dated, with the options shown.
- `glossary.md`: every term used.
- One `part-N-name.md` per part:
  - what changed, file by file and function by function;
  - why each choice was made, and every option rejected;
  - **every test written or run: what it checks, its exact command, and its result**, including
    mutation checks (remove the fix and show that the test fails);
  - live runs with their exact commands;
  - the independent review's findings and how each was fixed;
  - compatibility.
- `performance/NN-name.md`: one file per measurement and per optimization:
  - the question it answers;
  - the method: machine, load, binary, exact commands, rates, number of runs;
  - **the results before and after each optimization**, in tables;
  - the arithmetic of every derived number, shown step by step;
  - the noise between runs;
  - what the numbers prove and what they do not.
- `performance/results/`: the raw output of every run, unedited, as `.txt` (`.gitignore` ignores
  `*.log`).
- `performance/scripts/`: every script that produced a result.

Rules for every file:
- Every number gives its source and its unit (µs and ms of time; never "cycles" for time).
- Every derived figure shows its arithmetic.
- An estimate or an inference says that it is one.
- A mistake found later is corrected in place, and the correction says what was wrong.
- Update `PROJECT_DIRECTION.md` and `EXCHANGE_PIPELINE_TODO.md` after every part.

## Building, testing and measuring

The crate uses Unix-only APIs and does not build on Windows. `SETUP.md` lists every step to prepare
any Linux machine with Docker (the image in `tools/Dockerfile`, `tools/check.sh`, the network, the
database, the binaries, and how to run each script). The office Ubuntu machine is set up that way
(`ssh ubuntu@10.0.127.253`, key authentication):
- copy the tree to `~/stock-m24` or a milestone-specific folder;
- build inside the `stock-rust` image, with the `stock-cargo-registry` and `stock-cargo-target`
  volumes;
- the scripts live in `~/stock-scripts/` (originals in the milestone's `performance/scripts/`);
- `~/stock-scripts/check.sh` runs `cargo fmt -- --check` and `cargo test --locked`.

The office machine is shared with about 30 other containers, and its speed varies from day to day.
Compare measurements only when they were taken on the same day, interleaved, over several runs.

A part is done only when all of these hold:
- `cargo fmt -- --check` and `cargo test --locked` pass;
- the documents above are written;
- an independent review has run and its findings are fixed.
