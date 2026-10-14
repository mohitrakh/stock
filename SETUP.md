# Setting up a machine to build, test and measure

Every test and measurement in milestones 24 and 25 ran on the "office machine", a Linux host with
Docker, inside a container image called `stock-rust`. This file lists every step to set up another
Linux machine with Docker (a home PC, for example) the same way, so that the same scripts run there
unchanged. The scripts are in `docs/milestones/*/performance/scripts/`, and `tools/` holds the
image and the test script.

The crate uses Unix-only APIs. It does not build on Windows, but it builds in Linux containers.

## 1. What you need

- Linux with Docker (or Docker Desktop running Linux containers), plus `git`, `bash`, `curl`,
  `python3` (the mutation scripts use it) and `awk`.
- About 10 GB of free disk for the image, the cargo caches and the build volumes.
- Network access the first time, to pull `rust:latest` and `postgres:16`, and the crates.

## 2. The layout the scripts expect

The scripts were written for the office machine and use fixed paths:

| Path | What | Used by |
|---|---|---|
| `~/stock-m25` | the repository, on branch `dev-rakh` | milestone 25 scripts (`MIGRATIONS`, the mutate script) |
| `~/stock-m24` | the same repository (a symlink is fine) | milestone 24 scripts (`MIGRATIONS` default, the mutate script) |
| `~/stock-scripts/` | every script, `check.sh`, and the built binaries | every script mounts it at `/scripts` |
| `~/m24-out/`, `~/m25-out/` | where the outputs were written | your own redirections |

```bash
git clone <this repository's URL> ~/stock-m25
cd ~/stock-m25 && git checkout dev-rakh
ln -s ~/stock-m25 ~/stock-m24
mkdir -p ~/stock-scripts ~/m24-out ~/m25-out
cp tools/check.sh docs/milestones/*/performance/scripts/* ~/stock-scripts/
chmod +x ~/stock-scripts/*.sh
```

## 3. The image, the network and the database

```bash
docker build -t stock-rust ~/stock-m25/tools
docker network create stock-test-net
docker run -d --name stock-test-pg --network stock-test-net -e POSTGRES_PASSWORD=bench postgres:16
```

- `stock-rust` is `rust:latest` plus the PostgreSQL client, `curl` and `rustfmt`
  (`tools/Dockerfile`). The office image had cargo 1.98.1. A newer toolchain should build the same
  code; if anything differs, pin it with `FROM rust:1.98.1`.
- `stock-test-pg` is the PostgreSQL every measured exchange uses for its `users` table. The scripts
  start it if it is stopped, and create and drop their own databases in it (`exchange_m24`).
- The cargo volumes `stock-cargo-registry`, `stock-cargo-target` and `stock-cargo-target-mut` are
  created automatically the first time a script names them.

## 4. Build and test

The tests (normal build, then the `stage-timing` feature build):

```bash
docker run --rm -v ~/stock-m25:/src -v ~/stock-scripts:/scripts \
  -v stock-cargo-registry:/usr/local/cargo/registry -v stock-cargo-target:/target \
  -e CARGO_TARGET_DIR=/target -w /src stock-rust bash /scripts/check.sh
docker run --rm -v ~/stock-m25:/src -v stock-cargo-registry:/usr/local/cargo/registry \
  -v stock-cargo-target:/target -e CARGO_TARGET_DIR=/target -w /src stock-rust \
  cargo test --locked --features stage-timing
```

Expected at milestone 25 part 1: fmt exit 0; 213 unit tests passed in the normal build and 216 in
the feature build (1 ignored in each), plus the integration tests. The three PostgreSQL acceptance
tests are ignored unless `REPORTER_TEST_DATABASE_URL` is set.

## 5. The binaries the scripts run

The scripts run binaries from `~/stock-scripts/` by name. Build each one from the right commit,
using a separate git worktree per commit:

| Binary name | Commit | Build | What it is |
|---|---|---|---|
| `stock-m25p1` | milestone 25 part 1 (this commit or later) | normal | the milestone 25 "normal" build |
| `stock-m25p1-timed` | the same | `--features stage-timing` | the stage-timing build |
| `stock-m24p2` | `28e085a` | normal | milestone 24 after Part 2 |
| `stock-m24p1` | `27ce277` | normal | the milestone 23 exchange: milestone 24's "before" |

```bash
build() { # TREE BINARY-NAME [CARGO-FLAGS...]
  local tree=$1 name=$2; shift 2
  docker run --rm -v "$tree":/src -v ~/stock-scripts:/scripts \
    -v stock-cargo-registry:/usr/local/cargo/registry -v stock-cargo-target:/target \
    -e CARGO_TARGET_DIR=/target -w /src stock-rust \
    bash -c "touch src/main.rs && cargo build --release --locked $* && cp /target/release/stock /scripts/$name"
}
cd ~/stock-m25
git worktree add ~/stock-build-m24 28e085a
git worktree add ~/stock-build-m23 27ce277
build ~/stock-m25 stock-m25p1
build ~/stock-m25 stock-m25p1-timed --features stage-timing
build ~/stock-build-m24 stock-m24p2
build ~/stock-build-m23 stock-m24p1
```

(The `touch` makes cargo rebuild when one target volume is shared between different trees mounted
at the same `/src`. Without it, cargo can reuse a build of another tree whose files look newer.
`docs/milestones/25-latency/part-1-stage-timing.md` describes the trap.)

**A note on `stock-m24p1`.** In milestone 24 it was the milestone 23 exchange *plus* Part 1's new
tools (`--http-bench`, `--sync-probe`, `--bcrypt-probe`). That exact build is not a commit: Part 1
and Part 2 were committed together. The commit `27ce277` gives the same exchange behaviour, which
is what those measurements compare, but it does not have the tools. So:
- for the HTTP generator, set `GEN=/scripts/stock-m24p2`;
- for the probes (`m24p1-probes.sh`), run them with `BIN=stock-m24p2`. The probes measure the disk
  and bcrypt, not the exchange.

## 6. Running the measurements

Everything runs from the host. Every measured run prints the host's load average and container
count. Redirect each script into a file:

| Script | Command | Binaries it uses |
|---|---|---|
| `m24p1-probes.sh` | `BIN=stock-m24p2 bash ~/stock-scripts/m24p1-probes.sh > ~/m24-out/m24p1-probes.txt 2>&1` | probes only |
| `m24p1-enum.sh` | `bash ~/stock-scripts/m24p1-enum.sh > ~/m24-out/m24p1-enum.txt 2>&1` | `stock-m24p1` |
| `m24p1-http.sh` | `GEN=/scripts/stock-m24p2 bash ~/stock-scripts/m24p1-http.sh 3 > ~/m24-out/m24p1-http.txt 2>&1` | `stock-m24p1`, generator `stock-m24p2` |
| `m24p1-control.sh` | `GEN=/scripts/stock-m24p2 bash ~/stock-scripts/m24p1-control.sh 3 > ...` | the same |
| `m24p1-stall.sh` | `GEN=/scripts/stock-m24p2 bash ~/stock-scripts/m24p1-stall.sh 2 > ...` | the same |
| `m24p2-enum.sh` | `bash ~/stock-scripts/m24p2-enum.sh > ...` | `stock-m24p1`, `stock-m24p2` |
| `m24p2-stall.sh` | `LOGINS="0 4 16" bash ~/stock-scripts/m24p2-stall.sh 2 > ...` | both; generator `stock-m24p2` |
| `m24p2-mutate.sh` | `bash ~/stock-scripts/m24p2-mutate.sh > ...` | builds from `~/stock-m24` |
| `m25p1-overhead.sh` | `bash ~/stock-scripts/m25p1-overhead.sh 3 > ~/m25-out/m25p1-overhead.txt 2>&1` | `stock-m25p1`, `stock-m25p1-timed` |
| `m25p1-overhead-reversed.sh` | the same, `-reversed` | the same |
| `m25p1-stages.sh` | `bash ~/stock-scripts/m25p1-stages.sh 3 > ~/m25-out/m25p1-stages.txt 2>&1` | `stock-m25p1-timed`, generator `stock-m25p1` |
| `m25p1-mutate.sh` | `bash ~/stock-scripts/m25p1-mutate.sh > ~/m25-out/m25p1-mutate.txt 2>&1` | builds from `~/stock-m25` |

Then summarize the stage runs the same way the documents did:

```bash
cd ~/stock-m25/docs/milestones/25-latency/performance
awk -f scripts/m25p1-summarize.awk ~/m25-out/m25p1-stages.txt > ~/m25-out/m25p1-stages-summary.txt
awk -f scripts/m25p1-decompose.awk ~/m25-out/m25p1-stages-summary.txt
```

Running every script takes roughly two hours.

## 7. Before you compare numbers

- **Record the machine with its results.** The office figures carry the machine's description
  (`docs/milestones/24-gateway/decisions.md`). Write down yours:

  ```bash
  lscpu | grep -E "Model name|^CPU\(s\)|Thread|Core"; free -h
  lsblk -d -o NAME,MODEL,ROTA,SIZE | grep -v loop
  docker ps -q | wc -l; uptime
  ```

- **Do not compare across machines or days.** Numbers from another machine are a separate dataset.
  Even on one machine, the office runs varied by a factor of two between days and by about 10%
  within an hour. Compare only runs made on the same machine, on the same day, interleaved.
- **Alternate the order of the builds** in before-and-after comparisons. On the office machine the
  second of two back-to-back runs was about 9.5% faster, whichever build it was
  (`docs/milestones/25-latency/performance/01-instrumentation-overhead.md`).
- **A quiet machine is the point of measuring at home.** Note how many other containers and
  processes are running. The office machine had about 29 containers besides the measured ones.
- **Add your results; never overwrite.** Put them beside the office results, for example
  `performance/results/home-m25p1-stages.txt`, with a short note in the milestone's performance
  file saying which machine they came from.
