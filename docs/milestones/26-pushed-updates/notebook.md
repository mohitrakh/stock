# Milestone 26 — Lab notebook

Everything that went wrong, surprised us, or changed course while this milestone was built, in the
order it happened: what was seen, why it happened, and what was done. The part files say what was
built and proved; this file says how the work actually went, including the dead ends. The owner
asked for it on 2026-10-20 ("i want everything documentaize so i can read everything later"), so
entries before that date were written then, from the working conversation; entries after it are
written as they happen.

Each entry: **what was seen**, **why**, **what was done**, and where it is recorded in more detail.

---

## Part 1 — the public book feed (2026-10-19)

### 1. `watch::Ref` cannot be held across an `.await` in `select!`

- **Seen.** The first version of a connection's loop waited for the halt signal with
  `halted.wait_for(|halted| *halted)` inside `tokio::select!`. It did not compile: the future
  returned a `watch::Ref`, a guard over the channel's value, and that guard is not `Send`, so the
  task could not move between threads.
- **Why.** A `Ref` holds a read lock on the value; holding a lock across an `.await` would block the
  sender. Tokio makes it `!Send` so that the compiler refuses.
- **Done.** `halted.changed()` in the `select!`, then `*halted.borrow_and_update()` after it, with
  no guard alive across the await.

### 2. The halt mutation hung for 600 s

- **Seen.** In the first mutation run, the mutation "a halt does not reach idle connections" printed
  no result: the test never finished, and the script's 600 s limit killed it.
- **Why.** The test waited for a close with a 20 s timeout per message, but the server sent a ping
  every 15 s. Each ping arrived inside the 20 s, reset the wait, and the test waited forever.
- **Done.** The halt test pings every 60 s; `close_code` has one overall 20 s deadline instead of
  one per message; the script prints "no result (killed after 600 s)" instead of an empty line.
  Recorded in `part-1-public-book-feed.md` ("Mutation check").

### 3. A closed connection gave `None` instead of 1001

- **Seen.** The new silence test (a client that never answers pings is closed with 1001) received no
  close code at all.
- **Why.** The server sent its close frame and dropped the socket at once. The client had sent
  pongs that the server never read. Closing a TCP socket with unread data in its receive buffer
  makes the kernel send a reset (RST) instead of a normal close, and the reset can destroy the
  close frame before the client reads it.
- **Done.** The WebSocket closing handshake: after its close frame the server reads, within the
  write deadline, until the client's own close frame. Later moved into `book_feed::finish`, shared
  with the private feed. Recorded in `part-1-public-book-feed.md` ("Independent review").

### 4. Review findings

Two review rounds found: no resubscription test; an unsubscribe that aborted the forwarder without
waiting for it; rings never freed, on two paths; pings that detected nothing; timing tests that
assumed small loopback buffers; a queue-sizing comment that was per symbol instead of per
connection. All fixed; each is in `part-1-public-book-feed.md` ("Independent review").

### 5. Tooling on the Windows side

- **Heredocs containing apostrophes** broke in the shell wrapper used to drive the work. Scripts are
  written to files first, then run.
- **Windows Python cannot see Git Bash's `/tmp`.** Scratch files go in a directory both see.
- **The command proxy (`rtk`) reformats `grep` output**, and `rg` is not installed; searches use
  Python or the editor's search instead.
- **Mutation checks need their own cargo target volume** (`stock-cargo-target-mut`), or a mutated
  build overwrites the real one; trees are copied with `cp -rL` so a symlinked tree is copied, not
  the link; `TREE=` points the script at an uncommitted copy.

---

## Part 2 — the private feed (2026-10-20)

### 6. The halt `watch` channel was redundant in the private feed

- **Seen.** Writing the mutation list, "a halt does not reach connections" (remove the `watch`
  signal) looked like it would survive: no test could fail.
- **Why.** In the book feed a connection can be idle, with no subscription and so no ring; only the
  `watch` reaches it. A private connection always holds a receiver once it is served, so dropping
  the rings (`users.clear()`) already ends every forwarder with `Closed`, and the connection
  closes with 1011.
- **Done.** The `watch` was removed from `UserHub`; the mutation became "a halt leaves the rings
  open", which fails its test. Recorded in `part-2-private-feed.md` (`UserHub`).

### 7. All 12 tests passed the first time

- **Seen.** The first build of the private feed passed every new test.
- **Why it mattered.** Passing at once can mean the tests are too weak. That is what the mutation
  check is for.
- **Done.** 13 mutations, each failing its test (the first mutation run), before trusting them.

### 8. The race test took 60 s

- **Seen.** `a_join_racing_the_follower...` took 60 s, then 50 s.
- **First guess, wrong:** one journal sync per record (5,000 `fsync`s). Writing the records with one
  sync for all of them did not help (48 s).
- **Second guess, wrong:** the stream's cache too small, so the reader went to the journal. A
  larger cache did not help either.
- **Why, in the end.** The check, not the race: 1,000 receivers each drained up to 5,000 messages
  and parsed each as JSON, 5 million parses.
- **Done.** 100 joins over 2,000 batches: 2.3 s.

### 9. The independent review's findings

No cross-user leak and no gap after `ready` were found. Fixed: a ring per user sized for a whole
book (about 240 KB each; now 1,024 slots, about 60 KB, estimates); a token inside jsonwebtoken's
60 s leeway got `ready` and was closed at once; axum's 64 MiB default message size before
authentication; a vacuous assertion; no test that the join and `ready`'s sequence are one step;
an oracle that checked every message told was right but not that every expected message was told.
Each is in `part-2-private-feed.md` ("Independent review").

### 10. A mutation survived: the racing test is timing

- **Seen.** In the second mutation run, "`ready`'s sequence and the join under separate locks"
  passed its test (`a_join_racing_the_follower...`). In the third run the same mutation failed the
  same test.
- **Why.** The window the mutation opens is a few microseconds; whether a batch lands in it is luck.
- **Done.** A deterministic test, `a_joiner_holds_the_view_until_it_has_joined`: hold the hub, and
  check that the joiner still holds the view lock. **Lesson:** a locking or atomicity guarantee needs
  a test that holds one lock and checks the other, not only a race; a race test is an end-to-end
  extra.

### 11. Documents ran ahead of the evidence

- **Seen.** The verifier found the README and the TODO marking Part 2 complete, and
  `PROJECT_DIRECTION.md` claiming "17 mutations, each failing its test", before the final mutation
  run had reported. The second run then showed one survivor (entry 10).
- **Done.** Status stays "in progress" until the evidence is in; a placeholder holds the place of a
  number not yet measured. Four verifier rounds in all.

### 12. `rustfmt` ran on the build machine, not here

- **Seen.** `cargo fmt` on the office machine reformatted the remote copy, which the next sync
  overwrote.
- **Done.** `cargo fmt --check` prints the diff remotely, and each reflow is applied here, so the
  repository's files are the only source.

---

## Part 3 — measurement (2026-10-20)

### 13. The open-file limit stalled the exchange

(The pilots of entries 13 to 15 printed to the terminal only; their figures here are copied from
that output. Later pilots were saved to `results/m26p3-pilot-*.txt`.)

- **Seen.** The first pilot with 1,000 clients of each feed failed: `Too many open files (os error
  24)` in the feed bench. With the limit raised for the feed containers only, the next pilot at
  5,000 orders/s had 1,786 connection errors in the HTTP generator, 6 orders unanswered after
  120 s, and an 11 s maximum latency.
- **Why.** The image's default soft limit is 1,024 open files (hard limit 524,288; `ulimit -n` in
  the image). The saturated host slowed replies, so the open-loop generator held more than 1,024
  connections, and the exchange's accepts failed with `EMFILE`. A server whose accept fails pauses
  and retries, so every order stalled with it.
- **Done.** Every container of these runs gets `--ulimit nofile=65536:65536`, the exchange and the
  generator included. A limit changes nothing until it is reached; milestone 25's runs never reached
  it. Recorded in `part-3-measurement.md` ("Problems found while building it").

### 14. The feed bench stopped halfway through runs

- **Seen.** With 1,000 clients, the feed bench reported only part of the orders: its last message
  came 11–12 s into a 20 s run, and the last fifth of the orders had no latency at all.
- **First guess, wrong:** the MDP stalled or fell behind. Its log showed nothing.
- **Why.** The bench's idle check computed `now − last_message` in `u64`. A client can stamp an
  arrival just after the main loop reads `now`; the subtraction then wraps to about 1.8 × 10¹⁹,
  which reads as "idle for ages", and the bench stops. In a debug build it would have panicked;
  the release build wraps silently. More clients, more arrivals near each check, likelier.
- **Done.** `saturating_sub`. The client CPU figure also counted the 20 s wait before the start;
  it now starts at the start.

### 15. 1,000 + 1,000 clients at 5,000 orders/s saturate the machine

- **Seen.** Pilot, after entries 13 and 14: all 100,000 orders were answered and all 1,000,000
  book deltas arrived, but the host was busy on 3.99 of its 4 hardware threads. Book push latency
  p50 163 ms; the private feed's p50 3.4 s, and 432 of its 1,000 clients were closed as slow
  consumers (1013).
- **Reading (an inference, to be checked by the measurement):** each private message is one
  WebSocket write, flushed alone, so about 120,000 messages a second are about as many system
  calls; the private feed's process used 1.05 hardware threads. This is a result, not a bug: the
  curve of latency against clients is what Part 3 measures.

### 16. Push latency sits about 6–8 ms above the HTTP reply at low load

- **Seen.** At 1,000 orders/s with one client of each feed: HTTP reply p50 4.6–5.1 ms, push p50
  10.9–12.6 ms (the first runs of the aborted measurement, `results/m26p3-measure-aborted.txt`,
  entry 18). Per run the gap is 6.3–7.5 ms (book 10,919 − 4,571 and 12,103 − 5,131; user
  10,935 − 4,571 and 12,607 − 5,131). Those push figures carry that version's start-time bias, of
  unmeasured size (at least the wake-up delay; a millisecond is typical, an estimate), so the true
  gap is somewhat smaller; the full measurement gives it without the bias.
- **Candidate cause (an inference, not yet tested):** both feeds' followers sleep 10 ms when they
  find nothing new (`IDLE_POLL` in `market_data.rs` and `user_feed.rs`), so a batch waits for the
  next poll: on average about 5 ms (half of 10 ms, if arrivals are uniform). The rest of the gap,
  1.3–2.5 ms here less the bias, would be applying, serializing and sending.
- **Not done yet.** A shorter poll, or a wake-up from the stream, is an optimization for the owner
  to decide, measured before and after.

### 17. The stalled clients' close frame never arrives

- **Seen.** All 10 stalled clients (subscribed to every symbol, never reading) were closed by the
  server, but with no close code (`0`).
- **Why.** A client that does not read has full TCP buffers; the server's close frame cannot be
  written into them any more than the deltas could. The server gives up after the write deadline
  and drops the connection; the client finds the end of the stream, not a close frame, when it
  finally reads.

### 18. The review stopped the full measurement after 8 runs

- **Seen.** The independent review and the verifier, both on the bench while the first full
  measurement ran, found:
  - **a systematic bias:** `--http-bench` started its schedule when its sleep until `--start-at`
    ended, but a timer wakes late (tokio rounds up to its 1 ms tick, more on a busy host). Every
    order was then sent a little after the time `--feed-bench` assumed, and every push latency was
    inflated by that delay. The `early` counter could not catch it: the error only ever adds;
  - **misattribution:** a user client that connected after the start would time a later fill as if
    it were its order's first message;
  - **hangs:** a client whose connect or subscription never completed would keep the bench, and the
    whole script, waiting forever; and one failed client discarded every result of its run;
  - **the client CPU window** included the 10 s idle tail;
  - **the plan's wording:** the completion criteria's "six runs each, interleaved, with the order
    balanced" heads all three measured bullets, not only the with/without comparison;
  - **stalled clients** connected 20 s before the start, so the book feed's silence close (after
    about 45 s without a pong) could fall inside the run and be mistaken for a slow-consumer close;
  - **the summary** left out how far behind each feed finished, Section B's whole result;
  - **`--stream`** was listed as required but never checked, so a run without it failed only at
    its end.
- **Done.** The measurement was stopped after 8 of about 80 runs (kept as
  `results/m26p3-measure-aborted.txt`; used only for entry 16's low-load observation, with its
  bias stated, and for no result). Stopping it with
  `pkill -f m26p3-measure.sh` also killed the SSH session that ran the `pkill`, because that
  session's own command line contained the same text; the containers were removed in a second
  step. The fixes are in `part-3-measurement.md` ("Problems found while building it"), and every
  section is rerun with six interleaved runs.

### 19. The maximum rate cannot be measured over HTTP with this generator

- **Seen.** Pilot of section B with `--http-bench --rate 0` and 200,000 orders: the generator used
  2.42 hardware threads, 28,222 requests failed to connect, 152,562 orders were unanswered after
  the 120 s deadline, and the median latency was 59 s.
- **Why.** At `--rate 0` the open-loop generator spawns every order at once: 200,000 requests in
  flight, each wanting a connection. The generator, not the exchange, is the limit. Milestone 25
  never measured the maximum over HTTP: its maximum-rate figures are the in-process `--bench`,
  which pushes into the exchange's bounded command queue and so waits when the queue is full.
- **Done.** `--bench` gained `--start-at` and the same `start`/`last reply` lines; sections B and
  C's maximum-rate runs use it, with the feeds following its journal. Section C's 5,000/s runs stay
  on HTTP, for the mean and p99 the plan asks about.

### 20. A feed must not lose its network when the bench exits

- **Seen (in the design, before a run).** The feeds share the bench container's network namespace
  (their ports bind loopback). The bench container exits when the bench finishes; the feeds, still
  delivering what it committed, would lose their network mid-delivery.
- **Done.** The container runs `bench; echo bench-exited; sleep 3600` and is removed by the next
  `clean`. The CPU window's marks come from the bench's own printed wall-clock `start` and
  `last reply`, not from when the script read those lines.

### 21. With the feeds running, the maximum rate fell by about two thirds

- **Seen.** Pilot, in-process, alone, 200,000 orders: 59,490 orders/s without the feeds;
  18,831 orders/s with the MDP, the private feed and 100 clients of each (−68%); replicated with
  feeds 19,541 orders/s. The host was busy on 3.95 of 4 hardware threads; the exchange got 0.54
  of a hardware thread against 1.23 alone. Both feeds kept up: every order's messages arrived, the
  book's last message 1.1–2.5 s and the private feed's 2.3–3.9 s after the bench's last reply.
- **First reading:** CPU contention. The MDP (0.61), the private feed (0.78) and the feed clients
  (0.47) together take 0.61 + 0.78 + 0.47 = 1.86 hardware threads, nearly all of the second
  physical core (2 hardware threads) of a two-core machine.
- **Tested with pinning (pilot, one pair):** the exchange on physical core 0 (threads 0 and 2), the
  feeds and their clients on core 1 (threads 1 and 3): 30,802 orders/s with the feeds against
  51,271 without (−40%). Pinning recovers part of the loss, not all of it. Added as section E.
- **Candidate for the rest (an inference, untested):** the exchange still shares with the feeds
  the L3 cache, memory bandwidth, and the stream file's lock: the writer takes an exclusive `flock`
  on the stream for every publish, and each reader takes a shared one for every batch it reads
  (`Mapping::snapshot`), so at about 60,000 batches a second two readers can make the writer wait.
  A follow-up with the feeds running but no clients would separate reading from fan-out.

### 22. Two awk bugs in the summarizer

- **`>` inside `printf` arguments** is output redirection in awk: `printf "...", a > 0 ? b : c`
  tried to write to a file named by the empty string (`cannot open "" for output`). Fixed by
  computing the value first.
- **The section pattern `[ABCD]`** silently skipped section E's rows. Fixed: `[A-E]`.

### 23. Never edit a bash script while it runs

- **Why.** Bash reads a script file as it executes it, not all at once; changing the file under a
  running script can make it execute half-old, half-new lines. The full measurement runs from
  `~/stock-scripts/m26p3-measure.sh`, so changes go to the repository copy, and are copied to
  `~/stock-scripts` only between runs.

### 24. The verifier on the reworked bench: the lag includes the close

- **Seen.** The verifier confirmed every earlier finding fixed, and found that in an in-process
  run (sections B, C, E) each client's last message is the market close's, not the last order's:
  `--bench` closes the market right after its last reply, and the close sends a delta per book and
  an expiry per resting order. So the lag ("how far behind the exchange each feed finished") also
  counts the time to close the market and push the close out, and the CPU window's end with it.
- **Done.** Not changed in the code, because the full measurement was already running with this
  binary, and a result must come from the code that is committed. The reading instead: the bench
  prints the close's own duration (`close N ms` on its `day 1` line), so a lag beyond that is the
  feeds' delay; the write-up states it with each lag. A fix for later: report the last arrival
  matched to an order. Also from this round: `--bench --start-at` with `--days` above 1 is now
  refused (each day reuses the order ids, so a message could not be tied to its day); the README
  and `PROJECT_DIRECTION.md` now say Part 3 is in progress; the pilot outputs and the aborted
  measurement are now in `results/` (`m26p3-pilot-*.txt`, `m26p3-measure-aborted.txt`), copied
  byte for byte from the office machine.
- **A note on that last change to the code:** the refusal is checked before anything runs, and
  no script combines the two flags, so the running measurement's binary behaves the same as the
  committed code for every run it makes.

### 25. The full measurement finished; the owner could not see it running

- **Seen.** The owner asked whether the measurement was done, and where it ran: nothing in the
  chat app showed it.
- **Why.** It ran detached (`nohup ... &`) on the office machine, started over SSH, so no terminal
  in the app followed it. To watch such a run: `ssh ubuntu@10.0.127.253`, then
  `tail -f ~/m26-out/m26p3-measure.txt`.
- **Result.** It finished at 14:32: 156 runs in 2 h 21 min, with no client failure, warning,
  feed halt, unanswered order or rejected order. Written up in `performance/01-feeds-measured.md`.
- **What it found** (the details are there):
  - a pushed message arrives 5.3–7.4 ms after its order's HTTP reply (p50), up to 100 clients of
    each feed; the 10 ms idle poll is the likely floor (an inference);
  - 1,000 clients of each feed at 5,000/s overload the machine, and the private feed falls seconds
    behind and disconnects tens to hundreds of its clients;
  - the completion criterion's 10% is not met: +65.5% mean at 5,000/s alone, −61% at the maximum
    rate; pinning the feeds to the other core recovers about 44% of that loss;
  - stalled clients are cut off without delaying anyone.
- **Two surprises in the data, recorded there:** a lone book client's "lag" is negative (it watches
  one symbol of 100, so its last message can come before the last order); and at 5,000/s the
  exchange used less CPU with the feeds running (0.60–0.65 of a hardware thread) than without
  (0.93–1.03 in five of six runs) for the same orders: not explained.
- **Afterwards.** The full test suite ran again on the final code (252 / 255 / 6 integration, fmt
  clean, 12 release warnings), with the machine free.

### 26. My first write-up of the results had wrong numbers

The verifier recomputed every figure in `performance/01-feeds-measured.md` from the raw file and
found:
- **The close's median duration was wrong: 361 ms over "50" runs.** The shell pipeline that found
  it sorted the durations and picked the middle one wrongly, and there are 48 in-process runs, not
  50. The median over 48 is 440.5 ms; for the configuration the lag was computed for (B alone) it
  is 526.5 ms, so the feeds' own lag at the end is 0.55 s (book) and 2.23 s (private), not 0.7 and
  2.4 s. **Lesson:** a figure from a quick pipeline is checked against the raw lines before it is
  written down. (The correction itself first said 546.5 ms, a median miscomputed by hand from six
  correct values; the next verifier caught that too: (517 + 536) / 2 = 526.5.)
- **"Pinning halves the loss" and "most of it is CPU"** were rounder than the numbers: pinning
  recovers 16,717 of 38,076 lost orders/s, 43.9%.
- **"About 5–6 ms"** for the push gap was the low end; across every configuration up to 100
  clients (sections A, C and D) it is 5.3–7.4 ms, and the two feeds differ by 0.1–0.9 ms, not
  0.2–0.4, and the part left after a 5 ms poll is 0.3–2.4 ms. (The first correction said 5.3–7.2,
  0.1–0.8 and 0.3–2.2, from section A alone.)
- **"The rest is not CPU time"** was a conclusion stated as fact, while the 29 other containers were
  never pinned and can run on the exchange's core; now an inference, with that named.
- Smaller ones: the count of other containers (29 in every run, not 29–33), a lag compared across
  sections (C's 2.33 s, not B's 2.76 s), the replicated runs' disconnections (48–163) left out, the
  fifths stated for one configuration only, "in order" claimed though the bench does not check
  order, and three roundings.
All corrected in the write-up.

### 27. Part 3 closed on the explanation

The owner chose to close Part 3, accepting CPU contention on the shared two-core machine as the
explanation for missing the 10% criterion ("yes close part 3 i think the CPU contention is
issue"). Not done, and recorded in `decisions.md` for later: testing the rest of the loss (the
feeds with no clients, the stream lock), and the two optimizations the numbers point to (the 10 ms
idle poll, one write per message in the private feed). One mistake on the way: this entry was
first inserted in the middle of entry 26, splitting it; the verifier found it.
