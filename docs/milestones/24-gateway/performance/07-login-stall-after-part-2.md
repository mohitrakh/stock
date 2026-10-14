# 07 — The login stall, after Part 2

**Part 2. Measured 2026-10-12, 17:58 to 18:23 local time.** Raw output:
`results/m24p2-stall-first.txt` (C = 0 and C = 4) and `results/m24p2-stall.txt` (C = 16). Script:
`scripts/m24p2-stall.sh`.

## The question

`03-login-stall.md` measured how logins delay orders on the Part 1 gateway, where bcrypt runs on the
Tokio worker threads. Part 2 moved bcrypt to the blocking pool, at most 2 calls at a time. Does it
remove the stall, and what does it cost the logins?

## Method

The load is `03`'s: 1,000 orders/s over HTTP for 20 s, the exchange alone, C clients logging in back
to back, a 120 s deadline. What is new:
- **Both exchanges, interleaved, the same day.** For each C and each run, the Part 1 binary
  (`stock-m24p1`), then the Part 2 binary (`stock-m24p2`), each on a fresh exchange. C = 0, 4 and
  16, two runs each.
- **One generator for both.** The C = 16 runs use the Part 2 build's generator (`GEN`) against both
  exchanges.
- **CPU in the probes.** At C = 16 the probes also record the CPU use of the exchange and generator
  containers (`docker stats`).

**Why there are two raw files.** The first run of this script used each build's own generator. In
its Part 1 C = 16 run, the generator's login loops retried failed logins without pausing. The
generator was seen at 366% CPU with the exchange idle, and its 120 s deadline never fired, because
loops that never yield held every one of its runtime threads. That run was stopped by hand and the
loops were fixed: they now back off 10 ms after a failed request (`part-2-login.md`). Its C = 0 and
C = 4 runs had no failed logins, so they were unaffected and are kept (`m24p2-stall-first.txt`).
C = 16 was then run again with the fixed generator (`m24p2-stall.txt`).

Machine: load average 1.6 to 4.6, 31 containers running in total.

## Results

Order latency in µs. "logins/s" and "login p50" are for the background login loops.

| C | Run | Exchange | order p50 | p90 | p99 | p99.9 | max | logins/s | login p50 |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 1 | Part 1 | 6,075 | 9,647 | 13,951 | 22,463 | 29,631 | — | — |
| 0 | 1 | Part 2 | 5,843 | 8,935 | 12,799 | 21,823 | 31,199 | — | — |
| 0 | 2 | Part 1 | 4,363 | 8,783 | 14,535 | 47,487 | 65,791 | — | — |
| 0 | 2 | Part 2 | 4,723 | 8,839 | 14,407 | 24,783 | 32,895 | — | — |
| 4 | 1 | Part 1 | 214,399 | 462,591 | 741,887 | 982,527 | 1,001,471 | 14.4 | 261,247 |
| 4 | 1 | Part 2 | 3,831 | 7,915 | 18,831 | 117,311 | 137,215 | 8.1 | 479,487 |
| 4 | 2 | Part 1 | 254,207 | 568,831 | 872,447 | 1,056,767 | 1,193,983 | 14.3 | 259,711 |
| 4 | 2 | Part 2 | 4,005 | 7,231 | 14,447 | 26,879 | 38,111 | 8.1 | 477,439 |
| 16 | 1 | Part 1 | 581,631 | 1,412,095 | 2,643,967 | 2,914,303 | 20,381,695 | 15.1 | 987,135 |
| 16 | 1 | Part 2 | 2,753 | 6,187 | 10,455 | 17,679 | 26,335 | 8.3 | 1,889,279 |
| 16 | 2 | Part 1 | 431,359 | 933,887 | 1,437,695 | 1,777,663 | 2,027,519 | 15.2 | 992,255 |
| 16 | 2 | Part 2 | 3,839 | 6,827 | 10,007 | 16,167 | 20,271 | 8.3 | 1,901,567 |

Completeness at C = 16:

| Run | Exchange | answered | unanswered at 120 s | could not connect | failed logins |
|---|---|---|---|---|---|
| 1 | Part 1 | 19,986 | 14 | 1,415 | 117 |
| 1 | Part 2 | 20,000 | 0 | 0 | 0 |
| 2 | Part 1 | 19,986 | 14 | 33 | 0 |
| 2 | Part 2 | 20,000 | 0 | 0 | 0 |

Probes at C = 16 (new connections, 10 s limit):

| Run | Exchange | `/health` and operator `/session` at ~30 s and ~80 s | exchange CPU | generator CPU |
|---|---|---|---|---|
| 1 | Part 1 | no answer within 10 s, both times | 382.9%, 389.7% | 0.13%, 0.14% |
| 2 | Part 1 | no answer within 10 s, both times | 386.3%, 390.2% | 0.15%, 0.12% |
| 1 | Part 2 | answered in 0.26–0.43 ms, all three times | — | — |
| 2 | Part 2 | answered in 0.26–0.43 ms, all three times | — | — |

(The Part 2 probes show no CPU figures. Those runs had already finished their 20 s of orders before
the first probe, so the generator's container was gone, and `docker stats` printed nothing for the
pair. Their probes therefore show an exchange whose login load had ended. During the load itself,
what shows the gateway stayed open is that every order was answered, with a p99 of about 10 ms.)

## What the numbers say

**1. The stall is gone.** With 4 or 16 clients logging in back to back, Part 2's orders have a p50 of
2.8–4.0 ms and a p99 of 10–19 ms. That is the same as with no logins at all (p50 4.4–6.1 ms, p99
12.8–14.5 ms; C = 0 here happened to be slightly *slower*, within the noise between runs). Part 1,
in the same hour, gave a p50 of 0.21–0.58 s and a p99 of 0.74–2.64 s. At C = 16, the p50 is 581,631
against 2,753 µs in run 1, about 211 times lower; the p99 is 2,643,967 against 10,455 µs, about 253
times lower.

**2. New connections stay open.** At C = 16 the Part 1 exchange left new connections unanswered for
over a minute, failed 33–1,415 connection attempts and left 14 orders unanswered. Part 2 answered
every order and refused no connection. Its probes answered in under half a millisecond.

**3. The probes prove the Part 1 starvation was real.** In both Part 1 runs at C = 16 the exchange's
container ran at 383–390% CPU (4 hardware threads, all bcrypt) while the generator's ran at 0.1%.
These rows confirm `03-login-stall.md`'s C = 16 rows, whose generator did not spin (it used about
100 µs of CPU per order there).

**4. Logins are capped, as designed, and they wait instead.**

| C | Part 1 logins/s | Part 2 logins/s |
|---|---|---|
| 4 | 14.3–14.4 | 8.1 |
| 16 | 15.1–15.2 | 8.3 |

Part 2 runs exactly 2 bcrypt calls at a time. `decisions.md` predicted about 2 / 0.229 s ≈ 8.7 per
second from the single-call time. Measured, 8.1–8.3, so each call took about 2 / 8.3 ≈ 0.241 s:
slightly slower than alone, because the machine was busy.

The waiting is exactly what Little's law (theory §10.5) predicts. With C clients always waiting or
being served, and a throughput of λ logins per second, each login takes W = C / λ:
- C = 4: 4 / 8.1 ≈ 0.49 s; measured p50 0.477–0.479 s.
- C = 16: 16 / 8.3 ≈ 1.93 s; measured p50 1.89–1.90 s.

So under a login flood, Part 2 sacrifices the logins (each waits about 2 s at C = 16) to keep the
orders untouched. That is the intended trade, and the reason Part 5 must stop floods before they
reach bcrypt at all: per-IP limits on `login` and `register`.

**5. The remaining small effect.** Part 2's C = 4 run 1 shows a p99.9 of 117 ms, against 22–48 ms at
C = 0. Two bcrypt calls occupy 2 of the 4 hardware threads, so the exchange and its Tokio workers
share the other 2 with the rest of the machine, and a few orders wait for a CPU. Run 2 at C = 4
(p99.9 27 ms) and both C = 16 runs (16–18 ms) show no such tail, so with two runs it cannot be told
apart from noise.

## What this does not show

- Logins arriving faster than they are served, from many IP addresses, for a long time. Nothing
  bounds the number of waiting logins (`part-2-login.md`, "Known limitations"). Part 5's limits
  bound the arrival rate.
- Registrations as the flood. They take the same path and the same permit, so the same result is
  expected, but only logins were measured.
