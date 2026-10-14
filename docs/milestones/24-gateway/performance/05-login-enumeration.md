# 05 — What a login answer reveals

**Part 1 baseline. Measured 2026-10-12.** Raw output: `results/m24p1-enum.txt`. Script:
`scripts/m24p1-enum.sh`.

## The question

`00-selection.md` A.5 claimed that the milestone 23 login reveals whether an email has an account in
two ways:

- by its **content**: "user not found" against "invalid password";
- by its **time**: an unknown email returns straight after the database lookup, while a known one
  first runs one bcrypt.

This file checks both, on the real exchange process.

## Method

- One exchange (`stock-m24p1`, alone, a fresh journal and database), idle: no other requests.
  Machine as in `04-raw-sync.md`, load average 1.68 at the start.
- One account registered: `known@bench.test`.
- Then 20 rounds. Each round makes two requests, alternating so that any drift in background load
  affects both kinds equally:
  - a login with `known@bench.test` and a wrong password;
  - a login with `nobody-<i>@bench.test`, an email with no account.
- `curl` runs inside the exchange's container (the customer port binds loopback only). It prints
  the HTTP status, `time_total` (from the start of the connection to the last byte of the answer),
  and the body.

## Results

Every answer had status **400**, and the bodies were always **`invalid password`** for the known
email and **`user not found`** for the unknown one.

Times in milliseconds, from the 20 samples of each kind:

| email | mean | min | max |
|---|---|---|---|
| known (wrong password) | 230.98 | 228.81 | 239.99 |
| unknown | 0.652 | 0.517 | 1.215 |

How the means were computed:
- Known: the 20 values sum to 4.619647 s; 4.619647 / 20 = 0.230982 s = 230.98 ms.
- Unknown: the 20 values sum to 0.013038 s; 0.013038 / 20 = 0.000652 s = 0.652 ms.

The gap is 230.98 − 0.652 = **230.3 ms**, a ratio of 230.98 / 0.652 ≈ **354×**. The gap is one
bcrypt verify at cost 12: `02-bcrypt-cost.md` measured 228.6 ms at p50.

## What the numbers say

1. **The body alone tells them apart.** Anyone can learn whether an email has an account with one
   request.
2. **The time alone tells them apart too, with one request.** The fastest known answer (228.81 ms)
   is 188 times slower than the slowest unknown one (1.215 ms). The two ranges do not come close to
   overlapping. Over the internet, jitter of tens of milliseconds would not hide a 230 ms gap
   either. Making the two messages identical would therefore *not* be enough: the dummy-hash verify
   is required (`01-theory.md`, section 9).
3. **Both are answered as 400 (validation error).** A failed authentication should be 401.
4. **An unknown email costs the server almost nothing (0.65 ms, one PostgreSQL lookup).** A known
   one costs 231 ms of CPU. After Part 2 both cost one bcrypt, so the server spends 231 ms of CPU on
   *every* failed login. That makes the per-IP limit on `login` (Part 5) and the bcrypt cap
   (decision 7) more important, not less.

## Expected after Part 2

The same script, run on the Part 2 binary, should show identical status (401), identical bodies,
and time ranges that overlap: both about one bcrypt. That run will be recorded in
`06-login-after-part-2.md`.
