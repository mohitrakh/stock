# 06 — What a login answer reveals, after Part 2

**Part 2. Measured 2026-10-12, about 70 minutes after `05-login-enumeration.md`.** Raw output:
`results/m24p2-enum.txt`. Script: `scripts/m24p2-enum.sh`.

## The question

`05-login-enumeration.md` showed that the milestone 23 login tells an existing account from a
missing one by its body ("invalid password" against "user not found") and by its time (231 ms
against 0.65 ms). Part 2 changed the login to:
- answer every failure with `401 invalid email or password`;
- run exactly one full bcrypt computation on every path, against a dummy hash when the email has
  no account.

Does the answer still reveal anything?

## Method

The same as `05`, run once on the Part 1 binary and once on the Part 2 binary, one after the other
(Part 1 at 17:57:43, Part 2 at 17:57:55 local time). For each binary:
- a new idle exchange;
- one account registered, then one login with the right password;
- a `GET /users` (the placeholder list);
- then 20 rounds, each a login for the known email with a wrong password and one for an unknown
  email.

(A first run of this script asked for `GET /users/`, with a trailing slash, which is 404 on both
binaries because the route was `/users`. The script was corrected and run again. Only the corrected
run is kept.)

## Results

| Binary | login | n | status | body | mean | min | max |
|---|---|---|---|---|---|---|---|
| Part 1 | known email, wrong password | 20 | 400 | `invalid password` | 230.29 ms | 228.45 | 241.20 |
| Part 1 | unknown email | 20 | 400 | `user not found` | 0.589 ms | 0.528 | 0.781 |
| Part 2 | known email, wrong password | 20 | 401 | `invalid email or password` | 230.47 ms | 228.87 | 239.05 |
| Part 2 | unknown email | 20 | 401 | `invalid email or password` | 229.94 ms | 228.62 | 239.41 |

Means are the sums of the 20 samples divided by 20, from the raw file:
- Part 1 known: 4.605876 s / 20 = 230.29 ms; unknown: 0.011786 s / 20 = 0.589 ms.
- Part 2 known: 4.609416 s / 20 = 230.47 ms; unknown: 4.598890 s / 20 = 229.94 ms.

Also:
- `GET /users`: Part 1 answered `200` with `[{"id":1,"name":"Mohit"},{"id":2,"name":"John"}]`. Part 2
  answered `404`.
- A login with the right password: `200` on both, in 230.3 ms (Part 1) and 229.5 ms (Part 2). The
  correct path costs the same one bcrypt as before.

## What the numbers say

**1. The body no longer tells them apart.** Every failed login on Part 2 is
`401 invalid email or password`.

**2. The time no longer tells them apart either.** On Part 2 the two means differ by
230.47 − 229.94 = 0.53 ms. That is a quarter of one percent, and the ranges overlap completely: the
known logins span 228.87–239.05 ms, the unknown ones 228.62–239.41 ms. On Part 1 the gap was
230.29 − 0.589 = 229.7 ms, with no overlap at all.

Could an attacker still tell them apart by averaging many requests? Both paths now do the same
work: one database lookup, one bcrypt verify at cost 12, one 401 answer. The remaining differences
are tiny:
- the found row's `password_hash` is copied, and the dummy hash is cloned instead;
- the unknown path's lookup returns no row.

The hash copy takes well under a microsecond. Decoding a returned row into a `User`, against
finding no row, plausibly takes some microseconds. Both are tiny against a 229 ms bcrypt whose own
run-to-run spread is about 10 ms.
This is an argument from what the code does, not a measurement at that precision: 20 samples cannot
resolve sub-millisecond differences.

**3. The authentication failure has the right status.** 401 instead of 400.

**4. The price: every failed login now costs a full bcrypt.** On Part 1 an unknown email cost the
server 0.6 ms. On Part 2 it costs 230 ms of CPU, on the blocking pool, under the cap of 2. So anyone
can now spend the server's bcrypt capacity without knowing a single email. The cap keeps that from
reaching the order path (`07-login-stall-after-part-2.md` measures it), but it means real users'
logins can be crowded out by fake ones. The per-IP limit on `login` in Part 5 is what stops that.

## What is still revealed

**Registration.** `POST /users/register` still answers `user already exists` for a taken email
(theory §9). Fixing it properly needs email verification ("check your inbox" for every address,
with the message sent only for a new one), and this project sends no email. It is recorded as a
known limitation in `part-2-login.md` and in `PROJECT_DIRECTION.md`.
