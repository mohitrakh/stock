# Part 2 — Login and registration hardening

**Status: complete on 2026-10-12.**

## What this part is

Part 1 measured three faults in the gateway's handling of accounts:

1. **bcrypt on the Tokio worker threads.** `register` and `login` ran a cost-12 bcrypt (229 ms of
   CPU on the office machine) directly inside their `async fn`. Each call held one of the exchange's
   4 Tokio worker threads, the threads that parse and answer every order. One login loop pushed the
   order p99 from about 14 ms to 0.1–0.2 s. Four pushed the median to 0.15–0.19 s. With sixteen,
   new connections to the customer and operator ports got no answer for tens of seconds
   (`performance/03-login-stall.md`).
2. **User enumeration.** A login answered "user not found" for an unknown email and
   "invalid password" for a known one. The time also differed: 0.6 ms against 230 ms
   (`performance/05-login-enumeration.md`).
3. **Small gateway faults.** A hard-coded `GET /users` list. The JWT secret read from the
   environment on every request. Authentication failures answered as 400.

Part 2 fixes all three. The background is in `01-theory.md` sections 7 (bcrypt), 8 (Tokio's threads
and the blocking pool) and 9 (enumeration and timing side channels).

## What changed in the code

### `src/controllers/user_controller.rs`

**`bcrypt_off_the_workers(permits, work)`**, new. Every bcrypt call in the gateway goes through it:

```rust
async fn bcrypt_off_the_workers<T: Send + 'static>(
    permits: Arc<Semaphore>,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, AppError> {
    let permit = permits.acquire_owned().await.map_err(...)?;
    tokio::task::spawn_blocking(move || {
        let result = work();
        drop(permit);
        result
    })
    .await
    .map_err(...)
}
```

Line by line:
- `acquire_owned().await` waits, without occupying a thread, until one of the semaphore's permits
  is free. While waiting, the request is just a parked future, so a thousand waiting logins cost a
  thousand small futures, not a thousand threads.
- `spawn_blocking` runs the closure on Tokio's **blocking pool**, a separate set of threads made for
  work that would block (theory §8.4). The worker thread that awaited it is free at once and goes on
  serving other requests.
- **The permit moves into the closure** and is dropped there, right after the bcrypt finishes. That
  matters for one failure case. If the client disconnects mid-login, Axum drops the request's
  future, which stops awaiting the blocking task. But a blocking task cannot be cancelled: the
  bcrypt keeps running on its thread. If the permit lived in the request's future, it would be
  released at the disconnect, while its bcrypt was still running. A client that connects and
  disconnects in a loop could then run any number of bcrypt calls at once, and the cap would mean
  nothing. With the permit inside the closure, it is released only when the CPU work really ends.
- The function takes the semaphore as a parameter rather than reading the static itself, so the test
  can give it its own small semaphore.

**`BCRYPT_PERMITS`**, a `LazyLock<Arc<Semaphore>>` holding `max(1, available_parallelism / 2)`
permits: 2 on the office machine. The reasoning, from the Part 1 measurements, is recorded in
`decisions.md` ("The bcrypt cap's value"). Capacity is about 2 / 0.229 s ≈ 8.7 bcrypt calls per
second there.

**`DUMMY_HASH`**, a `LazyLock<String>`: a real bcrypt hash, at `DEFAULT_COST` (12), of a password no
account has. A login for an unknown email verifies against it. That costs the same bcrypt as a real
account's verify, so the answer arrives just as late. It must be *computed* at the same cost as real
hashes, not copied from somewhere at another cost, or the timing would differ again. A test checks
its cost.

**`prepare()`**, new and public. It computes `DUMMY_HASH` on the blocking pool. `serve_primary` calls
it before binding the customer port, so the first unknown-email login does not pay for an extra
bcrypt. That would itself be a one-time timing leak, and a 229 ms surprise for that user.

**`login`** now:
1. keeps the two request-only checks (empty email; password shorter than 8 bytes) as `400`. They
   depend only on the request, never on whether the account exists, so they reveal nothing;
2. looks up the email;
3. runs **exactly one full bcrypt computation**, through `bcrypt_off_the_workers`, against the account's hash or,
   when there is no account, against `DUMMY_HASH`;
4. answers `200` with a token only for an existing account with a matching password. Every other
   outcome is `401` with the body `invalid email or password` (`LOGIN_REFUSED`). That includes a
   stored hash that bcrypt cannot read. Such a hash can only come from a damaged row, because
   `register` always stores a valid one. The first version answered it `400` with bcrypt's error
   text, which would have revealed that the account exists; the second review found this. It is
   now logged (`eprintln!`) and refused like a wrong password. A third check found that the time
   still differed: bcrypt rejects an unreadable hash before doing any work, so that 401 would have
   come back in under a millisecond. So that path now also runs one verify against the dummy hash,
   and costs one bcrypt like every other path.

On a warm replica's promotion, `serve_primary` also runs `prepare()`, so a failover pays one bcrypt
(about 230 ms) before the new primary's customer port binds. The operator and replication ports are
not delayed by it.

**`register`** hashes the new password through `bcrypt_off_the_workers`. It still answers
`user already exists` for a taken email (see "Known limitation" below).

**`get_users` and `UserResponse`**, the hard-coded list, are deleted.

### `src/middleware/auth_middleware.rs`

- `jwt_secret()` now returns `Option<&'static str>` from a `OnceLock`. The first call reads
  `JWT_SECRET` from the environment, and every later call returns the stored value. The first call
  is the startup check, `require_jwt_secret` in `main`, so the environment is read exactly once,
  before any request. Before, every authenticated request called `std::env::var`, which takes the
  process's environment lock and copies the string.
- `decoding_key()`, new: the `DecodingKey` built once from that secret, also in a `OnceLock`. Before,
  every request built a new one, copying the key bytes.
- The extractor uses both. Its behaviour is unchanged: the same three rejections with the same
  statuses.

Why a `OnceLock` and not a field in `AppState`: `AuthUser` is an Axum extractor generic over any
state `S`, and `jwt_secret` is also used by `login`, by `main` and by `--http-bench`. A process-wide
value read once matches what the secret is: fixed for the life of the process. A test that needs a
different secret runs the binary as a new process, as `tests/startup.rs` does.

### `src/error/app_error.rs`

A new variant, `Unauthorized(String)`, answered as `401`.

### `src/routes/user_routes.rs`

The `GET /users` route is gone. `/users/register` and `/users/login` remain.

### `src/main.rs`

`serve_primary` calls `controllers::user_controller::prepare().await` before it binds port 4000. A
promoted warm replica goes through the same function, so it does too.

### `src/exchange/http_bench.rs` (the measurement tool, not the exchange)

Three fixes to the Part 1 generator, made during this part:
- **The login loops back off 10 ms after a failed request.** A login that failed at once (a refused
  connection) was retried at once, in a loop with no `.await` that ever returned `Pending`. A task
  like that never gives its runtime thread back (theory §8.1). Sixteen of them held all four of the
  generator's threads: the generator ran at 366% CPU, the exchange sat idle, and the 120 s deadline
  never fired. That is the same mistake Part 2 fixes in the gateway, a long computation on an async
  worker, made in the test tool. The pause also yields, and it is what a real client does.
- **Login accounts get unique usernames** (`bench-<run>-<i>`), found by the Part 1 review.
- **No throughput is printed for a run that hit its deadline**, found by the same review.

### `performance/scripts/m24-lib.sh`

`http_bench` runs the generator from `$GEN` when it is set, so that two exchange builds can be
measured with one and the same generator.

## Options considered and rejected

- **`tokio::task::block_in_place` instead of `spawn_blocking`.** It tells Tokio that the current
  worker is about to block and moves that worker's queued tasks to another thread. It keeps the
  data on the same thread, but it still makes one worker unavailable per call, and it needs a cap
  just the same. `spawn_blocking` keeps every worker free.
- **A dedicated thread pool (for example `rayon`) for password hashing.** It gives full control over
  the thread count, at the cost of a new dependency and more code. The blocking pool plus a
  semaphore gives the same bound with what Tokio already provides.
- **Lowering the bcrypt cost.** Cost 10 would make each call 4 times cheaper (57 ms,
  `performance/02`). But it would also make stolen hashes 4 times cheaper to crack, and existing
  hashes would keep cost 12 anyway. The cost is a security parameter, not a performance knob. This
  part moves *where* the work runs, not how much there is.
- **Refusing logins when all permits are taken** (an immediate `503`) instead of waiting. Under a
  flood, waiting means unbounded queues of parked futures. Refusing means real users fail while the
  flood lasts. Neither is right alone; the right answer is to stop the flood before it reaches
  bcrypt, which is the per-IP limit in Part 5. Until then, waiting is the choice that never fails a
  legitimate login outright.
- **A dummy hash stored as a string constant.** It works if it was made at the same cost. Computing
  it at startup guarantees that, and keeps working if `DEFAULT_COST` ever changes.

## Tests added

In `user_controller.rs`:

- `bcrypt_work_runs_off_the_worker_threads_and_never_beyond_the_cap`. A Tokio runtime with 2 worker
  threads runs 8 calls of 30 ms of blocking work through `bcrypt_off_the_workers` with a cap of 2.
  Beside them, an async "ticker" task increments a counter every 2 ms. The test checks two things:
  - the most calls running at once is exactly 2: the cap is reached and never exceeded;
  - the ticker advanced at least 20 times during the calls.

  If the calls ran on the worker threads, both workers would be blocked for the whole 4 × 30 ms and
  the ticker could advance at most once between calls (about 8 times). Off the workers it advances
  about 40–60 times.

  An earlier version checked only that the work "ran on another thread". That proves nothing,
  because Tokio's worker threads are other threads too. It was replaced before the test first ran.
- `the_dummy_hash_is_a_real_hash_at_the_accounts_cost`: `DUMMY_HASH` parses as a bcrypt hash and its
  cost is `DEFAULT_COST`.

**Mutation check** (`performance/scripts/m24p2-mutate.sh`, output in
`performance/results/m24p2-mutate.txt`). Each fix was removed in a copy of the tree, and the
covering test was run:

| Mutation | Covering test | Result |
|---|---|---|
| Run the work inline, on the calling task (no `spawn_blocking`) | `bcrypt_work_runs_off_the_worker_threads...` | FAILED, as it must |
| Take no permit (no cap) | the same | FAILED |
| Compute the dummy hash at cost 4 | `the_dummy_hash_is_a_real_hash...` | FAILED |

Verified on the office machine on 2026-10-12: `cargo fmt -- --check` is clean; `cargo test --locked`
passes 213 unit tests (Part 1's 211 plus these two) and every integration test; the release build
still has 12 warnings, none in the changed files.

## Measurements

- `performance/06-login-after-part-2.md`: the enumeration check on both binaries, the same day.
  Part 2 answers every failed login with `401 invalid email or password`. Known and unknown emails
  take 230.47 ms and 229.94 ms on average, with fully overlapping ranges (Part 1: 230.29 ms against
  0.589 ms). `GET /users` is 404.
- `performance/07-login-stall-after-part-2.md`: the login stall on both binaries, interleaved, the
  same hour.
  - **Orders.** With 4 or 16 clients logging in back to back, Part 2's orders keep a p50 of 2.8–4.0 ms
    and a p99 of 10–19 ms, the same as with no logins. Part 1's p50 is 0.21–0.58 s and its p99
    0.74–2.64 s.
  - **New connections.** At C = 16 Part 2 answers every order and refuses no connection, and
    `/health` and the operator port answer in under 0.5 ms. Part 1 leaves 14 orders unanswered,
    fails 33–1,415 connections, and runs its container at 383–390% CPU on bcrypt.
  - **Logins.** They are capped at 8.1–8.3 per second (predicted about 8.7). Under a flood they wait
    instead: the p50 is 0.48 s at C = 4 and 1.9 s at C = 16, exactly the C / λ that Little's law
    predicts.

## Independent review

A separate review read the code and documents and recomputed every number from the raw files. It
found no bug in the gateway code. It confirmed:
- the cap cannot be exceeded: a future dropped before `spawn_blocking` drops its permit, and after
  that the permit lives in the closure;
- there is no deadlock: `DUMMY_HASH` is forced on the blocking pool before the port binds;
- the `OnceLock` secret is safe, since no test sets the variable in-process;
- the generator's back-off really yields;
- the new test catches inline work.

Its findings, all fixed:
1. **(medium)** The first correction of `03-login-stall.md` claimed the Part 1 C = 16 runs had a
   spinning generator. Their own output rules that out: about 100 µs of CPU per order, and the
   deadline fired. Only one later run spun. `03` and `07` now say so.
2. **(low)** A stored hash that bcrypt cannot read answered `400` with bcrypt's error, revealing the
   account. It now answers `401` like any wrong password, and the fault is logged. A final check found
   its timing still gave it away (bcrypt fails such a hash at once), so it also runs one verify
   against the dummy hash.
3. **(low)** Wording:
   - the cap leaves half the *hardware threads*, not half the cores' capacity;
   - decoding a returned row may take microseconds, not "far below a microsecond";
   - two run times were off by a second;
   - a promotion now pays one bcrypt before the customer port binds.

## Known limitations

- **Registration still enumerates.** `user already exists` reveals that an email has an account.
  Fixing it needs email verification, which this project does not have (`01-theory.md` §9).
- **Failed logins now cost a full bcrypt.** An unknown email cost 0.6 ms before. Now it costs
  230 ms of capped CPU, so anyone can use up the login capacity (about 8.7 per second on the office
  machine) without knowing any email. Orders are protected by the cap, but real users' logins can
  be crowded out until Part 5's per-IP limit.
- **Waiting logins are unbounded.** Parked futures waiting for a permit are small, but nothing caps
  how many there are. Part 5's limits bound the arrival rate.
- **A JWT cannot be revoked**, as before (theory §2.2).

## Compatibility

No event, journal format or exchange behaviour changed. The customer API changed:
- `GET /users` is gone (404);
- a failed login is `401 invalid email or password` instead of a `400` with one of two messages;
- `POST /users/login` with an empty email or a password shorter than 8 bytes is still `400`, with
  the same messages.
