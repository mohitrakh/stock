//! `--http-bench`: an open-loop load generator that drives a running exchange through its customer
//! HTTP port, the way customers do: HTTP/1.1, JSON bodies, a JWT on every request.
//!
//! `--bench` measures the exchange behind the command queue and deliberately skips HTTP. The
//! design measures latency from the moment an order enters the exchange, which here is the HTTP
//! port, so this measures that. Run both at the same rate and the difference is the gateway's share
//! of the round trip.
//!
//! The workload is `--bench`'s, order for order: the same seed, users, symbols, price band,
//! quantities and order ids (`client_order_id` `o{i}`), and the same setup deposits. Traders' tokens
//! are signed here with `JWT_SECRET`, as the milestone 23 load client did, so no trader needs a
//! database account.
//!
//! `--logins C` adds the login-stall measurement: C real accounts are registered first, and during
//! the measured phase C tasks log in back to back, each waiting for its answer before sending the
//! next. They are the background load; only the orders are the measured traffic.
//!
//! Latency is measured from each order's *intended* send time (`start + i / rate`), never from when
//! it was actually sent, so a stall is charged to every order scheduled during it (coordinated
//! omission; see `docs/milestones/24-gateway/01-theory.md`, section 10). How late the generator
//! itself sent each order is reported separately as `send lag`, so a generator that cannot keep up
//! shows itself.
//!
//! See `docs/milestones/24-gateway/part-1-baseline.md`.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use hdrhistogram::Histogram;
use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request, StatusCode,
    body::Bytes,
    header::{AUTHORIZATION, CONTENT_TYPE},
};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use jsonwebtoken::{EncodingKey, Header, encode};
use serde_json::{Value, json};

use crate::{
    exchange::bench::{BAND_LOW, BAND_TICKS, CASH_PER_USER, Rng, SEED, SHARES_PER_SYMBOL},
    models::user::Claims,
};

const USAGE: &str = "usage: stock --http-bench [--orders N] [--rate ORDERS_PER_SEC|0] [--users N] [--symbols N] [--logins N] [--deadline SECONDS] [--target orders|health] [--base URL] [--operator URL] [--day YYYY-MM-DD] [--start-at UNIX_MS]";

/// Setup requests in flight at once: enough to fill the exchange's groups, few enough that setup
/// does not open thousands of connections.
const SETUP_CONCURRENCY: usize = 64;
const LOGIN_PASSWORD: &str = "bench-password-1";

struct Config {
    base: String,
    operator: String,
    orders: u64,
    rate: u64,
    users: u64,
    symbols: u64,
    logins: u64,
    day: String,
    /// `--target health`: the control run. The same open-loop schedule sends `GET /health`, which
    /// is answered by the gateway alone (no JWT, no command queue, no journal), so its latency is
    /// the floor of the HTTP stack plus this generator on this machine.
    health: bool,
    /// Seconds after the first order's intended time at which unanswered orders are abandoned
    /// and counted; 0 waits for every answer.
    deadline: u64,
    /// `--start-at`: the wall-clock time (Unix milliseconds) at which the first order is due, after
    /// setup. A feed client given the same time and rate knows every order's intended send time,
    /// `start_at + i / rate`, from its id alone (milestone 26 part 3, `--feed-bench`).
    start_at: Option<u64>,
}

fn parse(args: &[String]) -> Result<Config, String> {
    let mut config = Config {
        base: "http://127.0.0.1:4000".to_string(),
        operator: "http://127.0.0.1:4004".to_string(),
        orders: 100_000,
        rate: 1_000,
        users: 100,
        symbols: 100,
        logins: 0,
        day: "2026-01-02".to_string(),
        health: false,
        deadline: 0,
        start_at: None,
    };
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
        let number = || {
            value
                .parse::<u64>()
                .map_err(|_| format!("{flag} needs a whole number\n{USAGE}"))
        };
        match flag.as_str() {
            "--base" => config.base = value.trim_end_matches('/').to_string(),
            "--operator" => config.operator = value.trim_end_matches('/').to_string(),
            "--day" => config.day = value.clone(),
            "--orders" => config.orders = number()?.max(1),
            "--rate" => config.rate = number()?,
            "--users" => config.users = number()?.max(2),
            "--symbols" => config.symbols = number()?.max(1),
            "--logins" => config.logins = number()?,
            "--deadline" => config.deadline = number()?,
            "--start-at" => config.start_at = Some(number()?),
            "--target" => {
                config.health = match value.as_str() {
                    "orders" => false,
                    "health" => true,
                    _ => return Err(format!("--target is orders or health\n{USAGE}")),
                }
            }
            _ => return Err(USAGE.to_string()),
        }
    }
    Ok(config)
}

type HttpClient = Client<HttpConnector, Full<Bytes>>;

/// One request; returns the status and the whole body, so the time covers the complete answer.
async fn call(
    client: &HttpClient,
    method: Method,
    url: &str,
    authorization: Option<&str>,
    body: Option<&Value>,
) -> Result<(StatusCode, Bytes), String> {
    let mut request = Request::builder().method(method).uri(url);
    if let Some(authorization) = authorization {
        request = request.header(AUTHORIZATION, authorization);
    }
    let body = match body {
        Some(body) => {
            request = request.header(CONTENT_TYPE, "application/json");
            Full::new(Bytes::from(body.to_string()))
        }
        None => Full::new(Bytes::new()),
    };
    let request = request.body(body).map_err(|error| error.to_string())?;
    let response = client
        .request(request)
        .await
        .map_err(|error| format!("{url}: {error}"))?;
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| format!("{url}: {error}"))?
        .to_bytes();
    Ok((status, body))
}

/// Runs setup requests with at most `SETUP_CONCURRENCY` in flight; every one must succeed.
async fn all_succeed(
    client: &HttpClient,
    requests: Vec<(String, Option<String>, Value)>,
) -> Result<(), String> {
    let mut running = tokio::task::JoinSet::new();
    let check = |joined: Result<Result<(StatusCode, Bytes), String>, tokio::task::JoinError>| {
        let (status, body) = joined.map_err(|error| error.to_string())??;
        if status.is_success() {
            Ok(())
        } else {
            Err(format!(
                "setup request refused: {status} {}",
                String::from_utf8_lossy(&body)
            ))
        }
    };
    for (url, authorization, body) in requests {
        if running.len() >= SETUP_CONCURRENCY {
            if let Some(joined) = running.join_next().await {
                check(joined)?;
            }
        }
        let client = client.clone();
        running.spawn(async move {
            call(
                &client,
                Method::POST,
                &url,
                authorization.as_deref(),
                Some(&body),
            )
            .await
        });
    }
    while let Some(joined) = running.join_next().await {
        check(joined)?;
    }
    Ok(())
}

fn bearer(secret: &[u8], user: &str, expires: usize) -> Result<String, String> {
    let claims = Claims {
        sub: user.to_string(),
        exp: expires,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .map(|token| format!("Bearer {token}"))
    .map_err(|error| error.to_string())
}

/// This process's CPU time so far (user + system), in microseconds. `/proc/self/stat` counts in
/// clock ticks of `USER_HZ`, which Linux fixes at 100 per second for this file: a resolution of
/// 10 ms, ample over runs of many seconds.
pub(super) fn cpu_micros() -> u64 {
    cpu_micros_from(&std::fs::read_to_string("/proc/self/stat").unwrap_or_default())
}

fn cpu_micros_from(stat: &str) -> u64 {
    // The command name (field 2) is in parentheses and may contain spaces: count from after it.
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .map(|(_, rest)| rest.split_whitespace().collect())
        .unwrap_or_default();
    // After the name, utime and stime are the 12th and 13th fields (fields 14 and 15 overall).
    let ticks = |index: usize| {
        fields
            .get(index)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    };
    (ticks(11) + ticks(12)) * 10_000
}

pub(super) fn unix_micros(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_micros())
}

pub(super) fn percentiles(histogram: &Histogram<u64>) -> String {
    format!(
        "p50 {}  p90 {}  p99 {}  p99.9 {}  max {}  mean {:.1}",
        histogram.value_at_quantile(0.50),
        histogram.value_at_quantile(0.90),
        histogram.value_at_quantile(0.99),
        histogram.value_at_quantile(0.999),
        histogram.max(),
        histogram.mean()
    )
}

async fn setup(
    client: &HttpClient,
    config: &Config,
    tokens: &[Arc<str>],
    symbols: &[String],
) -> Result<(), String> {
    let (status, body) = call(
        client,
        Method::POST,
        &format!("{}/session/open", config.operator),
        None,
        Some(&json!({ "trading_day": config.day })),
    )
    .await?;
    if status != StatusCode::OK {
        return Err(format!(
            "could not open {} ({status} {}); run against a new journal",
            config.day,
            String::from_utf8_lossy(&body)
        ));
    }
    let deposit = format!("{}/exchange/deposit", config.base);
    let share_deposit = format!("{}/exchange/shares/deposit", config.base);
    let mut requests = Vec::new();
    for token in tokens {
        requests.push((
            deposit.clone(),
            Some(token.to_string()),
            json!({ "amount": CASH_PER_USER }),
        ));
        for symbol in symbols {
            requests.push((
                share_deposit.clone(),
                Some(token.to_string()),
                json!({ "symbol": symbol, "quantity": SHARES_PER_SYMBOL }),
            ));
        }
    }
    all_succeed(client, requests).await
}

pub async fn run(args: &[String]) -> Result<(), String> {
    let config = parse(args)?;
    let secret = crate::middleware::auth_middleware::jwt_secret().ok_or(
        "JWT_SECRET must be set to the exchange's secret: traders' tokens are signed here",
    )?;
    let client: HttpClient = Client::builder(TokioExecutor::new())
        .pool_max_idle_per_host(usize::MAX)
        .build_http();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?;
    let expires = (now.as_secs() + 86_400) as usize;
    let users: Vec<String> = (0..config.users).map(|u| format!("u{u}")).collect();
    let symbols: Vec<String> = (0..config.symbols).map(|s| format!("S{s:03}")).collect();
    let tokens: Vec<Arc<str>> = users
        .iter()
        .map(|user| bearer(secret.as_bytes(), user, expires).map(Arc::from))
        .collect::<Result<_, _>>()?;

    // Setup, untimed, as `--bench` does it: open the trading day on the operator port, then cash
    // for every user and shares in every symbol, through the customer port. The health control
    // needs none of it.
    if !config.health {
        setup(&client, &config, &tokens, &symbols).await?;
    }

    // Real accounts for the login load, one per login task, with addresses unique to this run.
    let run_tag = now.as_nanos();
    let accounts: Vec<String> = (0..config.logins)
        .map(|i| format!("bench-{run_tag}-{i}@bench.test"))
        .collect();
    let registrations = accounts
        .iter()
        .enumerate()
        .map(|(i, email)| {
            (
                format!("{}/users/register", config.base),
                None,
                json!({ "username": format!("bench-{run_tag}-{i}"), "email": email, "password": LOGIN_PASSWORD }),
            )
        })
        .collect();
    all_succeed(&client, registrations).await?;

    let rate = match config.rate {
        0 => "max".to_string(),
        rate => format!("{rate}/s"),
    };
    println!(
        "http-bench: {} {}, rate {rate}, {} symbols, {} users, {} login loop(s), against {}",
        config.orders,
        if config.health {
            "GET /health requests (control)"
        } else {
            "orders"
        },
        config.symbols,
        config.users,
        config.logins,
        config.base
    );

    // The login load: each task logs in, waits for the answer, and logs in again, until the
    // measured orders are all answered or the deadline passes. Each result is saved as it
    // arrives, so the tasks can be abandoned mid-request at the end without losing what they did.
    let stop = Arc::new(AtomicBool::new(false));
    let login_stats = Arc::new(Mutex::new((Vec::<u64>::new(), 0u64)));
    let login_url = format!("{}/users/login", config.base);
    let login_tasks: Vec<_> = accounts
        .into_iter()
        .map(|email| {
            let client = client.clone();
            let stop = Arc::clone(&stop);
            let stats = Arc::clone(&login_stats);
            let url = login_url.clone();
            tokio::spawn(async move {
                let body = json!({ "email": email, "password": LOGIN_PASSWORD });
                while !stop.load(Ordering::Relaxed) {
                    let started = Instant::now();
                    let ok = matches!(
                        call(&client, Method::POST, &url, None, Some(&body)).await,
                        Ok((status, _)) if status == StatusCode::OK
                    );
                    {
                        let mut stats = stats.lock().expect("login stats lock");
                        if ok {
                            stats.0.push(started.elapsed().as_micros() as u64);
                        } else {
                            stats.1 += 1;
                        }
                    }
                    // A request can fail at once (a refused connection), and retrying at once
                    // would spin: a loop that never yields holds its runtime thread, so a few of
                    // them stop this whole generator, deadline included, and burn the CPU the
                    // exchange is being measured on. A real client backs off too.
                    if !ok {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            })
        })
        .collect();

    // The measured orders.
    let path = if config.health {
        "/health"
    } else {
        "/exchange/orders"
    };
    let orders_url: Arc<str> = Arc::from(format!("{}{path}", config.base));
    let mut rng = Rng(SEED);
    let mut send_lag = Histogram::<u64>::new(3).map_err(|e| e.to_string())?;
    let mut pending = Vec::with_capacity(config.orders as usize);
    // A `stage-timing` exchange times only what follows; any other exchange answers 404, which
    // changes nothing.
    let stages_url = format!("{}/stages", config.operator);
    let _ = call(
        &client,
        Method::POST,
        &format!("{stages_url}/reset"),
        None,
        None,
    )
    .await;
    // The schedule's origin, on the monotonic clock and on the wall clock. With `--start-at` it is
    // that wall-clock time itself, not the moment the sleep happens to end: a timer wakes late (by
    // up to its 1 ms tick, more on a busy host), and a late origin would shift every order after
    // the time `--feed-bench` assumes. Any lateness instead shows as send lag, charged to latency.
    let (start, start_wall) = match config.start_at {
        Some(start_at) => {
            let due = UNIX_EPOCH + Duration::from_millis(start_at);
            let wait = due
                .duration_since(SystemTime::now())
                .map_err(|_| format!("--start-at {start_at} passed before setup finished"))?;
            let start = Instant::now() + wait;
            tokio::time::sleep_until(start.into()).await;
            (start, due)
        }
        None => (Instant::now(), SystemTime::now()),
    };
    let cpu_before = cpu_micros();
    println!("  start      : unix us {}", unix_micros(start_wall));
    for i in 0..config.orders {
        let intended = if config.rate == 0 {
            Instant::now()
        } else {
            start + Duration::from_nanos(i * 1_000_000_000 / config.rate)
        };
        if config.rate > 0 {
            tokio::time::sleep_until(intended.into()).await;
        }
        send_lag
            .record(Instant::now().duration_since(intended).as_micros() as u64)
            .map_err(|e| e.to_string())?;
        // The same draws, in the same order, as `--bench`.
        let user = rng.below(config.users) as usize;
        let symbol = &symbols[rng.below(config.symbols) as usize];
        let side = if rng.below(2) == 0 { "BUY" } else { "SELL" };
        let price = BAND_LOW + rng.below(BAND_TICKS);
        let quantity = 1 + rng.below(10);
        let body = json!({
            "symbol": symbol,
            "side": side,
            "price": price,
            "quantity": quantity,
            "client_order_id": format!("o{i}"),
        });
        let client = client.clone();
        let url = Arc::clone(&orders_url);
        let token = Arc::clone(&tokens[user]);
        let health = config.health;
        pending.push(tokio::spawn(async move {
            let result = if health {
                call(&client, Method::GET, &url, None, None).await
            } else {
                call(&client, Method::POST, &url, Some(&token), Some(&body)).await
            };
            (
                result.map(|(status, _)| status.as_u16()),
                intended,
                Instant::now(),
            )
        }));
    }

    // With `--deadline`, an order still unanswered that many seconds after the first was due is
    // abandoned and counted, so a run against a stalled exchange ends and says how many it lost.
    let deadline = (config.deadline > 0).then(|| start + Duration::from_secs(config.deadline));
    let mut latency = Histogram::<u64>::new(3).map_err(|e| e.to_string())?;
    let mut statuses = BTreeMap::new();
    let mut errors = 0u64;
    let mut first_error = None;
    let mut unanswered = 0u64;
    let mut last_reply = start;
    for mut task in pending {
        let joined = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline.into(), &mut task).await {
                Ok(joined) => joined,
                Err(_) => {
                    task.abort();
                    unanswered += 1;
                    continue;
                }
            },
            None => task.await,
        };
        let (result, intended, replied) = joined.map_err(|error| error.to_string())?;
        match result {
            Ok(status) => *statuses.entry(status).or_insert(0u64) += 1,
            Err(error) => {
                errors += 1;
                first_error.get_or_insert(error);
            }
        }
        latency
            .record(replied.duration_since(intended).as_micros() as u64)
            .map_err(|e| e.to_string())?;
        last_reply = last_reply.max(replied);
    }
    let cpu_used = cpu_micros().saturating_sub(cpu_before);
    let seconds = if unanswered > 0 {
        config.deadline as f64
    } else {
        last_reply.duration_since(start).as_secs_f64()
    };
    stop.store(true, Ordering::Relaxed);
    for task in &login_tasks {
        task.abort();
    }
    let answered = config.orders - unanswered;

    if unanswered > 0 {
        // Answers divided by the deadline would look like a rate, but the run did not finish.
        println!(
            "  throughput : not measured ({answered} answered before the {} s deadline)",
            config.deadline
        );
    } else {
        println!(
            "  throughput : {:.0} orders/s over {seconds:.3} s ({answered} answered)",
            answered as f64 / seconds
        );
    }
    if deadline.is_some() {
        println!(
            "  unanswered : {unanswered} (no answer within the {} s deadline)",
            config.deadline
        );
    }
    println!(
        "  latency us : {}{}",
        if latency.is_empty() {
            "no answers".to_string()
        } else {
            percentiles(&latency)
        },
        if config.rate == 0 {
            "  (queue-bound at max rate)"
        } else {
            ""
        }
    );
    println!("  send lag us: {}", percentiles(&send_lag));
    println!(
        "  last reply : unix us {}",
        unix_micros(start_wall + last_reply.duration_since(start))
    );
    let statuses: Vec<String> = statuses
        .iter()
        .map(|(status, count)| format!("{status} x {count}"))
        .collect();
    println!("  statuses   : {}", statuses.join(", "));
    println!(
        "  errors     : {errors}{}",
        first_error.map_or(String::new(), |error| format!(" (first: {error})"))
    );
    println!(
        "  client cpu : {:.1} us per order, {:.0}% of one hardware thread (user + system, includes the login loops)",
        cpu_used as f64 / config.orders as f64,
        cpu_used as f64 / 1e6 / seconds * 100.0
    );
    if !login_tasks.is_empty() {
        let mut logins = Histogram::<u64>::new(3).map_err(|e| e.to_string())?;
        let (latencies, failures) = login_stats.lock().expect("login stats lock").clone();
        for sample in latencies {
            logins.record(sample).map_err(|e| e.to_string())?;
        }
        println!(
            "  logins     : {} succeeded ({:.1}/s), {failures} failed; latency us {}",
            logins.len(),
            logins.len() as f64 / seconds,
            percentiles(&logins)
        );
    }
    if let Ok((status, report)) = call(&client, Method::GET, &stages_url, None, None).await {
        if status == StatusCode::OK {
            print!("{}", String::from_utf8_lossy(&report));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_time_is_utime_plus_stime_even_with_spaces_in_the_name() {
        // Fields 14 (utime) and 15 (stime) are 250 and 50 ticks: 300 ticks of 10 ms = 3 s.
        let stat =
            "4242 (stock (bench) x) S 1 4242 4242 0 -1 4194304 100 0 0 0 250 50 0 0 20 0 9 0";
        assert_eq!(cpu_micros_from(stat), 3_000_000);
        assert_eq!(cpu_micros_from(""), 0);
    }

    #[test]
    fn the_workload_flags_parse_and_defaults_hold() {
        let args: Vec<String> = ["--rate", "5000", "--logins", "8", "--base", "http://h:1/"]
            .map(String::from)
            .into();
        let config = parse(&args).expect("valid flags");
        assert_eq!(
            (config.rate, config.logins, config.orders),
            (5_000, 8, 100_000)
        );
        assert_eq!(config.base, "http://h:1");
        assert!(!config.health);
        let health = parse(&["--target".to_string(), "health".to_string()]).expect("valid");
        assert!(health.health);
        assert!(parse(&["--target".to_string(), "x".to_string()]).is_err());
        assert!(parse(&["--rate".to_string()]).is_err());
        assert!(parse(&["--nonsense".to_string(), "1".to_string()]).is_err());
    }
}
