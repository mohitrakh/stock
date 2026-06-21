# Exchange Pipeline TODO

Current decision:

- Use a bounded `tokio::sync::mpsc` command queue from the Axum HTTP layer into one exchange worker.
- Use `tokio::sync::oneshot` response channels so HTTP handlers can wait for command results.
- Let the exchange worker own `OrderManager`, `Wallet`, `RiskManager`, `Sequencer`, and `MatchingEngine`.

Why this is acceptable now:

- It removes direct `Mutex<OrderManager>` access from request handlers.
- It gives the exchange core a single owner and deterministic command order.
- It creates the right boundary: outside code sends commands; the exchange core mutates state.

Temporary limitation:

- Tokio `mpsc` is not the final queue for a serious low-latency exchange.
- Later, replace this queue with a purpose-built event pipeline such as a fixed-size ring buffer, shared-memory log, or mmap-backed event store.
- The architecture should keep the command/event boundary stable so the queue implementation can change without rewriting the engine.

Future direction:

- Split inbound commands from outbound events.
- Persist sequenced commands/events for replay.
- Publish executions to market data and reporting consumers.
- Consider per-symbol workers only after wallet, risk, sequencing, and replay semantics are clear.
