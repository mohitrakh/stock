# Chapter 29: Stock Exchange

In this chapter, we design an electronic stock exchange system.

The basic function of an exchange is to facilitate the matching of buyers and sellers efficiently. This fundamental function has not changed over time. Before the rise of computing, people exchanged tangible goods by bartering and shouting at each other to get matched. Today, orders are processed silently by supercomputers, and people trade not only for the exchange of products, but also for speculation and arbitrage. Technology has greatly changed the landscape of trading and exponentially boosted electronic market trading volume.

When it comes to stock exchanges, most people think about major market players like The New York Stock Exchange (NYSE) or Nasdaq, which have existed for over fifty years. In fact, there are many other types of exchange. Some focus on vertical segmentation of the financial industry and place special focus on technology, while others have an emphasis on fairness. Before diving into the design, it is important to check with the interviewer about the scale and the important characteristics of the exchange in question.

> ![[Figure 1 - Largest stock exchanges by market capitalization (Trillion-dollar club)]]

---

## Step 1 — Understand the Problem and Establish Design Scope

A modern exchange is a complicated system with stringent requirements on latency, throughput, and robustness. Before we start, let's ask the interviewer a few questions to clarify the requirements.

**Candidate:** Which securities are we going to trade? Stocks, options, or futures?
**Interviewer:** For simplicity, only stocks.

**Candidate:** Which types of order operations are supported: placing a new order, canceling an order, or replacing an order? Do we need to support limit order, market order, or conditional order?
**Interviewer:** We need to support the following: placing a new order and canceling an order. For the order type, we only need to consider the limit order.

**Candidate:** Does the system need to support after-hours trading?
**Interviewer:** No, we just need to support the normal trading hours.

**Candidate:** Could you describe the basic functions of the exchange? And the scale of the exchange, such as how many users, how many symbols, and how many orders?
**Interviewer:** A client can place new limit orders or cancel them, and receive matched trades in real-time. A client can view the real-time order book (the list of buy and sell orders). The exchange needs to support at least tens of thousands of users trading at the same time, and it needs to support at least 100 symbols. For the trading volume, we should support billions of orders per day. Also, the exchange is a regulated facility, so we need to make sure it runs risk checks.

**Candidate:** Could you please elaborate on risk checks?
**Interviewer:** Let's just do simple risk checks. For example, a user can only trade a maximum of 1 million shares of Apple stock in one day.

**Candidate:** I noticed you didn't mention user wallet management. Is it something we also need to consider?
**Interviewer:** Good catch! We need to make sure users have sufficient funds when they place orders. If an order is waiting in the order book to be filled, the funds required for the order need to be withheld to prevent overspending.

### Non-functional Requirements

After checking with the interviewer for the functional requirements, we should determine the non-functional requirements. In fact, requirements like "at least 100 symbols" and "tens of thousands of users" tell us that the interviewer wants us to design a small-to-medium scale exchange. On top of this, we should make sure the design can be extended to support more symbols and users. Many interviewers focus on extensibility as an area for follow-up questions.

Here is a list of non-functional requirements:

- **Availability.** At least 99.99%. Availability is crucial for exchanges. Downtime, even seconds, can harm reputation.
- **Fault tolerance.** Fault tolerance and a fast recovery mechanism are needed to limit the impact of a production incident.
- **Latency.** The round-trip latency should be at the millisecond level, with a particular focus on the 99th percentile latency. The round trip latency is measured from the moment a market order enters the exchange to the point where the market order returns as a filled execution. A persistently high 99th percentile latency causes a terrible user experience for a small number of users.
- **Security.** The exchange should have an account management system. For legal compliance, the exchange performs a KYC (Know Your Client) check to verify a user's identity before a new account is opened. For public resources, such as web pages containing market data, we should prevent distributed denial-of-service (DDoS) attacks.

### Back-of-the-Envelope Estimation

Let's do some simple back-of-the-envelope calculations to understand the scale of the system:

- 100 symbols
- 1 billion orders per day
- NYSE Stock Exchange is open Monday through Friday from 9:30 am to 4:00 pm Eastern Time. That's 6.5 hours in total.
- **QPS:** 1 billion / 6.5 / 3600 = ~43,000
- **Peak QPS:** 5 × QPS = 215,000. The trading volume is significantly higher when the market first opens in the morning and before it closes in the afternoon.

---

## Step 2 — Propose High-Level Design and Get Buy-In

Before we dive into the high-level design, let's briefly discuss some basic concepts and terminology that are helpful for designing an exchange.

### Business Knowledge 101

#### Broker

Most retail clients trade with an exchange via a broker. Some brokers whom you might be familiar with include Charles Schwab, Robinhood, Etrade, Fidelity, etc. These brokers provide a friendly user interface for retail users to place trades and view market data.

#### Institutional Client

Institutional clients trade in large volumes using specialized trading software. Different institutional clients operate with different requirements. For example, pension funds aim for a stable income. They trade infrequently, but when they do trade, the volume is large. They need features like order splitting to minimize the market impact of their sizable orders. Some hedge funds specialize in market making and earn income via commission rebates. They need low latency trading abilities, so obviously they cannot simply view market data on a web page or a mobile app, as retail clients do.

#### Limit Order

A limit order is a buy or sell order with a fixed price. It might not find a match immediately, or it might just be partially matched.

#### Market Order

A market order doesn't specify a price. It is executed at the prevailing market price immediately. A market order sacrifices cost in order to guarantee execution. It is useful in certain fast-moving market conditions.

#### Market Data Levels

The US stock market has three tiers of price quotes: L1 (level 1), L2, and L3.

- **L1 market data** contains the best bid price, ask price, and quantities. Bid price refers to the highest price a buyer is willing to pay for a stock. Ask price refers to the lowest price a seller is willing to sell the stock.
- **L2** includes more price levels than L1.
- **L3** shows price levels and the queued quantity at each price level.

> ![[Figure 2 - Level 1 data (best bid/ask price and quantities)]]

> ![[Figure 3 - Level 2 data (multiple price levels)]]

> ![[Figure 4 - Level 3 data (price levels with queued quantities)]]

#### Candlestick Chart

A candlestick chart represents the stock price for a certain period of time. A candlestick shows the market's open, close, high, and low price for a time interval. The common time intervals are one-minute, five-minute, one-hour, one-day, one-week, and one-month.

> ![[Figure 5 - A single candlestick chart showing open, close, high, and low price]]

#### FIX

FIX protocol, which stands for **Financial Information eXchange protocol**, was created in 1991. It is a vendor-neutral communications protocol for exchanging securities transaction information. See below for an example of a securities transaction encoded in FIX:

```
8=FIX.4.2 | 9=176 | 35=8 | 49=PHLX | 56=PERS | 52=20071123-05:30:00.000 |
11=ATOMNOCCC9990900 | 20=3 | 150=E | 39=E | 55=MSFT | 167=CS | 54=1 |
38=15 | 40=2 | 44=15 | 58=PHLX EQUITY TESTING | 59=0 | 47=C | 32=0 |
31=0 | 151=15 | 14=0 | 6=0 | 10=128 |
```

---

### High-Level Design

Now that we have some basic understanding of the key concepts, let's take a look at the high-level design.

> ![[Figure 6 - High-level design of the stock exchange system]]

Let's trace the life of an order through various components in the diagram to see how the pieces fit together.

#### Trading Flow (Critical Path)

Everything has to happen fast in this flow:

1. A client places an order via the broker's web or mobile app.
2. The broker sends the order to the exchange.
3. The order enters the exchange through the **client gateway**. The client gateway performs basic gatekeeping functions such as input validation, rate limiting, authentication, normalization, etc. The client gateway then forwards the order to the order manager.
4. The **order manager** performs risk checks based on rules set by the risk manager.
5. (Risk check continues)
6. After passing risk checks, the order manager verifies there are sufficient funds in the **wallet** for the order.
7. The order is sent to the **matching engine**.
8. When a match is found, the matching engine emits two executions (also called fills), with one each for the buy and sell sides.
9. Both orders and executions are sequenced in the **sequencer** to guarantee deterministic matching results when replayed.
10–14. The executions are returned to the client.

#### Market Data Flow

- **Step M1:** The matching engine generates a stream of executions (fills) as matches are made. The stream is sent to the **market data publisher**.
- **Step M2:** The market data publisher constructs the candlestick charts and the order books from the stream of executions and orders. It then sends market data to the **data service**.
- **Step M3:** The market data is saved to specialized storage for real-time analytics. The brokers connect to the data service to obtain timely market data.

#### Reporting Flow

- **Steps R1–R2:** The **reporter** collects all the necessary reporting fields (e.g. `client_id`, `price`, `quantity`, `order_type`, `filled_quantity`, `remaining_quantity`) from orders and executions, and writes the consolidated records to the database.

> **Note:** The trading flow (steps 1–14) is on the critical path, while the market data flow and reporting flow are not. They have different latency requirements.

---

### Trading Flow (Deep Dive)

#### Matching Engine

The matching engine is also called the **cross engine**. Here are its primary responsibilities:

- Maintain the **order book** for each symbol. An order book is a list of buy and sell orders for a symbol.
- **Match** buy and sell orders. A match results in two executions (fills), with one each for the buy and sell sides. The matching function must be fast and accurate.
- **Distribute** the execution stream as market data.

A highly available matching engine implementation must be able to produce matches in a **deterministic order**. That is, given a known sequence of orders as input, the matching engine must produce the same sequence of executions when replayed.

#### Sequencer

The sequencer is the key component that makes the matching engine deterministic. It stamps every incoming order with a **sequence ID** before it is processed by the matching engine. It also stamps every pair of executions completed by the matching engine with sequence IDs.

> ![[Figure 7 - Inbound and outbound sequencers with sequential sequence IDs]]

The incoming orders and outgoing executions are stamped with sequence IDs for these reasons:

- **Timeliness and fairness**
- **Fast recovery / replay**
- **Exactly-once guarantee**

The sequencer also functions as a **message queue** and an **event store** for the orders and executions. It is similar to having two Kafka event streams connected to the matching engine — one for incoming orders, one for outgoing executions.

#### Order Manager

The order manager receives orders on one end and receives executions on the other. It manages the orders' states.

The order manager receives inbound orders from the client gateway and performs the following:

- Sends the order for **risk checks** (e.g., verifying a user's trade volume is below $1M a day).
- Checks the order against the user's **wallet** and verifies there are sufficient funds.
- Sends the order to the **sequencer** where the order is stamped with a sequence ID.

On the other end, the order manager receives executions from the matching engine via the sequencer and returns them to the brokers via the client gateway.

The order manager uses **event sourcing** to manage the various state transitions. There can be tens of thousands of cases involved in a real exchange system.

#### Client Gateway

The client gateway is the gatekeeper for the exchange. It receives orders placed by clients and routes them to the order manager.

> ![[Figure 8 - Client gateway components (input validation, rate limiting, authentication, normalization, etc.)]]

The client gateway is on the critical path and is latency-sensitive. It should stay lightweight. There are different types of client gateways for retail and institutional clients.

> ![[Figure 9 - Different client gateway connections to an exchange, including colocation (colo) engine]]

An extreme example is the **colocation (colo) engine** — the trading engine software running on servers rented by the broker in the exchange's data center. The latency is literally the time it takes for light to travel from the colocated server to the exchange server.

---

### Market Data Flow (Detail)

The **market data publisher (MDP)** receives executions from the matching engine and builds the order books and candlestick charts from the stream of executions.

> ![[Figure 10 - Market Data Publisher and how it fits with other components]]

---

### Reporting Flow (Detail)

The reporter is not on the trading critical path, but it is a critical part of the system. It provides trading history, tax reporting, compliance reporting, settlements, etc. **Accuracy and compliance** are key factors for the reporter.

> ![[Figure 11 - Reporter flow components]]

---

## API Design

Clients interact with the stock exchange via the brokers to place orders, view executions, view market data, and download historical data. We use **RESTful conventions** for the API.

### Order

**`POST /v1/order`**
Places an order. Requires authentication.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | String | The stock symbol |
| `side` | String | `buy` or `sell` |
| `price` | Long | The price of the limit order |
| `orderType` | String | `limit` or `market` |
| `quantity` | Long | The quantity of the order |

**Response Body:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | Long | The ID of the order |
| `creationTime` | Long | The system creation time |
| `filledQuantity` | Long | Quantity successfully executed |
| `remainingQuantity` | Long | Quantity still to be executed |
| `status` | String | `new` / `canceled` / `filled` |

**Status Codes:** `200` success, `40x` error, `500` server error

---

### Execution

**`GET /execution?symbol={:symbol}&orderId={:orderId}&startTime={:startTime}&endTime={:endTime}`**
Queries execution info. Requires authentication.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | String | The stock symbol |
| `orderId` | String | (Optional) The ID of the order |
| `startTime` | Long | Query start time in epoch |
| `endTime` | Long | Query end time in epoch |

**Response Body:**

| Field | Type | Description |
|-------|------|-------------|
| `executions` | Array | Each execution in scope |
| `id` | Long | The ID of the execution |
| `orderId` | Long | The ID of the order |
| `symbol` | String | The stock symbol |
| `side` | String | `buy` or `sell` |
| `price` | Long | The price of the execution |
| `orderType` | String | `limit` or `market` |
| `quantity` | Long | The filled quantity |

**Status Codes:** `200` success, `40x` error, `500` server error

---

### Order Book

**`GET /marketdata/orderBook/L2?symbol={:symbol}&depth={:depth}`**
Queries L2 order book information for a symbol with designated depth.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | String | The stock symbol |
| `depth` | Int | Order book depth per side |

**Response Body:**

| Field | Type | Description |
|-------|------|-------------|
| `bids` | Array | Array with price and size |
| `asks` | Array | Array with price and size |

**Status Codes:** `200` success, `40x` error, `500` server error

---

### Historical Prices (Candlestick Charts)

**`GET /marketdata/candles?symbol={:symbol}&resolution={:resolution}&startTime={:startTime}&endTime={:endTime}`**
Queries candlestick chart data for a symbol given a time range and resolution.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | String | The stock symbol |
| `resolution` | Long | Window length in seconds |
| `startTime` | Long | Start time in epoch |
| `endTime` | Long | End time in epoch |

**Response Body:**

| Field | Type | Description |
|-------|------|-------------|
| `candles` | Array | Array of candlestick data |
| `open` | Double | Open price |
| `close` | Double | Close price |
| `high` | Double | High price |
| `low` | Double | Low price |

**Status Codes:** `200` success, `40x` error, `500` server error

---

## Data Models

There are three main types of data in the stock exchange:

1. Product, Order, and Execution
2. Order Book
3. Candlestick Chart

### Product, Order, and Execution

- **Product:** Describes the attributes of a traded symbol (product type, trading symbol, UI display symbol, settlement currency, lot size, tick size, etc.). This data doesn't change frequently. It is primarily used for UI display and is highly cacheable.
- **Order:** Represents the inbound instruction for a buy or sell order.
- **Execution:** Represents the outbound matched result. Also called a **fill**. Not every order has an execution. The output of the matching engine contains two executions, representing the buy and sell sides of a matched order.

> ![[Figure 12 - Logical model diagram showing relationships between Product, Order, and Execution]]

In the critical trading path, orders and executions are **not stored in a database**. To achieve high performance, this path executes trades in memory and leverages hard disk or shared memory to persist and share orders and executions.

### Order Book

An order book is a list of buy and sell orders for a specific security or financial instrument, organized by price level. It is a key data structure in the matching engine. An efficient data structure for an order book must satisfy these requirements:

- **Constant lookup time.** Getting volume at a price level or between price levels.
- **Fast add/cancel/execute operations**, preferably O(1) time complexity.
- **Fast update.** Replacing an order.
- **Query best bid/ask.**
- **Iterate through price levels.**

> ![[Figure 13 - Limit order book illustrated — large buy order matching against multiple sell price levels]]

The following code snippet shows an implementation of the order book:

```java
class PriceLevel {
    private Price limitPrice;
    private long totalVolume;
    private List<Order> orders;
}

class Book<Side> {
    private Side side;
    private Map<Price, PriceLevel> limitMap;
}

class OrderBook {
    private Book<Buy> buyBook;
    private Book<Sell> sellBook;
    private PriceLevel bestBid;
    private PriceLevel bestOffer;
    private Map<OrderID, Order> orderMap;
}
```

To achieve O(1) time complexity, change the data structure of `orders` to a **doubly-linked list**:

- **Placing a new order** → adding to the tail of PriceLevel → O(1)
- **Matching an order** → deleting from the head of PriceLevel → O(1)
- **Canceling an order** → use `orderMap` to find the order in O(1), then delete using doubly-linked list's previous pointer → O(1)

> ![[Figure 14 - Place, match, and cancel an order in O(1) using doubly-linked list]]

### Candlestick Chart

```java
class Candlestick {
    private long openPrice;
    private long closePrice;
    private long highPrice;
    private long lowPrice;
    private long volume;
    private long timestamp;
    private int interval;
}

class CandlestickChart {
    private LinkedList<Candlestick> sticks;
}
```

When the interval for the candlestick has elapsed, a new `Candlestick` instance is created for the next interval and added to the linked list in `CandlestickChart`.

**Memory optimizations:**
- Use **pre-allocated ring buffers** to hold sticks to reduce new object allocations.
- **Limit the number of sticks** in memory and persist the rest to disk.

Market data is usually persisted in an **in-memory columnar database** (e.g., KDB) for real-time analytics. After the market is closed, data is persisted in a historical database.

---

## Step 3 — Design Deep Dive

### Performance

Latency can be broken down as:

```
Latency = ∑ executionTime along critical path
```

Two ways to reduce latency:

1. **Decrease the number of tasks** on the critical path.
2. **Shorten the time spent on each task:**
   - Reducing or eliminating network and disk usage
   - Reducing execution time for each task

The critical trading path includes:

```
gateway → order manager → sequencer → matching engine
```

To eliminate network and disk latency, modern exchanges evolve to put everything on the **same server**, communicating via `mmap` as an event store.

> ![[Figure 15 - A low-latency single server exchange design with all components on one server]]

#### Application Loop

An **application loop** keeps polling for tasks to execute in a `while` loop and is the primary task execution mechanism. To meet strict latency budgets:

- Each application loop (main processing loop) is **single-threaded**.
- The thread is **pinned to a fixed CPU core**.

> ![[Figure 16 - Application loop thread in Order Manager pinned to CPU 1]]

Benefits of CPU pinning:
- **No context switch** — CPU 1 is fully allocated to the order manager's application loop.
- **No locks and no lock contention** — only one thread updates states.

Both contribute to a low 99th percentile latency.

#### mmap

`mmap(2)` is a POSIX-compliant UNIX system call that maps a file into the memory of a process. It provides a mechanism for **high-performance sharing of memory between processes**.

When the backing file is in `/dev/shm` (a memory-backed file system), access to the shared memory results in **no disk access at all**. Sending a message on the mmap message bus takes **sub-microsecond** time.

---

### Event Sourcing

In a **traditional application**, states are persisted in a database. Only current states are kept — there are no records of events that led to those states.

In **event sourcing**, instead of storing current states, an **immutable log of all state-changing events** is kept. These events are the golden source of truth.

> ![[Figure 17 - Non-event sourcing vs event sourcing comparison]]

> ![[Figure 18 - An event sourcing design using mmap event store as a message bus]]

**Key flow in event sourcing design:**

1. The external domain communicates with the trading domain using **FIX over Simple Binary Encoding (SBE)** for fast and compact encoding.
2. The gateway sends each order as a `NewOrderEvent` via the **Event Store Client**.
3. The **order manager** (embedded in the matching engine) receives the `NewOrderEvent`, validates it, adds it to internal order states, and sends the order to the matching core.
4. If the order gets matched, an `OrderFilledEvent` is generated and sent to the event store.
5. Other components (market data processor, reporter) **subscribe** to the event store and process events accordingly.

**Key design differences in event sourcing:**

- The **order manager** becomes a reusable library embedded in different components (instead of a centralized service). Each component maintains its own order states — guaranteed to be identical and replayable.
- The **sequencer** becomes a single writer that sequences events before sending to the event store. It is super fast and does only one simple thing.

> ![[Figure 19 - Sample design of Sequencer in a memory-map (MMap) environment with ring buffers]]

---

### High Availability

For high availability, the design aims for **4 nines (99.99%)** — only 8.64 seconds of downtime per day.

- **Stateless services** (e.g., client gateway) → horizontally scaled by adding more servers.
- **Stateful components** (e.g., order manager, matching engine) → state data is copied across replicas.

#### Hot-Warm Matching Engine

> ![[Figure 20 - Hot-warm matching engine setup with primary and warm instance]]

- The **hot matching engine** works as the primary instance.
- The **warm engine** receives and processes the exact same events but does **not** send any events out.
- When the primary goes down, the warm instance **immediately takes over**.
- When a warm instance restarts, it recovers all states from the event store.

**Heartbeats** are sent from the matching engine to detect problems in the primary.

To extend across multiple machines or data centers, the entire event store is replicated from the hot server to all warm replicas using **reliable UDP**.

---

### Fault Tolerance

When all warm instances go down, we replicate core data to data centers in **multiple cities** to mitigate the risk of natural disasters or large-scale power outages.

**Key questions to address:**
- If the primary goes down, how and when do we failover?
- How do we choose the leader among backup instances?
- What is the **Recovery Time Objective (RTO)**?
- What is the **Recovery Point Objective (RPO)**?

#### Challenges

- The system might send out **false alarms** causing unnecessary failovers.
- **Bugs** could bring down both primary and backup instances.

**Suggestions:**
- Start with **manual failover**, automate only after gaining operational experience.
- Use **Chaos engineering** to surface edge cases faster.

#### Raft Consensus Algorithm

> ![[Figure 21 - Event replication in Raft cluster with 5 servers, showing leader sending events to followers over RPC]]

- Minimum votes required for an operation: **(N/2 + 1)**, where N = number of cluster members.
- In a 5-server cluster, minimum is **3**.
- The leader sends **heartbeat messages** (`AppendEntries` with no content) to followers.
- If a follower doesn't receive heartbeats for a timeout period → triggers **election timeout** → initiates new election.
- The first follower to reach election timeout becomes a **candidate** and requests votes.
- If multiple followers become candidates simultaneously → **split vote** → election times out and a new election starts.

> ![[Figure 22 - Raft terms — time divided into intervals representing normal operation and elections]]

#### Recovery Objectives

- **RTO (Recovery Time Objective):** For a stock exchange, we need **second-level RTO** requiring automatic failover.
- **RPO (Recovery Point Objective):** Data loss is **not acceptable**. RPO is near zero. With Raft, many copies of data exist and state consensus is guaranteed.

---

### Matching Algorithms

```java
Context handleOrder(OrderBook orderBook, OrderEvent orderEvent) {
    if (orderEvent.getSequenceId() != nextSequence) {
        return Error(OUT_OF_ORDER, nextSequence);
    }
    if (!validateOrder(symbol, price, quantity)) {
        return ERROR(INVALID_ORDER, orderEvent);
    }
    Order order = createOrderFromEvent(orderEvent);
    switch (msgType):
        case NEW:
            return handleNew(orderBook, order);
        case CANCEL:
            return handleCancel(orderBook, order);
        default:
            return ERROR(INVALID_MSG_TYPE, msgType);
}

Context handleNew(OrderBook orderBook, Order order) {
    if (BUY.equals(order.side)) {
        return match(orderBook.sellBook, order);
    } else {
        return match(orderBook.buyBook, order);
    }
}

Context handleCancel(OrderBook orderBook, Order order) {
    if (!orderBook.orderMap.contains(order.orderId)) {
        return ERROR(CANNOT_CANCEL_ALREADY_MATCHED, order);
    }
    removeOrder(order);
    setOrderStatus(order, CANCELED);
    return SUCCESS(CANCEL_SUCCESS, order);
}

Context match(OrderBook book, Order order) {
    Quantity leavesQuantity = order.quantity - order.matchedQuantity;
    Iterator<Order> limitIter = book.limitMap.get(order.price).orders;
    while (limitIter.hasNext() && leavesQuantity > 0) {
        Quantity matched = min(limitIter.next.quantity, order.quantity);
        order.matchedQuantity += matched;
        leavesQuantity = order.quantity - order.matchedQuantity;
        remove(limitIter.next);
        generateMatchedFill();
    }
    return SUCCESS(MATCH_SUCCESS, order);
}
```

The pseudocode uses the **FIFO (First In First Out)** matching algorithm. The order that comes in first at a certain price level gets matched first.

Other common matching algorithms:
- **FIFO with LMM (Lead Market Maker):** Allocates a certain quantity to the LMM based on a predefined ratio ahead of the FIFO queue.
- Used in futures trading, dark pools, and many other scenarios.

---

### Determinism

There are two types of determinism:

#### Functional Determinism
The design choices (sequencer + event sourcing) guarantee that if events are replayed in the same order, the results will be the same. What matters is the **order of events**, not the actual time they occur.

> ![[Figure 23 - Time in event sourcing — discrete uneven timestamps converted to continuous sequence, greatly reducing replay/recovery time]]

#### Latency Determinism
Having almost the same latency through the system for each trade. Measured by **99th percentile latency** or even **99.99th percentile latency**.

Tools:
- Use **HdrHistogram** to calculate latency.

Common sources of large latency fluctuations in Java:
- **HotSpot JVM Stop-the-World garbage collection** (safe points).

---

### Market Data Publisher Optimizations

The **market data publisher (MDP)** receives matched results from the matching engine and rebuilds the order book and candlestick charts, then publishes to subscribers.

> ![[Figure 24 - Market Data Publisher design with ring buffers for order book and candlestick chart data]]

Key design:
- Uses **ring buffers** (circular buffers) — fixed-size queues with head connected to tail.
- Space in a ring buffer is **pre-allocated** — no object creation or deallocation.
- The data structure is **lock-free**.
- **Padding** ensures that the ring buffer's sequence number is never in a cache line with anything else.

MDP has **multiple service levels** (e.g., retail clients view 5 levels of L2 data by default, can pay for 10 levels). The MDP has an upper limit on the number of candlesticks it holds in memory.

---

### Distribution Fairness of Market Data

In stock trading, having lower latency than others is like having an oracle that can see the future. For a regulated exchange, it is important that **all receivers of market data get that data at the same time**.

Problem: If subscribers are ordered by connection time, smart clients will fight to be first on the list when the market opens.

**Mitigations:**
- **Multicast using reliable UDP** to broadcast updates to many participants at once.
- Assign a **random order** when a subscriber connects.

#### Multicast

Three types of data transport protocols:

| Protocol | Description |
|----------|-------------|
| **Unicast** | From one source to one destination |
| **Broadcast** | From one source to an entire subnetwork |
| **Multicast** | From one source to a set of hosts on different subnetworks |

By configuring several receivers in the same **multicast group**, they will in theory receive data at the same time. UDP is unreliable, so retransmission solutions (e.g., NACK-Oriented Reliable Multicast) are used to handle packet loss.

---

### Colocation

Many exchanges offer **colocation services** — placing hedge funds' or brokers' servers in the same data center as the exchange. Latency in placing an order is essentially proportional to the **length of the cable**. Colocation can be considered a **paid-for VIP service** and does not break the notion of fairness.

---

### Network Security

An exchange usually provides public interfaces, and DDoS attacks are a real challenge.

**Techniques to combat DDoS:**

- **Isolate** public services and data from private services. Have multiple read-only copies to isolate problems.
- Use a **caching layer** to store infrequently updated data. Most queries won't hit databases.
- **Harden URLs** against DDoS attacks. Use simple, cacheable URLs (e.g., `/data/recent`) instead of complex query strings (e.g., `/data?from=123&to=456`). Cache at the CDN layer.
- Use an effective **safelist/blocklist** mechanism (many network gateway products provide this).
- Use **rate limiting** to defend against DDoS attacks.

---

## Wrap Up

After reading this chapter, you may come to the conclusion that an ideal deployment model for a big exchange is to put everything on a **single gigantic server** or even one single process. Indeed, this is exactly how some exchanges are designed!

With the recent development of the cryptocurrency industry, many crypto exchanges use **cloud infrastructure** to deploy their services. Some decentralized finance projects are based on the notion of **AMM (Automatic Market Making)** and don't even need an order book.

The convenience provided by the cloud ecosystem changes some of the designs and lowers the threshold for entering the industry, injecting innovative energy into the financial world.

---

## Reference Materials

1. LMAX exchange (Disruptor): https://www.lmax.com/exchange
2. IEX — "Flash Boys Exchange": https://en.wikipedia.org/wiki/IEX
3. NYSE matched volume: https://www.nyse.com/markets/us-equity-volumes
4. HKEX daily trading volume: https://www.hkex.com.hk/Market-Data/Statistics/Consolidated-Reports/Securities-Statistics-Archive/Trading_Value_Volume_And_Number_Of_Deals
5. All of the World's Stock Exchanges by Size: http://money.visualcapitalist.com/all-of-the-worlds-stock-exchanges-by-size/
6. Denial of service attack: https://en.wikipedia.org/wiki/Denial-of-service_attack
7. Market impact: https://en.wikipedia.org/wiki/Market_impact
8. Fix trading: https://www.fixtrading.org/
9. Event Sourcing: https://martinfowler.com/eaaDev/EventSourcing.html
10. CME Co-Location and Data Center Services: https://www.cmegroup.com/trading/colocation/co-location-services.html
11. Epoch: https://www.epoch101.com/
12. Order book (Investopedia): https://www.investopedia.com/terms/o/order-book.asp
13. Order book (Wikipedia): https://en.wikipedia.org/wiki/Order_book
14. How to Build a Fast Limit Order Book: https://bit.ly/3ngMtEO
15. Developing with kdb+ and the q language: https://code.kx.com/q/
16. Latency Numbers Every Programmer Should Know: https://gist.github.com/jboner/2841832
17. mmap: https://en.wikipedia.org/wiki/Memory_map
18. Context switch: https://bit.ly/3pva7A6
19. Reliable User Datagram Protocol: https://en.wikipedia.org/wiki/Reliable_User_Datagram_Protocol
20. Aeron Design Overview: https://github.com/real-logic/aeron/wiki/Design-Overview
21. Chaos engineering: https://en.wikipedia.org/wiki/Chaos_engineering
22. Raft: https://raft.github.io/
23. Designing for Understandability — the Raft Consensus Algorithm: https://raft.github.io/slides/uiuc2016.pdf
24. Supported Matching Algorithms (CME): https://bit.ly/3aYoCEo
25. Dark pool: https://www.investopedia.com/terms/d/dark-pool.asp
26. HdrHistogram: http://hdrhistogram.org/
27. HotSpot (virtual machine): https://en.wikipedia.org/wiki/HotSpot_(virtual_machine)
28. Cache line padding: https://bit.ly/3lZTFWz
29. NACK-Oriented Reliable Multicast: https://en.wikipedia.org/wiki/NACK-Oriented_Reliable_Multicast
30. AWS Coinbase Case Study: https://aws.amazon.com/solutions/case-studies/coinbase/
