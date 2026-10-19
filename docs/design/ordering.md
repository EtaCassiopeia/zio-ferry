# Ordering, lateness and grouping: a strategy-agnostic kernel with pluggable policies

Status: partly built. Built: the pluggable order with `Order.arrival` and `Order.by`
([0011](../decisions/0011-pluggable-dispatch-order.md)), the `directIdleKeys` switch
([0010](../decisions/0010-direct-calls-for-idle-keys.md)) and the `FoldWrite` helper
([0012](../decisions/0012-fold-write-helper.md)). Still proposals, each waiting for a store that needs it: the
`eventTime` hold, the `Late` verdict and `partition`. The draft records in Part B use their own numbering and are
superseded by the accepted records where they overlap. Part A is the design note; Part C is the verification plan,
build order and migration path.

---

## Part A. Design note

### A.1 The problem

`Ferry.perKey` batches inputs per key, keeps one call in flight per key, and dispatches a key's inputs in the order
they were admitted. "Admitted" means the instant the admission transaction committed: **arrival order**.

Some inputs carry a time of their own: the moment the thing they describe happened (**event time**), or a sequence
number the producer assigned. Inputs arrive with varying latency, so arrival order and event-time order disagree now and
then. Whether that matters depends entirely on the store behind the batch call:

- a store that can place an input older than its newest stored one where it belongs (a timeline it re-evaluates, a log it
  can insert into, an aggregate that is commutative) needs nothing from the batcher beyond what it does today;
- a store that can only **append** (strictly increasing timestamps, "next expected sequence number", an all-or-nothing
  group operation that refuses an input older than the group's newest) needs the batcher to present inputs in event-time
  order, and needs a rule for an input that arrives too late to be placed.

The batcher decides **dispatch order**: the order in which a key's inputs reach the store. It never decides what the
store does with them. That split matters because the batcher's view is local (one process, one clock), while the
store's view is authoritative (it holds the state). A batcher that remembered "the newest event time I sent for this
key" would hold state the store already holds, be wrong after a routing handover, and cost memory per key ever seen; it
is the same mistake as a write-ahead log in the batcher
([0005](../decisions/0005-fenced-single-writer.md)). So lateness *across* batches is the store's verdict; the batcher
can only give a verdict relative to its own clock, and only when asked to hold inputs for a bound.

### A.2 Strategies and what each costs

| Strategy | Dispatch order within a key | Waits? | Latency | Memory | What it guarantees | What it leaves to the store |
|---|---|---|---|---|---|---|
| **Arrival** (today) | admission order | no | none; idle keys go direct | the backlog | order = arrival | everything about event time |
| **Sorted** (`by(rank)`) | ascending rank among the inputs waiting together; ties by arrival | no | none; idle keys go direct | the backlog | each batch is in rank order; under load, everything that arrived during the previous round trip is sorted together (a settling window equal to one round trip, for free) | an input older than an earlier batch |
| **Event time with a bound** (`eventTime(at, lateness)`) | ascending event time; an input is held until `at + lateness` on the local clock | yes, `lateness` per input | at least `lateness` on every input; no direct path | `rate × lateness` per key, within `maxQueueing` | **ordered unless late**: among inputs not reported late, dispatch is in event-time order; an input whose event time is already more than `lateness` old at admission is *late*, detected without per-key state | what to do with an admitted late input; clock skew beyond the bound |
| **Sequence numbers with gap waiting** | by sequence; wait for a missing number | yes, until a timeout | unbounded on a lost input | per-key "next expected" state, kept forever or wrong | gap-free order | — |
| **Don't wait; the store handles late inputs** | arrival or sorted | no | none | the backlog | nothing about event time | placement of late inputs |

Sequence numbers with gap waiting are not built: the "next expected" number is state only the store can hold correctly
(it is the fence of 0005: a conditional write on the expected sequence), and waiting for a gap deadlocks on an input that
was lost. Sorting by sequence number *within a batch* is the Sorted strategy with `rank = sequence`.

The bound in the event-time strategy is a **processing-time watermark**: an input is dispatchable once the local clock
has passed `at + lateness`, and late if the clock had already passed it at admission. The guarantee holds for any
interleaving (proof sketch in 0011) and needs no per-key state. The bound must cover clock skew between producers and
this process plus the spread of delivery latency; the batcher cannot measure either, but the application can, because it
has `arrival − at` for every input. Clock-ordered consensus protocols use the same shape, a settling window sized by
measured clock uncertainty, and *halt* when the time guarantee is lost rather than reorder quietly; here "halt" is the
`Late.Reject` verdict.

### A.3 Is the lane kernel still the right core?

The question was asked openly: redesign the library if a better core exists. Four candidates were weighed against the
guarantees the use cases need (one writer per key, order per key, bounded memory, no answer before the store's answer,
no second source of truth, verifiable by model checking).

**Log-first / sequencer.** Assign every input a position in a per-key log at admission, and have one applier per key
consume it. This is what a lane already is: the admission transaction assigns the position, the worker (or the direct
caller) is the applier, and "one call in flight per key" is the single applier. A *durable* log would add crash
survival of waiting inputs, which 0005 rejects on purpose: nothing is acknowledged before the store's write, redelivery
is the client's, and a second durable copy makes the process a source of truth. A durable partitioned log *in front of*
the fleet is infrastructure, not this library. The sequencer idea is therefore a description of the kernel, not a
replacement; the design below names it as such.

**Per-entity actors.** One fiber and one mailbox per active key, draining the mailbox into a batch, calling, repeating.
Same per-key guarantees, and the group commit falls out naturally. What is lost: the global bounds (`maxInFlightBatches`,
`maxQueueing`, `maxQueuedWeight`) and cross-key fairness stop being one pure value in one `TRef` and become a semaphore
plus counters shared by many fibers; the model would have to cover the semaphore and the mailbox handoffs, and every
active key costs a fiber. The current kernel is the actor design with the scheduling pulled into one pure structure.
**This is the second-best option**, and it would be the fallback if the pure structure ever proved to be a throughput
limit ([0008](../decisions/0008-deferred.md) says what measurement would trigger that).

**A windowing operator** (event-time windows and watermarks in front of the batch call). As a core it drops the
bulkhead: bounded admission with fast rejection, per-caller results, close semantics. As a *policy inside the lane* it
is the event-time strategy.

**Kernel plus pure policies (chosen).** Keep the lane kernel, whose protocol is model-checked with the direct path
(five mechanisms, each shown load-bearing by a variant that fails), and move every decision that is not a concurrency
mechanism out of `Lanes` into pure functions evaluated once per input at admission. The kernel's invariants are then
proven once for any policy; each policy's own guarantee is a law, tested on the policy alone.

What the current design limits, concretely: order (FIFO), the due rule (oldest arrival plus delay), the direct-path
rule (`delay == 0`), and batch boundaries (size and weight only) are hard-wired into `Lanes`; a store that must not see
two event-time buckets in one batch cannot be served without a fork; and `maxBatchDelay` conflates "latency ceiling"
with "wait for order". Nothing in the public API is dropped. The defaults reproduce today's behaviour exactly.

### A.4 Extension points

Rule: an extension point is a **pure function supplied by the application, returning a closed decision type the
kernel understands**. The kernel evaluates every policy function **once per input, at admission, outside the STM
transaction**, and stores the results in the item; transactions only compare the stored numbers. Policy functions must
therefore be total, deterministic and cheap, and must not depend on anything but their arguments (STM transactions
retry, and a policy that ran inside one would run more than once).

Every extension point below has a use case behind it; A.5 lists the ones considered and left out.

| Point | Decides | Given | Returns | Pure? | Default |
|---|---|---|---|---|---|
| `key: I => K` | which lane | input | key | yes | required (exists) |
| `weigh: I => Long` | batch and waiting-room weight | input | weight, clamped at 0 | yes | `1` (exists, [0007](../decisions/0007-bound-by-weight.md)) |
| `Order[I]` | dispatch order within a key; when an input may be sent at the earliest; whether it is late | input, arrival instant (wall clock) | `rank: Long`, `holdUntil: Instant`, `late: Option[Duration]` | yes | `Order.arrival` |
| `late: (I, Duration) => Late` | what to do with a late input | input, how late | `Late.Reject` or `Late.Admit` | yes | `Late.Admit` (only consulted when `Order` reports lateness) |
| `partition: I => P` | batch boundaries: inputs with different `P` never share a batch | input | a partition token with an `Equal` | yes | `_ => ()` (one partition) |
| `directIdleKeys: Boolean` (config) | whether an idle key's input is sent by its caller or by a worker | — | — | — | `true` |
| `call: (K, NonEmptyChunk[I]) => ZIO[R, E, B]` | grouping: fold in process, or hand the group to the store's group operation | key, the batch in dispatch order | the batch result | no (effect) | required (exists) |
| `extract: (I, B) => A` | each caller's share | input, batch result | outcome | yes | required (exists) |

#### `Order[I]`

```scala
import java.time.Instant
import zio.Duration

/** Dispatch order within a key. Evaluated once per input, at admission. */
trait Order[-I]:
  /** Position among the key's inputs: ascending. Ties keep arrival order. */
  def rank(input: I, arrival: Instant): Long
  /** The earliest instant, on this process's clock, at which the input may be dispatched. Never before `arrival`. */
  def holdUntil(input: I, arrival: Instant): Instant
  /** How late the input is at admission, if the strategy can tell; `None` means "not late" or "cannot tell". */
  def late(input: I, arrival: Instant): Option[Duration]

object Order:
  /** Admission order; nothing is held; nothing is late. Today's behaviour. */
  val arrival: Order[Any]
  /** Ascending `rank` among the inputs waiting together; nothing is held; nothing is late. */
  def by[I](rank: I => Long): Order[I]
  /**
   * Ascending event time; an input is held until `at + lateness` has passed on this clock, and is late if that instant
   * had already passed at admission. Among inputs not reported late, dispatch is in event-time order.
   */
  def eventTime[I](at: I => Instant, lateness: Duration): Order[I]
```

`arrival` is the default because it is the only strategy that costs nothing and assumes nothing about inputs, and
because it is what the store expects when it assigns its own positions at arrival (as end-to-end batching databases
do: the client submits no timestamp, the cluster stamps the input when it reaches the primary).

#### `Late`

```scala
enum Late:
  /** Fail the caller with `FerryError.Late(by)`; the input is never sent. */
  case Reject
  /** Send it anyway, in rank order among what is waiting; the store sees its event time and decides. */
  case Admit
```

The handler is a function so that a window can be expressed without a new strategy, e.g. admit up to a day late and
reject beyond. `Admit` is the default because the common store can place a late input itself, and a reject would turn a
clock skew into a failed call. `Reject` is for stores that cannot: it fails fast, before a round trip, and keeps one
input's lateness from poisoning a batch.

#### `partition`

Inputs whose tokens differ never travel in one batch; a lane is taken in rank order and the batch stops at the first
input whose token differs from the batch's first. Use case: a group operation that refuses a group spanning two event-time
buckets (a day boundary, "historical" versus "current"), as end-to-end batching databases refuse mixing imported events
with current ones in one request. Without this point, such a store needs the application to split inside `call` and
make two round trips while the key is busy, or to reject the batch.

#### Plugging in a custom strategy

```scala
final case class Update(entity: String, id: String, at: Instant, day: LocalDate)

/** Order by event time, hold for 250 ms, admit anything up to one day late, and never mix days in one batch. */
val order = Order.eventTime[Update](_.at, lateness = 250.millis)

val writer: URIO[Scope, Ferry[Update, StoreError, Either[Rejection, Position]]] =
  for
    config <- ZIO.fromEither(PerKeyConfig.make(maxBatchSize = 25, maxInFlightBatches = 8, maxQueueing = 1000))
    ferry  <- Ferry.perKey(
      config,
      key = (u: Update) => u.entity,
      order = order,
      late = (u: Update, by: Duration) => if by <= 1.day then Late.Admit else Late.Reject,
      partition = (u: Update) => u.day
    )((entity, updates) => store.append(entity, updates))((u, outcomes) => outcomes(u.id))
  yield ferry
```

A strategy of one's own is a value of `Order[I]`; `OrderLaws` (A.6) checks it before it is trusted.

#### Today's behaviours as configurations

| Today | Configuration |
|---|---|
| FIFO per key, group commit, idle keys direct | `Order.arrival`, `maxBatchDelay = 0`, `directIdleKeys = true` (the defaults) |
| Batch every input, even an idle key's (`maxBatchDelay > 0`) | `Order.arrival`, `maxBatchDelay = d`: the direct path is off because a positive delay means "wait for company" |
| — (new) | `Order.by(rank)`: sorted batches, direct path still on |
| — (new) | `Order.eventTime(at, L)`: direct path off, because `holdUntil > arrival`; `maxBatchDelay` still caps the wait *after* an input became eligible |

The direct path's rule becomes: a caller claims an idle key iff `directIdleKeys` and `holdUntil == arrival` for its
input and `maxBatchDelay == 0`. The `DirectGuard` and `AwaitCall` mechanisms are unchanged; only the predicate that
enables a claim changes, and the predicate is evaluated from stored item fields.

The lane's "due" bookkeeping becomes: an input is **eligible** once `holdUntil` has passed; a lane is **ready** when it
holds an eligible input and either the eligible prefix (in rank order, cut at `maxBatchSize`, `maxBatchWeight` and the
first partition change) is full, or the oldest eligible input's `arrival + maxBatchDelay` has passed; with the default
zero delay, as soon as it holds an eligible input and the key is free. `holdUntil` is converted to the monotonic clock
at admission (`submittedAt + max(0, holdUntil − now)`), so `Lanes` keeps a single clock.

### A.5 Not extension points, and why

- **Cross-key scheduling** (which ready lane a worker takes next). FIFO with re-queue at the back is fair and no use
  case asked for priorities. A plug-in here could starve keys, which would break a kernel guarantee.
- **Retry, backoff, circuit breaking.** Around `call`, with the application's resilience library, as today.
- **Deduplication within a batch.** `call` receives duplicates and answers by identifier
  ([0006](../decisions/0006-per-input-outcomes.md)); a store-side dedup is part of the write pattern (0005).
- **A per-key fold with state kept between batches** (a cached entity state to skip the read). It would make the process
  a source of truth; a plug-in that wants it keeps it inside `call`, behind the store's fence.
- **Routing, membership, ownership.** Out of scope (0005).
- **Adaptive lateness** (sizing the bound from observed skew). The application has `arrival − at`; a bound that moves
  changes the guarantee's meaning mid-run. Revisit with a use case.

### A.6 What the kernel guarantees regardless of the plugged-in strategy

Kernel invariants, model-checked in `formal/PerKey.tla` for any policy (the model's `Take` chooses *any* eligible
subset of a lane up to `MaxBatch`; a policy only narrows that choice):

- **one call in flight per key**, batched or direct (`OneInFlightPerKey`);
- **each input is sent at most once** (`AtMostOnce`) and **turned-away inputs are never sent** (`TurnedAwayNeverSent`);
- **an input is owned by one place at a time**: a lane or one worker (`SingleOwner`);
- **a batch holds at most `maxBatchSize` inputs and, beyond one, at most `maxBatchWeight`** (`BatchBounded`);
- **the waiting room holds at most `maxQueueing` inputs and `maxQueuedWeight`**; beyond that, `Rejected` at once;
- **no caller is stranded**: every admitted caller gets a result, a failure or an interruption, including on close
  (`NoneStrandedAfterClose`, `EveryCallerAnswered`, `EveryDirectCallerDone`);
- **a caller interrupted before dispatch is left out of its batch**;
- **ready lanes are served first come first served, and a lane with more joins the back**: no key starves another;
- **batches are per key and in rank order, ties by arrival**, and **never straddle a partition token**;
- **nothing is dispatched before its `holdUntil`**.

The plug-in's responsibilities, stated as laws and shipped as a reusable property suite (`ferry.laws.OrderLaws`, in
the test kit) so a custom `Order` can be checked without reading the kernel:

- `rank`, `holdUntil`, `late` are pure and total, and `holdUntil(i, a) >= a`;
- *order-unless-late* holds iff `holdUntil(i, a) == t(i) + L` and `late(i, a) == Some(a − L − t(i))` exactly when
  `t(i) < a − L`, for some `t` consistent with `rank` and some `L >= 0`; `Order.eventTime` satisfies this, `arrival`
  and `by` satisfy it trivially with `L = ∞` (never late, never held) and claim nothing across batches;
- an event time ahead of this clock is held until it is due; an untrusted clock is validated before submission, because
  a far-future event time holds a waiting-room slot for that long;
- `partition` must be stable for an input and cheap: it is evaluated once.

What becomes the plug-in's problem, not the kernel's: whether an admitted late input is correct for the store; the size
of the bound; the meaning of `rank` across keys (none).

### A.7 Grouping modes: nothing new in the kernel

Two ways to process a group both fit the existing batch call:

- **Fold in process** (one read of the key's state, apply the inputs in dispatch order in memory, one conditional write,
  one outcome per input): the fold lives inside `call`. The library adds no reducer, because a reducer with state across
  batches is the cached state 0005 forbids, and a reducer without it is a function the application already owns. The
  `OrderedWrites` example shows the shape; a `foldWrite(read)(step)(write)` helper that packages read, dedup, apply,
  fence, retry and per-input outcomes is worth promoting into the library when a second application wants it.
- **Hand the group to the store's group operation**: `call` passes the batch through. What such a store needs from the
  batcher is already there or is in this design: a weight bound for its request-size limit (0007), per-input outcomes so
  one invalid input does not fail the group (0006), rank order so the group is presented in event-time order, and
  `partition` so the group never straddles a boundary the operation refuses.

An all-or-nothing group operation that fails the whole group on one invalid input stays the application's concern: the
call can validate before the round trip, or split and retry inside its slot.

---

## Part B. Draft decision records

### 0010. A strategy-agnostic kernel with policies as pure functions

Status: Proposed

#### Context

[0003](../decisions/0003-per-key-batching.md) made every per-key guarantee a property of one pure value, `Lanes`, and
[0004](../decisions/0004-dispatch-on-completion.md) and the direct path made dispatch timing a function of load. The
protocol around that value is model-checked with five mechanisms shown to be load-bearing. But `Lanes` also decides
things that are not concurrency mechanisms: FIFO order within a key, the due rule, the direct-path rule, and where a
batch may be cut. Applications now need to vary them: a store that needs event-time order, a store that refuses a
group spanning two time buckets, a store with a hard connection limit that wants no direct calls. Each would need a
fork.

#### Decision

Split `Lanes` into a **kernel** and **policies**:

- The kernel owns admission, the waiting room and its bounds, taking, busy marking, release, close, the direct path's
  claim and await, and cross-key fairness. It never looks at an input.
- A policy is a pure function the application supplies, evaluated **once per input at admission, outside any
  transaction**; its results (`rank`, `holdUntil` in monotonic nanos, `weight`, `partition`, `late`) are stored in the
  item. Transactions compare numbers only.
- The policy points are those of A.4, and only those: `key`, `weigh`, `Order`, `late`, `partition`, `directIdleKeys`,
  `call`, `extract`. Each returns a closed decision type.
- `Ferry.perKey` keeps its signature; new policies are optional parameters whose defaults reproduce today's behaviour.
  `Ferry.make` is unchanged: it is a different kernel (several batches of one stream in flight), with its own model.

#### Consequences

- The kernel's invariants (A.6) are proven once, for any policy: the model's `Take` picks any eligible subset, which is a
  superset of every policy's behaviour.
- A policy's guarantee is a law on the policy, checked by `OrderLaws` and by a policy-specific model refinement (0011).
- An application cannot break a kernel invariant through a policy, only its own ordering claim.
- The direct path is no longer a special case of `maxBatchDelay == 0`: it is a configuration (`directIdleKeys`) that the
  kernel enables when the policy holds nothing, which is also what a positive delay disables.
- Second-best alternative considered: per-entity actors (A.3). Rejected because the global bounds and fairness would
  leave the one pure value, at a cost in verifiability and a fiber per active key, for no extra guarantee.

### 0011. Dispatch order is a per-ferry strategy: arrival, sorted, or event time with a bound

Status: Proposed

#### Context

Inputs may carry an event time or a sequence number, and may arrive out of that order. Stores differ in whether they
can place an input older than their newest. The batcher controls dispatch order only; lateness across batches is the
store's verdict, because the store holds the state and the batcher's view is one process and one clock (A.1).

#### Decision

`Order[I]` (A.4) with three built-ins:

- `Order.arrival` (default): admission order, no holding, nothing late.
- `Order.by(rank)`: a lane is kept in ascending rank, ties by arrival; batches take the smallest ranks first; no holding.
  This gives a settling window for free under load: everything that arrived during the previous round trip is sorted
  together.
- `Order.eventTime(at, lateness)`: rank by event time; hold until `at + lateness` on this clock; late at admission iff
  `at < arrival − lateness`. The guarantee **ordered unless late**: for inputs `a` sent before `b` for one key, if `b`
  was not late then `at(a) <= at(b)`. Sketch: if `b` was waiting when `a`'s batch was taken, it was either not yet
  eligible (`at(b) + L > now >= at(a) + L`) or ranked after `a`; if `b` arrived after, `b` not late gives
  `at(b) >= arrival(b) − L > take(a) − L >= at(a)`.
- A late input goes to the `late` handler: `Late.Admit` (default; sorted in among what is waiting) or `Late.Reject`
  (`FerryError.Late(by)`, never sent). `FerryError` gains the case `Late(by: Duration) extends FerryError[Nothing]`;
  `fold` gains a parameter.
- `maxBatchDelay` keeps its meaning as a ceiling on waiting *after* eligibility; `holdUntil` is a floor. The direct path
  needs a zero floor and a zero ceiling.
- Event times ahead of this clock are held until due. Sequence numbers with gap waiting are not built (A.2).

#### Consequences

- The only new numeric knob is `lateness`, and it exists only inside the one strategy that has a bound; it is the
  settling window, and the application sizes it from `arrival − at`, which it has.
- With a bound, every input pays the bound in latency, idle keys included; that is the price of the guarantee, and the
  reason the default has none.
- `Order.by` changes `InOrderPerKey` from "admission order" to "rank order, ties by admission"; the arrival strategy is
  the case `rank = admission`.
- Wall-clock time is read for `holdUntil` and the late verdict only, at admission; nothing the batcher stores or sends
  is derived from when a batch happened to be dispatched (as in 0005).
- A late input admitted with `Late.Admit` carries its event time; the store, which knows what it holds, decides. No
  flag is passed to `call`: a flag would restate a clock-relative verdict the store can make better, and would change
  the batch call's signature for every user. Sending late inputs in batches of their own (`Late.Apart`) is deferred
  until a store that accepts late inputs only in separate requests appears; `partition` covers the bucket-shaped cases
  today.

### 0012. Batches never straddle a partition token

Status: Proposed

#### Context

Some group operations refuse a group that spans a boundary: two event-time buckets, historical versus current inputs.
Without a rule in the batcher, the application splits inside `call`, which costs a second round trip while the key is
busy, or lets one input fail the group.

#### Decision

`partition: I => P` (default: one partition). A batch is the longest rank-ordered prefix of a lane's eligible inputs
that fits `maxBatchSize` and `maxBatchWeight` and shares one token. The remainder stays in the lane and goes next, so a
boundary adds one round trip at most, and only when both sides have inputs waiting.

#### Consequences

- A batch is homogeneous in the token; the group operation can rely on it.
- A lane whose head belongs to a different partition from most of its inputs sends a small batch first; this is by
  design (order is preserved across the boundary) and bounded (one extra round trip).
- Cross-key fairness is untouched: a lane is one unit in the ready line whatever its partitions.
- The token needs an `Equal` only; the kernel never orders partitions, `rank` does.

### 0013. Grouping is the batch call; the library keeps no state between batches

Status: Proposed

#### Context

A group can be processed by folding in memory from one read and writing once, or by handing it to the store's group
operation. Both were asked about; a per-key reducer in the library was considered.

#### Decision

No new API. `call(key, inputs)` receives the group in dispatch order and is the fold, the hand-off, or both. The
library holds no state across a key's batches. The `OrderedWrites` example remains the reference shape; a `foldWrite`
helper is deferred until a second application needs it.

#### Consequences

- The process never becomes a source of truth (0005); a crash loses nothing that was acknowledged.
- Requirements a group operation puts on the batcher are met by existing or proposed policies: size and weight (0007),
  per-input outcomes (0006), rank order (0011), homogeneity (0012).
- An all-or-nothing group operation that rejects a whole group on one invalid input is handled inside `call`.

---

## Part C. Verification plan, build order, migration

### C.1 Formal models

| Model | Change | Must pass | Weakened variants that must fail |
|---|---|---|---|
| `PerKey.tla` (kernel) | `Take` picks **any** subset of a lane's inputs marked eligible, up to `MaxBatch`, instead of a prefix; a new `Eligible(c)` oracle set by a `Tick` action nondeterministically (abstracting `holdUntil`); the claim predicate requires the caller's input to be eligible at admission; `InOrderPerKey` is removed from the kernel's `Safety` and moves to the refinements | `PerKey`, `PerKeyBatchOnly` with the existing invariants minus order, plus `NotBeforeEligible` | the existing five: `NoBusyGuard`, `NoDirectGuard`, `NoAwaitCall`, `NonAtomicTake`, `NonAtomicAdmission` |
| `PerKeyArrival.tla` (refinement) | `Take` picks the admission-order prefix of eligible inputs; all eligible at once | `InOrderPerKey` as today | `UnsortedTake` (any subset) violates it |
| `PerKeyEventTime.tla` (refinement) | constants `At ∈ [Callers → Nat]`, `Lateness`; a `clock` variable advanced by `Tick`; `Eligible(c) == At[c] + Lateness <= clock`; `late[c]` recorded at admission; `Take` picks the smallest `At` among eligible, ties by admission; `Partition ∈ [Callers → Tokens]` with the cut rule | `OrderedUnlessLate`: `Before(log[k], a, b) ∧ ¬late[b] ⇒ At[a] <= At[b]`; `NotBeforeHold`: `c ∈ Range(log[k]) ⇒ At[c] + Lateness <= clock` at send; `Homogeneous`: a worker's `taken` shares one token; `LateVerdict`: `late[c] ⇔ At[c] + Lateness < clock` at admission | `NoHold` (take eligible regardless of clock) violates `OrderedUnlessLate`; `UnsortedTake` violates it; `NoCut` violates `Homogeneous` |
| `FencedWriter.tla` | commands carry `At`; nodes send batches in `At` order; property `StoreOrderedUnlessLate` on the store's log, with a crash and a lost answer in the way | passes | `Unsorted` fails |

Symmetry reduction stays off for liveness configurations (0008); the new refinements check safety only and may use it.
Trace validation (0008) becomes more valuable once policies vary: log admit/take/complete events from `PerKeyStressSpec`
under each built-in `Order` and check each trace against the kernel model.

### C.2 Model-based and simulation tests

- `LanesSpec`: the executable model gains a priority queue per lane keyed by `(rank, arrivalSeq)`, an eligibility
  predicate, and the partition cut; properties: the taken batch equals the model's; `holdUntil` respected; cut rule.
- `PerKeySimulation`: the scenario generator draws an `Order` (arrival, `by`, `eventTime` with `lateness ∈ {0, 5, 20} ms`),
  a `late` handler (`Admit`, `Reject`, a window), and a `partition` (one, or `at.day`); inputs get `at = submittedAt −
  skew` with skew sometimes beyond `lateness`. Policy-free checks stay as they are. New checks per trace:
  - "within a key, inputs are dispatched in rank order unless reported late" (`OrderedUnlessLate` on the trace);
  - "no input is dispatched before `holdUntil`";
  - "an input is reported late iff `at < submittedAt − lateness`, and a rejected late input was never sent";
  - "no batch mixes partition tokens";
  - "a direct call happens only when the policy holds nothing and the delay is zero";
  - "no eligible input waits longer than the bound": an eligible input is in the next batch taken for its key unless
    that batch was full by size, weight or partition (the same shape as today's "partial call sent before its
    deadline" check, with the floor and ceiling made explicit).
- `FencedWriterSimulation`: commands carry event times; the store's per-key log is checked for `OrderedUnlessLate`
  through crashes, lost answers, throttling and misrouting.
- `OrderLaws` (test kit): property suite over an arbitrary `Order[I]` and a generator of inputs and arrival instants:
  purity (same answer twice), `holdUntil >= arrival`, the order-unless-late law for the strategy's own `(t, L)`, and
  the trivial law for strategies that never hold.
- Stress spec: a per-key history check in the style of a linearizability checker, with the sequential model "apply in
  rank order among non-late inputs", partitioned by key.

### C.3 Build order

1. **0010 kernel/policy split, no API change.** Factor `Lanes` into kernel + stored policy results; defaults equal
   today; kernel model generalised (`Take` any eligible subset), `PerKeyArrival` refinement added. Justified on its own
   by the stronger model (every existing invariant now holds for any policy).
2. **0011 `Order.by` and `partition` (0012).** Smallest useful step; serves stores that re-sequence themselves and group
   operations with boundaries; `LanesSpec` and simulation checks for rank order and homogeneity.
3. **0011 `Order.eventTime` + `late` + `FerryError.Late` + `OrderLaws` + `PerKeyEventTime.tla`.** The asked-for
   pluggable decision with the "ordered unless late" guarantee. Build it even if the first consumer does not need it:
   the public design must serve append-only stores, and the guarantee is the one worth model-checking.
4. **`directIdleKeys` switch.** Trivial once 1 is in; for downstreams with a hard connection limit.
5. **Deferred, each with its trigger:** `Late.Apart` (a store accepting late inputs only in separate requests);
   `foldWrite` helper (a second application with the one-read/fold/one-write shape); adaptive lateness (a use case where
   a fixed bound is either wasteful or wrong); gap waiting (never: the store owns the sequence).

Out of scope, as application concerns: the store's placement of late inputs, idempotency and the fence (0005, a
pattern), retries, membership and routing, two-phase or pending workflows, a durable log in front of the fleet.
(Two-phase workflows: reversed by [0014](../decisions/0014-commit-then-project.md), which packages the
commit-then-project shape as `ProjectedWrite`.)

### C.4 Migration

- Source compatibility: every new parameter of `Ferry.perKey` is optional with today's behaviour as default;
  `PerKeyConfig.make` gains `directIdleKeys: Boolean = true`.
- Binary/semantic break: `FerryError.Late` is a new enum case, so exhaustive matches and `fold` callers must add a
  branch; shipped with step 3 and a minor-version bump, called out in the changelog.
- `Lanes` and its tests are internal; the split happens behind `private[ferry]`.
- Formal: `PerKey.tla` keeps its name and its five variants; order moves to the refinements, so `check.sh` gains two
  passing and three failing configurations.
- Nothing is dropped from the public API, and `Ferry.make` is untouched.
