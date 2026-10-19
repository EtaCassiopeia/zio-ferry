# Running on a fleet

`perKey` makes a process the only local writer of each key. Across a fleet of processes, that holds only if each key's
traffic reaches one process, which is a routing concern: consistent or rendezvous hashing of the key over the fleet's
members. Routing is never perfect:

- during a membership change, two processes may both believe they own a key;
- a crashed owner may have writes still on the wire;
- a response may be lost after its write was applied;
- clients retry.

## Correctness comes from the write

Routing to a single owner is an optimisation: it keeps batches full and contention away. Safety comes from the write,
which must:

1. **read** the key's current state, including which inputs it already holds;
2. **leave out inputs already applied**, by a client-chosen id, so a retry after a lost response is answered from what
   was stored;
3. **write the rest conditionally** on the state not having changed since the read (a version, a sequence number, a
   log length), so a stale or concurrent writer is refused rather than interleaved;
4. **retry the whole round trip** on a refused or failed write, with backoff and jitter. The retry re-reads, so it sees
   whatever landed;
5. **answer each caller only after the write is durable**.

[`FoldWrite`](../guide/fold-write.md) is this round trip. For a store that writes in two steps, a commit and then a
projection of a read-side view, [`ProjectedWrite`](../guide/projected-write.md) adds the second, fenced write and a way
to finish it from the store after a crash.

![The fence refuses a stale writer; deduplication answers a retry](../assets/fleet.svg)

Both safeguards are needed, and each is shown to be needed:

- `formal/FencedWriter.tla` models any number of processes writing one key with arbitrary routing, crashes with writes
  in flight, and lost answers. It checks that no input is applied twice and every answer is true. Without the fence,
  two writers interleave. Without deduplication, a retry after a lost answer applies an input twice.
- `FencedWriterSimulation` runs the real `perKey` through the same faults on virtual time, plus throttling and
  misrouted clients.

## What a crash costs

Ferry keeps no write-ahead log: that would make the process a second source of truth. A crash loses what was waiting
and in flight on that process, at most `maxQueueing` inputs, and turns it into a wave of client retries. Nothing
acknowledged is lost, because nothing is acknowledged before the store's write.

Retries must carry the same id and the same content. A store that checks both turns a mismatched retry into a clear
error instead of a silent duplicate.

Membership and routing are out of scope for this library.
