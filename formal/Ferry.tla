------------------------------- MODULE Ferry -------------------------------
(***************************************************************************)
(* The protocol behind ferry.Ferry.make and ferry.Ferry.perKey, action for *)
(* action as implemented by ferry.Protocol: each action below is one       *)
(* method of Protocol, applied by the shell in one STM transaction, and    *)
(* both sides describe it with the same record, kept here in `act`.        *)
(*                                                                         *)
(* Callers submit inputs for a key. Each key has a lane of waiting inputs  *)
(* in ascending rank, ties in admission order. A lane is waiting for its   *)
(* deadline, ready (in the ready line), or blocked (its key has a call in  *)
(* flight and allows only one). Workers take the first ready lane's       *)
(* longest prefix that fits a batch, call it, and complete it. With Direct,*)
(* a caller whose key is idle claims it and sends its input itself.        *)
(* Callers may give up while they wait. Closing the scope closes admission *)
(* and empties every lane, after which workers take nothing more.          *)
(*                                                                         *)
(* The model's free choices are only the environment's: arrivals (Submit), *)
(* the passing of time (Due), the outcome of calls (Complete,              *)
(* CompleteDirect), callers giving up (Cancel) and the scope closing       *)
(* (Close). Which lane a worker takes and how much of it is decided as the *)
(* code decides it. That is what lets a behaviour of this model be         *)
(* replayed through the code, and a run of the code be checked against it. *)
(*                                                                         *)
(* Make (Ferry.make) is the case of one key, OneCallPerKey = FALSE and     *)
(* Direct = FALSE.                                                         *)
(*                                                                         *)
(* Five switches weaken the protocol, to show each mechanism is needed:    *)
(*   Sorted          - an input joins its lane in rank order, not at the   *)
(*                     back                                                *)
(*   BusyGuard       - a lane whose key has its one call in flight is      *)
(*                     blocked                                             *)
(*   DirectGuard     - only an idle key is claimed for a direct call       *)
(*   AtomicAdmission - checking that admission is open and joining the     *)
(*                     lane are one transaction                            *)
(*   DrainOnClose    - closing empties every lane                          *)
(* The implementation has all five.                                        *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Callers, Keys, Workers,
  MaxBatch, MaxWeight, MaxQueue, MaxQueuedWeight,
  Direct, OneCallPerKey,
  RankOf, WeightOf,          \* each caller's rank and weight
  Sorted, BusyGuard, DirectGuard, AtomicAdmission, DrainOnClose,
  LowRank, Heavy             \* used only by MCRank and MCWeight, for model checking

ASSUME MaxBatch >= 1 /\ MaxQueue >= 1

VARIABLES
  caller,   \* how each caller's submission was received
  answer,   \* each caller's answer: "none" until answered
  lane,     \* each key's waiting inputs, in lane order
  sched,    \* each key's lane: "none", "waiting", "ready" or "blocked"
  ready,    \* the line of ready keys
  busy,     \* calls in flight per key
  batch,    \* each worker's batch (empty when idle)
  wkey,     \* the key of each worker's batch
  dkey,     \* the key of each direct caller's call in flight
  open,     \* admission is open
  pick,     \* the key a caller is joining, between check and join (only without AtomicAdmission)
  admitted, \* history: each key's inputs in admission order
  log,      \* history: each key's inputs in the order they were sent
  gone,     \* history: the inputs of its key already sent when each caller was admitted
  sent,     \* history: how many calls carried each caller's input
  act       \* the action just taken, as a record: the shared event schema

vars == <<caller, answer, lane, sched, ready, busy, batch, wkey, dkey, open, pick, admitted, log, gone, sent, act>>

NoKey == "none"
Outcomes == {"Succeeded", "Failed", "Interrupted"}

\* The names of the actions the code takes; the vocabulary shared with ferry.Protocol.Action.
Vocabulary == {"Enqueue", "Claim", "Reject", "Refuse", "Due", "Take", "Complete", "CompleteDirect", "Cancel", "Close"}

MCRank == [c \in Callers |-> IF c \in LowRank THEN 0 ELSE 1]
MCWeight == [c \in Callers |-> IF c \in Heavy THEN 2 ELSE 1]

Range(s) == {s[i] : i \in 1..Len(s)}
RECURSIVE SumW(_)
SumW(s) == IF s = <<>> THEN 0 ELSE WeightOf[Head(s)] + SumW(Tail(s))
Queued == UNION {Range(lane[k]) : k \in Keys}
QueuedWeight == LET RECURSIVE Sum(_) Sum(ks) == IF ks = {} THEN 0 ELSE LET k == CHOOSE x \in ks : TRUE IN SumW(lane[k]) + Sum(ks \ {k})
                IN Sum(Keys)
Pending == {c \in Callers : caller[c] \in {"queued", "direct"} /\ answer[c] = "none"}
Idle(k) == lane[k] = <<>> /\ busy[k] = 0
Blocked(k, b) == BusyGuard /\ OneCallPerKey /\ b[k] > 0
Full(s) == Len(s) >= MaxBatch \/ SumW(s) >= MaxWeight
Remove(s, x) == SelectSeq(s, LAMBDA y : y # x)

(* `c` placed after every input of a rank up to its own. *)
Insert(s, c) ==
  IF Sorted
  THEN SelectSeq(s, LAMBDA x : RankOf[x] <= RankOf[c]) \o <<c>> \o SelectSeq(s, LAMBDA x : RankOf[x] > RankOf[c])
  ELSE Append(s, c)

(* The longest prefix of `s` that fits a batch: at least one input, at most MaxBatch, within MaxWeight. *)
RECURSIVE Prefix(_, _, _)
Prefix(s, n, w) ==
  IF n < Len(s) /\ n < MaxBatch /\ w + WeightOf[s[n + 1]] <= MaxWeight THEN Prefix(s, n + 1, w + WeightOf[s[n + 1]]) ELSE n
BatchSize(s) == Prefix(s, 1, WeightOf[s[1]])

(* Protocol.place: key `k`'s lane becomes `s`, given the calls in flight `b`; returns <<sched, ready>>. *)
Place(k, s, b) ==
  IF s = <<>> THEN <<"none", Remove(ready, k)>>
  ELSE IF Blocked(k, b) THEN <<"blocked", Remove(ready, k)>>
  ELSE IF sched[k] = "ready" THEN <<"ready", ready>>
  ELSE IF sched[k] = "waiting" /\ ~Full(s) THEN <<"waiting", ready>>
  ELSE IF Full(s) THEN <<"ready", Append(ready, k)>>
  ELSE <<"waiting", ready>>

TypeOK ==
  /\ caller \in [Callers -> {"idle", "admitted", "queued", "direct", "rejected", "refused"}]
  /\ answer \in [Callers -> {"none"} \cup Outcomes]
  /\ lane \in [Keys -> Seq(Callers)]
  /\ sched \in [Keys -> {"none", "waiting", "ready", "blocked"}]
  /\ ready \in Seq(Keys)
  /\ busy \in [Keys -> Nat]
  /\ batch \in [Workers -> Seq(Callers)]
  /\ wkey \in [Workers -> Keys \cup {NoKey}]
  /\ dkey \in [Callers -> Keys \cup {NoKey}]
  /\ open \in BOOLEAN
  /\ act.name \in Vocabulary \cup {"Init", "Admit"}

Init ==
  /\ caller = [c \in Callers |-> "idle"]
  /\ answer = [c \in Callers |-> "none"]
  /\ lane = [k \in Keys |-> <<>>]
  /\ sched = [k \in Keys |-> "none"]
  /\ ready = <<>>
  /\ busy = [k \in Keys |-> 0]
  /\ batch = [w \in Workers |-> <<>>]
  /\ wkey = [w \in Workers |-> NoKey]
  /\ dkey = [c \in Callers |-> NoKey]
  /\ open = TRUE
  /\ pick = [c \in Callers |-> NoKey]
  /\ admitted = [k \in Keys |-> <<>>]
  /\ log = [k \in Keys |-> <<>>]
  /\ gone = [c \in Callers |-> {}]
  /\ sent = [c \in Callers |-> 0]
  /\ act = [name |-> "Init"]

Arrival(name, c, k) == [name |-> name, caller |-> c, key |-> k, weight |-> WeightOf[c], rank |-> RankOf[c]]

(* Protocol.submit, past the check that admission is open: claim an idle key, join its lane, or be rejected. *)
Join(c, k) ==
  LET ahead == Range(log[k]) IN
  IF Direct /\ (DirectGuard => Idle(k))
  THEN /\ caller' = [caller EXCEPT ![c] = "direct"]
       /\ busy' = [busy EXCEPT ![k] = @ + 1]
       /\ dkey' = [dkey EXCEPT ![c] = k]
       /\ admitted' = [admitted EXCEPT ![k] = Append(@, c)]
       /\ log' = [log EXCEPT ![k] = Append(@, c)]
       /\ sent' = [sent EXCEPT ![c] = @ + 1]
       /\ gone' = [gone EXCEPT ![c] = ahead]
       /\ act' = Arrival("Claim", c, k)
       /\ UNCHANGED <<lane, sched, ready>>
  ELSE IF Cardinality(Queued) < MaxQueue /\ QueuedWeight + WeightOf[c] <= MaxQueuedWeight
  THEN LET s == Insert(lane[k], c) IN
       /\ caller' = [caller EXCEPT ![c] = "queued"]
       /\ lane' = [lane EXCEPT ![k] = s]
       /\ sched' = [sched EXCEPT ![k] = Place(k, s, busy)[1]]
       /\ ready' = Place(k, s, busy)[2]
       /\ admitted' = [admitted EXCEPT ![k] = Append(@, c)]
       /\ gone' = [gone EXCEPT ![c] = ahead]
       /\ act' = Arrival("Enqueue", c, k)
       /\ UNCHANGED <<busy, dkey, log, sent>>
  ELSE /\ caller' = [caller EXCEPT ![c] = "rejected"]
       /\ act' = Arrival("Reject", c, k)
       /\ UNCHANGED <<lane, sched, ready, busy, dkey, admitted, log, gone, sent>>

Submit(c, k) ==
  /\ caller[c] = "idle"
  /\ IF ~open
     THEN /\ caller' = [caller EXCEPT ![c] = "refused"]
          /\ act' = Arrival("Refuse", c, k)
          /\ UNCHANGED <<lane, sched, ready, busy, dkey, admitted, log, gone, sent, pick>>
     ELSE IF AtomicAdmission
          THEN Join(c, k) /\ UNCHANGED pick
          ELSE /\ caller' = [caller EXCEPT ![c] = "admitted"]
               /\ pick' = [pick EXCEPT ![c] = k]
               /\ act' = [name |-> "Admit", caller |-> c, key |-> k]
               /\ UNCHANGED <<lane, sched, ready, busy, dkey, admitted, log, gone, sent>>
  /\ UNCHANGED <<answer, batch, wkey, open>>

(* Only without AtomicAdmission: the caller joins after, and apart from, the check. *)
Enter(c) ==
  /\ caller[c] = "admitted"
  /\ Join(c, pick[c])
  /\ UNCHANGED <<answer, batch, wkey, open, pick>>

(* Time passes: a waiting lane's deadline is due, and it joins the ready line. *)
Due(k) ==
  /\ sched[k] = "waiting"
  /\ sched' = [sched EXCEPT ![k] = "ready"]
  /\ ready' = Append(ready, k)
  /\ act' = [name |-> "Due", key |-> k]
  /\ UNCHANGED <<caller, answer, lane, busy, batch, wkey, dkey, open, pick, admitted, log, gone, sent>>

(* Protocol.take: the first ready lane gives its longest fitting prefix; the key becomes busy. *)
Take(w) ==
  /\ open
  /\ batch[w] = <<>>
  /\ ready # <<>>
  /\ LET k    == Head(ready)
         n    == BatchSize(lane[k])
         got  == SubSeq(lane[k], 1, n)
         rest == SubSeq(lane[k], n + 1, Len(lane[k]))
         b    == [busy EXCEPT ![k] = @ + 1]
     IN  /\ batch' = [batch EXCEPT ![w] = got]
         /\ wkey' = [wkey EXCEPT ![w] = k]
         /\ busy' = b
         /\ lane' = [lane EXCEPT ![k] = rest]
         \* The lane leaves the line, then is placed afresh.
         /\ LET placed == IF rest = <<>> THEN <<"none", Tail(ready)>>
                          ELSE IF Blocked(k, b) THEN <<"blocked", Tail(ready)>>
                          ELSE IF Full(rest) THEN <<"ready", Append(Tail(ready), k)>>
                          ELSE <<"waiting", Tail(ready)>>
            IN  /\ sched' = [sched EXCEPT ![k] = placed[1]]
                /\ ready' = placed[2]
         /\ log' = [log EXCEPT ![k] = @ \o got]
         /\ sent' = [c \in Callers |-> IF c \in Range(got) THEN sent[c] + 1 ELSE sent[c]]
         /\ act' = [name |-> "Take", worker |-> w, key |-> k, batch |-> got]
  /\ UNCHANGED <<caller, answer, dkey, open, pick, admitted, gone>>

(* A call for `k` is over: a lane blocked behind it goes back in line once the key is free. *)
Free(k) ==
  LET b == [busy EXCEPT ![k] = @ - 1] IN
  /\ busy' = b
  /\ IF sched[k] = "blocked" /\ ~Blocked(k, b)
     THEN IF Full(lane[k])
          THEN sched' = [sched EXCEPT ![k] = "ready"] /\ ready' = Append(ready, k)
          ELSE sched' = [sched EXCEPT ![k] = "waiting"] /\ UNCHANGED ready
     ELSE UNCHANGED <<sched, ready>>

(* An outcome "Interrupted" comes only from the scope closing, which happens after admission closes, or, for a  *)
(* direct call, from its caller giving up.                                                                       *)
Outcome == IF open THEN {"Succeeded", "Failed"} ELSE Outcomes
DirectOutcome(c) == IF open /\ c \in Pending THEN {"Succeeded", "Failed"} ELSE Outcomes

(* Protocol.complete: the batch's pending callers are answered, and the key is freed. *)
Complete(w) ==
  /\ batch[w] # <<>>
  /\ \E o \in [Range(batch[w]) -> Outcome] :
       /\ answer' = [c \in Callers |-> IF c \in Range(batch[w]) /\ c \in Pending THEN o[c] ELSE answer[c]]
       /\ act' = [name |-> "Complete", worker |-> w, outcomes |-> o]
  /\ Free(wkey[w])
  /\ batch' = [batch EXCEPT ![w] = <<>>]
  /\ wkey' = [wkey EXCEPT ![w] = NoKey]
  /\ UNCHANGED <<caller, lane, dkey, open, pick, admitted, log, gone, sent>>

CompleteDirect(c) ==
  /\ dkey[c] # NoKey
  /\ \E o \in DirectOutcome(c) :
       /\ answer' = IF c \in Pending THEN [answer EXCEPT ![c] = o] ELSE answer
       /\ act' = [name |-> "CompleteDirect", caller |-> c, outcome |-> o]
  /\ Free(dkey[c])
  /\ dkey' = [dkey EXCEPT ![c] = NoKey]
  /\ UNCHANGED <<caller, lane, batch, wkey, open, pick, admitted, log, gone, sent>>

(* Protocol.cancel: a pending caller gives up; if its input still waits, it leaves the lane at once. *)
Cancel(c) ==
  /\ c \in Pending
  /\ answer' = [answer EXCEPT ![c] = "Interrupted"]
  /\ IF \E k \in Keys : c \in Range(lane[k])
     THEN LET k == CHOOSE x \in Keys : c \in Range(lane[x])
              s == Remove(lane[k], c)
          IN  /\ lane' = [lane EXCEPT ![k] = s]
              /\ sched' = [sched EXCEPT ![k] = Place(k, s, busy)[1]]
              /\ ready' = Place(k, s, busy)[2]
     ELSE UNCHANGED <<lane, sched, ready>>
  /\ act' = [name |-> "Cancel", caller |-> c]
  /\ UNCHANGED <<caller, busy, batch, wkey, dkey, open, pick, admitted, log, gone, sent>>

(* Protocol.close: admission closes and every waiting input leaves its lane; its caller is interrupted. *)
Close ==
  /\ open
  /\ open' = FALSE
  /\ IF DrainOnClose
     THEN /\ answer' = [c \in Callers |-> IF c \in Queued THEN "Interrupted" ELSE answer[c]]
          /\ lane' = [k \in Keys |-> <<>>]
          /\ sched' = [k \in Keys |-> "none"]
          /\ ready' = <<>>
          /\ act' = [name |-> "Close", drained |-> Queued]
     ELSE /\ act' = [name |-> "Close", drained |-> {}]
          /\ UNCHANGED <<answer, lane, sched, ready>>
  /\ UNCHANGED <<caller, busy, batch, wkey, dkey, pick, admitted, log, gone, sent>>

Next ==
  \/ \E c \in Callers : \/ \E k \in Keys : Submit(c, k)
                        \/ Enter(c) \/ CompleteDirect(c) \/ Cancel(c)
  \/ \E k \in Keys : Due(k)
  \/ \E w \in Workers : Take(w) \/ Complete(w)
  \/ Close

(* Time passes, workers take and calls return; arrivals, giving up and closing need not happen. *)
Fairness ==
  /\ \A k \in Keys : WF_vars(Due(k))
  /\ \A w \in Workers : WF_vars(Take(w)) /\ WF_vars(Complete(w))
  /\ \A c \in Callers : WF_vars(CompleteDirect(c)) /\ WF_vars(Enter(c))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Safety *)

(* A key never has two calls in flight, batched or direct. *)
OneInFlightPerKey ==
  OneCallPerKey =>
    \A k \in Keys :
      Cardinality({w \in Workers : wkey[w] = k}) + Cardinality({c \in Callers : dkey[c] = k}) <= 1

(* Each key's inputs are sent in rank order, ties in admission order, except that an input admitted once another had *)
(* already been sent (a late low rank) may follow it whatever its rank.                                             *)
Before(s, a, b) == \E i, j \in 1..Len(s) : i < j /\ s[i] = a /\ s[j] = b
Precedes(k, a, b) == RankOf[a] < RankOf[b] \/ (RankOf[a] = RankOf[b] /\ Before(admitted[k], a, b))
InOrderPerKey ==
  \A k \in Keys : \A a, b \in Range(log[k]) : Before(log[k], a, b) => Precedes(k, a, b) \/ a \in gone[b]

AtMostOnce == \A c \in Callers : sent[c] <= 1

TurnedAwayNeverSent == \A c \in Callers : caller[c] \in {"rejected", "refused"} => sent[c] = 0

BatchBounded ==
  \A w \in Workers : Len(batch[w]) <= MaxBatch /\ (Len(batch[w]) > 1 => SumW(batch[w]) <= MaxWeight)

QueueBounded == Cardinality(Queued) <= MaxQueue /\ QueuedWeight <= MaxQueuedWeight

(* Once admission has closed, every caller still pending has its input in a call, which will answer it. *)
NoneStrandedAfterClose ==
  ~open => \A c \in Pending : (\E w \in Workers : c \in Range(batch[w])) \/ dkey[c] # NoKey

Safety ==
  /\ OneInFlightPerKey /\ InOrderPerKey /\ AtMostOnce /\ TurnedAwayNeverSent
  /\ BatchBounded /\ QueueBounded /\ NoneStrandedAfterClose

(* Liveness: every caller that was let in is eventually answered. *)
EveryCallerAnswered == \A c \in Callers : (caller[c] \in {"queued", "direct"}) ~> (answer[c] # "none")
=============================================================================
