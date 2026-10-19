------------------------ MODULE ProjectedWriterPartial ------------------------
(***************************************************************************)
(* ProjectedWriter, for a store whose view is one row per entry, as a      *)
(* per-entry ProjectedWrite projects it (docs/decisions/0015), for one key.*)
(*                                                                         *)
(* A node reads the key's log and marker. It commits the commands not yet  *)
(* in the log, fenced on the log's length; a commit sets the marker. Then  *)
(* it writes a row for each entry of the log it knows that is not yet in   *)
(* the view, each its own write, and once every row it sent has landed it  *)
(* clears the marker, fenced on the log's length. A row write may be       *)
(* refused (throttled) and a response may be lost; a node whose rows did   *)
(* not all land answers per command, "projected" for those whose row       *)
(* landed and "committed" for the rest, or reads again. A sweeper projects *)
(* any record whose marker is set, in the same way. Routing, crashes and   *)
(* give-ups are as in ProjectedWriter.                                     *)
(*                                                                         *)
(* Four switches weaken the writer, each must break a property:            *)
(*   ConditionalRows   - a row already in the view is not written again    *)
(*   ClearWhenComplete - the marker is cleared only once every row landed  *)
(*   FencedClear       - a clear is refused if the log has moved           *)
(*   Resume            - a node may read again, and a sweeper runs         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Commands, Nodes, MaxCrashes, MaxFaults, MaxGiveUps, ConditionalRows, ClearWhenComplete, FencedClear, Resume

Sweeper == "sweeper"
Projectors == Nodes \cup {Sweeper}

VARIABLES
  log,     \* the key's log in the store
  rows,    \* the view: a row per entry, in landing order; a row twice is what ConditionalRows prevents
  flag,    \* the marker: TRUE while a projection is pending; a commit sets it, a clear clears it
  client,  \* each command's client: idle, waiting at a node, or answered
  answer,  \* the position each answered command was given
  status,  \* how it was answered: "committed" or "projected"
  inbox,   \* commands sent to each node, not yet in a batch
  node,    \* each projector's state; the sweeper is a projector with no clients
  batch,   \* each node's current batch
  known,   \* the log as the projector knows it: as read, plus what it committed
  todo,    \* the rows a projecting projector has not yet seen land
  wire,    \* writes sent to the store, not yet applied
  crashes, faults, gaveUp

vars == <<log, rows, flag, client, answer, status, inbox, node, batch, known, todo, wire, crashes, faults, gaveUp>>

Range(s) == {s[i] : i \in 1..Len(s)}
Position(s, c) == CHOOSE i \in 1..Len(s) : s[i] = c
AsSeq(S) == CHOOSE f \in [1..Cardinality(S) -> S] : \A i, j \in 1..Cardinality(S) : i # j => f[i] # f[j]

(* Writes on the wire. A commit and a clear carry the log length their writer knew; a row carries its entry. *)
Commit(n, len, ids) == [kind |-> "commit", from |-> n, length |-> len, ids |-> ids]
Row(from, c) == [kind |-> "row", from |-> from, length |-> 0, ids |-> <<c>>]
Clear(from, len) == [kind |-> "clear", from |-> from, length |-> len, ids |-> <<>>]

RowsOnWire(p) == \E m \in wire : m.kind = "row" /\ m.from = p

TypeOK ==
  /\ log \in Seq(Commands)
  /\ rows \in Seq(Commands)
  /\ flag \in BOOLEAN
  /\ client \in [Commands -> {"idle", "answered"} \cup Nodes]
  /\ status \in [Commands -> {"none", "committed", "projected"}]
  /\ node \in [Projectors -> {"idle", "reading", "committing", "projecting", "clearing", "down"}]
  /\ batch \in [Nodes -> SUBSET Commands]
  /\ inbox \in [Nodes -> SUBSET Commands]
  /\ todo \in [Projectors -> SUBSET Commands]

Init ==
  /\ log = <<>>
  /\ rows = <<>>
  /\ flag = FALSE
  /\ client = [c \in Commands |-> "idle"]
  /\ answer = [c \in Commands |-> 0]
  /\ status = [c \in Commands |-> "none"]
  /\ inbox = [n \in Nodes |-> {}]
  /\ node = [p \in Projectors |-> "idle"]
  /\ batch = [n \in Nodes |-> {}]
  /\ known = [p \in Projectors |-> <<>>]
  /\ todo = [p \in Projectors |-> {}]
  /\ wire = {}
  /\ crashes = 0
  /\ faults = 0
  /\ gaveUp = 0

Send(c, n) ==
  /\ client[c] = "idle"
  /\ node[n] # "down"
  /\ client' = [client EXCEPT ![c] = n]
  /\ inbox' = [inbox EXCEPT ![n] = @ \cup {c}]
  /\ UNCHANGED <<log, rows, flag, answer, status, node, batch, known, todo, wire, crashes, faults, gaveUp>>

Take(n) ==
  /\ node[n] = "idle"
  /\ inbox[n] # {}
  /\ batch' = [batch EXCEPT ![n] = inbox[n]]
  /\ inbox' = [inbox EXCEPT ![n] = {}]
  /\ node' = [node EXCEPT ![n] = "reading"]
  /\ UNCHANGED <<log, rows, flag, client, answer, status, known, todo, wire, crashes, faults, gaveUp>>

(* Answers the node's batch: each command "projected" if its row is known to be in the view, else "committed". *)
AnswerEach(n, s, missing) ==
  /\ client' = [c \in Commands |-> IF c \in batch[n] THEN "answered" ELSE client[c]]
  /\ answer' = [c \in Commands |-> IF c \in batch[n] THEN Position(s, c) ELSE answer[c]]
  /\ status' = [c \in Commands |-> IF c \in batch[n] THEN IF c \in missing THEN "committed" ELSE "projected" ELSE status[c]]
  /\ batch' = [batch EXCEPT ![n] = {}]
  /\ node' = [node EXCEPT ![n] = "idle"]

Answer(n, s, how) == AnswerEach(n, s, IF how = "committed" THEN Commands ELSE {})

(* Starts projecting the log `s`: sends a row for each of its entries not in the view, perhaps none. *)
StartProjection(p, s, sent) ==
  /\ known' = [known EXCEPT ![p] = s]
  /\ todo' = [todo EXCEPT ![p] = Range(s) \ Range(rows)]
  /\ wire' = sent \cup {Row(p, c) : c \in Range(s) \ Range(rows)}

(* Read the log and marker. Commit what is new; else answer if nothing is pending; else project, as the code does *)
(* for a replayed batch whose record is pending.                                                                   *)
Read(n) ==
  /\ node[n] = "reading"
  /\ LET fresh == batch[n] \ Range(log) IN
       IF fresh # {}
       THEN /\ wire' = wire \cup {Commit(n, Len(log), AsSeq(fresh))}
            /\ known' = [known EXCEPT ![n] = log]
            /\ node' = [node EXCEPT ![n] = "committing"]
            /\ UNCHANGED <<client, answer, status, batch, todo>>
       ELSE IF ~flag
       THEN Answer(n, log, "projected") /\ UNCHANGED <<wire, known, todo>>
       ELSE /\ StartProjection(n, log, wire)
            /\ node' = [node EXCEPT ![n] = "projecting"]
            /\ UNCHANGED <<client, answer, status, batch>>
  /\ UNCHANGED <<log, rows, flag, inbox, crashes, faults, gaveUp>>

(* The store applies a commit if the log has not grown since it was read, and sets the marker. A sender that      *)
(* learns it landed projects the log it now knows; one that does not reads again.                                 *)
DeliverCommit(m) ==
  /\ m \in wire
  /\ m.kind = "commit"
  /\ LET applies == Len(log) = m.length
         waiting == node[m.from] = "committing"
         after   == known[m.from] \o m.ids
     IN  /\ log' = IF applies THEN log \o m.ids ELSE log
         /\ flag' = (flag \/ applies)
         /\ \E answered \in {TRUE, FALSE} :
              /\ answered \/ faults < MaxFaults
              /\ faults' = IF answered THEN faults ELSE faults + 1
              /\ IF waiting /\ applies /\ answered
                 THEN /\ StartProjection(m.from, after, wire \ {m})
                      /\ node' = [node EXCEPT ![m.from] = "projecting"]
                 ELSE /\ wire' = wire \ {m}
                      /\ UNCHANGED <<known, todo>>
                      /\ node' = IF waiting THEN [node EXCEPT ![m.from] = "reading"] ELSE node
  /\ UNCHANGED <<rows, client, answer, status, inbox, batch, crashes, gaveUp>>

(* The store applies a row write, unless rows are conditional and the row is already in the view; or it refuses  *)
(* it, applying nothing (a throttle); or it applies it and the answer is lost. Its sender sees it land only when it *)
(* applied, or was found present, and was answered.                                                               *)
DeliverRow(m) ==
  /\ m \in wire
  /\ m.kind = "row"
  /\ LET c == m.ids[1]
         present == ConditionalRows /\ c \in Range(rows)
     IN  \E outcome \in {"landed", "refused", "lost"} :
           /\ outcome = "landed" \/ faults < MaxFaults
           /\ faults' = IF outcome = "landed" THEN faults ELSE faults + 1
           /\ rows' = IF outcome = "refused" \/ present THEN rows ELSE Append(rows, c)
           /\ todo' = IF outcome = "landed" /\ node[m.from] = "projecting"
                      THEN [todo EXCEPT ![m.from] = @ \ {c}]
                      ELSE todo
  /\ wire' = wire \ {m}
  /\ UNCHANGED <<log, flag, client, answer, status, inbox, node, batch, known, crashes, gaveUp>>

(* A projector none of whose rows is on the wire clears the marker if every row landed, or at once if the clear  *)
(* does not wait for that. Otherwise a node answers per command, or reads again; the sweeper tries again later.   *)
RowsDone(p) ==
  /\ node[p] = "projecting"
  /\ ~RowsOnWire(p)
  /\ IF todo[p] = {} \/ ~ClearWhenComplete
     THEN /\ wire' = wire \cup {Clear(p, Len(known[p]))}
          /\ node' = [node EXCEPT ![p] = "clearing"]
          /\ UNCHANGED <<client, answer, status, batch>>
     ELSE IF p = Sweeper
     THEN /\ node' = [node EXCEPT ![p] = "idle"]
          /\ UNCHANGED <<client, answer, status, batch, wire>>
     ELSE /\ \/ AnswerEach(p, known[p], todo[p])
             \/ /\ Resume
                /\ node' = [node EXCEPT ![p] = "reading"]
                /\ UNCHANGED <<client, answer, status, batch>>
          /\ UNCHANGED wire
  /\ UNCHANGED <<log, rows, flag, inbox, known, todo, crashes, faults, gaveUp>>

(* The store clears the marker if it is set and, when fenced, the log has the length its writer knew. A node that *)
(* learns its clear landed answers; one whose clear was refused or lost reads again, or without resume answers    *)
(* "committed". The sweeper goes back to idle either way.                                                         *)
DeliverClear(m) ==
  /\ m \in wire
  /\ m.kind = "clear"
  /\ LET p == m.from
         applies == flag /\ (~FencedClear \/ Len(log) = m.length)
         waiting == node[p] = "clearing"
     IN  /\ flag' = (flag /\ ~applies)
         /\ wire' = wire \ {m}
         /\ \E answered \in {TRUE, FALSE} :
              /\ answered \/ faults < MaxFaults
              /\ faults' = IF answered THEN faults ELSE faults + 1
              /\ IF ~waiting
                 THEN UNCHANGED <<node, client, answer, status, batch>>
                 ELSE IF p = Sweeper
                 THEN node' = [node EXCEPT ![p] = "idle"] /\ UNCHANGED <<client, answer, status, batch>>
                 ELSE IF applies /\ answered
                 THEN AnswerEach(p, known[p], todo[p])
                 ELSE IF Resume
                 THEN node' = [node EXCEPT ![p] = "reading"] /\ UNCHANGED <<client, answer, status, batch>>
                 ELSE Answer(p, known[p], "committed")
  /\ UNCHANGED <<log, rows, inbox, known, todo, crashes, gaveUp>>

(* A node out of retries answers "committed", if its commands are durable. *)
GiveUp(n) ==
  /\ node[n] = "reading"
  /\ batch[n] # {}
  /\ batch[n] \subseteq Range(log)
  /\ gaveUp < MaxGiveUps
  /\ gaveUp' = gaveUp + 1
  /\ Answer(n, log, "committed")
  /\ UNCHANGED <<log, rows, flag, inbox, known, todo, wire, crashes, faults>>

(* The sweeper finds the marker set and projects the record, like any projector. *)
Sweep ==
  /\ Resume
  /\ node[Sweeper] = "idle"
  /\ flag
  /\ StartProjection(Sweeper, log, wire)
  /\ node' = [node EXCEPT ![Sweeper] = "projecting"]
  /\ UNCHANGED <<log, rows, flag, client, answer, status, inbox, batch, crashes, faults, gaveUp>>

(* A crash loses what the node held, even part way through a projection; its writes on the wire still land. *)
Crash(n) ==
  /\ node[n] # "down"
  /\ crashes < MaxCrashes
  /\ node' = [node EXCEPT ![n] = "down"]
  /\ client' = [c \in Commands |-> IF client[c] = n THEN "idle" ELSE client[c]]
  /\ inbox' = [inbox EXCEPT ![n] = {}]
  /\ batch' = [batch EXCEPT ![n] = {}]
  /\ todo' = [todo EXCEPT ![n] = {}]
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<log, rows, flag, answer, status, known, wire, faults, gaveUp>>

Next ==
  \/ \E c \in Commands, n \in Nodes : Send(c, n)
  \/ \E n \in Nodes : Take(n) \/ Read(n) \/ GiveUp(n) \/ Crash(n)
  \/ \E p \in Projectors : RowsDone(p)
  \/ \E m \in wire : DeliverCommit(m) \/ DeliverRow(m) \/ DeliverClear(m)
  \/ Sweep

(* Live projectors keep working and writes on the wire are delivered; clients may stop sending, and crashes and    *)
(* give-ups are never forced. So a pending marker is left behind only by a crash, a give-up or a refused row, and  *)
(* the view must still catch up with the log.                                                                      *)
Fairness ==
  /\ \A n \in Nodes : WF_vars(Take(n)) /\ WF_vars(Read(n))
  /\ \A p \in Projectors : WF_vars(RowsDone(p))
  /\ WF_vars(\E m \in wire : DeliverCommit(m) \/ DeliverRow(m) \/ DeliverClear(m))
  /\ WF_vars(Sweep)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
AtMostOnce == \A i, j \in 1..Len(log) : i # j => log[i] # log[j]

(* No row is written twice, and every row is of an entry in the log. *)
RowsAtMostOnce == \A i, j \in 1..Len(rows) : i # j => rows[i] # rows[j]
RowsFromLog == Range(rows) \subseteq Range(log)

(* The marker is clear only when every entry of the log has its row. *)
MarkerHonest == ~flag => Range(log) \subseteq Range(rows)

AnswersTruthful == \A c \in Commands : client[c] = "answered" => answer[c] \in 1..Len(log) /\ log[answer[c]] = c

(* A command answered "projected" has its row in the view. *)
ProjectedInView == \A c \in Commands : status[c] = "projected" => c \in Range(rows)

Safety == AtMostOnce /\ RowsAtMostOnce /\ RowsFromLog /\ MarkerHonest /\ AnswersTruthful /\ ProjectedInView

(* Every committed entry eventually has its row, and the marker is eventually clear, and both stay so. *)
EventuallyProjected == <>[](Range(rows) = Range(log) /\ ~flag)

(* Keeps the unconditional variant finite: once a row is written twice the invariant is violated anyway. *)
RowBound == Len(rows) <= Cardinality(Commands) + 1
=============================================================================
