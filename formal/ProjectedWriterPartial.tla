------------------------ MODULE ProjectedWriterPartial ------------------------
(***************************************************************************)
(* ProjectedWriter, for a store whose view is one row per entry, as a      *)
(* per-entry ProjectedWrite projects it (docs/decisions/0015), for one key.*)
(*                                                                         *)
(* A node reads the key's log and marker. It commits the commands not yet  *)
(* in the log, fenced on the log's length; a commit sets the marker. To   *)
(* project, it reads which rows the view has, then writes a row for each  *)
(* entry of the log it knows that is missing, one at a time, in log order, *)
(* and once every row it wrote has landed it clears the marker, fenced on *)
(* the log's length. A row write may be refused (throttled) and a         *)
(* response may be lost; a node whose rows did not all land answers per   *)
(* command, "projected" for those whose row landed and "committed" for    *)
(* the rest. A node whose clear fails reads again. A sweeper projects any *)
(* record whose marker is set, in the same way. Routing, crashes, give-ups *)
(* and failed commits are as in ProjectedWriter.                           *)
(*                                                                         *)
(* The steps are the code's steps (Ferry.perKey's actions, and the        *)
(* store's reads, writes and deliveries, as a per-entry ProjectedWrite    *)
(* makes them), and each sets `act` to a record of itself, the record the *)
(* code writes for the same step. ProjectedWriterPartialTrace.tla checks  *)
(* the code's recorded runs against this spec.                            *)
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
  log,      \* the key's log in the store
  rows,     \* the view: a row per entry, in landing order; a row twice is what ConditionalRows prevents
  flag,     \* the marker: TRUE while a projection is pending; a commit sets it, a clear clears it
  client,   \* each command's client: idle, waiting at a node, or answered
  answer,   \* the position each answered command was given
  status,   \* how it was answered: "committed" or "projected"
  inbox,    \* commands sent to each node, not yet in a batch
  up,       \* whether each node is alive
  node,     \* where each projector is; the sweeper is a projector with no clients
  batch,    \* each node's current batch
  known,    \* the log as the projector knows it: as read, plus what it committed
  flagSeen, \* the marker as the projector last read it
  pend,     \* the rows a projecting projector has still to write, in log order
  missing,  \* the rows it wrote that are not known to have landed
  wire,     \* writes sent to the store, not yet applied
  crashes, faults, gaveUp,
  act       \* the last step, as a record

vars == <<log, rows, flag, client, answer, status, inbox, up, node, batch, known, flagSeen, pend, missing, wire,
          crashes, faults, gaveUp, act>>

Range(s) == {s[i] : i \in 1..Len(s)}
Position(s, c) == CHOOSE i \in 1..Len(s) : s[i] = c
Orders(S) == {f \in [1..Cardinality(S) -> S] : \A i, j \in 1..Cardinality(S) : i # j => f[i] # f[j]}

(* Writes on the wire. A commit and a clear carry the log length their writer knew; a row carries its entry. *)
Commit(n, len, ids) == [kind |-> "commit", from |-> n, length |-> len, ids |-> ids]
Row(from, c) == [kind |-> "row", from |-> from, length |-> 0, ids |-> <<c>>]
Clear(from, len) == [kind |-> "clear", from |-> from, length |-> len, ids |-> <<>>]

Fresh(n) == batch[n] \ Range(known[n])


TypeOK ==
  /\ log \in Seq(Commands)
  /\ rows \in Seq(Commands)
  /\ flag \in BOOLEAN
  /\ client \in [Commands -> {"idle", "answered"} \cup Nodes]
  /\ status \in [Commands -> {"none", "committed", "projected"}]
  /\ up \in [Nodes -> BOOLEAN]
  /\ node \in [Projectors -> {"idle", "reading", "read", "committing", "committed", "projecting", "rowing",
                              "clearing", "replying"}]
  /\ batch \in [Nodes -> SUBSET Commands]
  /\ inbox \in [Nodes -> SUBSET Commands]
  /\ missing \in [Projectors -> SUBSET Commands]

Init ==
  /\ log = <<>>
  /\ rows = <<>>
  /\ flag = FALSE
  /\ client = [c \in Commands |-> "idle"]
  /\ answer = [c \in Commands |-> 0]
  /\ status = [c \in Commands |-> "none"]
  /\ inbox = [n \in Nodes |-> {}]
  /\ up = [n \in Nodes |-> TRUE]
  /\ node = [p \in Projectors |-> "idle"]
  /\ batch = [n \in Nodes |-> {}]
  /\ known = [p \in Projectors |-> <<>>]
  /\ flagSeen = [p \in Projectors |-> FALSE]
  /\ pend = [p \in Projectors |-> <<>>]
  /\ missing = [p \in Projectors |-> {}]
  /\ wire = {}
  /\ crashes = 0
  /\ faults = 0
  /\ gaveUp = 0
  /\ act = [name |-> "Init"]

Send(c, n) ==
  /\ client[c] = "idle"
  /\ up[n]
  /\ client' = [client EXCEPT ![c] = n]
  /\ inbox' = [inbox EXCEPT ![n] = @ \cup {c}]
  /\ act' = [name |-> "Send", command |-> c, node |-> n]
  /\ UNCHANGED <<log, rows, flag, answer, status, up, node, batch, known, flagSeen, pend, missing, wire, crashes,
                 faults, gaveUp>>

Take(n) ==
  /\ up[n]
  /\ node[n] = "idle"
  /\ inbox[n] # {}
  /\ batch' = [batch EXCEPT ![n] = inbox[n]]
  /\ inbox' = [inbox EXCEPT ![n] = {}]
  /\ node' = [node EXCEPT ![n] = "reading"]
  /\ act' = [name |-> "Take", node |-> n, batch |-> inbox[n]]
  /\ UNCHANGED <<log, rows, flag, client, answer, status, up, known, flagSeen, pend, missing, wire, crashes, faults,
                 gaveUp>>

(* Read the record: the log and its marker. *)
Read(n) ==
  /\ node[n] = "reading"
  /\ known' = [known EXCEPT ![n] = log]
  /\ flagSeen' = [flagSeen EXCEPT ![n] = flag]
  /\ node' = [node EXCEPT ![n] = "read"]
  /\ act' = [name |-> "Read", node |-> n]
  /\ UNCHANGED <<log, rows, flag, client, answer, status, inbox, up, batch, pend, missing, wire, crashes, faults,
                 gaveUp>>

(* The sweeper is free between projections, and also after one whose rows did not all land, which it leaves. *)
SweeperFree ==
  \/ node[Sweeper] = "idle"
  \/ node[Sweeper] = "projecting" /\ pend[Sweeper] = <<>> /\ missing[Sweeper] # {} /\ ClearWhenComplete

(* The sweeper reads the record and finds the marker set. *)
Sweep ==
  /\ Resume
  /\ SweeperFree
  /\ flag
  /\ known' = [known EXCEPT ![Sweeper] = log]
  /\ flagSeen' = [flagSeen EXCEPT ![Sweeper] = TRUE]
  /\ node' = [node EXCEPT ![Sweeper] = "read"]
  /\ act' = [name |-> "Sweep"]
  /\ UNCHANGED <<log, rows, flag, client, answer, status, inbox, up, batch, pend, missing, wire, crashes, faults,
                 gaveUp>>

SendCommit(n) ==
  /\ node[n] = "read"
  /\ Fresh(n) # {}
  /\ \E ids \in Orders(Fresh(n)) :
       /\ wire' = wire \cup {Commit(n, Len(known[n]), ids)}
       /\ act' = [name |-> "Commit", node |-> n, length |-> Len(known[n]), ids |-> ids]
  /\ node' = [node EXCEPT ![n] = "committing"]
  /\ UNCHANGED <<log, rows, flag, client, answer, status, inbox, up, batch, known, flagSeen, pend, missing, crashes,
                 faults, gaveUp>>

(* The store applies a commit if the log has not grown since it was read, and sets the marker. *)
DeliverCommit(m) ==
  /\ m \in wire
  /\ m.kind = "commit"
  /\ wire' = wire \ {m}
  /\ LET applies == Len(log) = m.length
         waiting == node[m.from] = "committing"
     IN  /\ log' = IF applies THEN log \o m.ids ELSE log
         /\ flag' = (flag \/ applies)
         /\ \E answered \in {TRUE, FALSE} :
              /\ answered \/ faults < MaxFaults
              /\ faults' = IF answered THEN faults ELSE faults + 1
              /\ act' = [name |-> "DeliverCommit", from |-> m.from, length |-> m.length, ids |-> m.ids,
                         applied |-> applies, answered |-> answered]
              /\ IF waiting /\ applies /\ answered
                 THEN /\ known' = [known EXCEPT ![m.from] = @ \o m.ids]
                      /\ node' = [node EXCEPT ![m.from] = "committed"]
                 ELSE /\ node' = IF waiting THEN [node EXCEPT ![m.from] = "reading"] ELSE node
                      /\ UNCHANGED known
  /\ UNCHANGED <<rows, client, answer, status, inbox, up, batch, flagSeen, pend, missing, crashes, gaveUp>>

(* To project the log it knows, a projector reads which rows the view has: it will write the others, in order. *)
ReadRows(p) ==
  /\ \/ node[p] = "committed"
     \/ node[p] = "read" /\ (IF p = Sweeper THEN TRUE ELSE Fresh(p) = {}) /\ flagSeen[p]
  /\ pend' = [pend EXCEPT ![p] = SelectSeq(known[p], LAMBDA c : c \notin Range(rows))]
  /\ missing' = [missing EXCEPT ![p] = {}]
  /\ node' = [node EXCEPT ![p] = "projecting"]
  /\ act' = [name |-> "ReadRows", from |-> p]
  /\ UNCHANGED <<log, rows, flag, client, answer, status, inbox, up, batch, known, flagSeen, wire, crashes, faults,
                 gaveUp>>

SendRow(p) ==
  /\ node[p] = "projecting"
  /\ pend[p] # <<>>
  /\ wire' = wire \cup {Row(p, Head(pend[p]))}
  /\ pend' = [pend EXCEPT ![p] = Tail(@)]
  /\ node' = [node EXCEPT ![p] = "rowing"]
  /\ act' = [name |-> "Row", from |-> p, row |-> Head(pend[p])]
  /\ UNCHANGED <<log, rows, flag, client, answer, status, inbox, up, batch, known, flagSeen, missing, crashes, faults,
                 gaveUp>>

(* The store applies a row write, unless rows are conditional and the row is already in the view; or it refuses  *)
(* it, applying nothing (a throttle); or it applies it and the answer is lost. Its sender sees it land only when  *)
(* it applied, or was found present, and was answered.                                                            *)
DeliverRow(m) ==
  /\ m \in wire
  /\ m.kind = "row"
  /\ wire' = wire \ {m}
  /\ LET c       == m.ids[1]
         present == ConditionalRows /\ c \in Range(rows)
         waiting == node[m.from] = "rowing"
     IN  \E outcome \in {"landed", "refused", "lost"} :
           /\ outcome = "landed" \/ faults < MaxFaults
           /\ faults' = IF outcome = "landed" THEN faults ELSE faults + 1
           /\ rows' = IF outcome = "refused" \/ present THEN rows ELSE Append(rows, c)
           /\ act' = [name |-> "DeliverRow", from |-> m.from, row |-> c, outcome |-> outcome,
                      applied |-> outcome # "refused" /\ ~present]
           /\ node' = IF waiting THEN [node EXCEPT ![m.from] = "projecting"] ELSE node
           /\ missing' = IF waiting /\ outcome # "landed" THEN [missing EXCEPT ![m.from] = @ \cup {c}] ELSE missing
  /\ UNCHANGED <<log, flag, client, answer, status, inbox, up, batch, known, flagSeen, pend, crashes, gaveUp>>

(* Every row written: clear the marker if every row landed, or at once if the clear does not wait for that. *)
SendClear(p) ==
  /\ node[p] = "projecting"
  /\ pend[p] = <<>>
  /\ missing[p] = {} \/ ~ClearWhenComplete
  /\ wire' = wire \cup {Clear(p, Len(known[p]))}
  /\ node' = [node EXCEPT ![p] = "clearing"]
  /\ act' = [name |-> "Clear", from |-> p, length |-> Len(known[p])]
  /\ UNCHANGED <<log, rows, flag, client, answer, status, inbox, up, batch, known, flagSeen, pend, missing, crashes,
                 faults, gaveUp>>

(* The store clears the marker, unless the clear is fenced and the marker is clear or the log has moved. A node   *)
(* that learns its clear landed answers; one whose clear was refused or lost reads again, or without resume       *)
(* answers "committed". The sweeper goes back to idle either way.                                                  *)
DeliverClear(m) ==
  /\ m \in wire
  /\ m.kind = "clear"
  /\ wire' = wire \ {m}
  /\ LET p       == m.from
         applies == ~FencedClear \/ (flag /\ Len(log) = m.length)
         waiting == node[p] = "clearing"
     IN  /\ flag' = (flag /\ ~applies)
         /\ \E answered \in {TRUE, FALSE} :
              /\ answered \/ faults < MaxFaults
              /\ faults' = IF answered THEN faults ELSE faults + 1
              /\ act' = [name |-> "DeliverClear", from |-> p, length |-> m.length, applied |-> applies,
                         answered |-> answered]
              /\ node' = IF ~waiting THEN node
                         ELSE IF p = Sweeper THEN [node EXCEPT ![p] = "idle"]
                         ELSE IF applies /\ answered THEN [node EXCEPT ![p] = "replying"]
                         ELSE IF Resume THEN [node EXCEPT ![p] = "reading"]
                         ELSE [node EXCEPT ![p] = "replying"]
              /\ missing' = IF waiting /\ p # Sweeper /\ ~(applies /\ answered) /\ ~Resume
                            THEN [missing EXCEPT ![p] = Commands]
                            ELSE missing
  /\ UNCHANGED <<log, rows, client, answer, status, inbox, up, batch, known, flagSeen, pend, crashes, gaveUp>>

(* The store refuses a commit or a clear without looking at it: throttling. Its sender reads again. *)
Throttle(m) ==
  /\ m \in wire
  /\ m.kind \in {"commit", "clear"}
  /\ faults < MaxFaults
  /\ wire' = wire \ {m}
  /\ faults' = faults + 1
  /\ node' = IF node[m.from] \in {"committing", "clearing"}
             THEN [node EXCEPT ![m.from] = IF m.from = Sweeper THEN "idle" ELSE "reading"]
             ELSE node
  /\ act' = [name |-> "Throttle", from |-> m.from, kind |-> m.kind, length |-> m.length, ids |-> m.ids]
  /\ UNCHANGED <<log, rows, flag, client, answer, status, inbox, up, batch, known, flagSeen, pend, missing, crashes,
                 gaveUp>>

Done(n) ==
  /\ batch' = [batch EXCEPT ![n] = {}]
  /\ node' = [node EXCEPT ![n] = "idle"]

(* Answers the node's batch: each command "projected" unless its row is in `unsure`, then "committed". *)
AnswerEach(n, s, unsure) ==
  /\ client' = [c \in Commands |-> IF c \in batch[n] THEN "answered" ELSE client[c]]
  /\ answer' = [c \in Commands |-> IF c \in batch[n] THEN Position(s, c) ELSE answer[c]]
  /\ status' = [c \in Commands |-> IF c \in batch[n] THEN IF c \in unsure THEN "committed" ELSE "projected"
                                   ELSE status[c]]
  /\ Done(n)

(* The node answers its batch: after its clear, or after rows that did not all land, per command; after a read  *)
(* that found nothing to do, "projected"; after giving up on a failing projection, "committed".                   *)
Reply(n) ==
  /\ \/ node[n] = "replying" /\ AnswerEach(n, known[n], missing[n]) /\ UNCHANGED gaveUp
     \/ /\ node[n] = "projecting" /\ pend[n] = <<>> /\ missing[n] # {} /\ ClearWhenComplete
        /\ AnswerEach(n, known[n], missing[n]) /\ UNCHANGED gaveUp
     \/ node[n] = "read" /\ Fresh(n) = {} /\ ~flagSeen[n] /\ AnswerEach(n, known[n], {}) /\ UNCHANGED gaveUp
     \/ /\ node[n] = "reading"
        /\ batch[n] # {}
        /\ batch[n] \subseteq Range(log)
        /\ gaveUp < MaxGiveUps
        /\ gaveUp' = gaveUp + 1
        /\ AnswerEach(n, log, Commands)
  /\ act' = [name |-> "Reply", node |-> n, batch |-> batch[n]]
  /\ UNCHANGED <<log, rows, flag, inbox, up, known, flagSeen, pend, missing, wire, crashes, faults>>

(* A node whose commit keeps failing fails its batch; its clients send again. *)
Fail(n) ==
  /\ node[n] = "reading"
  /\ client' = [c \in Commands |-> IF c \in batch[n] THEN "idle" ELSE client[c]]
  /\ Done(n)
  /\ act' = [name |-> "Fail", node |-> n, batch |-> batch[n]]
  /\ UNCHANGED <<log, rows, flag, answer, status, inbox, up, known, flagSeen, pend, missing, wire, crashes, faults,
                 gaveUp>>

Crash(n) ==
  /\ up[n]
  /\ crashes < MaxCrashes
  /\ up' = [up EXCEPT ![n] = FALSE]
  /\ client' = [c \in Commands |-> IF c \in inbox[n] THEN "idle" ELSE client[c]]
  /\ inbox' = [inbox EXCEPT ![n] = {}]
  /\ crashes' = crashes + 1
  /\ act' = [name |-> "Crash", node |-> n, drained |-> inbox[n]]
  /\ UNCHANGED <<log, rows, flag, answer, status, node, batch, known, flagSeen, pend, missing, wire, faults, gaveUp>>

(* A crashed node's batch is abandoned at any point before it is answered, even part way through a projection; *)
(* its clients send again. Its writes on the wire still land.                                                     *)
Abandon(n) ==
  /\ ~up[n]
  /\ node[n] # "idle"
  /\ client' = [c \in Commands |-> IF c \in batch[n] THEN "idle" ELSE client[c]]
  /\ Done(n)
  /\ act' = [name |-> "Abandon", node |-> n, batch |-> batch[n]]
  /\ UNCHANGED <<log, rows, flag, answer, status, inbox, up, known, flagSeen, pend, missing, wire, crashes, faults,
                 gaveUp>>

Next ==
  \/ \E c \in Commands, n \in Nodes : Send(c, n)
  \/ \E n \in Nodes : Take(n) \/ Read(n) \/ SendCommit(n) \/ Reply(n) \/ Fail(n) \/ Crash(n) \/ Abandon(n)
  \/ \E p \in Projectors : ReadRows(p) \/ SendRow(p) \/ SendClear(p)
  \/ \E m \in wire : DeliverCommit(m) \/ DeliverRow(m) \/ DeliverClear(m) \/ Throttle(m)
  \/ Sweep

(* Every name `act` can take, for the vocabulary check against the code's recorder. *)
Vocabulary == {"Send", "Take", "Read", "Sweep", "Commit", "DeliverCommit", "ReadRows", "Row", "DeliverRow", "Clear",
               "DeliverClear", "Throttle", "Reply", "Fail", "Crash", "Abandon"}

(* Live projectors keep working and writes on the wire are delivered; clients may stop sending, and crashes,     *)
(* give-ups and faults are never forced. So a pending marker is left behind only by a crash, a give-up or a      *)
(* refused row, and the view must still catch up with the log.                                                   *)
Fairness ==
  /\ \A n \in Nodes : WF_vars(Take(n)) /\ WF_vars(Read(n)) /\ WF_vars(SendCommit(n))
  /\ \A p \in Projectors : WF_vars(ReadRows(p)) /\ WF_vars(SendRow(p)) /\ WF_vars(SendClear(p))
  /\ WF_vars(\E m \in wire : DeliverCommit(m) \/ DeliverRow(m) \/ DeliverClear(m))
  /\ WF_vars(Sweep)

Spec == Init /\ [][Next]_vars /\ Fairness

(* The state without `act`: model checking need not tell apart states reached by different last steps. *)
View == <<log, rows, flag, client, answer, status, inbox, up, node, batch, known, flagSeen, pend, missing, wire,
          crashes, faults, gaveUp>>

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
