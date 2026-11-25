--------------------------- MODULE ProjectedWriter ---------------------------
(***************************************************************************)
(* FencedWriter, for a store that writes a key in two steps, as            *)
(* ProjectedWrite does (docs/decisions/0014), for one key.                 *)
(*                                                                         *)
(* The key's record is its log and a marker: how much of the log is       *)
(* projected into the view. A node reads the record. It commits the        *)
(* commands not yet in the log, fenced on the log's length; then it        *)
(* projects the unprojected suffix into the view, fenced on the log and    *)
(* the marker still being as it knew them, and moves the marker. A node    *)
(* that finds its commands committed but not projected projects them      *)
(* (resume). A sweeper projects any record left pending. A node whose     *)
(* projection keeps failing gives up and answers "committed"; it answers  *)
(* "projected" only once it knows the view has its commands. A node whose *)
(* commit keeps failing fails its clients, who send again. Routing,       *)
(* crashes, lost answers and throttling are as in FencedWriter.           *)
(*                                                                         *)
(* The steps are the code's steps (Ferry.perKey's actions, and the        *)
(* store's reads, writes and deliveries, as ProjectedWrite makes them),   *)
(* and each sets `act` to a record of itself, the record the code writes  *)
(* for the same step. ProjectedWriterTrace.tla checks the code's recorded *)
(* runs against this spec.                                                *)
(*                                                                         *)
(* Three switches weaken the writer, each must break a property:           *)
(*   FencedProject - a projection is refused if the record has moved       *)
(*   Resume        - a failed projection is retried, and a sweeper runs    *)
(*   EarlyAnswer   - answer "projected" as soon as the commit lands        *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Commands, Nodes, MaxCrashes, MaxLost, MaxThrottled, MaxGiveUps, FencedProject, Resume, EarlyAnswer

Sweeper == "sweeper"

VARIABLES
  log,       \* the key's log in the store
  marker,    \* how much of the log is projected, as the record says
  view,      \* its projection: the log's entries, in order, each once
  client,    \* each command's client: idle, waiting at a node, or answered
  answer,    \* the position each answered command was given
  status,    \* how it was answered: "committed" or "projected"
  inbox,     \* commands sent to each node, not yet in a batch
  up,        \* whether each node is alive
  node,      \* where each node is in its batch
  batch,     \* each node's current batch
  known,     \* the log as the node knows it: as read, plus what it committed
  seen,      \* the marker as the node last read it
  how,       \* how a replying node answers
  wire,      \* writes sent to the store, not yet applied
  sweeping,  \* the sweeper has a projection on the wire
  crashes, lost, throttled, gaveUp,
  act        \* the last step, as a record

vars == <<log, marker, view, client, answer, status, inbox, up, node, batch, known, seen, how, wire, sweeping,
          crashes, lost, throttled, gaveUp, act>>

Range(s) == {s[i] : i \in 1..Len(s)}
Position(s, c) == CHOOSE i \in 1..Len(s) : s[i] = c
Orders(S) == {f \in [1..Cardinality(S) -> S] : \A i, j \in 1..Cardinality(S) : i # j => f[i] # f[j]}
Suffix(s, from) == SubSeq(s, from + 1, Len(s))

(* Writes on the wire. A commit carries the log length its writer read; a projection carries the log length and *)
(* the marker it knew, and the entries it appends.                                                                *)
Commit(n, len, ids) == [kind |-> "commit", from |-> n, length |-> len, marker |-> 0, ids |-> ids]
Project(from, len, mark, rows) == [kind |-> "project", from |-> from, length |-> len, marker |-> mark, ids |-> rows]

(* The commands of a node's batch not in the log it knows. *)
Fresh(n) == batch[n] \ Range(known[n])

TypeOK ==
  /\ log \in Seq(Commands)
  /\ view \in Seq(Commands)
  /\ marker \in Nat
  /\ client \in [Commands -> {"idle", "answered"} \cup Nodes]
  /\ status \in [Commands -> {"none", "committed", "projected"}]
  /\ up \in [Nodes -> BOOLEAN]
  /\ node \in [Nodes -> {"idle", "reading", "read", "committing", "committed", "projecting", "replying"}]
  /\ batch \in [Nodes -> SUBSET Commands]
  /\ inbox \in [Nodes -> SUBSET Commands]
  /\ sweeping \in BOOLEAN

Init ==
  /\ log = <<>>
  /\ marker = 0
  /\ view = <<>>
  /\ client = [c \in Commands |-> "idle"]
  /\ answer = [c \in Commands |-> 0]
  /\ status = [c \in Commands |-> "none"]
  /\ inbox = [n \in Nodes |-> {}]
  /\ up = [n \in Nodes |-> TRUE]
  /\ node = [n \in Nodes |-> "idle"]
  /\ batch = [n \in Nodes |-> {}]
  /\ known = [n \in Nodes |-> <<>>]
  /\ seen = [n \in Nodes |-> 0]
  /\ how = [n \in Nodes |-> "none"]
  /\ wire = {}
  /\ sweeping = FALSE
  /\ crashes = 0
  /\ lost = 0
  /\ throttled = 0
  /\ gaveUp = 0
  /\ act = [name |-> "Init"]

Send(c, n) ==
  /\ client[c] = "idle"
  /\ up[n]
  /\ client' = [client EXCEPT ![c] = n]
  /\ inbox' = [inbox EXCEPT ![n] = @ \cup {c}]
  /\ act' = [name |-> "Send", command |-> c, node |-> n]
  /\ UNCHANGED <<log, marker, view, answer, status, up, node, batch, known, seen, how, wire, sweeping, crashes, lost,
                 throttled, gaveUp>>

Take(n) ==
  /\ up[n]
  /\ node[n] = "idle"
  /\ inbox[n] # {}
  /\ batch' = [batch EXCEPT ![n] = inbox[n]]
  /\ inbox' = [inbox EXCEPT ![n] = {}]
  /\ node' = [node EXCEPT ![n] = "reading"]
  /\ act' = [name |-> "Take", node |-> n, batch |-> inbox[n]]
  /\ UNCHANGED <<log, marker, view, client, answer, status, up, known, seen, how, wire, sweeping, crashes, lost,
                 throttled, gaveUp>>

(* Read the record: the log and its marker. What to do next is the next step's. *)
Read(n) ==
  /\ node[n] = "reading"
  /\ known' = [known EXCEPT ![n] = log]
  /\ seen' = [seen EXCEPT ![n] = marker]
  /\ node' = [node EXCEPT ![n] = "read"]
  /\ act' = [name |-> "Read", node |-> n]
  /\ UNCHANGED <<log, marker, view, client, answer, status, inbox, up, batch, how, wire, sweeping, crashes, lost,
                 throttled, gaveUp>>

(* Commit the commands not yet in the log, in an order of the node's choosing, fenced on the log's length. *)
SendCommit(n) ==
  /\ node[n] = "read"
  /\ Fresh(n) # {}
  /\ \E ids \in Orders(Fresh(n)) :
       /\ wire' = wire \cup {Commit(n, Len(known[n]), ids)}
       /\ act' = [name |-> "Commit", node |-> n, length |-> Len(known[n]), ids |-> ids]
  /\ node' = [node EXCEPT ![n] = "committing"]
  /\ UNCHANGED <<log, marker, view, client, answer, status, inbox, up, batch, known, seen, how, sweeping, crashes,
                 lost, throttled, gaveUp>>

(* Project the record as the node knows it: after its commit landed, or after a read found it pending. *)
SendProject(n) ==
  /\ \/ node[n] = "committed" /\ ~EarlyAnswer
     \/ node[n] = "read" /\ Fresh(n) = {}
  /\ seen[n] < Len(known[n])
  /\ LET m == Project(n, Len(known[n]), seen[n], Suffix(known[n], seen[n])) IN
       /\ wire' = wire \cup {m}
       /\ act' = [name |-> "Project", node |-> n, length |-> m.length, marker |-> m.marker, ids |-> m.ids]
  /\ node' = [node EXCEPT ![n] = "projecting"]
  /\ UNCHANGED <<log, marker, view, client, answer, status, inbox, up, batch, known, seen, how, sweeping, crashes,
                 lost, throttled, gaveUp>>

Done(n) ==
  /\ batch' = [batch EXCEPT ![n] = {}]
  /\ node' = [node EXCEPT ![n] = "idle"]
  /\ how' = [how EXCEPT ![n] = "none"]

Answer(n, s, h) ==
  /\ client' = [c \in Commands |-> IF c \in batch[n] THEN "answered" ELSE client[c]]
  /\ answer' = [c \in Commands |-> IF c \in batch[n] THEN Position(s, c) ELSE answer[c]]
  /\ status' = [c \in Commands |-> IF c \in batch[n] THEN h ELSE status[c]]
  /\ Done(n)

(* The node answers its batch. "Projected": its projection landed, or a read found nothing to commit or project, or *)
(* (EarlyAnswer) its commit landed. "Committed": it gave up on a projection after its commands were committed.      *)
Reply(n) ==
  /\ \/ node[n] = "replying" /\ Answer(n, known[n], how[n]) /\ UNCHANGED gaveUp
     \/ node[n] = "read" /\ Fresh(n) = {} /\ (seen[n] = Len(known[n]) \/ EarlyAnswer)
        /\ Answer(n, known[n], "projected") /\ UNCHANGED gaveUp
     \/ node[n] = "committed" /\ EarlyAnswer /\ Answer(n, known[n], "projected") /\ UNCHANGED gaveUp
     \/ /\ node[n] = "reading"
        /\ batch[n] # {}
        /\ batch[n] \subseteq Range(log)
        /\ gaveUp < MaxGiveUps
        /\ gaveUp' = gaveUp + 1
        /\ Answer(n, log, "committed")
  /\ act' = [name |-> "Reply", node |-> n, batch |-> batch[n]]
  /\ UNCHANGED <<log, marker, view, inbox, up, known, seen, wire, sweeping, crashes, lost, throttled>>

(* A node whose commit keeps failing fails its batch; its clients send again. *)
Fail(n) ==
  /\ node[n] = "reading"
  /\ client' = [c \in Commands |-> IF c \in batch[n] THEN "idle" ELSE client[c]]
  /\ Done(n)
  /\ act' = [name |-> "Fail", node |-> n, batch |-> batch[n]]
  /\ UNCHANGED <<log, marker, view, answer, status, inbox, up, known, seen, wire, sweeping, crashes, lost, throttled,
                 gaveUp>>

(* The store applies a commit if the log has not grown since it was read. A sender that learns it landed knows the *)
(* log it committed; one that does not reads again.                                                                 *)
DeliverCommit(m) ==
  /\ m \in wire
  /\ m.kind = "commit"
  /\ wire' = wire \ {m}
  /\ LET applies == Len(log) = m.length
         waiting == node[m.from] = "committing"
     IN  /\ log' = IF applies THEN log \o m.ids ELSE log
         /\ \E answered \in {TRUE, FALSE} :
              /\ answered \/ lost < MaxLost
              /\ lost' = IF answered THEN lost ELSE lost + 1
              /\ act' = [name |-> "DeliverCommit", from |-> m.from, length |-> m.length, ids |-> m.ids,
                         applied |-> applies, answered |-> answered]
              /\ IF waiting /\ applies /\ answered
                 THEN /\ known' = [known EXCEPT ![m.from] = @ \o m.ids]
                      /\ node' = [node EXCEPT ![m.from] = "committed"]
                 ELSE /\ node' = IF waiting THEN [node EXCEPT ![m.from] = "reading"] ELSE node
                      /\ UNCHANGED known
  /\ UNCHANGED <<marker, view, client, answer, status, inbox, up, batch, seen, how, sweeping, crashes, throttled,
                 gaveUp>>

(* The store applies a projection if the log and the marker are as it was made from, or always if it is unfenced, *)
(* and moves the marker. Its sender answers "projected" once it learns it landed; otherwise it reads again, or,    *)
(* without resume, answers "committed".                                                                             *)
Lands(m) == ~FencedProject \/ (Len(log) = m.length /\ marker = m.marker)

DeliverProject(m) ==
  /\ m \in wire
  /\ m.kind = "project"
  /\ m.from # Sweeper
  /\ wire' = wire \ {m}
  /\ view' = IF Lands(m) THEN view \o m.ids ELSE view
  /\ marker' = IF Lands(m) THEN m.length ELSE marker
  /\ LET waiting == node[m.from] = "projecting" IN
       \E answered \in {TRUE, FALSE} :
         /\ answered \/ lost < MaxLost
         /\ lost' = IF answered THEN lost ELSE lost + 1
         /\ act' = [name |-> "DeliverProject", from |-> m.from, length |-> m.length, marker |-> m.marker,
                    ids |-> m.ids, applied |-> Lands(m), answered |-> answered]
         /\ IF waiting /\ Lands(m) /\ answered
            THEN /\ node' = [node EXCEPT ![m.from] = "replying"]
                 /\ how' = [how EXCEPT ![m.from] = "projected"]
            ELSE IF waiting /\ Resume
            THEN node' = [node EXCEPT ![m.from] = "reading"] /\ UNCHANGED how
            ELSE IF waiting
            THEN /\ node' = [node EXCEPT ![m.from] = "replying"]
                 /\ how' = [how EXCEPT ![m.from] = "committed"]
            ELSE UNCHANGED <<node, how>>
  /\ UNCHANGED <<log, client, answer, status, inbox, up, batch, known, seen, sweeping, crashes, throttled, gaveUp>>

(* The sweeper reads the record, finds it pending, and projects it, fenced like any projection. *)
Sweep ==
  /\ Resume
  /\ ~sweeping
  /\ marker < Len(log)
  /\ LET m == Project(Sweeper, Len(log), marker, Suffix(log, marker)) IN
       /\ wire' = wire \cup {m}
       /\ act' = [name |-> "Sweep", length |-> m.length, marker |-> m.marker, ids |-> m.ids]
  /\ sweeping' = TRUE
  /\ UNCHANGED <<log, marker, view, client, answer, status, inbox, up, node, batch, known, seen, how, crashes, lost,
                 throttled, gaveUp>>

DeliverSweep(m) ==
  /\ m \in wire
  /\ m.from = Sweeper
  /\ wire' = wire \ {m}
  /\ view' = IF Lands(m) THEN view \o m.ids ELSE view
  /\ marker' = IF Lands(m) THEN m.length ELSE marker
  /\ sweeping' = FALSE
  /\ act' = [name |-> "DeliverSweep", length |-> m.length, marker |-> m.marker, ids |-> m.ids, applied |-> Lands(m)]
  /\ UNCHANGED <<log, client, answer, status, inbox, up, node, batch, known, seen, how, crashes, lost, throttled,
                 gaveUp>>

(* The store refuses a write without looking at it: throttling, or a refused projection. Its sender reads again. *)
Throttle(m) ==
  /\ m \in wire
  /\ throttled < MaxThrottled
  /\ wire' = wire \ {m}
  /\ throttled' = throttled + 1
  /\ IF m.from = Sweeper
     THEN sweeping' = FALSE /\ UNCHANGED node
     ELSE /\ node' = IF node[m.from] \in {"committing", "projecting"} THEN [node EXCEPT ![m.from] = "reading"] ELSE node
          /\ UNCHANGED sweeping
  /\ act' = [name |-> "Throttle", from |-> m.from, kind |-> m.kind, length |-> m.length, marker |-> m.marker,
             ids |-> m.ids]
  /\ UNCHANGED <<log, marker, view, client, answer, status, inbox, up, batch, known, seen, how, crashes, lost, gaveUp>>

(* A node crashes: it takes nothing more, and the clients still in its inbox send again. *)
Crash(n) ==
  /\ up[n]
  /\ crashes < MaxCrashes
  /\ up' = [up EXCEPT ![n] = FALSE]
  /\ client' = [c \in Commands |-> IF c \in inbox[n] THEN "idle" ELSE client[c]]
  /\ inbox' = [inbox EXCEPT ![n] = {}]
  /\ crashes' = crashes + 1
  /\ act' = [name |-> "Crash", node |-> n, drained |-> inbox[n]]
  /\ UNCHANGED <<log, marker, view, answer, status, node, batch, known, seen, how, wire, sweeping, lost, throttled,
                 gaveUp>>

(* A crashed node's batch is abandoned at any point before it is answered, even between a commit and its *)
(* projection; its clients send again. Its writes on the wire still land.                                  *)
Abandon(n) ==
  /\ ~up[n]
  /\ node[n] # "idle"
  /\ client' = [c \in Commands |-> IF c \in batch[n] THEN "idle" ELSE client[c]]
  /\ Done(n)
  /\ act' = [name |-> "Abandon", node |-> n, batch |-> batch[n]]
  /\ UNCHANGED <<log, marker, view, answer, status, inbox, up, known, seen, wire, sweeping, crashes, lost, throttled,
                 gaveUp>>

Next ==
  \/ \E c \in Commands, n \in Nodes : Send(c, n)
  \/ \E n \in Nodes : Take(n) \/ Read(n) \/ SendCommit(n) \/ SendProject(n) \/ Reply(n) \/ Fail(n) \/ Crash(n)
                      \/ Abandon(n)
  \/ \E m \in wire : DeliverCommit(m) \/ DeliverProject(m) \/ DeliverSweep(m) \/ Throttle(m)
  \/ Sweep

(* Every name `act` can take, for the vocabulary check against the code's recorder. *)
Vocabulary == {"Send", "Take", "Read", "Commit", "Project", "Reply", "Fail", "DeliverCommit", "DeliverProject",
               "Sweep", "DeliverSweep", "Throttle", "Crash", "Abandon"}

(* Live nodes keep working and writes on the wire are delivered; clients may stop sending, and crashes, give-ups *)
(* and throttling are never forced. So a pending record is left behind only by a crash or a give-up between the  *)
(* two writes, and the view must still catch up with the log.                                                    *)
Fairness ==
  /\ \A n \in Nodes : WF_vars(Take(n)) /\ WF_vars(Read(n)) /\ WF_vars(SendCommit(n)) /\ WF_vars(SendProject(n))
  /\ WF_vars(\E m \in wire : DeliverCommit(m) \/ DeliverProject(m))
  /\ WF_vars(Sweep) /\ WF_vars(\E m \in wire : DeliverSweep(m))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
AtMostOnce == \A i, j \in 1..Len(log) : i # j => log[i] # log[j]

(* No entry is projected twice, and the view is always a projection of a prefix of the log. *)
ViewAtMostOnce == \A i, j \in 1..Len(view) : i # j => view[i] # view[j]
ViewIsPrefix == Len(view) <= Len(log) /\ view = SubSeq(log, 1, Len(view))

AnswersTruthful == \A c \in Commands : client[c] = "answered" => answer[c] \in 1..Len(log) /\ log[answer[c]] = c

(* A command answered "projected" is in the view. *)
ProjectedInView == \A c \in Commands : status[c] = "projected" => c \in Range(view)

Safety == AtMostOnce /\ ViewAtMostOnce /\ ViewIsPrefix /\ AnswersTruthful /\ ProjectedInView

(* Every committed entry is eventually projected, and stays so. *)
EventuallyProjected == <>[](Len(view) = Len(log))

(* Keeps the unfenced variant finite: once an entry is projected twice the invariant is violated anyway. *)
ViewBound == Len(view) <= Cardinality(Commands) + 1
=============================================================================
