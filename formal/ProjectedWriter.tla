--------------------------- MODULE ProjectedWriter ---------------------------
(***************************************************************************)
(* FencedWriter, for a store that writes a key in two steps, as            *)
(* ProjectedWrite does (docs/decisions/0014), for one key.                 *)
(*                                                                         *)
(* A node reads the key's log and view. It commits the commands not yet in *)
(* the log, fenced on the log's length; then it projects the log's         *)
(* unprojected suffix into the view, fenced on both lengths still being    *)
(* the ones it knew. A node that finds its commands committed but not      *)
(* projected projects them (resume). A sweeper projects any record that is *)
(* left pending. A node may give up on a projection that keeps failing and *)
(* answer "committed"; it answers "projected" only once its projection     *)
(* landed. Routing, crashes and lost answers are as in FencedWriter.       *)
(*                                                                         *)
(* Three switches weaken the writer, each must break a property:           *)
(*   FencedProject - a projection is refused if the record has moved       *)
(*   Resume        - a failed projection is retried, and a sweeper runs    *)
(*   EarlyAnswer   - answer "projected" as soon as the commit lands        *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Commands, Nodes, MaxCrashes, MaxLost, MaxGiveUps, FencedProject, Resume, EarlyAnswer

Sweeper == "sweeper"

VARIABLES
  log,      \* the key's log in the store
  view,     \* its projection: the log's entries, in order, each once
  client,   \* each command's client: idle, waiting at a node, or answered
  answer,   \* the position each answered command was given
  status,   \* how it was answered: "committed" or "projected"
  inbox,    \* commands sent to each node, not yet in a batch
  node,     \* each node's state
  batch,    \* each node's current batch
  known,    \* the log as the node knows it: as read, plus what it committed
  seen,     \* the view length the node last read
  wire,     \* writes sent to the store, not yet applied
  sweeping, \* the sweeper has a projection on the wire
  crashes, lost, gaveUp

vars == <<log, view, client, answer, status, inbox, node, batch, known, seen, wire, sweeping, crashes, lost, gaveUp>>

Range(s) == {s[i] : i \in 1..Len(s)}
Position(s, c) == CHOOSE i \in 1..Len(s) : s[i] = c
AsSeq(S) == CHOOSE f \in [1..Cardinality(S) -> S] : \A i, j \in 1..Cardinality(S) : i # j => f[i] # f[j]

(* Writes on the wire. A commit carries the log length its writer read; a projection carries both lengths it knew *)
(* and the entries it appends.                                                                                    *)
Commit(n, len, ids) == [kind |-> "commit", from |-> n, length |-> len, viewLength |-> 0, ids |-> ids]
Project(from, len, vlen, rows) == [kind |-> "project", from |-> from, length |-> len, viewLength |-> vlen, ids |-> rows]
Suffix(s, from) == SubSeq(s, from + 1, Len(s))

TypeOK ==
  /\ log \in Seq(Commands)
  /\ view \in Seq(Commands)
  /\ client \in [Commands -> {"idle", "answered"} \cup Nodes]
  /\ status \in [Commands -> {"none", "committed", "projected"}]
  /\ node \in [Nodes -> {"idle", "reading", "committing", "projecting", "down"}]
  /\ batch \in [Nodes -> SUBSET Commands]
  /\ inbox \in [Nodes -> SUBSET Commands]
  /\ sweeping \in BOOLEAN

Init ==
  /\ log = <<>>
  /\ view = <<>>
  /\ client = [c \in Commands |-> "idle"]
  /\ answer = [c \in Commands |-> 0]
  /\ status = [c \in Commands |-> "none"]
  /\ inbox = [n \in Nodes |-> {}]
  /\ node = [n \in Nodes |-> "idle"]
  /\ batch = [n \in Nodes |-> {}]
  /\ known = [n \in Nodes |-> <<>>]
  /\ seen = [n \in Nodes |-> 0]
  /\ wire = {}
  /\ sweeping = FALSE
  /\ crashes = 0
  /\ lost = 0
  /\ gaveUp = 0

Send(c, n) ==
  /\ client[c] = "idle"
  /\ node[n] # "down"
  /\ client' = [client EXCEPT ![c] = n]
  /\ inbox' = [inbox EXCEPT ![n] = @ \cup {c}]
  /\ UNCHANGED <<log, view, answer, status, node, batch, known, seen, wire, sweeping, crashes, lost, gaveUp>>

Take(n) ==
  /\ node[n] = "idle"
  /\ inbox[n] # {}
  /\ batch' = [batch EXCEPT ![n] = inbox[n]]
  /\ inbox' = [inbox EXCEPT ![n] = {}]
  /\ node' = [node EXCEPT ![n] = "reading"]
  /\ UNCHANGED <<log, view, client, answer, status, known, seen, wire, sweeping, crashes, lost, gaveUp>>

Answer(n, s, how) ==
  /\ client' = [c \in Commands |-> IF c \in batch[n] THEN "answered" ELSE client[c]]
  /\ answer' = [c \in Commands |-> IF c \in batch[n] THEN Position(s, c) ELSE answer[c]]
  /\ status' = [c \in Commands |-> IF c \in batch[n] THEN how ELSE status[c]]
  /\ batch' = [batch EXCEPT ![n] = {}]
  /\ node' = [node EXCEPT ![n] = "idle"]

(* Read the log and view. Commit what is new; else answer if nothing is pending; else project what is pending, as *)
(* the code does for a replayed batch whose record is pending.                                                     *)
Read(n) ==
  /\ node[n] = "reading"
  /\ known' = [known EXCEPT ![n] = log]
  /\ seen' = [seen EXCEPT ![n] = Len(view)]
  /\ LET fresh == batch[n] \ Range(log) IN
       IF fresh # {}
       THEN /\ wire' = wire \cup {Commit(n, Len(log), AsSeq(fresh))}
            /\ node' = [node EXCEPT ![n] = "committing"]
            /\ UNCHANGED <<client, answer, status, batch>>
       ELSE IF Len(view) = Len(log)
       THEN Answer(n, log, "projected") /\ UNCHANGED wire
       ELSE /\ wire' = wire \cup {Project(n, Len(log), Len(view), Suffix(log, Len(view)))}
            /\ node' = [node EXCEPT ![n] = "projecting"]
            /\ UNCHANGED <<client, answer, status, batch>>
  /\ UNCHANGED <<log, view, inbox, sweeping, crashes, lost, gaveUp>>

(* The store applies a commit if the log has not grown since it was read. A sender that learns it landed projects *)
(* the log it now knows, from the view length it read; one that does not reads again.                            *)
DeliverCommit(m) ==
  /\ m \in wire
  /\ m.kind = "commit"
  /\ LET applies == Len(log) = m.length
         waiting == node[m.from] = "committing"
         after   == known[m.from] \o m.ids
     IN  /\ log' = IF applies THEN log \o m.ids ELSE log
         /\ \E answered \in {TRUE, FALSE} :
              /\ answered \/ lost < MaxLost
              /\ lost' = IF answered THEN lost ELSE lost + 1
              /\ IF waiting /\ applies /\ answered
                 THEN IF EarlyAnswer
                      THEN /\ Answer(m.from, after, "projected")
                           /\ wire' = wire \ {m}
                           /\ UNCHANGED known
                      ELSE /\ known' = [known EXCEPT ![m.from] = after]
                           /\ wire' = (wire \ {m}) \cup {Project(m.from, Len(after), seen[m.from], Suffix(after, seen[m.from]))}
                           /\ node' = [node EXCEPT ![m.from] = "projecting"]
                           /\ UNCHANGED <<client, answer, status, batch>>
                 ELSE /\ wire' = wire \ {m}
                      /\ UNCHANGED known
                      /\ IF waiting
                         THEN node' = [node EXCEPT ![m.from] = "reading"] /\ UNCHANGED <<client, answer, status, batch>>
                         ELSE UNCHANGED <<node, client, answer, status, batch>>
  /\ UNCHANGED <<view, inbox, seen, sweeping, crashes, gaveUp>>

(* The store applies a projection if the log and view still have the lengths it was made from, or always if the   *)
(* projection is unfenced. Its sender answers "projected" once it learns it landed; otherwise it reads again and  *)
(* tries once more, or, without resume, gives up and answers "committed".                                         *)
DeliverProject(m) ==
  /\ m \in wire
  /\ m.kind = "project"
  /\ m.from # Sweeper
  /\ LET applies == ~FencedProject \/ (Len(log) = m.length /\ Len(view) = m.viewLength)
         waiting == node[m.from] = "projecting"
     IN  /\ view' = IF applies THEN view \o m.ids ELSE view
         /\ wire' = wire \ {m}
         /\ \E answered \in {TRUE, FALSE} :
              /\ answered \/ lost < MaxLost
              /\ lost' = IF answered THEN lost ELSE lost + 1
              /\ IF waiting /\ applies /\ answered
                 THEN Answer(m.from, known[m.from], "projected")
                 ELSE IF waiting /\ Resume
                 THEN node' = [node EXCEPT ![m.from] = "reading"] /\ UNCHANGED <<client, answer, status, batch>>
                 ELSE IF waiting
                 THEN Answer(m.from, known[m.from], "committed")
                 ELSE UNCHANGED <<node, client, answer, status, batch>>
  /\ UNCHANGED <<log, inbox, known, seen, sweeping, crashes, gaveUp>>

(* A node out of retries answers "committed", if its commands are durable. *)
GiveUp(n) ==
  /\ node[n] = "reading"
  /\ batch[n] # {}
  /\ batch[n] \subseteq Range(log)
  /\ gaveUp < MaxGiveUps
  /\ gaveUp' = gaveUp + 1
  /\ Answer(n, log, "committed")
  /\ UNCHANGED <<log, view, inbox, known, seen, wire, sweeping, crashes, lost>>

(* The sweeper finds the record pending and projects it, fenced like any projection. *)
Sweep ==
  /\ Resume
  /\ ~sweeping
  /\ Len(view) < Len(log)
  /\ wire' = wire \cup {Project(Sweeper, Len(log), Len(view), Suffix(log, Len(view)))}
  /\ sweeping' = TRUE
  /\ UNCHANGED <<log, view, client, answer, status, inbox, node, batch, known, seen, crashes, lost, gaveUp>>

DeliverSweep(m) ==
  /\ m \in wire
  /\ m.from = Sweeper
  /\ LET applies == ~FencedProject \/ (Len(log) = m.length /\ Len(view) = m.viewLength)
     IN  view' = IF applies THEN view \o m.ids ELSE view
  /\ wire' = wire \ {m}
  /\ sweeping' = FALSE
  /\ UNCHANGED <<log, client, answer, status, inbox, node, batch, known, seen, crashes, lost, gaveUp>>

(* A crash loses what the node held, even between a commit and its projection; its writes on the wire still land. *)
Crash(n) ==
  /\ node[n] # "down"
  /\ crashes < MaxCrashes
  /\ node' = [node EXCEPT ![n] = "down"]
  /\ client' = [c \in Commands |-> IF client[c] = n THEN "idle" ELSE client[c]]
  /\ inbox' = [inbox EXCEPT ![n] = {}]
  /\ batch' = [batch EXCEPT ![n] = {}]
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<log, view, answer, status, known, seen, wire, sweeping, lost, gaveUp>>

Next ==
  \/ \E c \in Commands, n \in Nodes : Send(c, n)
  \/ \E n \in Nodes : Take(n) \/ Read(n) \/ GiveUp(n) \/ Crash(n)
  \/ \E m \in wire : DeliverCommit(m) \/ DeliverProject(m) \/ DeliverSweep(m)
  \/ Sweep

(* Live nodes keep working and writes on the wire are delivered; clients may stop sending, and crashes and give-ups *)
(* are never forced. So a pending record is left behind only by a crash or a give-up between the two writes, and   *)
(* the view must still catch up with the log.                                                                      *)
Fairness ==
  /\ \A n \in Nodes : WF_vars(Take(n)) /\ WF_vars(Read(n))
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
