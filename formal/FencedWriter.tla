---------------------------- MODULE FencedWriter ----------------------------
(***************************************************************************)
(* The deployment pattern Ferry.perKey is meant for, for one key (keys  *)
(* are independent).                                                      *)
(*                                                                         *)
(* Clients send commands to nodes. Each node runs one batch for the key at *)
(* a time: it reads the key's log, leaves out commands already applied,   *)
(* and asks the store to append the rest only if the log still has the    *)
(* length it read (the fence). The store answers, or the answer is lost,  *)
(* or it refuses the append outright (throttling). On a refused write or  *)
(* a lost answer the node reads again and retries, and after a few tries  *)
(* it gives up and fails its clients, who send again. Answers report each *)
(* command's position in the log.                                         *)
(*                                                                         *)
(* Ownership is not modelled as a rule: a client may send to any live     *)
(* node, so two nodes may write the key at once, as during an ownership   *)
(* handover, or when a client cannot reach the owner. Nodes may crash: a  *)
(* crashed node takes nothing more, the clients still in its inbox send   *)
(* again, and the batch it was running is abandoned or, if its result was *)
(* already in hand, still answered. Its appends already on the wire are   *)
(* still delivered later. The pattern must be safe without help from      *)
(* routing.                                                               *)
(*                                                                         *)
(* The steps are the code's steps (Ferry.perKey's actions and the store's *)
(* reads and deliveries), and each sets `act` to a record of itself, the   *)
(* record the code writes for the same step. FencedWriterTrace.tla checks *)
(* the code's recorded runs against this spec.                            *)
(*                                                                         *)
(* Two switches weaken the writer, to show each safeguard is needed:      *)
(*   Fenced       - the store refuses an append if the log has grown      *)
(*   Deduplicated - a node leaves out commands already in the log         *)
(* Without either, a command is applied twice.                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Commands, Nodes, MaxCrashes, MaxLost, MaxThrottled, Fenced, Deduplicated

VARIABLES
  log,       \* the key's log in the store
  client,    \* each command's client: idle, waiting at a node, or answered
  answer,    \* the position each answered command was given
  inbox,     \* commands sent to each node, not yet in a batch
  up,        \* whether each node is alive
  node,      \* where each node is in its batch
  batch,     \* each node's current batch
  read,      \* the log as the node last read it
  result,    \* the log a node answers its batch from, once it knows it
  wire,      \* appends sent to the store, not yet applied: [from, length, ids]
  crashes,   \* crashes so far
  lost,      \* lost answers so far
  throttled, \* refused appends so far
  act        \* the last step, as a record

vars == <<log, client, answer, inbox, up, node, batch, read, result, wire, crashes, lost, throttled, act>>

Range(s) == {s[i] : i \in 1..Len(s)}
Position(s, c) == CHOOSE i \in 1..Len(s) : s[i] = c

(* Every order of a set of commands, as sequences: the order of an append is the node's to choose. *)
Orders(S) == {f \in [1..Cardinality(S) -> S] : \A i, j \in 1..Cardinality(S) : i # j => f[i] # f[j]}

TypeOK ==
  /\ log \in Seq(Commands)
  /\ client \in [Commands -> {"idle", "answered"} \cup Nodes]
  /\ up \in [Nodes -> BOOLEAN]
  /\ node \in [Nodes -> {"idle", "reading", "writing", "replying"}]
  /\ batch \in [Nodes -> SUBSET Commands]
  /\ inbox \in [Nodes -> SUBSET Commands]

Init ==
  /\ log = <<>>
  /\ client = [c \in Commands |-> "idle"]
  /\ answer = [c \in Commands |-> 0]
  /\ inbox = [n \in Nodes |-> {}]
  /\ up = [n \in Nodes |-> TRUE]
  /\ node = [n \in Nodes |-> "idle"]
  /\ batch = [n \in Nodes |-> {}]
  /\ read = [n \in Nodes |-> <<>>]
  /\ result = [n \in Nodes |-> <<>>]
  /\ wire = {}
  /\ crashes = 0
  /\ lost = 0
  /\ throttled = 0
  /\ act = [name |-> "Init"]

(* A client sends its command to any live node: the owner, or not. *)
Send(c, n) ==
  /\ client[c] = "idle"
  /\ up[n]
  /\ client' = [client EXCEPT ![c] = n]
  /\ inbox' = [inbox EXCEPT ![n] = @ \cup {c}]
  /\ act' = [name |-> "Send", command |-> c, node |-> n]
  /\ UNCHANGED <<log, answer, up, node, batch, read, result, wire, crashes, lost, throttled>>

(* The node's key is free: its next batch takes what has arrived. *)
Take(n) ==
  /\ up[n]
  /\ node[n] = "idle"
  /\ inbox[n] # {}
  /\ batch' = [batch EXCEPT ![n] = inbox[n]]
  /\ inbox' = [inbox EXCEPT ![n] = {}]
  /\ node' = [node EXCEPT ![n] = "reading"]
  /\ act' = [name |-> "Take", node |-> n, batch |-> inbox[n]]
  /\ UNCHANGED <<log, client, answer, up, read, result, wire, crashes, lost, throttled>>

(* Read the log; if every command of the batch is already in it, the answer is known. Otherwise append the rest. *)
Read(n) ==
  /\ node[n] = "reading"
  /\ read' = [read EXCEPT ![n] = log]
  /\ LET fresh == IF Deduplicated THEN batch[n] \ Range(log) ELSE batch[n] IN
       IF fresh = {}
       THEN /\ node' = [node EXCEPT ![n] = "replying"]
            /\ result' = [result EXCEPT ![n] = log]
            /\ act' = [name |-> "Read", node |-> n, ids |-> <<>>]
            /\ UNCHANGED wire
       ELSE \E ids \in Orders(fresh) :
            /\ wire' = wire \cup {[from |-> n, length |-> Len(log), ids |-> ids]}
            /\ node' = [node EXCEPT ![n] = "writing"]
            /\ act' = [name |-> "Read", node |-> n, ids |-> ids]
            /\ UNCHANGED result
  /\ UNCHANGED <<log, client, answer, inbox, up, batch, crashes, lost, throttled>>

(* The store applies an append, or refuses it if the log has grown since it was read. The sender, if still waiting *)
(* for it, learns the result, unless the answer is lost: then it reads again, like after a refusal. A node has at  *)
(* most one append of its own on the wire while it writes, so the one delivered is the one it waits for.         *)
Deliver(m) ==
  /\ m \in wire
  /\ wire' = wire \ {m}
  /\ LET applies == ~Fenced \/ Len(log) = m.length
         waiting == node[m.from] = "writing"
     IN  /\ log' = IF applies THEN log \o m.ids ELSE log
         /\ \E answered \in {TRUE, FALSE} :
              /\ answered \/ lost < MaxLost
              /\ lost' = IF answered THEN lost ELSE lost + 1
              /\ act' = [name |-> "Deliver", from |-> m.from, length |-> m.length, ids |-> m.ids,
                         applied |-> applies, answered |-> answered]
              /\ IF waiting /\ applies /\ answered
                 THEN /\ node' = [node EXCEPT ![m.from] = "replying"]
                      /\ result' = [result EXCEPT ![m.from] = read[m.from] \o m.ids]
                 ELSE /\ node' = IF waiting THEN [node EXCEPT ![m.from] = "reading"] ELSE node
                      /\ UNCHANGED result
  /\ UNCHANGED <<client, answer, inbox, up, batch, read, crashes, throttled>>

(* The store refuses an append without looking at it: throttling. The sender reads again. *)
Throttle(m) ==
  /\ m \in wire
  /\ throttled < MaxThrottled
  /\ wire' = wire \ {m}
  /\ node' = IF node[m.from] = "writing" THEN [node EXCEPT ![m.from] = "reading"] ELSE node
  /\ throttled' = throttled + 1
  /\ act' = [name |-> "Throttle", from |-> m.from, length |-> m.length, ids |-> m.ids]
  /\ UNCHANGED <<log, client, answer, inbox, up, batch, read, result, crashes, lost>>

Done(n) ==
  /\ batch' = [batch EXCEPT ![n] = {}]
  /\ node' = [node EXCEPT ![n] = "idle"]

(* The node answers its batch: each command with its position in the log the node knows. *)
Reply(n) ==
  /\ node[n] = "replying"
  /\ client' = [c \in Commands |-> IF c \in batch[n] THEN "answered" ELSE client[c]]
  /\ answer' = [c \in Commands |-> IF c \in batch[n] THEN Position(result[n], c) ELSE answer[c]]
  /\ Done(n)
  /\ act' = [name |-> "Reply", node |-> n, batch |-> batch[n]]
  /\ UNCHANGED <<log, inbox, up, read, result, wire, crashes, lost, throttled>>

(* The node runs out of retries and fails its batch; its clients send again. *)
GiveUp(n) ==
  /\ node[n] = "reading"
  /\ client' = [c \in Commands |-> IF c \in batch[n] THEN "idle" ELSE client[c]]
  /\ Done(n)
  /\ act' = [name |-> "GiveUp", node |-> n, batch |-> batch[n]]
  /\ UNCHANGED <<log, answer, inbox, up, read, result, wire, crashes, lost, throttled>>

(* A node crashes: it takes nothing more, and the clients still in its inbox see a failure and send again. *)
Crash(n) ==
  /\ up[n]
  /\ crashes < MaxCrashes
  /\ up' = [up EXCEPT ![n] = FALSE]
  /\ client' = [c \in Commands |-> IF c \in inbox[n] THEN "idle" ELSE client[c]]
  /\ inbox' = [inbox EXCEPT ![n] = {}]
  /\ crashes' = crashes + 1
  /\ act' = [name |-> "Crash", node |-> n, drained |-> inbox[n]]
  /\ UNCHANGED <<log, answer, node, batch, read, result, wire, lost, throttled>>

(* A crashed node's batch is abandoned at any point before it is answered; its clients send again. Its append, if *)
(* on the wire, is still delivered.                                                                             *)
Abandon(n) ==
  /\ ~up[n]
  /\ node[n] # "idle"
  /\ client' = [c \in Commands |-> IF c \in batch[n] THEN "idle" ELSE client[c]]
  /\ Done(n)
  /\ act' = [name |-> "Abandon", node |-> n, batch |-> batch[n]]
  /\ UNCHANGED <<log, answer, inbox, up, read, result, wire, crashes, lost, throttled>>

Next ==
  \/ \E c \in Commands, n \in Nodes : Send(c, n)
  \/ \E n \in Nodes : Take(n) \/ Read(n) \/ Reply(n) \/ GiveUp(n) \/ Crash(n) \/ Abandon(n)
  \/ \E m \in wire : Deliver(m) \/ Throttle(m)

Spec == Init /\ [][Next]_vars

(* The state without `act`: model checking need not tell apart states reached by different last steps. *)
View == <<log, client, answer, inbox, up, node, batch, read, result, wire, crashes, lost, throttled>>

(* Every name `act` can take, for the vocabulary check against the code's recorder. *)
Vocabulary == {"Send", "Take", "Read", "Deliver", "Throttle", "Reply", "GiveUp", "Crash", "Abandon"}

-----------------------------------------------------------------------------
(* No command is ever applied twice. *)
AtMostOnce == \A i, j \in 1..Len(log) : i # j => log[i] # log[j]

(* An answer is the truth: the command is in the log, at the position given. *)
AnswersTruthful == \A c \in Commands : client[c] = "answered" => answer[c] \in 1..Len(log) /\ log[answer[c]] = c

Safety == AtMostOnce /\ AnswersTruthful

(* Keeps the unfenced variants finite: once a command is applied twice the invariant is violated anyway. *)
LogBound == Len(log) <= Cardinality(Commands) + 1
=============================================================================
