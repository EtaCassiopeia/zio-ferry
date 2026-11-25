------------------------ MODULE ProjectedWriterTrace ------------------------
(***************************************************************************)
(* Trace validation for the projected writer: is a recorded run of        *)
(* ProjectedWriterSimulation, projected on one key, a behaviour of the    *)
(* model?                                                                  *)
(*                                                                         *)
(* The simulation records Ferry.perKey's actions and the store's reads,   *)
(* writes and deliveries in one STM log, so a trace is in the order the   *)
(* steps took effect. Each event has the shape of the model's `act`. As   *)
(* in FerryTrace.tla, each step is one step of Next whose `act` is the    *)
(* next event, and the violation of `Unfinished` is the verdict           *)
(* "accepted".                                                             *)
(***************************************************************************)
EXTENDS ProjectedWriter, TLC

CONSTANT Trace

VARIABLE i

TraceInit == Init /\ i = 0

TraceNext ==
  /\ i < Len(Trace)
  /\ Next
  /\ act' = Trace[i + 1]
  /\ i' = i + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<vars, i>>

Unfinished == i < Len(Trace)
=============================================================================
