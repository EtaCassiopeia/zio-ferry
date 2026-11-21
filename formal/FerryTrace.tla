----------------------------- MODULE FerryTrace -----------------------------
(***************************************************************************)
(* Trace validation for Ferry: is a recorded run of the code a behaviour   *)
(* of the model?                                                           *)
(*                                                                         *)
(* The code records every protocol step it takes (ferry.Protocol.Action),  *)
(* inside the STM transaction that takes it, so a trace is in the order    *)
(* the steps took effect. Each event is a record with the same shape as    *)
(* the model's `act`. A trace module (written by the tests) extends this   *)
(* one, defines `TraceLog` and the instance, and maps the constants onto   *)
(* them.                                                                   *)
(*                                                                         *)
(* Each step of the trace spec is one step of Ferry's Next whose `act` is  *)
(* the next event: the model computes every other variable itself, so an   *)
(* event the model cannot take from the state it reached, or a state the   *)
(* code reached differently, leaves no successor. TLC checks `Unfinished`  *)
(* as an invariant: its violation is the verdict "accepted", since it      *)
(* means the end of the trace was reached.                                 *)
(***************************************************************************)
EXTENDS Ferry, TLC

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
