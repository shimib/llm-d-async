------------------------------ MODULE MsgLifecycle ------------------------------
(***************************************************************************)
(* Model of the Pub/Sub message-completion correlation in the Async        *)
(* Processor (pkg/pubsub/pubsubimpl.go).                                    *)
(*                                                                         *)
(* For every received message the per-message callback:                    *)
(*   1. stores a completion channel in a PROCESS-GLOBAL map keyed by the    *)
(*      Pub/Sub message id:  resultChannels.Store(msg.ID, ch)   (:494)      *)
(*   2. dispatches the request to a worker                                  *)
(*   3. blocks on a BARE channel receive:  result := <-resultsChannel (:554)*)
(*      (not guarded by ctx)                                                *)
(*   4. on return runs, UNCONDITIONALLY:  defer ...Delete(msg.ID)    (:495) *)
(*                                                                         *)
(* The result worker signals completion by loading the map and sending to   *)
(* WHATEVER channel is currently stored for that id (:350-356) -- not the   *)
(* specific delivery whose work just finished.                              *)
(*                                                                         *)
(* Pub/Sub is at-least-once: the SAME msg.ID can be delivered again while   *)
(* a prior delivery is still being processed (server-side ack-deadline       *)
(* expiry during a long pool-gate park, or the budget-change cancel() at     *)
(* :422-444 tearing down Receive while a callback is blocked on the bare     *)
(* receive). So msg.ID is NOT unique across overlapping deliveries, yet it   *)
(* is the correlation key.                                                  *)
(*                                                                         *)
(* The CONSTANT AtomicCorrelation switches between:                        *)
(*   FALSE -> the current design (map keyed by msg.ID, signal the current   *)
(*            map owner, unconditional delete).                             *)
(*   TRUE  -> a corrected design (each delivery has its own correlation;    *)
(*            the worker signals the delivery that produced the work, and    *)
(*            a callback only clears its own registration).                 *)
(*                                                                         *)
(* Properties:                                                             *)
(*   AckIntegrity   (safety)   a message is Ack'd only by a callback whose  *)
(*                             OWN dispatched work actually completed.       *)
(*   NoStuckCallback(liveness) every callback that blocks on its channel    *)
(*                             eventually unblocks (no leaked goroutine).    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    MaxDeliveries,      \* bound on redeliveries of the one modeled message
    AtomicCorrelation   \* FALSE = current design, TRUE = corrected design

Deliveries == 1..MaxDeliveries

(* --algorithm MsgLifecycle
variables
    acked         = FALSE,  \* the broker considers the message acknowledged (terminal)
    ackedBy       = 0,      \* which delivery performed the (first) Ack
    spawned       = 0,      \* how many deliveries of the message have been created
    regOwner      = 0,      \* delivery currently stored in resultChannels[msg.ID] (0 = empty)
    phase         = [d \in Deliveries |-> "none"],  \* none | await | done
    sig           = [d \in Deliveries |-> FALSE],   \* this delivery's channel received a value
    workDone      = [d \in Deliveries |-> FALSE],   \* the worker finished this delivery's dispatch
    dispatchCount = 0,      \* HTTP dispatches to the inference gateway (duplicates possible)
    resultCount   = 0;      \* results published to the result topic (duplicates possible)

define
    TypeOK ==
        /\ acked    \in BOOLEAN
        /\ ackedBy  \in 0..MaxDeliveries
        /\ spawned  \in 0..MaxDeliveries
        /\ regOwner \in 0..MaxDeliveries
        /\ phase    \in [Deliveries -> {"none", "await", "done"}]
        /\ sig      \in [Deliveries -> BOOLEAN]
        /\ workDone \in [Deliveries -> BOOLEAN]
        /\ dispatchCount \in 0..MaxDeliveries
        /\ resultCount   \in 0..MaxDeliveries

    \* SAFETY: the delivery credited with the Ack actually completed its own work.
    \* (If a callback is woken by another delivery's completion, it Acks the message
    \*  while its own dispatched request is still outstanding -> wrong result acked.)
    AckIntegrity == acked => (ackedBy \in Deliveries /\ workDone[ackedBy])

    \* LIVENESS: no callback blocks forever on its completion channel.
    NoStuckCallback == \A d \in Deliveries : (phase[d] = "await") ~> (phase[d] = "done")
end define;

\* The broker (Pub/Sub). At-least-once: it may (re)deliver the same message while a
\* prior delivery is still in flight, until the message is acknowledged. Not fair:
\* redelivery is permitted, not forced.
process broker = -1
begin
  Deliver:
    while spawned < MaxDeliveries /\ ~acked do
      \* callback: resultChannels.Store(msg.ID, ch) [overwrites], then hands the
      \* request to the worker pool (one HTTP dispatch), then blocks on its channel.
      spawned       := spawned + 1       ||
      phase[spawned + 1] := "await"      ||
      regOwner      := spawned + 1       ||
      dispatchCount := dispatchCount + 1;
    end while;
end process;

\* The worker pool + result worker: finishes a dispatched request, publishes its
\* result, and signals the completion channel.
fair process worker = -2
begin
  Work:
    while TRUE do
      with d \in {dd \in Deliveries : phase[dd] = "await" /\ ~workDone[dd]} do
        workDone[d]   := TRUE              ||
        resultCount   := resultCount + 1   ||
        \* resultChannels.Load(msg.ID); ch <- true
        sig := IF AtomicCorrelation
                 THEN [sig EXCEPT ![d] = TRUE]                       \* corrected: signal the dispatching delivery
                 ELSE IF regOwner = 0
                        THEN sig                                     \* "not found" -> no signal (leak)
                        ELSE [sig EXCEPT ![regOwner] = TRUE];        \* current: signal whoever owns the map now
      end with;
    end while;
end process;

\* One callback per delivery. It blocks until its channel is signaled, then Acks the
\* message and runs its deferred map delete.
fair process callback \in Deliveries
begin
  Wake:
    await phase[self] = "await" /\ sig[self];
    acked        := TRUE                                    ||
    ackedBy      := IF acked THEN ackedBy ELSE self         ||  \* first Ack wins at the broker
    phase[self]  := "done"                                  ||
    sig[self]    := FALSE                                   ||
    regOwner     := IF AtomicCorrelation
                      THEN (IF regOwner = self THEN 0 ELSE regOwner)  \* corrected: clear only own entry
                      ELSE 0;                                          \* current: defer Delete(msg.ID) unconditional
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "185e1f81" /\ chksum(tla) = "b25c210a")
VARIABLES acked, ackedBy, spawned, regOwner, phase, sig, workDone, 
          dispatchCount, resultCount, pc

(* define statement *)
TypeOK ==
    /\ acked    \in BOOLEAN
    /\ ackedBy  \in 0..MaxDeliveries
    /\ spawned  \in 0..MaxDeliveries
    /\ regOwner \in 0..MaxDeliveries
    /\ phase    \in [Deliveries -> {"none", "await", "done"}]
    /\ sig      \in [Deliveries -> BOOLEAN]
    /\ workDone \in [Deliveries -> BOOLEAN]
    /\ dispatchCount \in 0..MaxDeliveries
    /\ resultCount   \in 0..MaxDeliveries




AckIntegrity == acked => (ackedBy \in Deliveries /\ workDone[ackedBy])


NoStuckCallback == \A d \in Deliveries : (phase[d] = "await") ~> (phase[d] = "done")


vars == << acked, ackedBy, spawned, regOwner, phase, sig, workDone, 
           dispatchCount, resultCount, pc >>

ProcSet == {-1} \cup {-2} \cup (Deliveries)

Init == (* Global variables *)
        /\ acked = FALSE
        /\ ackedBy = 0
        /\ spawned = 0
        /\ regOwner = 0
        /\ phase = [d \in Deliveries |-> "none"]
        /\ sig = [d \in Deliveries |-> FALSE]
        /\ workDone = [d \in Deliveries |-> FALSE]
        /\ dispatchCount = 0
        /\ resultCount = 0
        /\ pc = [self \in ProcSet |-> CASE self = -1 -> "Deliver"
                                        [] self = -2 -> "Work"
                                        [] self \in Deliveries -> "Wake"]

Deliver == /\ pc[-1] = "Deliver"
           /\ IF spawned < MaxDeliveries /\ ~acked
                 THEN /\ /\ dispatchCount' = dispatchCount + 1
                         /\ phase' = [phase EXCEPT ![spawned + 1] = "await"]
                         /\ regOwner' = spawned + 1
                         /\ spawned' = spawned + 1
                      /\ pc' = [pc EXCEPT ![-1] = "Deliver"]
                 ELSE /\ pc' = [pc EXCEPT ![-1] = "Done"]
                      /\ UNCHANGED << spawned, regOwner, phase, dispatchCount >>
           /\ UNCHANGED << acked, ackedBy, sig, workDone, resultCount >>

broker == Deliver

Work == /\ pc[-2] = "Work"
        /\ \E d \in {dd \in Deliveries : phase[dd] = "await" /\ ~workDone[dd]}:
             /\ resultCount' = resultCount + 1
             /\ sig' = IF AtomicCorrelation
                         THEN [sig EXCEPT ![d] = TRUE]
                         ELSE IF regOwner = 0
                                THEN sig
                                ELSE [sig EXCEPT ![regOwner] = TRUE]
             /\ workDone' = [workDone EXCEPT ![d] = TRUE]
        /\ pc' = [pc EXCEPT ![-2] = "Work"]
        /\ UNCHANGED << acked, ackedBy, spawned, regOwner, phase, 
                        dispatchCount >>

worker == Work

Wake(self) == /\ pc[self] = "Wake"
              /\ phase[self] = "await" /\ sig[self]
              /\ /\ acked' = TRUE
                 /\ ackedBy' = IF acked THEN ackedBy ELSE self
                 /\ phase' = [phase EXCEPT ![self] = "done"]
                 /\ regOwner' = IF AtomicCorrelation
                                  THEN (IF regOwner = self THEN 0 ELSE regOwner)
                                  ELSE 0
                 /\ sig' = [sig EXCEPT ![self] = FALSE]
              /\ pc' = [pc EXCEPT ![self] = "Done"]
              /\ UNCHANGED << spawned, workDone, dispatchCount, resultCount >>

callback(self) == Wake(self)

Next == broker \/ worker
           \/ (\E self \in Deliveries: callback(self))

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(worker)
        /\ \A self \in Deliveries : WF_vars(callback(self))

\* END TRANSLATION 
=============================================================================
