---------------------------- MODULE GateReservationLeak ----------------------------
(***************************************************************************)
(* Model of the per-queue gate capacity-reservation lifecycle in the       *)
(* Redis sorted-set flow (pkg/redis/sortedset_impl.go + pkg/asyncworker).   *)
(*                                                                         *)
(* When the queue gate returns ActionContinue it reserves capacity (a      *)
(* concurrency slot: LocalConcurrencyGate `inFlight++`, or a redis-quota    *)
(* INCR) and stores the release closure in a per-id map:                    *)
(*                                                                         *)
(*     r.activeReleases.Store(rview.ReqID(), releases)   (:527)  <- OVERWRITES *)
(*                                                                         *)
(* The release runs in exactly ONE place -- when a ResultMessage for that   *)
(* id flows through resultWorker:                                          *)
(*                                                                         *)
(*     if val, ok := r.activeReleases.LoadAndDelete(m.ID); ok { ... }  (:648)  *)
(*                                                                         *)
(* But every RETRY path in the worker emits a RetryMessage and NO result    *)
(* (worker.go:67,129,169,183,282,427,491), so a retried request is          *)
(* re-enqueued WITHOUT releasing its reservation. Two consequences:         *)
(*                                                                         *)
(*   1. Leak: the reservation is held for a request that is no longer in    *)
(*      flight. `inFlight` only ever grows on the retry path.               *)
(*   2. Orphan: when the same id is re-dispatched, `Store` (:527) OVERWRITES *)
(*      the previous closure -- the earlier reservation becomes unreachable  *)
(*      and can never be released, even by a later result.                  *)
(*                                                                         *)
(* LocalConcurrencyGate has no TTL, and Budget()=0 once inFlight>=limit      *)
(* (local_concurrency_gate.go:59-61) -> batchSize floors to 0 -> the queue  *)
(* stops dispatching. So leaked slots wedge the queue permanently.          *)
(*                                                                         *)
(* Flip `ReleaseOnRetry`:                                                   *)
(*   FALSE -> current code: retry re-enqueues without releasing.            *)
(*   TRUE  -> the fix: the retry path releases the reservation (equivalently *)
(*            LoadAndDelete before re-Store, or carry the reservation).      *)
(*                                                                         *)
(* Properties (see the .cfg files):                                        *)
(*   AccountingExact (safety)   inFlight equals the number of ids that       *)
(*                              actually hold a live reservation.           *)
(*   EventuallyServed (liveness) every request is eventually completed.      *)
(*                                                                         *)
(* ReleaseOnRetry = TRUE  -> both hold.                                     *)
(* ReleaseOnRetry = FALSE ->                                                *)
(*   - AccountingExact violated (Limit>=2): a re-dispatch double-counts       *)
(*     inFlight while the id is reserved only once (the Store overwrite).    *)
(*   - EventuallyServed violated (Limit=1): a request that retries can never  *)
(*     be re-dispatched -- its own un-released reservation occupies the only  *)
(*     slot -- so it, and the whole queue, wedge forever.                    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Requests,       \* set of request ids, e.g. {r1, r2}
    Limit,          \* gate concurrency limit (LocalConcurrencyGate.limit)
    MaxAttempts,    \* bound on retries per request (keeps the model finite)
    ReleaseOnRetry  \* TRUE = fix (release on retry); FALSE = current code (leak)

(* --algorithm GateReservationLeak
variables
    inFlight = 0,                          \* the gate's concurrency counter
    reserved = [r \in Requests |-> FALSE], \* id currently holds a live release closure
    rstate   = [r \in Requests |-> "queued"]; \* queued | dispatched | done

define
    TypeOK ==
        /\ inFlight \in 0..(Limit + Cardinality(Requests))
        /\ reserved \in [Requests -> BOOLEAN]
        /\ rstate   \in [Requests -> {"queued","dispatched","done"}]

    \* SAFETY: the counter matches reality -- every counted slot maps to an id that
    \* actually holds a reservation. The Store-overwrite breaks this.
    AccountingExact == inFlight = Cardinality({r \in Requests : reserved[r]})

    \* LIVENESS: every request eventually completes (no permanent wedge).
    EventuallyServed == \A r \in Requests : <>(rstate[r] = "done")
end define;

fair process req \in Requests
variables att = 0;
begin
  Loop:
    while rstate[self] # "done" do
      either
        \* Dispatch: the gate admits only while inFlight < Limit (Apply passes and
        \* Budget() > 0). Reserve a slot and Store the release closure -- Store
        \* OVERWRITES any closure already held for this id (the orphan).
        await rstate[self] = "queued" /\ inFlight < Limit;
        inFlight       := inFlight + 1 ||
        reserved[self] := TRUE ||
        rstate[self]   := "dispatched";
      or
        \* Succeed: a ResultMessage flows back -> LoadAndDelete releases the slot.
        await rstate[self] = "dispatched";
        if reserved[self] then
          inFlight       := inFlight - 1 ||
          reserved[self] := FALSE ||
          rstate[self]   := "done";
        else
          rstate[self] := "done";
        end if;
      or
        \* Retry: re-enqueue. Current code (ReleaseOnRetry=FALSE) emits a
        \* RetryMessage and does NOT release the reservation.
        await rstate[self] = "dispatched" /\ att < MaxAttempts;
        att := att + 1;
        if ReleaseOnRetry then
          inFlight       := inFlight - 1 ||
          reserved[self] := FALSE ||
          rstate[self]   := "queued";
        else
          rstate[self] := "queued";
        end if;
      end either;
    end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "6dc9d5ac" /\ chksum(tla) = "e559f3e")
VARIABLES inFlight, reserved, rstate, pc

(* define statement *)
TypeOK ==
    /\ inFlight \in 0..(Limit + Cardinality(Requests))
    /\ reserved \in [Requests -> BOOLEAN]
    /\ rstate   \in [Requests -> {"queued","dispatched","done"}]



AccountingExact == inFlight = Cardinality({r \in Requests : reserved[r]})


EventuallyServed == \A r \in Requests : <>(rstate[r] = "done")

VARIABLE att

vars == << inFlight, reserved, rstate, pc, att >>

ProcSet == (Requests)

Init == (* Global variables *)
        /\ inFlight = 0
        /\ reserved = [r \in Requests |-> FALSE]
        /\ rstate = [r \in Requests |-> "queued"]
        (* Process req *)
        /\ att = [self \in Requests |-> 0]
        /\ pc = [self \in ProcSet |-> "Loop"]

Loop(self) == /\ pc[self] = "Loop"
              /\ IF rstate[self] # "done"
                    THEN /\ \/ /\ rstate[self] = "queued" /\ inFlight < Limit
                               /\ /\ inFlight' = inFlight + 1
                                  /\ reserved' = [reserved EXCEPT ![self] = TRUE]
                                  /\ rstate' = [rstate EXCEPT ![self] = "dispatched"]
                               /\ att' = att
                            \/ /\ rstate[self] = "dispatched"
                               /\ IF reserved[self]
                                     THEN /\ /\ inFlight' = inFlight - 1
                                             /\ reserved' = [reserved EXCEPT ![self] = FALSE]
                                             /\ rstate' = [rstate EXCEPT ![self] = "done"]
                                     ELSE /\ rstate' = [rstate EXCEPT ![self] = "done"]
                                          /\ UNCHANGED << inFlight, reserved >>
                               /\ att' = att
                            \/ /\ rstate[self] = "dispatched" /\ att[self] < MaxAttempts
                               /\ att' = [att EXCEPT ![self] = att[self] + 1]
                               /\ IF ReleaseOnRetry
                                     THEN /\ /\ inFlight' = inFlight - 1
                                             /\ reserved' = [reserved EXCEPT ![self] = FALSE]
                                             /\ rstate' = [rstate EXCEPT ![self] = "queued"]
                                     ELSE /\ rstate' = [rstate EXCEPT ![self] = "queued"]
                                          /\ UNCHANGED << inFlight, reserved >>
                         /\ pc' = [pc EXCEPT ![self] = "Loop"]
                    ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                         /\ UNCHANGED << inFlight, reserved, rstate, att >>

req(self) == Loop(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in Requests: req(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Requests : WF_vars(req(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 
=============================================================================
