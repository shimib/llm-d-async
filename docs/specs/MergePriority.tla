------------------------------- MODULE MergePriority -------------------------------
(***************************************************************************)
(* Model of the tier-priority merge policy's scheduler (PR #294,           *)
(* pkg/async/mergepolicy/tierpriority/tier_priority_policy.go).            *)
(*                                                                         *)
(* The scheduler holds requests in 6 priority buckets                      *)
(* (classification x tier, index = classPri*3 + tierPri) and its Pop()     *)
(* serves them in STRICT priority order:                                    *)
(*                                                                         *)
(*     for _, b := range s.buckets { if mm, ok := b.pop(); ok { ... } }    *)
(*                                                                         *)
(* i.e. it always drains a higher bucket completely before ever touching a  *)
(* lower one. Under sustained high-priority load a lower-priority lane is   *)
(* therefore never served -- starvation. (The same design also head-of-    *)
(* line blocks: each input channel has a single FIFO reader goroutine that  *)
(* blocks in Push() when its target bucket is full, so a high-priority      *)
(* message queued behind a low-priority one on the same channel cannot      *)
(* advance -- priority inversion. Both stem from unconditional strict       *)
(* ordering; this model checks the starvation half, which is the cleaner    *)
(* liveness statement.)                                                     *)
(*                                                                         *)
(* Abstracted to two priority classes -- a HIGH bucket under continuous     *)
(* load and one distinguished LOW request -- which is the minimal shape     *)
(* that exhibits starvation. Flip the `StrictPriority` constant:            *)
(*                                                                         *)
(*   TRUE  -> serve HIGH whenever any is buffered, else LOW (current Pop).  *)
(*   FALSE -> give the waiting LOW request its turn (stands in for any      *)
(*            starvation-free policy: aging, weighted, or round-robin).     *)
(*                                                                         *)
(* Property (see the .cfg files):                                          *)
(*   NoStarvation (liveness)  a buffered LOW request is eventually served.  *)
(*     TRUE  -> TLC finds a fair lasso: arrivals keep the HIGH bucket       *)
(*              non-empty, Pop() forever serves HIGH, LOW never served.     *)
(*     FALSE -> "No error has been found."                                  *)
(***************************************************************************)
EXTENDS Integers

CONSTANTS
    StrictPriority   \* TRUE = strict-priority Pop (bug); FALSE = starvation-free (fix)

\* HIGH-bucket capacity, in buffered requests. Any Cap >= 1 exhibits the starvation;
\* 2 makes the "arrivals refill before the bucket ever empties" lasso obvious. Fixed
\* here (not in the .cfg) to keep the model small and self-contained.
Cap == 2

(* --algorithm MergePriority
variables
    hi        = 0,       \* requests buffered in the HIGH bucket (0..Cap)
    loArrived = FALSE,   \* the one LOW request has been enqueued
    loWaiting = FALSE,   \* the LOW request is buffered, not yet served
    loServed  = FALSE;   \* the LOW request has been dispatched

define
    TypeOK ==
        /\ hi \in 0..Cap
        /\ loArrived \in BOOLEAN
        /\ loWaiting \in BOOLEAN
        /\ loServed  \in BOOLEAN

    \* LIVENESS: once the LOW request has arrived it is eventually served.
    NoStarvation == loArrived ~> loServed
end define;

\* Sustained high-priority load: keep refilling the HIGH bucket while it has room.
fair process arrivals = "arrivals"
begin
  Fill:
    while TRUE do
      await hi < Cap;
      hi := hi + 1;
    end while;
end process;

\* A single low-priority request arrives once.
fair process lowArrival = "low"
begin
  Arrive:
    await ~loArrived;
    loArrived := TRUE ||
    loWaiting := TRUE;
end process;

\* The merge reader: one Pop() per iteration.
fair process consumer = "consumer"
begin
  Consume:
    while TRUE do
      await hi > 0 \/ loWaiting;
      if StrictPriority then
        \* Current Pop(): scan buckets high -> low, serve the first non-empty.
        if hi > 0 then
          hi := hi - 1;
        elsif loWaiting then
          loWaiting := FALSE || loServed := TRUE;
        end if;
      else
        \* Starvation-free: the waiting LOW request gets its turn.
        if loWaiting then
          loWaiting := FALSE || loServed := TRUE;
        elsif hi > 0 then
          hi := hi - 1;
        end if;
      end if;
    end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "749b5a9" /\ chksum(tla) = "c0aa0869")
VARIABLES hi, loArrived, loWaiting, loServed, pc

(* define statement *)
TypeOK ==
    /\ hi \in 0..Cap
    /\ loArrived \in BOOLEAN
    /\ loWaiting \in BOOLEAN
    /\ loServed  \in BOOLEAN


NoStarvation == loArrived ~> loServed


vars == << hi, loArrived, loWaiting, loServed, pc >>

ProcSet == {"arrivals"} \cup {"low"} \cup {"consumer"}

Init == (* Global variables *)
        /\ hi = 0
        /\ loArrived = FALSE
        /\ loWaiting = FALSE
        /\ loServed = FALSE
        /\ pc = [self \in ProcSet |-> CASE self = "arrivals" -> "Fill"
                                        [] self = "low" -> "Arrive"
                                        [] self = "consumer" -> "Consume"]

Fill == /\ pc["arrivals"] = "Fill"
        /\ hi < Cap
        /\ hi' = hi + 1
        /\ pc' = [pc EXCEPT !["arrivals"] = "Fill"]
        /\ UNCHANGED << loArrived, loWaiting, loServed >>

arrivals == Fill

Arrive == /\ pc["low"] = "Arrive"
          /\ ~loArrived
          /\ /\ loArrived' = TRUE
             /\ loWaiting' = TRUE
          /\ pc' = [pc EXCEPT !["low"] = "Done"]
          /\ UNCHANGED << hi, loServed >>

lowArrival == Arrive

Consume == /\ pc["consumer"] = "Consume"
           /\ hi > 0 \/ loWaiting
           /\ IF StrictPriority
                 THEN /\ IF hi > 0
                            THEN /\ hi' = hi - 1
                                 /\ UNCHANGED << loWaiting, loServed >>
                            ELSE /\ IF loWaiting
                                       THEN /\ /\ loServed' = TRUE
                                               /\ loWaiting' = FALSE
                                       ELSE /\ TRUE
                                            /\ UNCHANGED << loWaiting, 
                                                            loServed >>
                                 /\ hi' = hi
                 ELSE /\ IF loWaiting
                            THEN /\ /\ loServed' = TRUE
                                    /\ loWaiting' = FALSE
                                 /\ hi' = hi
                            ELSE /\ IF hi > 0
                                       THEN /\ hi' = hi - 1
                                       ELSE /\ TRUE
                                            /\ hi' = hi
                                 /\ UNCHANGED << loWaiting, loServed >>
           /\ pc' = [pc EXCEPT !["consumer"] = "Consume"]
           /\ UNCHANGED loArrived

consumer == Consume

Next == arrivals \/ lowArrival \/ consumer

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(arrivals)
        /\ WF_vars(lowArrival)
        /\ WF_vars(consumer)

\* END TRANSLATION 
=============================================================================
