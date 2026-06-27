-------------------------------- MODULE GateQuota --------------------------------
(***************************************************************************)
(* Formal model of the Async Processor's per-team concurrency quota gate  *)
(* (the `redis-quota` gate, mode=concurrency).                            *)
(*                                                                         *)
(* Workers admit a request by ACQUIRING a quota slot for the request's    *)
(* `team` attribute, dispatch it, then RELEASE the slot on completion.    *)
(* The shared `counter` models the Redis counter that the gate's Lua      *)
(* script atomically checks-and-increments. A worker may CRASH while      *)
(* holding a slot (the release never runs) — the slot is "orphaned" and   *)
(* only reclaimed later by Redis key TTL expiry (the `reaper`).           *)
(*                                                                         *)
(* This mirrors a real observation: after killing pods mid-flight, the    *)
(* quota counters were left over-counted until their TTL expired.         *)
(*                                                                         *)
(* Properties checked (see GateQuota.cfg):                                 *)
(*   NoOverAdmission     (safety)  counter[t] never exceeds the quota     *)
(*   ConcurrencyBounded  (safety)  real in-flight per team <= quota       *)
(*   AccountingConsistent(safety)  counter == in-flight + orphaned        *)
(*   NoPermanentLeak     (liveness) orphaned slots are eventually freed   *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Workers     \* set of worker identities, assigned as model values, e.g. {w1, w2, w3}

\* The per-team concurrency quota. Defined here (not as a model constant) because
\* TLC's .cfg cannot evaluate the :> / @@ function constructors. Edit these limits
\* to explore other tier shapes; Teams is just the domain of Limit.
Limit == ("standard" :> 2 @@ "batch" :> 1)
Teams == DOMAIN Limit

\* A distinguished value meaning "this worker holds no slot". Any value outside Teams.
NULL == "__idle__"

(* --algorithm GateQuota
variables
    counter   = [t \in Teams |-> 0],      \* Redis quota counter per team
    holder    = [w \in Workers |-> NULL], \* team a worker is holding (NULL = idle)
    orphan    = [t \in Teams |-> 0],      \* slots leaked by crashes, awaiting TTL
    crashable = [w \in Workers |-> TRUE]; \* a worker may crash at most once (bounds the model)

define
    RealInFlight(t) == Cardinality({w \in Workers : holder[w] = t})

    \* Well-formedness of the state.
    TypeOK ==
        /\ counter   \in [Teams -> 0..Cardinality(Workers)]
        /\ holder    \in [Workers -> Teams \cup {NULL}]
        /\ orphan    \in [Teams -> 0..Cardinality(Workers)]
        /\ crashable \in [Workers -> BOOLEAN]

    \* SAFETY: the atomic check-and-increment never over-admits.
    NoOverAdmission == \A t \in Teams : counter[t] <= Limit[t]

    \* SAFETY: the counter exactly equals live holders plus orphaned (leaked) slots —
    \* no phantom slots, no under-count.
    AccountingConsistent == \A t \in Teams : counter[t] = RealInFlight(t) + orphan[t]

    \* SAFETY (derived): actual concurrency per team is bounded by the quota.
    ConcurrencyBounded == \A t \in Teams : RealInFlight(t) <= Limit[t]

    \* LIVENESS: leaked slots do not pin capacity forever — TTL eventually reclaims
    \* them, so the system returns to (and stays at) zero orphans.
    NoPermanentLeak == <>[](\A t \in Teams : orphan[t] = 0)
end define;

\* A worker repeatedly acquires / releases (or crashes while holding).
fair process worker \in Workers
begin
  Step:
    while TRUE do
      either
        \* ACQUIRE — atomic, models the redis-quota Lua check-and-incr.
        with t \in Teams do
          if holder[self] = NULL /\ counter[t] < Limit[t] then
            counter[t]   := counter[t] + 1 ||
            holder[self] := t;
          end if;
        end with;
      or
        \* RELEASE on successful completion.
        if holder[self] # NULL then
          counter[holder[self]] := counter[holder[self]] - 1 ||
          holder[self]          := NULL;
        end if;
      or
        \* CRASH while holding: the deferred release never runs -> slot leaks.
        if holder[self] # NULL /\ crashable[self] then
          orphan[holder[self]] := orphan[holder[self]] + 1 ||
          holder[self]         := NULL ||
          crashable[self]      := FALSE;
        end if;
      end either;
    end while;
end process;

\* Redis key TTL: eventually reclaims orphaned slots. Disabled when none are orphaned,
\* so weak fairness forces it to fire whenever a leak exists.
fair process reaper = "reaper"
begin
  Reap:
    while TRUE do
      with t \in {tt \in Teams : orphan[tt] > 0} do
        counter[t] := counter[t] - 1 ||
        orphan[t]  := orphan[t] - 1;
      end with;
    end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "d44b8dfa" /\ chksum(tla) = "be038ef")
VARIABLES counter, holder, orphan, crashable

(* define statement *)
RealInFlight(t) == Cardinality({w \in Workers : holder[w] = t})


TypeOK ==
    /\ counter   \in [Teams -> 0..Cardinality(Workers)]
    /\ holder    \in [Workers -> Teams \cup {NULL}]
    /\ orphan    \in [Teams -> 0..Cardinality(Workers)]
    /\ crashable \in [Workers -> BOOLEAN]


NoOverAdmission == \A t \in Teams : counter[t] <= Limit[t]



AccountingConsistent == \A t \in Teams : counter[t] = RealInFlight(t) + orphan[t]


ConcurrencyBounded == \A t \in Teams : RealInFlight(t) <= Limit[t]



NoPermanentLeak == <>[](\A t \in Teams : orphan[t] = 0)


vars == << counter, holder, orphan, crashable >>

ProcSet == (Workers) \cup {"reaper"}

Init == (* Global variables *)
        /\ counter = [t \in Teams |-> 0]
        /\ holder = [w \in Workers |-> NULL]
        /\ orphan = [t \in Teams |-> 0]
        /\ crashable = [w \in Workers |-> TRUE]

worker(self) == \/ /\ \E t \in Teams:
                        IF holder[self] = NULL /\ counter[t] < Limit[t]
                           THEN /\ /\ counter' = [counter EXCEPT ![t] = counter[t] + 1]
                                   /\ holder' = [holder EXCEPT ![self] = t]
                           ELSE /\ TRUE
                                /\ UNCHANGED << counter, holder >>
                   /\ UNCHANGED <<orphan, crashable>>
                \/ /\ IF holder[self] # NULL
                         THEN /\ /\ counter' = [counter EXCEPT ![holder[self]] = counter[holder[self]] - 1]
                                 /\ holder' = [holder EXCEPT ![self] = NULL]
                         ELSE /\ TRUE
                              /\ UNCHANGED << counter, holder >>
                   /\ UNCHANGED <<orphan, crashable>>
                \/ /\ IF holder[self] # NULL /\ crashable[self]
                         THEN /\ /\ crashable' = [crashable EXCEPT ![self] = FALSE]
                                 /\ holder' = [holder EXCEPT ![self] = NULL]
                                 /\ orphan' = [orphan EXCEPT ![holder[self]] = orphan[holder[self]] + 1]
                         ELSE /\ TRUE
                              /\ UNCHANGED << holder, orphan, crashable >>
                   /\ UNCHANGED counter

reaper == /\ \E t \in {tt \in Teams : orphan[tt] > 0}:
               /\ counter' = [counter EXCEPT ![t] = counter[t] - 1]
               /\ orphan' = [orphan EXCEPT ![t] = orphan[t] - 1]
          /\ UNCHANGED << holder, crashable >>

Next == reaper
           \/ (\E self \in Workers: worker(self))

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Workers : WF_vars(worker(self))
        /\ WF_vars(reaper)

\* END TRANSLATION 
=============================================================================
