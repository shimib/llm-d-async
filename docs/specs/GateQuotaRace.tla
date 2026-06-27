------------------------------ MODULE GateQuotaRace ------------------------------
(***************************************************************************)
(* Counter-example companion to GateQuota.                                 *)
(*                                                                         *)
(* This is the SAME quota gate, but with a deliberately BROKEN acquire:    *)
(* the check (`counter[t] < Limit[t]`) and the increment happen in two     *)
(* separate atomic steps instead of one. This is what you get if the       *)
(* gate does GET then SET on Redis instead of a single atomic Lua         *)
(* check-and-increment (or an INCR with compare).                          *)
(*                                                                         *)
(* TLC finds a trace where two workers both pass the check before either   *)
(* increments, and NoOverAdmission is violated. Run:                       *)
(*                                                                         *)
(*   java -cp tla2tools.jar tlc2.TLC GateQuotaRace.tla                      *)
(*                                                                         *)
(* Expected result: "Invariant NoOverAdmission is violated" with a trace.  *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS Workers

Limit == ("standard" :> 1)   \* a single tier, limit 1, to make the race tiny
Teams == DOMAIN Limit
NULL  == "__idle__"

(* --algorithm GateQuotaRace
variables
    counter = [t \in Teams |-> 0],
    holder  = [w \in Workers |-> NULL],
    pend    = [w \in Workers |-> NULL];   \* team a worker has decided to admit, not yet counted

define
    RealInFlight(t) == Cardinality({w \in Workers : holder[w] = t})
    NoOverAdmission == \A t \in Teams : counter[t] <= Limit[t]
end define;

fair process worker \in Workers
begin
  Step:
    while TRUE do
      either
        \* CHECK (step 1 of a NON-atomic acquire): pass the limit test, remember the team.
        with t \in Teams do
          if holder[self] = NULL /\ pend[self] = NULL /\ counter[t] < Limit[t] then
            pend[self] := t;
          end if;
        end with;
      or
        \* COMMIT (step 2): increment and take the slot. Another worker may have
        \* already committed in between -> counter can exceed Limit.
        if pend[self] # NULL then
          counter[pend[self]] := counter[pend[self]] + 1 ||
          holder[self]        := pend[self] ||
          pend[self]          := NULL;
        end if;
      or
        \* RELEASE.
        if holder[self] # NULL then
          counter[holder[self]] := counter[holder[self]] - 1 ||
          holder[self]          := NULL;
        end if;
      end either;
    end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "195188ee" /\ chksum(tla) = "fc629649")
VARIABLES counter, holder, pend

(* define statement *)
RealInFlight(t) == Cardinality({w \in Workers : holder[w] = t})
NoOverAdmission == \A t \in Teams : counter[t] <= Limit[t]


vars == << counter, holder, pend >>

ProcSet == (Workers)

Init == (* Global variables *)
        /\ counter = [t \in Teams |-> 0]
        /\ holder = [w \in Workers |-> NULL]
        /\ pend = [w \in Workers |-> NULL]

worker(self) == \/ /\ \E t \in Teams:
                        IF holder[self] = NULL /\ pend[self] = NULL /\ counter[t] < Limit[t]
                           THEN /\ pend' = [pend EXCEPT ![self] = t]
                           ELSE /\ TRUE
                                /\ pend' = pend
                   /\ UNCHANGED <<counter, holder>>
                \/ /\ IF pend[self] # NULL
                         THEN /\ /\ counter' = [counter EXCEPT ![pend[self]] = counter[pend[self]] + 1]
                                 /\ holder' = [holder EXCEPT ![self] = pend[self]]
                                 /\ pend' = [pend EXCEPT ![self] = NULL]
                         ELSE /\ TRUE
                              /\ UNCHANGED << counter, holder, pend >>
                \/ /\ IF holder[self] # NULL
                         THEN /\ /\ counter' = [counter EXCEPT ![holder[self]] = counter[holder[self]] - 1]
                                 /\ holder' = [holder EXCEPT ![self] = NULL]
                         ELSE /\ TRUE
                              /\ UNCHANGED << counter, holder >>
                   /\ pend' = pend

Next == (\E self \in Workers: worker(self))

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Workers : WF_vars(worker(self))

\* END TRANSLATION 
==================================================================================
