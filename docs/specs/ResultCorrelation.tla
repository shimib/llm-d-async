---------------------------- MODULE ResultCorrelation ----------------------------
(***************************************************************************)
(* Model of result delivery in the producer library                        *)
(* (producer/redis_sortedset_producer.go).                                 *)
(*                                                                         *)
(* A caller submits a request and later calls GetResult, which does        *)
(*                                                                         *)
(*     BRPOP <resultQueueName>            (:208)                           *)
(*                                                                         *)
(* on the producer's ONE shared `resultQueueName`. The async processor     *)
(* pushes each request's ResultMessage (tagged with the request id) onto   *)
(* that same shared list. BRPOP hands the caller WHATEVER result is at the  *)
(* end of the list -- there is no correlation between the caller and the    *)
(* result it pops. With N concurrent callers sharing one result queue, a    *)
(* caller can dequeue ANOTHER caller's result.                             *)
(*                                                                         *)
(* `api.RedisRequest` already carries a per-request `ResultQueueName`       *)
(* override; routing each request's result to its own list makes GetResult  *)
(* correlated. The CONSTANT PerRequestQueue switches between the two:       *)
(*                                                                         *)
(*   FALSE -> one shared result list (current producer default).           *)
(*   TRUE  -> one result list per request (the per-request override).       *)
(*                                                                         *)
(* Property (see the .cfg files):                                          *)
(*   EachCallerGetsOwnResult (safety)  a caller only ever receives the      *)
(*                                      result for the id it submitted.     *)
(*     FALSE -> TLC prints a trace where a caller pops another's result.    *)
(*     TRUE  -> "No error has been found."                                  *)
(*                                                                         *)
(* This is the correlation limitation behind issue #308 (an OpenAI-style    *)
(* synchronous gateway in front of the queues needs per-request results).   *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Callers,        \* set of concurrent callers, e.g. {c1, c2}. Each submits one request.
    PerRequestQueue \* TRUE = per-request result list (fix); FALSE = one shared list (bug)

\* The request id submitted by a caller. Each caller submits a distinct id equal to
\* the caller itself, so "caller got its own result" is exactly "popped id = caller".
\* 0 is the "nothing popped yet" sentinel (no caller equals 0).
Ids == Callers

\* The result-queue key a request id maps to: its own id when per-request, else the
\* single shared key (modeled as the sentinel 0, distinct from every caller/id).
QKeyOf(id) == IF PerRequestQueue THEN id ELSE 0
QKeys      == IF PerRequestQueue THEN Callers ELSE {0}

(* --algorithm ResultCorrelation
variables
    \* One result list per key. Producer appends result ids; callers BRPOP (pop tail).
    queue    = [k \in QKeys |-> <<>>],
    produced = [id \in Ids |-> FALSE],   \* the processor has pushed this id's result
    got      = [c \in Callers |-> 0];    \* the id a caller popped (0 = none yet)

define
    \* SAFETY: every caller that received a result received its OWN.
    EachCallerGetsOwnResult == \A c \in Callers : got[c] \in {0, c}

    TypeOK ==
        /\ produced \in [Ids -> BOOLEAN]
        /\ got \in [Callers -> Ids \cup {0}]
        /\ \A k \in QKeys : \A i \in 1..Len(queue[k]) : queue[k][i] \in Ids
end define;

\* The async processor: for each submitted request, push its result (tagged with the
\* request id) onto the request's result list. Every caller here submits, so we can
\* produce any not-yet-produced id at any time (models arbitrary completion order).
fair process producer = "processor"
begin
  Produce:
    while \E id \in Ids : ~produced[id] do
      with id \in {i \in Ids : ~produced[i]} do
        queue[QKeyOf(id)] := Append(queue[QKeyOf(id)], id) ||
        produced[id]      := TRUE;
      end with;
    end while;
end process;

\* Each caller does one blocking BRPOP on its result key and records what it got.
\* BRPOP returns the TAIL of the list (right pop). With the shared key, the tail may
\* be another caller's result.
fair process caller \in Callers
begin
  Recv:
    await queue[QKeyOf(self)] # <<>>;
    got[self]            := queue[QKeyOf(self)][Len(queue[QKeyOf(self)])] ||
    queue[QKeyOf(self)]  := SubSeq(queue[QKeyOf(self)], 1, Len(queue[QKeyOf(self)]) - 1);
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "e0d073a4" /\ chksum(tla) = "b6fd9996")
VARIABLES queue, produced, got, pc

(* define statement *)
EachCallerGetsOwnResult == \A c \in Callers : got[c] \in {0, c}

TypeOK ==
    /\ produced \in [Ids -> BOOLEAN]
    /\ got \in [Callers -> Ids \cup {0}]
    /\ \A k \in QKeys : \A i \in 1..Len(queue[k]) : queue[k][i] \in Ids


vars == << queue, produced, got, pc >>

ProcSet == {"processor"} \cup (Callers)

Init == (* Global variables *)
        /\ queue = [k \in QKeys |-> <<>>]
        /\ produced = [id \in Ids |-> FALSE]
        /\ got = [c \in Callers |-> 0]
        /\ pc = [self \in ProcSet |-> CASE self = "processor" -> "Produce"
                                        [] self \in Callers -> "Recv"]

Produce == /\ pc["processor"] = "Produce"
           /\ IF \E id \in Ids : ~produced[id]
                 THEN /\ \E id \in {i \in Ids : ~produced[i]}:
                           /\ produced' = [produced EXCEPT ![id] = TRUE]
                           /\ queue' = [queue EXCEPT ![QKeyOf(id)] = Append(queue[QKeyOf(id)], id)]
                      /\ pc' = [pc EXCEPT !["processor"] = "Produce"]
                 ELSE /\ pc' = [pc EXCEPT !["processor"] = "Done"]
                      /\ UNCHANGED << queue, produced >>
           /\ got' = got

producer == Produce

Recv(self) == /\ pc[self] = "Recv"
              /\ queue[QKeyOf(self)] # <<>>
              /\ /\ got' = [got EXCEPT ![self] = queue[QKeyOf(self)][Len(queue[QKeyOf(self)])]]
                 /\ queue' = [queue EXCEPT ![QKeyOf(self)] = SubSeq(queue[QKeyOf(self)], 1, Len(queue[QKeyOf(self)]) - 1)]
              /\ pc' = [pc EXCEPT ![self] = "Done"]
              /\ UNCHANGED produced

caller(self) == Recv(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == producer
           \/ (\E self \in Callers: caller(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(producer)
        /\ \A self \in Callers : WF_vars(caller(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 
=============================================================================
