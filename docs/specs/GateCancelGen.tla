------------------------------ MODULE GateCancelGen ------------------------------
(***************************************************************************)
(* Model of generation-aware request cancellation (PR #307).               *)
(*                                                                         *)
(* A request id can be submitted more than once (a retry / resubmission,   *)
(* or an at-least-once redelivery), so the SAME id names several           *)
(* "generations". #307 distinguishes them with a per-submission random     *)
(* token and two Redis keys:                                               *)
(*                                                                         *)
(*   request-active:<id>   the token of the currently-active generation    *)
(*   request-cancel:<id>   the token that has been cancelled               *)
(*                                                                         *)
(* The protocol (each op is atomic -- a Lua script or a MULTI/EXEC):       *)
(*                                                                         *)
(*   Submit(g)   DEL cancel; SET active := token(g); enqueue               *)
(*               (producer/redis_sortedset_producer.go SubmitRequest)      *)
(*   Cancel      active := GET active; if active != nil: SET cancel:=active *)
(*               (markRequestCancelledScript -- captures the CURRENT gen)   *)
(*   IsCancelled(g)  return GET cancel == token(g)                         *)
(*               (redisCancellationChecker.IsCancelled -- token compare)    *)
(*   Cleanup(g)  if GET active==token(g): DEL active;                       *)
(*               if GET cancel==token(g): DEL cancel                        *)
(*               (cleanupRequestStateScript -- TOKEN-GUARDED delete)        *)
(*                                                                         *)
(* The subtle part is the interaction of a stale generation's deferred     *)
(* Cleanup with a newer generation: cleanup runs after the result flush,   *)
(* which can be AFTER the id has been resubmitted. The token guard on both  *)
(* IsCancelled and Cleanup is what makes this safe. Flip `TokenGuarded`:    *)
(*                                                                         *)
(*   TRUE  -> #307: IsCancelled compares tokens; Cleanup deletes only its   *)
(*            own generation's keys.                                        *)
(*   FALSE -> an id-only design (no token discrimination): IsCancelled is    *)
(*            "a cancel marker exists"; Cleanup deletes the keys            *)
(*            unconditionally.                                             *)
(*                                                                         *)
(* Properties (see the .cfg files):                                        *)
(*   NoSpuriousCancel (safety)  a generation is reported cancelled only if  *)
(*                              the user asked to cancel THAT generation.   *)
(*   NoLostCancel     (safety)  a generation the user cancelled while it     *)
(*                              was still queued is never dispatched.       *)
(*   NoKeyLeak        (liveness) both keys are eventually cleared.          *)
(*                                                                         *)
(* TokenGuarded = TRUE  -> all hold.                                        *)
(* TokenGuarded = FALSE -> both safety properties fail. The ABA: after the   *)
(*   id is resubmitted as gen 2 and gen 2 is cancelled, gen 1's deferred     *)
(*   unconditional Cleanup clobbers gen 2's live keys -- it DELs the cancel   *)
(*   marker (or the active token) that belongs to gen 2 -- so gen 2 then     *)
(*   dispatches despite being cancelled (NoLostCancel). And with no token     *)
(*   compare, gen 1 matches a cancel marker meant for gen 2 and is itself    *)
(*   dropped (NoSpuriousCancel). The token guard on IsCancelled and Cleanup  *)
(*   closes both.                                                            *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    MaxGen,       \* number of times the one request id is (re)submitted, e.g. 2
    TokenGuarded  \* TRUE = #307 token-guarded design; FALSE = id-only design (bug)

\* Generations of the single modeled request id. token(g) == g; 0 means "key absent".
Gens == 1..MaxGen

(* --algorithm GateCancelGen
variables
    activeTok      = 0,                        \* request-active:<id> (0 = absent)
    cancelTok      = 0,                        \* request-cancel:<id> (0 = absent)
    submittedCount = 0,                        \* generations submitted so far
    gstate         = [g \in Gens |-> "none"],  \* none|queued|dispatched|completed|cleaned
    reported       = [g \in Gens |-> "none"],  \* worker verdict: none|cancelled|dispatched
    intendedCancel = {},                       \* generations the user cancelled while queued
    cancelFired    = FALSE;                    \* the one modeled Cancel has fired

define
    TypeOK ==
        /\ activeTok \in 0..MaxGen
        /\ cancelTok \in 0..MaxGen
        /\ submittedCount \in 0..MaxGen
        /\ gstate   \in [Gens -> {"none","queued","dispatched","completed","cleaned"}]
        /\ reported \in [Gens -> {"none","cancelled","dispatched"}]
        /\ intendedCancel \subseteq Gens
        /\ cancelFired \in BOOLEAN

    \* SAFETY: no generation is cancelled unless the user targeted THAT generation.
    NoSpuriousCancel ==
        \A g \in Gens : reported[g] = "cancelled" => g \in intendedCancel

    \* SAFETY: a generation cancelled while still queued is never dispatched.
    NoLostCancel ==
        \A g \in Gens :
            (g \in intendedCancel /\ reported[g] # "none") => reported[g] = "cancelled"

    \* LIVENESS: the per-request keys do not leak; they are eventually cleared.
    NoKeyLeak == <>[](activeTok = 0 /\ cancelTok = 0)
end define;

\* The producer (re)submits the id. Each submission clears any stale cancel marker
\* and stamps its own generation as active -- SubmitRequest's TxPipeline.
fair process submitter = "submitter"
begin
  Submit:
    while submittedCount < MaxGen do
      with g = submittedCount + 1 do
        submittedCount := g          ||
        cancelTok      := 0          ||
        activeTok      := g          ||
        gstate         := [gstate EXCEPT ![g] = "queued"];
      end with;
    end while;
end process;

\* The flow worker: authoritative pre-dispatch cancellation check, then dispatch.
fair process worker = "worker"
begin
  Work:
    while TRUE do
      with g \in {gg \in Gens : gstate[gg] = "queued"} do
        if (IF TokenGuarded THEN cancelTok = g ELSE cancelTok # 0) then
          reported := [reported EXCEPT ![g] = "cancelled"] ||
          gstate   := [gstate   EXCEPT ![g] = "completed"];   \* cancelled result emitted
        else
          reported := [reported EXCEPT ![g] = "dispatched"] ||
          gstate   := [gstate   EXCEPT ![g] = "dispatched"];
        end if;
      end with;
    end while;
end process;

\* A dispatched request eventually finishes.
fair process completer = "completer"
begin
  Complete:
    while TRUE do
      with g \in {gg \in Gens : gstate[gg] = "dispatched"} do
        gstate := [gstate EXCEPT ![g] = "completed"];
      end with;
    end while;
end process;

\* Deferred cleanup after the result flush. This is the ABA window: it can run after
\* the id has already been resubmitted as a newer generation.
fair process cleaner = "cleaner"
begin
  Clean:
    while TRUE do
      with g \in {gg \in Gens : gstate[gg] = "completed"} do
        if TokenGuarded then
          activeTok := IF activeTok = g THEN 0 ELSE activeTok ||
          cancelTok := IF cancelTok = g THEN 0 ELSE cancelTok ||
          gstate    := [gstate EXCEPT ![g] = "cleaned"];
        else
          activeTok := 0 ||
          cancelTok := 0 ||
          gstate    := [gstate EXCEPT ![g] = "cleaned"];
        end if;
      end with;
    end while;
end process;

\* The producer cancels the request id once, targeting the CURRENT (latest) generation
\* while it is still queued. The mark script captures whatever token is active NOW.
\* We fire only on the FINAL generation (submittedCount = MaxGen): this models
\* "cancel the current request, with no later resubmission". A resubmit AFTER a cancel
\* legitimately revives the id (Submit DELs the cancel marker) -- that is a different
\* scenario, not a lost cancel, so it is out of scope for NoLostCancel.
fair process canceller = "canceller"
begin
  Cancel:
    await ~cancelFired /\ submittedCount = MaxGen /\ gstate[submittedCount] = "queued";
    intendedCancel := intendedCancel \cup {submittedCount} ||
    cancelFired    := TRUE ||
    cancelTok      := IF activeTok # 0 THEN activeTok ELSE cancelTok;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "bb1f26be" /\ chksum(tla) = "943e13e1")
VARIABLES activeTok, cancelTok, submittedCount, gstate, reported, 
          intendedCancel, cancelFired, pc

(* define statement *)
TypeOK ==
    /\ activeTok \in 0..MaxGen
    /\ cancelTok \in 0..MaxGen
    /\ submittedCount \in 0..MaxGen
    /\ gstate   \in [Gens -> {"none","queued","dispatched","completed","cleaned"}]
    /\ reported \in [Gens -> {"none","cancelled","dispatched"}]
    /\ intendedCancel \subseteq Gens
    /\ cancelFired \in BOOLEAN


NoSpuriousCancel ==
    \A g \in Gens : reported[g] = "cancelled" => g \in intendedCancel


NoLostCancel ==
    \A g \in Gens :
        (g \in intendedCancel /\ reported[g] # "none") => reported[g] = "cancelled"


NoKeyLeak == <>[](activeTok = 0 /\ cancelTok = 0)


vars == << activeTok, cancelTok, submittedCount, gstate, reported, 
           intendedCancel, cancelFired, pc >>

ProcSet == {"submitter"} \cup {"worker"} \cup {"completer"} \cup {"cleaner"} \cup {"canceller"}

Init == (* Global variables *)
        /\ activeTok = 0
        /\ cancelTok = 0
        /\ submittedCount = 0
        /\ gstate = [g \in Gens |-> "none"]
        /\ reported = [g \in Gens |-> "none"]
        /\ intendedCancel = {}
        /\ cancelFired = FALSE
        /\ pc = [self \in ProcSet |-> CASE self = "submitter" -> "Submit"
                                        [] self = "worker" -> "Work"
                                        [] self = "completer" -> "Complete"
                                        [] self = "cleaner" -> "Clean"
                                        [] self = "canceller" -> "Cancel"]

Submit == /\ pc["submitter"] = "Submit"
          /\ IF submittedCount < MaxGen
                THEN /\ LET g == submittedCount + 1 IN
                          /\ activeTok' = g
                          /\ cancelTok' = 0
                          /\ gstate' = [gstate EXCEPT ![g] = "queued"]
                          /\ submittedCount' = g
                     /\ pc' = [pc EXCEPT !["submitter"] = "Submit"]
                ELSE /\ pc' = [pc EXCEPT !["submitter"] = "Done"]
                     /\ UNCHANGED << activeTok, cancelTok, submittedCount, 
                                     gstate >>
          /\ UNCHANGED << reported, intendedCancel, cancelFired >>

submitter == Submit

Work == /\ pc["worker"] = "Work"
        /\ \E g \in {gg \in Gens : gstate[gg] = "queued"}:
             IF (IF TokenGuarded THEN cancelTok = g ELSE cancelTok # 0)
                THEN /\ /\ gstate' = [gstate   EXCEPT ![g] = "completed"]
                        /\ reported' = [reported EXCEPT ![g] = "cancelled"]
                ELSE /\ /\ gstate' = [gstate   EXCEPT ![g] = "dispatched"]
                        /\ reported' = [reported EXCEPT ![g] = "dispatched"]
        /\ pc' = [pc EXCEPT !["worker"] = "Work"]
        /\ UNCHANGED << activeTok, cancelTok, submittedCount, intendedCancel, 
                        cancelFired >>

worker == Work

Complete == /\ pc["completer"] = "Complete"
            /\ \E g \in {gg \in Gens : gstate[gg] = "dispatched"}:
                 gstate' = [gstate EXCEPT ![g] = "completed"]
            /\ pc' = [pc EXCEPT !["completer"] = "Complete"]
            /\ UNCHANGED << activeTok, cancelTok, submittedCount, reported, 
                            intendedCancel, cancelFired >>

completer == Complete

Clean == /\ pc["cleaner"] = "Clean"
         /\ \E g \in {gg \in Gens : gstate[gg] = "completed"}:
              IF TokenGuarded
                 THEN /\ /\ activeTok' = (IF activeTok = g THEN 0 ELSE activeTok)
                         /\ cancelTok' = (IF cancelTok = g THEN 0 ELSE cancelTok)
                         /\ gstate' = [gstate EXCEPT ![g] = "cleaned"]
                 ELSE /\ /\ activeTok' = 0
                         /\ cancelTok' = 0
                         /\ gstate' = [gstate EXCEPT ![g] = "cleaned"]
         /\ pc' = [pc EXCEPT !["cleaner"] = "Clean"]
         /\ UNCHANGED << submittedCount, reported, intendedCancel, cancelFired >>

cleaner == Clean

Cancel == /\ pc["canceller"] = "Cancel"
          /\ ~cancelFired /\ submittedCount = MaxGen /\ gstate[submittedCount] = "queued"
          /\ /\ cancelFired' = TRUE
             /\ cancelTok' = (IF activeTok # 0 THEN activeTok ELSE cancelTok)
             /\ intendedCancel' = (intendedCancel \cup {submittedCount})
          /\ pc' = [pc EXCEPT !["canceller"] = "Done"]
          /\ UNCHANGED << activeTok, submittedCount, gstate, reported >>

canceller == Cancel

Next == submitter \/ worker \/ completer \/ cleaner \/ canceller

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(submitter)
        /\ WF_vars(worker)
        /\ WF_vars(completer)
        /\ WF_vars(cleaner)
        /\ WF_vars(canceller)

\* END TRANSLATION 
=============================================================================
