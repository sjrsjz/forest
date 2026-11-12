import Forest.Basic

namespace Forest.Mutex

/-- A local task identity. Each Wasm generation has its own State and lock table. -/
abbrev Owner := Nat

structure Claim where
  owner : Owner
  operation : Nat
  deriving DecidableEq, Repr

inductive Lease where
  | granted (claim : Claim)
  | held (owner : Owner)
  deriving DecidableEq, Repr

/-- One arbitrary key in one Wasm instance. Keys and instances are independent. -/
structure State where
  lease : Option Lease
  waiting : List Claim
  closed : Owner → Bool
  native : Owner → Nat

def initial : State := ⟨none, [], fun _ => false, fun _ => 0⟩

def live (s : State) (owner : Owner) : Prop := s.closed owner = false

def Safe (s : State) : Prop :=
  (∀ claim, s.lease = some (.granted claim) → live s claim.owner) ∧
  (∀ claim ∈ s.waiting, live s claim.owner)

def cancel (s : State) (claim : Claim) : State :=
  { s with
    lease := if s.lease = some (.granted claim) then none else s.lease
    waiting := s.waiting.filter (fun other => other != claim) }

/-- Stop one task or all tasks of an instance. Undelivered grants are cancelled;
held permits survive until submitted non-cancellable native work has ended. -/
def stop (s : State) (retiring : Owner → Bool) : State :=
  { s with
    lease := match s.lease with
      | some (.granted claim) => if retiring claim.owner then none else s.lease
      | other => other
    waiting := s.waiting.filter (fun claim => !retiring claim.owner)
    closed := fun owner => retiring owner || s.closed owner }

inductive Event where
  | request (claim : Claim) | grant (claim : Claim) | deliver (claim : Claim)
  | cancel (claim : Claim) | unlock (owner : Owner)
  | submitNative (owner : Owner) | finishNative (owner : Owner)
  | stop (retiring : Owner → Bool) | reclaim (owner : Owner)

inductive Step : State → Event → State → Prop
  | request (s : State) (claim : Claim) (active : live s claim.owner) :
      Step s (.request claim) { s with waiting := s.waiting ++ [claim] }
  | grant (s : State) (claim : Claim) (tail : List Claim)
      (idle : s.lease = none) (head : s.waiting = claim :: tail) :
      Step s (.grant claim) { s with lease := some (.granted claim), waiting := tail }
  | deliver (s : State) (claim : Claim) (active : live s claim.owner)
      (grant : s.lease = some (.granted claim)) :
      Step s (.deliver claim) { s with lease := some (.held claim.owner) }
  | cancel (s : State) (claim : Claim) : Step s (.cancel claim) (cancel s claim)
  | unlock (s : State) (owner : Owner) (active : live s owner)
      (held : s.lease = some (.held owner)) :
      Step s (.unlock owner) { s with lease := none }
  | submitNative (s : State) (owner : Owner) (active : live s owner) :
      Step s (.submitNative owner)
        { s with native := fun other => if other = owner then s.native other + 1 else s.native other }
  | finishNative (s : State) (owner : Owner) (pending : 0 < s.native owner) :
      Step s (.finishNative owner)
        { s with native := fun other => if other = owner then s.native other - 1 else s.native other }
  | stop (s : State) (retiring : Owner → Bool) : Step s (.stop retiring) (stop s retiring)
  | reclaim (s : State) (owner : Owner) (closed : s.closed owner = true)
      (finished : s.native owner = 0) :
      Step s (.reclaim owner)
        { s with lease := if s.lease = some (.held owner) then none else s.lease }

theorem initial_safe : Safe initial := by simp [Safe, initial]

theorem step_preserves {s s' : State} {event : Event} (safe : Safe s)
    (transition : Step s event s') : Safe s' := by
  cases transition with
  | request claim active =>
      refine ⟨safe.1, ?_⟩
      intro other member
      rcases List.mem_append.mp member with before | last
      · exact safe.2 other before
      · simpa using (List.mem_singleton.mp last ▸ active)
  | grant claim tail _ head =>
      refine ⟨?_, ?_⟩
      · intro other held
        cases Option.some.inj held
        exact safe.2 claim (by simp [head])
      · intro other member
        exact safe.2 other (by simp [head, member])
  | deliver _ _ _ | unlock _ _ _ =>
      exact ⟨by simp, safe.2⟩
  | cancel claim =>
      refine ⟨?_, ?_⟩
      · intro other held
        dsimp [cancel] at held
        split at held
        · contradiction
        · exact safe.1 other held
      · intro other member
        exact safe.2 other (List.mem_filter.mp member).1
  | submitNative _ _ | finishNative _ _ => exact safe
  | stop retiring =>
      refine ⟨?_, ?_⟩
      · intro claim held
        cases lease : s.lease with
        | none => simp [stop, lease] at held
        | some leaseValue =>
            cases leaseValue with
            | held owner => simp [stop, lease] at held
            | granted other =>
                by_cases retired : retiring other.owner = true
                · simp [stop, lease, retired] at held
                · have active : retiring other.owner = false := by simpa using retired
                  have same : other = claim := by simpa [stop, lease, active] using held
                  subst other
                  change (retiring claim.owner || s.closed claim.owner) = false
                  simp only [active, Bool.false_or]
                  exact safe.1 claim lease
      · intro claim member
        have filtered := List.mem_filter.mp member
        have active : retiring claim.owner = false := by simpa using filtered.2
        change (retiring claim.owner || s.closed claim.owner) = false
        simp only [active, Bool.false_or]
        exact safe.2 claim filtered.1
  | reclaim owner _ _ =>
      refine ⟨?_, safe.2⟩
      intro claim held
      split at held
      · contradiction
      · exact safe.1 claim held

theorem reachable_safe {s : State} {events : List Event}
    (run : Trace Step initial events s) : Safe s :=
  Forest.Trace.invariant (P := Safe) (fun hs step => step_preserves hs step) run initial_safe

theorem lease_unique {s : State} {a b : Lease}
    (first : s.lease = some a) (second : s.lease = some b) : a = b :=
  Option.some.inj (first.symm.trans second)

theorem grant_is_fifo {s s' : State} {claim : Claim} (step : Step s (.grant claim) s') :
    s.waiting = claim :: s'.waiting := by cases step; assumption

theorem cancel_removes_undelivered (s : State) (claim : Claim) :
    (cancel s claim).lease ≠ some (.granted claim) ∧ claim ∉ (cancel s claim).waiting := by
  constructor
  · simp [cancel]
  · simp [cancel]

theorem cancel_keeps_delivered {s : State} (claim : Claim) (owner : Owner)
    (held : s.lease = some (.held owner)) :
    (cancel s claim).lease = some (.held owner) := by simp [cancel, held]

theorem other_task_cannot_unlock {s s' : State} {holder caller : Owner}
    (held : s.lease = some (.held holder)) (different : caller ≠ holder) :
    ¬ Step s (.unlock caller) s' := by
  intro transition
  cases transition with
  | unlock _ _ claimed =>
      have same := Option.some.inj (claimed.symm.trans held)
      exact different (Lease.held.inj same)

theorem stop_instance_blocks_all (s : State) (owner : Owner) :
    ¬ live (stop s (fun _ => true)) owner := by
  simp [live, stop]

/-- Updating one instance cannot change another instance's lock or waiters. -/
def InstanceStep (before : Nat → State) (generation : Nat) (event : Event)
    (after : Nat → State) : Prop :=
  ∃ next, Step (before generation) event next ∧ after = update before generation next

theorem instance_step_is_local {before after : Nat → State} {generation other : Nat}
    {event : Event} (different : other ≠ generation)
    (step : InstanceStep before generation event after) : after other = before other := by
  rcases step with ⟨next, _, changed⟩
  subst after
  exact update_other before generation other next different

theorem stopped_cannot_deliver {s s' : State} {claim : Claim}
    (closed : s.closed claim.owner = true) : ¬ Step s (.deliver claim) s' := by
  intro transition
  cases transition with
  | deliver _ active _ => simp [live, closed] at active

/-- Once stopped, a held lock cannot be reclaimed while native writes remain.
This concerns host cleanup; the module may explicitly unlock while still live. -/
theorem cleanup_keeps_held_until_native_finishes {s s' : State} {event : Event} {owner : Owner}
    (held : s.lease = some (.held owner)) (closed : s.closed owner = true)
    (pending : 0 < s.native owner) (step : Step s event s') :
    s'.lease = some (.held owner) := by
  cases step with
  | request _ _ | submitNative _ _ | finishNative _ _ => exact held
  | grant _ _ idle _ => simp [held] at idle
  | deliver claim _ grant => simp [held] at grant
  | cancel claim => exact cancel_keeps_delivered claim owner held
  | unlock other active claimed =>
      have same := Lease.held.inj (Option.some.inj (claimed.symm.trans held))
      subst other
      simp [live, closed] at active
  | stop retiring => simp [stop, held]
  | reclaim other _ finished =>
      have different : other ≠ owner := by
        intro same
        subst other
        omega
      have differentLease : s.lease ≠ some (.held other) := by
        simpa [held] using Ne.symm different
      simp only [differentLease, if_false]
      exact held

end Forest.Mutex
