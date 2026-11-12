import Forest.Basic

namespace Forest.Abi

/-- A logical allocation occurrence, not a numeric Wasm address. Addresses may
be reused by the allocator. Length belongs to the same allocation capability. -/
structure Buffer where
  id : Nat
  length : Nat
  deriving DecidableEq, Repr

inductive Phase where
  | idle
  | allocated (buffer : Buffer)
  | written (buffer : Buffer)
  | invoked (buffer : Buffer)
  | poll
  | output (buffer : Buffer)
  | borrowing (buffer : Buffer) (epoch : Nat)
  | decoded (buffer : Buffer)
  | finished
  | closed
  deriving DecidableEq, Repr

def Phase.buffer : Phase → Option Buffer
  | .allocated b | .written b | .invoked b | .output b | .borrowing b _ | .decoded b => some b
  | _ => none

def Phase.noBorrow : Phase → Prop
  | .borrowing _ _ => False
  | _ => True

def Phase.borrowValid (phase : Phase) (epoch : Nat) : Prop :=
  match phase with
  | .borrowing _ borrowedEpoch => borrowedEpoch = epoch
  | _ => True

structure State where
  phase : Phase
  buffer : Option Buffer
  next : Nat
  released : Nat → Bool
  epoch : Nat

def initial : State := ⟨.idle, none, 0, fun _ => false, 0⟩

/-- The ledger is ghost state. Production code does not store this history. -/
structure Safe (s : State) : Prop where
  ownership : s.buffer = s.phase.buffer
  live : ∀ b, s.buffer = some b → b.id < s.next ∧ s.released b.id = false
  releasedBound : ∀ n, s.released n = true → n < s.next
  borrow : s.phase.borrowValid s.epoch

inductive Event where
  | allocate (buffer : Buffer)
  | writeInput | invoke | preparePoll | poll (buffer : Buffer)
  | borrow | decode | free (buffer : Buffer)
  | finish | grow | preempt | dispose
  deriving DecidableEq, Repr

def release (s : State) (b : Buffer) (phase : Phase) : State :=
  { s with phase, buffer := none, released := update s.released b.id true }

def dispose (s : State) : State :=
  { s with
    phase := .closed
    buffer := none
    released := match s.buffer with
      | none => s.released
      | some b => update s.released b.id true }

/-- Guards are protocol positions, not an assumed pointer-validity oracle.
The allocation/free preconditions below are consequences of `Safe`, proved later. -/
inductive Step : State → Event → State → Prop
  | allocate (s : State) (length : Nat) (h : s.phase = .idle) :
      Step s (.allocate ⟨s.next, length⟩)
        { s with
          phase := .allocated ⟨s.next, length⟩
          buffer := some ⟨s.next, length⟩
          next := s.next + 1 }
  | writeInput (s : State) (b : Buffer) (h : s.phase = .allocated b) :
      Step s .writeInput { s with phase := .written b }
  | invoke (s : State) (b : Buffer) (h : s.phase = .written b) :
      Step s .invoke { s with phase := .invoked b }
  | freeInput (s : State) (b : Buffer) (h : s.phase = .invoked b) :
      Step s (.free b) (release s b .poll)
  | preparePoll (s : State) (h : s.phase = .idle) :
      Step s .preparePoll { s with phase := .poll }
  | poll (s : State) (length : Nat) (h : s.phase = .poll) :
      Step s (.poll ⟨s.next, length⟩)
        { s with
          phase := .output ⟨s.next, length⟩
          buffer := some ⟨s.next, length⟩
          next := s.next + 1 }
  | borrow (s : State) (b : Buffer) (h : s.phase = .output b) :
      Step s .borrow { s with phase := .borrowing b s.epoch }
  -- Ending the Rust borrow also covers decode errors. On error the host drops
  -- the Store after leaving the borrow scope, without another guest export.
  | decode (s : State) (b : Buffer) (epoch : Nat) (h : s.phase = .borrowing b epoch) :
      Step s .decode { s with phase := .decoded b }
  | freeOutput (s : State) (b : Buffer) (h : s.phase = .decoded b) :
      Step s (.free b) (release s b .finished)
  | finish (s : State) (h : s.phase = .finished) :
      Step s .finish { s with phase := .idle }
  | grow (s : State) (h : s.phase.noBorrow) (openStore : s.phase ≠ .closed) :
      Step s .grow { s with epoch := s.epoch + 1 }
  | preempt (s : State) : Step s .preempt s
  | dispose (s : State) (h : s.phase.noBorrow) : Step s .dispose (dispose s)

theorem initial_safe : Safe initial := by
  constructor <;> simp [initial, Phase.buffer, Phase.borrowValid]

private theorem change_phase {s : State} (safe : Safe s) (phase : Phase)
    (same : phase.buffer = s.phase.buffer) (borrow : phase.borrowValid s.epoch) :
    Safe { s with phase } := by
  exact ⟨safe.ownership.trans same.symm, safe.live, safe.releasedBound, borrow⟩

private theorem allocate_safe {s : State} (safe : Safe s) (length : Nat) (phase : Phase)
    (buffer : phase.buffer = some ⟨s.next, length⟩)
    (borrow : phase.borrowValid s.epoch) :
    Safe { s with phase, buffer := some ⟨s.next, length⟩, next := s.next + 1 } := by
  refine ⟨buffer.symm, ?_, ?_, borrow⟩
  · intro b h
    cases Option.some.inj h
    refine ⟨by change s.next < s.next + 1; omega, ?_⟩
    cases value : s.released s.next
    · rfl
    · have := safe.releasedBound s.next value
      omega
  · intro n h
    have := safe.releasedBound n h
    change n < s.next + 1
    omega

private theorem release_safe {s : State} (safe : Safe s) (b : Buffer)
    (owns : s.buffer = some b) (phase : Phase) (empty : phase.buffer = none)
    (borrow : phase.borrowValid s.epoch) : Safe (release s b phase) := by
  refine ⟨empty.symm, ?_, ?_, borrow⟩
  · simp [release]
  · intro n h
    by_cases same : n = b.id
    · subst n
      exact (safe.live b owns).1
    · exact safe.releasedBound n (by simpa [release, update, same] using h)

theorem step_preserves {s s' : State} {e : Event} (safe : Safe s)
    (step : Step s e s') : Safe s' := by
  cases step with
  | allocate length _ => exact allocate_safe safe length _ rfl trivial
  | writeInput b h =>
      exact change_phase safe (.written b) (by simp [h, Phase.buffer]) trivial
  | invoke b h =>
      exact change_phase safe (.invoked b) (by simp [h, Phase.buffer]) trivial
  | freeInput b h =>
      exact release_safe safe b (by simpa [h, Phase.buffer] using safe.ownership) .poll rfl trivial
  | preparePoll h =>
      exact change_phase safe .poll (by simp [h, Phase.buffer]) trivial
  | poll length _ => exact allocate_safe safe length _ rfl trivial
  | borrow b h =>
      exact change_phase safe (.borrowing b s.epoch) (by simp [h, Phase.buffer]) rfl
  | decode b epoch h =>
      exact change_phase safe (.decoded b) (by simp [h, Phase.buffer]) trivial
  | freeOutput b h =>
      exact release_safe safe b (by simpa [h, Phase.buffer] using safe.ownership) .finished rfl trivial
  | finish h => exact change_phase safe .idle (by simp [h, Phase.buffer]) trivial
  | grow h _ =>
      refine ⟨safe.ownership, safe.live, safe.releasedBound, ?_⟩
      cases phase : s.phase <;> simp_all [Phase.noBorrow, Phase.borrowValid]
  | preempt => exact safe
  | dispose h =>
      refine ⟨rfl, ?_, ?_, trivial⟩
      · simp [dispose]
      · intro n released
        cases buffer : s.buffer with
        | none => exact safe.releasedBound n (by simpa [dispose, buffer] using released)
        | some b =>
            by_cases same : n = b.id
            · subst n; exact (safe.live b buffer).1
            · exact safe.releasedBound n (by simpa [dispose, buffer, update, same] using released)

theorem reachable_safe {s : State} {events : List Event}
    (run : Trace Step initial events s) : Safe s :=
  Forest.Trace.invariant (P := Safe) (fun hs step => step_preserves hs step) run initial_safe

theorem free_valid {s s' : State} {b : Buffer} (safe : Safe s)
    (step : Step s (.free b) s') :
    s.buffer = some b ∧ s.released b.id = false ∧ s'.released b.id = true := by
  cases step <;> rename_i h <;>
    have owns : s.buffer = some b := by simpa [h, Phase.buffer] using safe.ownership
  all_goals exact ⟨owns, (safe.live b owns).2, by simp [release]⟩

theorem released_monotone {s s' : State} {e : Event} (step : Step s e s')
    (n : Nat) (h : s.released n = true) : s'.released n = true := by
  cases step <;> simp_all [release, dispose, update]
  all_goals cases buffer : s.buffer <;> simp_all [update]

theorem trace_released_monotone {s s' : State} {events : List Event}
    (run : Trace Step s events s') (n : Nat) (h : s.released n = true) :
    s'.released n = true := by
  induction run with
  | nil => exact h
  | cons step _ ih => exact ih (released_monotone step n h)

theorem no_double_free {s a b c : State} {buffer : Buffer} {before middle : List Event}
    (reach : Trace Step initial before s) (first : Step s (.free buffer) a)
    (between : Trace Step a middle b) (second : Step b (.free buffer) c) : False := by
  have one := free_valid (reachable_safe reach) first
  have safeA := step_preserves (reachable_safe reach) first
  have safeB := Forest.Trace.invariant (P := Safe) (fun hs step => step_preserves hs step) between safeA
  have two := free_valid safeB second
  have released := trace_released_monotone between buffer.id one.2.2
  simp_all

theorem borrowed_reference_valid {s : State} {b : Buffer} {epoch : Nat}
    (safe : Safe s) (h : s.phase = .borrowing b epoch) :
    s.buffer = some b ∧ s.released b.id = false ∧ epoch = s.epoch := by
  have owns : s.buffer = some b := by simpa [h, Phase.buffer] using safe.ownership
  exact ⟨owns, (safe.live b owns).2, by simpa [h, Phase.borrowValid] using safe.borrow⟩

theorem step_closed {s s' : State} {e : Event} (h : s.phase = .closed)
    (step : Step s e s') : s'.phase = .closed := by
  cases step <;> simp_all [dispose, Phase.noBorrow]

theorem step_open {s s' : State} {e : Event} (h : s.phase ≠ .closed)
    (step : Step s e s') (notDispose : e ≠ .dispose) : s'.phase ≠ .closed := by
  cases step <;> simp_all [release]

theorem no_exports_after_close {s s' : State} {e : Event} (h : s.phase = .closed)
    (step : Step s e s') : e = .preempt ∨ e = .dispose := by
  cases step <;> simp_all [Phase.noBorrow]

end Forest.Abi
