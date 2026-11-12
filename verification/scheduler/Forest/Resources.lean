import Std

namespace Forest.Resources

/-- Host listeners may wait without a guest task. Their captured generation can
only deliver into that generation while its directory entry remains Serving. -/
structure State where
  generation : Nat
  accepting : Bool
  listeners : Nat
  callbacks : List Nat
  deriving DecidableEq, Repr

def Safe (s : State) : Prop := ∀ tag ∈ s.callbacks, tag = s.generation

inductive Step : State → State → Prop
  | watch (s : State) (enabled : s.accepting = true) :
      Step s { s with listeners := s.listeners + 1 }
  | enqueue (s : State) (tag : Nat) (enabled : s.accepting = true)
      (current : tag = s.generation) :
      Step s { s with callbacks := s.callbacks ++ [tag] }
  | close (s : State) : Step s { s with accepting := false }
  | join (s : State) (closed : s.accepting = false) :
      Step s { s with listeners := 0 }
  | discard (s : State) (closed : s.accepting = false) :
      Step s { s with callbacks := [] }
  | replace (s : State) (closed : s.accepting = false)
      (joined : s.listeners = 0) (settled : s.callbacks = []) :
      Step s {
        generation := s.generation + 1, accepting := true,
        listeners := 0, callbacks := [] }

theorem step_preserves {s t : State} (safe : Safe s) (step : Step s t) : Safe t := by
  cases step with
  | watch => exact safe
  | enqueue tag _ current =>
    intro item member
    simp only [List.mem_append, List.mem_singleton] at member
    rcases member with old | added
    · exact safe item old
    · exact added.trans current
  | close => exact safe
  | join => exact safe
  | discard => simp [Safe]
  | replace => simp [Safe]

theorem closed_rejects_admission (s : State) (closed : s.accepting = false)
    (enabled : s.accepting = true) : False := by
  simp [closed] at enabled

theorem stale_generation_rejected (s : State) (tag : Nat)
    (stale : tag ≠ s.generation) (current : tag = s.generation) : False :=
  stale current

theorem replacement_contains_no_old_callbacks (s : State) :
    Safe {
      generation := s.generation + 1, accepting := true,
      listeners := 0, callbacks := [] } := by
  simp [Safe]

end Forest.Resources
