import Std

namespace Forest

def update {α : Type} (f : Nat → α) (key : Nat) (value : α) : Nat → α :=
  fun n => if n = key then value else f n

@[simp] theorem update_same {α : Type} (f : Nat → α) (key : Nat) (value : α) :
    update f key value key = value := by simp [update]

@[simp] theorem update_other {α : Type} (f : Nat → α) (key n : Nat) (value : α)
    (h : n ≠ key) : update f key value n = f n := by simp [update, h]

/-- Finite traces have arbitrary length; this is not bounded state enumeration. -/
inductive Trace {S E : Type} (step : S → E → S → Prop) : S → List E → S → Prop
  | nil (s : S) : Trace step s [] s
  | cons {a b c : S} {e : E} {es : List E} :
      step a e b → Trace step b es c → Trace step a (e :: es) c

theorem Trace.invariant {S E : Type} {step : S → E → S → Prop} {P : S → Prop}
    (preserve : ∀ {s e s'}, P s → step s e s' → P s')
    {s s' : S} {events : List E} (run : Trace step s events s') (initial : P s) : P s' := by
  induction run with
  | nil => exact initial
  | cons transition _ ih => exact ih (preserve initial transition)

end Forest
