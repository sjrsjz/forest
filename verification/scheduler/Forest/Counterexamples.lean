import Forest.Reload

namespace Forest.Counterexamples

/-- A nonempty allocation/invoke/output/free execution, checked by the kernel. -/
theorem normal_abi_roundtrip : ∃ final,
    Trace Abi.Step Abi.initial
      [.allocate ⟨0, 32⟩, .writeInput, .invoke, .free ⟨0, 32⟩,
       .poll ⟨1, 48⟩, .borrow, .decode, .free ⟨1, 48⟩, .finish] final ∧
    final.phase = .idle ∧ final.released 0 = true ∧ final.released 1 = true := by
  let final : Abi.State :=
    ⟨.idle, none, 2, update (update (fun _ => false) 0 true) 1 true, 0⟩
  refine ⟨final, ?_, rfl, ?_, ?_⟩
  · exact .cons (.allocate _ 32 rfl)
      (.cons (.writeInput _ ⟨0, 32⟩ rfl)
      (.cons (.invoke _ ⟨0, 32⟩ rfl)
      (.cons (.freeInput _ ⟨0, 32⟩ rfl)
      (.cons (.poll _ 48 rfl)
      (.cons (.borrow _ ⟨1, 48⟩ rfl)
      (.cons (.decode _ ⟨1, 48⟩ 0 rfl)
      (.cons (.freeOutput _ ⟨1, 48⟩ rfl)
      (.cons (.finish _ rfl) (.nil _)))))))))
  · simp [final, update]
  · simp [final, update]

def buffer : Abi.Buffer := ⟨0, 32⟩

def borrowed : Abi.State :=
  { phase := .borrowing buffer 0, buffer := some buffer,
    next := 1, released := fun _ => false, epoch := 0 }

theorem borrowed_safe : Abi.Safe borrowed := by
  refine ⟨rfl, ?_, ?_, rfl⟩
  · intro b h
    cases Option.some.inj h
    exact ⟨by decide, rfl⟩
  · simp [borrowed]

/-- A single-threaded use-after-grow: no data race is needed. -/
theorem grow_while_borrowed_is_unsafe : ¬ Abi.Safe { borrowed with epoch := 1 } := by
  intro safe
  have invalid := safe.borrow
  simp [borrowed, Abi.Phase.borrowValid] at invalid

def advancing : Store.State :=
  { generation := 0
    phase := .serving
    owner := some (.task 0)
    tasks := fun _ => .live
    create := .done
    destroy := .fresh
    abi := {
      phase := .allocated buffer
      buffer := some buffer
      next := 1
      released := fun _ => false
      epoch := 0 } }

theorem advancing_safe : Store.Safe advancing := by
  refine ⟨?_, ?_, ?_, ?_⟩
  · refine ⟨rfl, ?_, ?_, trivial⟩
    · intro b h
      cases Option.some.inj h
      exact ⟨by decide, rfl⟩
    · simp [advancing]
  · simp [advancing, Store.Phase.isOpen]
  · simp [advancing]
  · intro owner held
    cases Option.some.inj held
    exact ⟨Or.inl rfl, rfl⟩

/-- Clearing the owner at epoch preemption permits unsafe reentry while the
advancing Future and its live ABI allocation still exist. -/
theorem early_unlock_is_unsafe : ¬ Store.Safe { advancing with owner := none } := by
  intro safe
  have invalid := safe.2.2.1 rfl
  simp [advancing] at invalid

theorem queued_is_quiet : Store.quiet { Store.initial with tasks := fun _ => .queued } := by
  simp [Store.quiet, Store.Task.unfinished]

theorem suspended_is_not_quiet : ¬ Store.quiet { Store.initial with tasks := fun _ => .live } := by
  intro quiet
  exact quiet 0 (Or.inl rfl)

end Forest.Counterexamples
