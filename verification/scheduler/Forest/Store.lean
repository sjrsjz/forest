import Forest.Abi

namespace Forest.Store

inductive Phase where
  | creating | serving | stopping | destroying | releasing | staged
  deriving DecidableEq, Repr

def Phase.isOpen : Phase → Prop
  | .creating | .serving | .stopping | .destroying => True
  | .releasing | .staged => False

inductive Task where
  | unseen | queued | live | cleaning | done
  deriving DecidableEq, Repr

def Task.unfinished (task : Task) : Prop := task = .live ∨ task = .cleaning

instance (task : Task) : Decidable task.unfinished := by
  unfold Task.unfinished
  infer_instance

inductive Life where
  | fresh | pending | done
  deriving DecidableEq, Repr

inductive Owner where
  | create | task (id : Nat) | destroy
  deriving DecidableEq, Repr

structure State where
  generation : Nat
  phase : Phase
  owner : Option Owner
  tasks : Nat → Task
  create : Life
  destroy : Life
  abi : Abi.State

def initial : State :=
  ⟨0, .creating, none, fun _ => .unseen, .fresh, .fresh, Abi.initial⟩

def allowed (s : State) : Owner → Prop
  | .create => s.phase = .creating ∧ s.create = .pending
  | .task id => (s.phase = .serving ∨ s.phase = .stopping) ∧ s.tasks id = .live
  | .destroy => s.phase = .destroying ∧ s.destroy = .pending

def Safe (s : State) : Prop :=
  Abi.Safe s.abi ∧
  (s.phase.isOpen ↔ s.abi.phase ≠ .closed) ∧
  (s.owner = none → s.abi.phase = .idle ∨ s.abi.phase = .closed) ∧
  (∀ owner, s.owner = some owner → allowed s owner)

def quiet (s : State) : Prop := ∀ id, ¬ (s.tasks id).unfinished

/-- Retire suspended guest futures only after the current Wasmtime advancement
has returned. Submitted host work has a separate cleaning obligation. -/
def retire (tasks : Nat → Task) : Nat → Task := fun id =>
  match tasks id with
  | .queued | .live => .done
  | other => other

def fault (s : State) : State :=
  { s with
    phase := .releasing
    owner := none
    abi := Abi.dispose s.abi
    tasks := fun id => if (s.tasks id).unfinished then .cleaning else s.tasks id }

inductive Event where
  | enqueue (id : Nat) | start (id : Nat) | resume (id : Nat)
  | beginCreate | resumeCreate | created | publish
  | beginDestroy | resumeDestroy | destroyed
  | abi (event : Abi.Event)
  | yield | complete | cleaned (id : Nat)
  | stopDrain | stopAtYield | enterDestroy | dropNormal | trap | force
  | reclaimed | replace | preempt
  deriving DecidableEq, Repr

inductive Step : State → Event → State → Prop
  | enqueue (s : State) (id : Nat) (phase : s.phase = .serving)
      (fresh : s.tasks id = .unseen) :
      Step s (.enqueue id) { s with tasks := update s.tasks id .queued }
  | start (s : State) (id : Nat) (phase : s.phase = .serving)
      (idle : s.owner = none) (queued : s.tasks id = .queued) :
      Step s (.start id) { s with owner := some (.task id), tasks := update s.tasks id .live }
  | resume (s : State) (id : Nat) (phase : s.phase = .serving)
      (idle : s.owner = none) (live : s.tasks id = .live) :
      Step s (.resume id) { s with owner := some (.task id) }
  | beginCreate (s : State) (phase : s.phase = .creating)
      (idle : s.owner = none) (fresh : s.create = .fresh) :
      Step s .beginCreate { s with owner := some .create, create := .pending }
  | resumeCreate (s : State) (phase : s.phase = .creating)
      (idle : s.owner = none) (pending : s.create = .pending) :
      Step s .resumeCreate { s with owner := some .create }
  | created (s : State) (owner : s.owner = some .create) (abi : s.abi.phase = .idle) :
      Step s .created { s with owner := none, create := .done }
  | publish (s : State) (phase : s.phase = .creating)
      (idle : s.owner = none) (ready : s.create = .done) :
      Step s .publish { s with phase := .serving }
  | abi (s : State) (owner : Owner) (owned : s.owner = some owner)
      (next : Abi.State) (event : Abi.Event) (step : Abi.Step s.abi event next)
      (ordinary : event ≠ .dispose) : Step s (.abi event) { s with abi := next }
  | yield (s : State) (owner : Owner) (owned : s.owner = some owner)
      (abi : s.abi.phase = .idle) : Step s .yield { s with owner := none }
  | complete (s : State) (id : Nat) (owner : s.owner = some (.task id))
      (abi : s.abi.phase = .idle) :
      Step s .complete { s with owner := none, tasks := update s.tasks id .cleaning }
  | cleaned (s : State) (id : Nat) (cleaning : s.tasks id = .cleaning) :
      Step s (.cleaned id) { s with tasks := update s.tasks id .done }
  | stopDrain (s : State) (phase : s.phase = .serving) (quiet : quiet s) :
      Step s .stopDrain { s with phase := .stopping }
  | stopAtYield (s : State) (phase : s.phase = .serving) :
      Step s .stopAtYield { s with phase := .stopping }
  | enterDestroy (s : State) (phase : s.phase = .stopping) (idle : s.owner = none)
      (cleaned : ∀ id, s.tasks id ≠ .cleaning) :
      Step s .enterDestroy { s with phase := .destroying, tasks := retire s.tasks }
  | beginDestroy (s : State) (phase : s.phase = .destroying)
      (idle : s.owner = none) (fresh : s.destroy = .fresh) :
      Step s .beginDestroy { s with owner := some .destroy, destroy := .pending }
  | resumeDestroy (s : State) (phase : s.phase = .destroying)
      (idle : s.owner = none) (pending : s.destroy = .pending) :
      Step s .resumeDestroy { s with owner := some .destroy }
  | destroyed (s : State) (owner : s.owner = some .destroy) (abi : s.abi.phase = .idle) :
      Step s .destroyed { s with owner := none, destroy := .done }
  | dropNormal (s : State) (phase : s.phase = .destroying)
      (idle : s.owner = none) (done : s.destroy = .done) :
      Step s .dropNormal { s with phase := .releasing, abi := Abi.dispose s.abi }
  | trap (s : State) (noBorrow : s.abi.phase.noBorrow) : Step s .trap (fault s)
  | force (s : State) (noBorrow : s.abi.phase.noBorrow) : Step s .force (fault s)
  | reclaimed (s : State) (phase : s.phase = .releasing) (quiet : quiet s) :
      Step s .reclaimed { s with phase := .staged }
  | replace (s : State) (phase : s.phase = .staged) :
      Step s .replace
        { generation := s.generation + 1, phase := .creating, owner := none,
          tasks := fun _ => .unseen, create := .fresh, destroy := .fresh, abi := Abi.initial }
  | preempt (s : State) : Step s .preempt s

theorem initial_safe : Safe initial := by
  exact ⟨Abi.initial_safe, by simp [initial, Phase.isOpen, Abi.initial],
    by simp [initial, Abi.initial], by simp [initial]⟩

theorem idle_abi {s : State} (safe : Safe s) (openStore : s.phase.isOpen)
    (idle : s.owner = none) : s.abi.phase = .idle := by
  rcases safe.2.2.1 idle with idle | closed
  · exact idle
  · exact False.elim ((safe.2.1.mp openStore) closed)

theorem owner_unique {s : State} {a b : Owner}
    (first : s.owner = some a) (second : s.owner = some b) : a = b := by
  exact Option.some.inj (first.symm.trans second)

theorem drain_has_no_owner {s : State} (safe : Safe s) (phase : s.phase = .serving)
    (quiescent : quiet s) : s.owner = none := by
  cases owner : s.owner with
  | none => rfl
  | some current =>
      have permission := safe.2.2.2 current owner
      cases current with
      | create => simp_all [allowed]
      | destroy => simp_all [allowed]
      | task id =>
          exact False.elim (quiescent id (Or.inl permission.2))

/-- An old completion cannot select a task in a replacement instance. Actual
Rust uses instance-owned operation tables and Weak<TaskSignal>, not a guest token. -/
def acceptsCompletion (s : State) (generation id : Nat) : Prop :=
  generation = s.generation ∧ s.tasks id = .live ∧ s.phase = .serving

theorem stale_completion_rejected (s : State) (generation id : Nat)
    (stale : generation ≠ s.generation) : ¬ acceptsCompletion s generation id := by
  intro accepted
  exact stale accepted.1

theorem queued_unbound {s : State} {id : Nat} (safe : Safe s)
    (queued : s.tasks id = .queued) : s.owner ≠ some (.task id) := by
  intro owner
  have live := (safe.2.2.2 (.task id) owner).2
  simp_all

private theorem change_task {s : State} (safe : Safe s) (id : Nat) (task : Task)
    (unowned : s.owner ≠ some (.task id)) : Safe { s with tasks := update s.tasks id task } := by
  refine ⟨safe.1, safe.2.1, safe.2.2.1, ?_⟩
  intro owner held
  have permission := safe.2.2.2 owner held
  cases owner with
  | create => exact permission
  | destroy => exact permission
  | task other =>
      have different : other ≠ id := by intro same; subst other; exact unowned held
      exact ⟨permission.1, by simpa [allowed, update, different] using permission.2⟩

private theorem fault_safe {s : State} (safe : Safe s) (noBorrow : s.abi.phase.noBorrow) :
    Safe (fault s) := by
  refine ⟨Abi.step_preserves safe.1 (.dispose s.abi noBorrow), ?_, ?_, ?_⟩
  · simp [fault, Phase.isOpen, Abi.dispose]
  · simp [fault, Abi.dispose]
  · simp [fault]

theorem step_preserves {s s' : State} {event : Event} (safe : Safe s)
    (transition : Step s event s') : Safe s' := by
  cases transition with
  | enqueue id _ fresh =>
      apply change_task safe id .queued
      intro held
      have live := (safe.2.2.2 (.task id) held).2
      simp_all
  | start id phase idle _ =>
      refine ⟨safe.1, safe.2.1, by simp, ?_⟩
      intro owner held
      cases Option.some.inj held
      exact ⟨Or.inl phase, by simp⟩
  | resume id phase _ live =>
      refine ⟨safe.1, safe.2.1, by simp, ?_⟩
      intro owner held
      cases Option.some.inj held
      exact ⟨Or.inl phase, live⟩
  | beginCreate phase _ _ =>
      refine ⟨safe.1, safe.2.1, by simp, ?_⟩
      intro owner held
      cases Option.some.inj held
      exact ⟨phase, rfl⟩
  | resumeCreate phase _ pending =>
      refine ⟨safe.1, safe.2.1, by simp, ?_⟩
      intro owner held
      cases Option.some.inj held
      exact ⟨phase, pending⟩
  | created _ abi =>
      exact ⟨safe.1, safe.2.1, by simp [abi], by simp⟩
  | publish phase idle _ =>
      refine ⟨safe.1, ?_, safe.2.2.1, ?_⟩
      · simpa [phase, Phase.isOpen] using safe.2.1
      · simp [idle]
  | abi owner held next event step notDispose =>
      have permission := safe.2.2.2 owner held
      have openStore : s.phase.isOpen := by
        cases owner with
        | create => simp [permission.1, Phase.isOpen]
        | destroy => simp [permission.1, Phase.isOpen]
        | task id =>
            rcases permission.1 with phase | phase <;> simp [phase, Phase.isOpen]
      have openAbi := Abi.step_open (safe.2.1.mp openStore) step notDispose
      refine ⟨Abi.step_preserves safe.1 step, ⟨fun _ => openAbi, fun _ => openStore⟩, ?_, ?_⟩
      · simp [held]
      · exact safe.2.2.2
  | yield _ _ abi => exact ⟨safe.1, safe.2.1, by simp [abi], by simp⟩
  | complete _ _ abi => exact ⟨safe.1, safe.2.1, by simp [abi], by simp⟩
  | cleaned id cleaning =>
      apply change_task safe id .done
      intro held
      have live := (safe.2.2.2 (.task id) held).2
      simp_all
  | stopDrain phase _ | stopAtYield phase =>
      refine ⟨safe.1, ?_, safe.2.2.1, ?_⟩
      · simpa [phase, Phase.isOpen] using safe.2.1
      · intro owner held
        have permission := safe.2.2.2 owner held
        cases owner <;> simp_all [allowed]
  | enterDestroy phase idle _ =>
      refine ⟨safe.1, ?_, safe.2.2.1, ?_⟩
      · simpa [phase, Phase.isOpen] using safe.2.1
      · simp [idle]
  | beginDestroy phase _ _ =>
      refine ⟨safe.1, safe.2.1, by simp, ?_⟩
      intro owner held
      cases Option.some.inj held
      exact ⟨phase, rfl⟩
  | resumeDestroy phase _ pending =>
      refine ⟨safe.1, safe.2.1, by simp, ?_⟩
      intro owner held
      cases Option.some.inj held
      exact ⟨phase, pending⟩
  | destroyed _ abi => exact ⟨safe.1, safe.2.1, by simp [abi], by simp⟩
  | dropNormal phase idle _ =>
      have abi := idle_abi safe (by simp [phase, Phase.isOpen]) idle
      have noBorrow : s.abi.phase.noBorrow := by simp [abi, Abi.Phase.noBorrow]
      refine ⟨Abi.step_preserves safe.1 (.dispose s.abi noBorrow), ?_, ?_, ?_⟩
      · simp [Phase.isOpen, Abi.dispose]
      · simp [Abi.dispose]
      · simp [idle]
  | trap noBorrow | force noBorrow => exact fault_safe safe noBorrow
  | reclaimed phase _ =>
      refine ⟨safe.1, ?_, safe.2.2.1, ?_⟩
      · simpa [phase, Phase.isOpen] using safe.2.1
      · intro owner held
        have permission := safe.2.2.2 owner held
        cases owner <;> simp_all [allowed]
  | replace _ =>
      exact ⟨Abi.initial_safe, by simp [Phase.isOpen, Abi.initial],
        by simp [Abi.initial], by simp⟩
  | preempt => exact safe

theorem reachable_safe {s : State} {events : List Event}
    (run : Trace Step initial events s) : Safe s :=
  Forest.Trace.invariant (P := Safe) (fun hs step => step_preserves hs step) run initial_safe

theorem preemption_keeps_owner (s : State) {s' : State} (step : Step s .preempt s') :
    s'.owner = s.owner ∧ s'.abi = s.abi := by cases step; exact ⟨rfl, rfl⟩

theorem destroyed_store_cannot_resume {s : State} {generation id : Nat}
    (closed : s.phase = .releasing ∨ s.phase = .staged) :
    ¬ acceptsCompletion s generation id := by
  intro accepted
  rcases closed with h | h <;> simp_all [acceptsCompletion]

def StagedQuiet (s : State) : Prop := s.phase = .staged → quiet s

theorem step_staged_quiet {s s' : State} {event : Event} (safe : Safe s)
    (staged : StagedQuiet s) (step : Step s event s') : StagedQuiet s' := by
  cases step with
  | complete id held _ =>
      intro phase
      exact False.elim (staged phase id (Or.inl (safe.2.2.2 (.task id) held).2))
  | cleaned id cleaning =>
      intro phase
      exact False.elim (staged phase id (Or.inr cleaning))
  | _ => simp_all [StagedQuiet, quiet, Task.unfinished, fault]

theorem reachable_protocol_safe {s : State} {events : List Event}
    (run : Trace Step initial events s) : Safe s ∧ StagedQuiet s := by
  apply Forest.Trace.invariant (P := fun state => Safe state ∧ StagedQuiet state) ?_ run
  · exact ⟨initial_safe, by simp [StagedQuiet, initial]⟩
  · intro a event b invariant step
    exact ⟨step_preserves invariant.1 step, step_staged_quiet invariant.1 invariant.2 step⟩

end Forest.Store
