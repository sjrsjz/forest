import Forest.Store

namespace Forest.Reload

/-- Transferable object-addressed requests only. Instance-bound activation and
I/O notifications are accounted separately and never enter the handoff queue.
No Store, Memory, guest pointer, Future or borrowed slice occurs here. -/
structure Request where
  identity : Nat
  payload : List UInt8
  origin : Nat
  caller : Nat
  reply : Nat
  deriving DecidableEq, Repr

structure Member where
  store : Store.State
  queued : Nat → List Request
  deferred : Nat → List Request
  pinnedQueued : Nat
  control : Nat

abbrev World := Nat → Member

def Safe (world : World) : Prop := ∀ m, Store.Safe (world m).store

def CanCommit (targets : List Nat) (world : World) : Prop :=
  ∀ m, m ∈ targets → (world m).store.phase = .serving ∧ Store.quiet (world m).store ∧
    (world m).pinnedQueued = 0 ∧ (world m).control = 0

def commitMember (member : Member) : Member :=
  { member with
    store := { member.store with phase := .stopping },
    queued := fun _ => [],
    deferred := fun interface => member.queued interface ++ member.deferred interface }

/-- One directory-lock transition, not a loop of independently visible changes. -/
def commit (targets : List Nat) (world : World) : World :=
  fun m => if m ∈ targets then commitMember (world m) else world m

theorem commit_preserves {targets : List Nat} {world : World}
    (safe : Safe world) (ready : CanCommit targets world) : Safe (commit targets world) := by
  intro m
  by_cases selected : m ∈ targets
  · have conditions := ready m selected
    exact Store.step_preserves (safe m)
      (by simpa [commit, selected, commitMember] using
        Store.Step.stopDrain (world m).store conditions.1 conditions.2.1)
  · simpa [commit, selected] using safe m

theorem commit_has_no_old_owner {targets : List Nat} {world : World} {m : Nat}
    (safe : Safe world) (ready : CanCommit targets world) (selected : m ∈ targets) :
    (commit targets world m).store.owner = none := by
  have conditions := ready m selected
  simpa [commit, selected, commitMember] using
    Store.drain_has_no_owner (safe m) conditions.1 conditions.2.1

theorem commit_transfers_exact_requests (targets : List Nat) (world : World)
    (m interface : Nat) (selected : m ∈ targets) :
    (commit targets world m).deferred interface =
      (world m).queued interface ++ (world m).deferred interface := by
  simp [commit, selected, commitMember]

theorem commit_clears_old_queue (targets : List Nat) (world : World)
    (m interface : Nat) (selected : m ∈ targets) :
    (commit targets world m).queued interface = [] := by
  simp [commit, selected, commitMember]

theorem commit_preserves_other_modules (targets : List Nat) (world : World)
    (m : Nat) (other : m ∉ targets) : commit targets world m = world m := by
  simp [commit, other]

/-- The production code derives this conservation from linear Admission ownership;
these counts are specification quantities, not extra runtime counters. -/
theorem admission_accounting (available queued pinned live cleaning capacity : Nat)
    (conserved : available + queued + pinned + live + cleaning = capacity)
    (commitCheck : available + queued = capacity) : pinned = 0 ∧ live = 0 ∧ cleaning = 0 := by
  omega

theorem commit_has_no_instance_callbacks {targets : List Nat} {world : World} {m : Nat}
    (ready : CanCommit targets world) (selected : m ∈ targets) :
    (commit targets world m).pinnedQueued = 0 ∧ (commit targets world m).control = 0 := by
  simpa [commit, selected, commitMember] using (ready m selected).2.2

theorem separate_control_admission (available used : Nat)
    (conserved : available + used = 1) (quiet : available = 1) : used = 0 := by
  omega

def appendDeferred (member : Member) (interface : Nat) (request : Request) : Member :=
  { member with deferred := update member.deferred interface (member.deferred interface ++ [request]) }

theorem old_requests_precede_new (targets : List Nat) (world : World)
    (m interface : Nat) (request : Request) (selected : m ∈ targets) :
    (appendDeferred (commit targets world m) interface request).deferred interface =
      (world m).queued interface ++ (world m).deferred interface ++ [request] := by
  simp [appendDeferred, commit, selected, commitMember]

def CanPublish (targets : List Nat) (world : World) : Prop :=
  ∀ m, m ∈ targets → (world m).store.phase = .creating ∧
    (world m).store.owner = none ∧ (world m).store.create = .done

def publish (targets : List Nat) (world : World) : World :=
  fun m => if m ∈ targets then
    { (world m) with
      store := { (world m).store with phase := .serving }
      queued := (world m).deferred
      deferred := fun _ => [] }
    else world m

theorem publish_preserves {targets : List Nat} {world : World}
    (safe : Safe world) (ready : CanPublish targets world) : Safe (publish targets world) := by
  intro m
  by_cases selected : m ∈ targets
  · have conditions := ready m selected
    exact Store.step_preserves (safe m)
      (by simpa [publish, selected] using
        Store.Step.publish (world m).store conditions.1 conditions.2.1 conditions.2.2)
  · simpa [publish, selected] using safe m

theorem publish_all_together (targets : List Nat) (world : World) :
    ∀ m, m ∈ targets → (publish targets world m).store.phase = .serving := by
  intro m selected
  simp [publish, selected]

theorem publication_handoff (targets : List Nat) (world : World)
    (m interface : Nat) (selected : m ∈ targets) :
    (publish targets world m).queued interface = (world m).deferred interface := by
  simp [publish, selected]

def CanReplace (targets : List Nat) (world : World) : Prop :=
  ∀ m, m ∈ targets → (world m).store.phase = .staged

def replace (targets : List Nat) (world : World) : World :=
  fun m => if m ∈ targets then
    { world m with store :=
      { generation := (world m).store.generation + 1, phase := .creating, owner := none,
        tasks := fun _ => .unseen, create := .fresh, destroy := .fresh, abi := Abi.initial } }
    else world m

theorem replacement_preserves {targets : List Nat} {world : World}
    (safe : Safe world) (ready : CanReplace targets world) : Safe (replace targets world) := by
  intro m
  by_cases selected : m ∈ targets
  · exact Store.step_preserves (safe m)
      (by simpa [replace, selected] using Store.Step.replace (world m).store (ready m selected))
  · simpa [replace, selected] using safe m

theorem all_old_stores_closed {targets : List Nat} {world : World}
    (safe : Safe world) (ready : CanReplace targets world) :
    ∀ m, m ∈ targets → (world m).store.abi.phase = .closed := by
  intro m selected
  by_cases closed : (world m).store.abi.phase = .closed
  · exact closed
  · have openStore := (safe m).2.1.mpr closed
    simp [ready m selected, Store.Phase.isOpen] at openStore

inductive Management where
  | pending | cancelled | committed
  deriving DecidableEq, Repr

inductive ManagementStep (targets : List Nat) :
    Management × World → Management × World → Prop
  | cancel (world : World) : ManagementStep targets (.pending, world) (.cancelled, world)
  | commit (world : World) (ready : CanCommit targets world) :
      ManagementStep targets (.pending, world) (.committed, commit targets world)

theorem cancellation_leaves_world (targets : List Nat) {world world' : World}
    (step : ManagementStep targets (.pending, world) (.cancelled, world')) : world' = world := by
  cases step
  rfl

theorem no_commit_after_cancel (targets : List Nat) (world : World)
    (next : Management × World) : ¬ ManagementStep targets (.cancelled, world) next := by
  intro step
  cases step

end Forest.Reload
