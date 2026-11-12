# OneBot v11 adapter

An ordinary Forest Wasm package at `/module/onebot`. The scheduler has no OneBot connection, account or group policy.

`protocol/` contains the shared v11 protocol library. `tests/host.rs` verifies this plugin against the generic CLI as an external executable; the CLI has no OneBot test dependency.

Build from the repository root:

```sh
cargo run --locked -j 2 -p forest-onebot-module --bin package-onebot
```

Load a consuming package such as `packages/forest-test`, or load `packages/onebot` directly. The adapter is included in a consumer's static dependency closure.

Edit `/module/onebot` → `/settings` in the console and reload this package:

| Field | Default | Meaning |
| --- | --- | --- |
| `websocket` | `ws://127.0.0.1:8080` | Forward v11 WebSocket endpoint |
| `token` | empty | Optional Bearer credential |
| `connect_timeout_ms` | 10000 | One connection attempt |
| `action_timeout_ms` | 30000 | Sending through receipt, one combined deadline |
| `reconnect_min_ms` | 1000 | Initial retry interval |
| `reconnect_max_ms` | 30000 | Maximum base retry interval; jitter adds up to 25% |
| `allowed_groups` | empty | Optional test restriction; empty means unrestricted |

The settings are read at create. The post-publication `/activated` handler connects; `/io` handles socket readiness and reconnect timers. Idle listeners do not keep Drain pending. Disconnect settles outstanding actions with `ReceiptLost`; a deadline produces `Timeout`. Neither condition proves the remote action was not executed, so actions are never replayed.

`/group_message` is a typed event. Readers and API callers allow `/module/**`; API callers also allow `/user/admin`. Emission binds `External { issuer: /module/onebot, subject: /user/<id> }`; receivers decide which external users are authorized.

The package exposes `/send_group_message`, `/send_private_message` and `/query`. Query uses the typed read-only action enum. Explicit group restrictions also constrain queries and sends and disable private sends. Reverse WebSocket and HTTP event listeners require future generic host listener resources.
