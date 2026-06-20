# Dynamic scaling

How a Mycelium cluster grows and shrinks itself — and how to *see* it. The model
is **management = intent + local reconcile**: you publish a desired-state
*intent* (evaporating soft-state, not a command), and nodes self-elect locally to
satisfy it. There is no controller; if the operator vanishes, the cluster keeps
running on the last intent and self-heals.

> Audience: **DevOps**. The mechanism's design is in
> [00 · Concepts](../guide/00-concepts.md) (Intent, MembershipGovernor) and the
> philosophy doc.

## See it in 30 seconds

```bash
cargo run -p mycelium-coop-examples --bin elastic_intent
```

This runs the whole story locally: an operator publishes "keep `rush-pool` in
`[2, 3]` depots", five candidates self-elect into a *subset* in the band, the
**operator is then killed** and the band still holds, and finally a pool member
is killed and the governors **self-heal** the count back to the minimum. Watch
the printed member counts move. (Walkthrough:
[`examples/coop/README.md`](../../examples/coop/README.md) §03.)

## Elastic membership (groups)

A `MembershipGovernor` on each node reconciles a group's live size toward a
band. Publish the intent over the gateway:

```bash
curl -X POST http://node:8080/gateway/govern/membership \
  -H "Authorization: Bearer $GOVERN_TOKEN" \
  -d '{"group":"rush-pool","min":2,"max":3}'
```

- The intent gossips to every node and **evaporates** after its TTL — re-POST to
  keep it in force; stop and the group reverts to emergent (un-bounded) membership.
- Nodes **self-elect** probabilistically (idle nodes join first; busy nodes shed
  first), so the count converges without a thundering herd and without a
  coordinator picking who.
- `drain: [node, …]` cooperatively removes named nodes (they leave and stay out).
- **Bounds are convergence targets, not guarantees** — a node's sovereign veto
  wins, and an exact/guaranteed count would imply a coordinator (that's a Tier-3
  consensus escalation, not this Tier-2 governor).

> Requires the group to be governor-owned (a live intent); the emergent-join
> watcher defers to the governor — see the
> [patterns chapter](../guide/14-patterns-and-pitfalls.md) §6.

## Elastic capacity (provisioning)

Membership bounds *who is in a group*; **provisioning** bounds *how many
providers of a capability exist*. An unmet requirement is demand a provisioner
re-satisfies (scale up); a `supervise_band(filter, min, max)` policy sheds
providers over `max` (scale down). This is the autonomic loop in the
[`provisioning`](../../examples/coop/src/bin/provisioning.rs) and
[`catalog`](../../examples/coop/src/bin/catalog.rs) demos —
[operations/artifacts.md](artifacts.md).

## Live tuning (config, not size)

The same intent transport tunes config scalars without a restart — the WS-C
tuning governor:

```bash
curl -X POST http://node:8080/gateway/govern/tuning \
  -H "Authorization: Bearer $GOVERN_TOKEN" -d '{"writer_channel_depth":4096}'
curl -s http://node:8080/gateway/govern        # this node's effective tuning snapshot
```

Each node clamps its *own* scalar (local-pin beats fleet intent), so a per-node
override sticks. Full knob reference: [tuning.md](tuning.md).

## Auth

`/gateway/govern*` is scope-gated (`govern:read` / `govern:write`,
deny-by-default) and audited (WS2). Issue a token with those scopes — see
[rbac.md](rbac.md). The public observability endpoints
([observability.md](observability.md)) stay uncredentialed.

## What "no coordinator" means here

There is no autoscaler process to run or keep alive. The intent is a row in the
gossip KV store; the governors are loops on the nodes themselves. Kill the
operator, the management API, even most of the cluster — the survivors keep
reconciling toward the last intent until it evaporates. The litmus: *if
management vanishes, does the cluster keep working?* — demonstrated by killing the
operator in the `elastic_intent` demo.
