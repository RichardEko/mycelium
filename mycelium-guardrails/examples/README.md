# Guardrails — example suite

## Objective

Mycelium's substrate is **detection, not prevention** — Layer I never enforces a higher
layer's law; it leaves tripwires and counters instead. The `mycelium-guardrails` companion
adds an **opt-in policy layer** on top of that substrate that *can* structurally stop a
caller: a spectrum of policy strengths from **soft-warn** up to **hard-prevent (Tier C)**,
where an off-allowlist caller's tool invocation is refused at the provider's gate. Every
denial is then folded back into the detection story — **tamper-evidently sealed** as a
compliance audit record a neutral observer can reconstruct and verify.

These are **layer-IV** demos (capability/agent + security/policy), built entirely on the
public API. They run a small constructive-domain co-op (neighbourhood microgrid /
surplus-food-rescue) as a real tls mesh — tls because audit records are Ed25519-signed and
the *sealed principal* must be the signature-verified caller, not a self-asserted string.

> **Honest bounds (CFT, not Byzantine).** The proof attests that a provider *tamper-evidently
> sealed stopping* a caller — **provable-stopping**, per node. It is **not** a global "could
> not have acted anywhere" claim, and it is not tamper-proof against a malicious node: the
> chain is per-node, and Tiers A/B are *self-imposed* (an honest node declining to act
> outside its remit). That asymmetry is exactly why Tier C — provider-side hard prevention —
> exists.

## How to run

Everything shares the [repo setup](../../examples/README.md#shared-setup) for the toolchain;
then run any single example below. The two CLI demos print an `OK` marker and exit 0 (both
asserted by `ci_smoke.sh`); the browser showcase runs continuously.

### `guardrail_wedge`

**Objective.** The guardrails wedge made runnable: an off-allowlist caller is **structurally
stopped** at a Tier-C `authorized_callers` gate, and a third **observer** node — holding no
special role — reconstructs the provider's chain and prints the cryptographic denial proof.

**How to run.**
```bash
cargo run -p mycelium-guardrails --example guardrail_wedge --features compliance
```
Prints `WEDGE OK` on success.

**What it demonstrates.** One node *provides* a governed tool (`agent.tool.invoke`) behind a
Tier-C gate. Two peers invoke it: the unauthorized one is refused (not merely failed) and its
denial is **sealed** into the provider's tamper-evident audit chain; the authorized one is
admitted. The observer then calls `prove_denials` → a `DenialProof` with `chain_verified`,
reconstructed from the `SealedDenial` chain (content hashes · HLC · seq) — verifiable by a
party holding no role. Those seals are compliance audit records: they surface at the
provider's `/gateway/audit` and in the Ops Console **Audit** tab.

### `guardrail_fleet`

**Objective.** All three policy strength tiers composed in **one** co-op fleet, each shown
**observably firing** — not merely declared.

**How to run.**
```bash
cargo run -p mycelium-guardrails --example guardrail_fleet --features compliance
```
Prints `FLEET OK` on success.

**What it demonstrates.** A surplus-food-rescue / community-energy co-op across two regions
exercises each tier:
- **Tier A — boundary drop (self-imposed).** A `region-north`-only agent structurally never
  acts on a `region-south` dispatch — dropped at its admission boundary, before any handler.
- **Tier B — denied tool blocked at the state transition (self-imposed).** A `planner` whose
  policy denies `wire_transfer` is refused at its own `→ Invoking` transition
  (`PolicyViolation::ToolDenied`), while an allowed tool transitions fine.
- **Tier C — unauthorized caller rejected at the provider gate, then sealed and proven (hard
  prevention).** A `settlement` provider guards `coop.settle`; a `rogue` node is rejected and
  its denial sealed, while the `coordinator` is admitted — and a neutral observer proves it.

### `guardrail_viz`

**Objective.** The **browser showcase** of the wedge: fire authorized and unauthorized tool
calls from a dashboard and watch — live — an agent structurally stopped, each denial sealed,
and the observer prove the stop.

**How to run.**
```bash
cargo run -p mycelium-guardrails --example guardrail_viz --features compliance,gateway,metrics-export
```
Open **http://127.0.0.1:8096/** — runs continuously, Ctrl-C to stop.

**What it demonstrates.** The same provider / authorized / unauthorized / observer roles as
the wedge, driven interactively. It follows the [UI-example
contract](../../docs/wiki/dev/ui-example-contract.md): the `compliance,gateway,metrics-export`
feature set (it uses `metrics-export` rather than `metrics` to avoid a dependency-name
collision), a concepts box explaining what you're seeing, and an Ops Console link via `ui/viz`.
Because the nodes run `compliance`, the provider's gateway exposes `/gateway/audit` — that
endpoint **is** the seal: point the Ops Console at `:9096` and its **Audit** tab populates
with the very same Ed25519-signed denial records.

## CI

`guardrail_wedge` and `guardrail_fleet` run Docker-free and are asserted on their printed
markers (`WEDGE OK` / `FLEET OK`) by `ci_smoke.sh`.
