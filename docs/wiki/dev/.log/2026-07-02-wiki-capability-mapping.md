## [2026-07-02] ingest | wiki ↔ Capability/Skill/Group mapping folded in

Folded the identity/access clarification into both the mycelium-wiki sketch (a concise
"How it maps to Capability/Skill/Group" section) and the Phase 0 design record (a fuller
normative §4). The load-bearing distinction: **competence and role are Capabilities;
knowledge content is group-scoped Layer-I state, not a capability.** Composition: advertise
competence cap → auto-join the CapabilityGroupDef group → group membership grants
Boundary::admits over wiki/{group}/* → the skill consumes the wiki. Access control layers
(authorized_callers, WS1 clearance per page) refine but don't replace the
capability→group→boundary chain. Federation: AgentFacts publishes competence, never wiki
content. Normative anti-pattern recorded: never advertise knowledge *content* as
capabilities (collapses discovery into storage, explodes cap/).
