## [2026-07-02] ingest | curator 3-way reconcile worked example (§2.2.1)

Added §2.2.1 to wiki-concurrent-edit.md: a concrete curator 3-way merge — two responders
concurrently edit one incident "resolution" section against the same base_hlc (B appends a
step, C corrects a different line); the curator drains both in one pass, runs
LLM_3way(base, current, [edits]), and writes ONE merged value. Makes concrete: non-overlapping
edits compose (LLM only adjudicates genuine overlap); base_hlc distinguishes addition from
change and layers a stale-based edit onto current rather than clobbering; the crash-after-write
-before-tombstone re-drain is a no-op-equivalent (idempotent, single-writer → converges). No
mechanism change — illustrates §2.2/§3.2; cross-linked from the reconcile-loop pseudocode.
