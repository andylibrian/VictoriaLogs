# Storage Engine Course - Comprehensive Review

**Reviewer:** Claude Opus 4.6
**Date:** 2026-02-20
**Scope:** All files across levels 1-10 (including 5a-5h), ~90 markdown files

---

## Overall Verdict: Strong Curriculum Design, Needs Scaffolding

The course is architecturally excellent - the topic sequencing, source code anchoring, and progressive complexity are well thought out. It's clearly designed by someone who deeply understands both the VictoriaLogs codebase and storage engine pedagogy. However, it has systemic gaps that would make it difficult for someone to actually complete without significant hand-holding.

---

## What Works Well

### 1. Source code references are 100% accurate

Every function, constant, and file path verified across all 10+ levels points to real, existing code. Constants like `minMergeMultiplier = 1.7`, `bloomFilterHashesCount = 6`, `bloomFilterBitsPerItem = 16` all match the actual source. This is rare and valuable - most code-anchored curricula drift from reality.

### 2. The level sequencing is pedagogically sound

```
Algorithms (1) → Write path (2) → Block format (3) → Bloom filters (4)
    → LSM theory (5a) → WAL (5b) → SSTable (5c) → Read path (5d)
    → Compaction (5e) → Recovery (5f) → Concurrency (5g) → Capstone (5h)
    → VictoriaLogs LSM specifics (6) → Merge heuristics (7)
    → Query pruning (8) → IndexDB/mergeset (9) → Ops/retention/capstone (10)
```

Each level has genuine dependencies on prior levels. The Level 5 explosion into 5a-5h sub-levels was a smart design decision - the original single-level LSM coverage would have been too shallow.

### 3. Checkpoint questions are sophisticated

They test *rationale*, not just mechanics:

- "Why not merge any available pair of parts immediately?" (Level 7)
- "Why keep inmemory, small, and big separate?" (Level 6)
- "Why can non-final merge be skipped when disk reservation fails?" (Level 6)

These force the learner to understand tradeoffs, not just trace code.

### 4. Levels 6 and 7 are the strongest

They have the most specific code anchors (10+ verified functions each), the clearest guided reading tasks, and the most well-scoped labs. These levels successfully bridge generic LSM knowledge to VictoriaLogs-specific implementation.

### 5. Level 10 capstone rubric is excellent

Technical correctness, completeness, tradeoff quality, and verifiability. The retention incident drill and the requirement to "defend your system model under detailed engineering questioning" are operationally grounded.

---

## What Needs Improvement

### Critical Issue 1: All Deliverable Files Are Empty Shells

Every single supplementary file across all levels (`checkpoint.md`, `lab-results.md`, `bloom-implementation.md`, `merge-trace.md`, etc.) contains only this:

```markdown
# [Title]
## Context
- Date:
- Owner:
## Work
## Evidence
## Conclusions
## Open Questions
```

This generic template gives zero guidance on what to produce. The README explains the lab goal, but the deliverable file doesn't reinforce it. A learner opens `merge-trace.md` and sees nothing to anchor their work against.

**Recommendation:** Each deliverable should include:
- The specific lab instructions (copied or linked from README)
- Expected output format (e.g., "trace table with columns: step, heap state, output")
- A brief example showing depth/style expected
- Self-assessment checklist tied to pass criteria

### Critical Issue 2: No Worked Examples Anywhere

Across 90+ files, there is not a single worked example. No sample trace, no example Bloom calculation, no reference simulator output, no example merge state diagram. The course asks learners to:

- Build an event-driven LSM simulator (5a) - no pseudocode skeleton
- Implement a full WAL format (5b) - no serialization details (endianness, alignment)
- Create a complete mini-LSM engine (5h) - no starter code
- Trace heap merge state transitions (7) - no example trace format

**Recommendation:** Add at least one substantive worked example per level. Not full solutions - just enough to calibrate expectations. A 10-line trace example for Level 7 would save hours of confusion about granularity.

### Critical Issue 3: Missing Foundational Formulas and Definitions

- **Level 4:** Bloom false-positive probability formula never appears (`(1 - e^(-kn/m))^k`). Lab 2 asks for a parameter sweep that's purely empirical without theoretical grounding.
- **Level 5a:** Policy names "eager merge", "multiplier-threshold merge", "bounded fan-in merge" appear without definitions. Learner must infer meaning from VictoriaLogs constants.
- **Level 5e:** "Compaction debt" is mentioned as a concept but never defined.
- **Level 5g:** "Actionable metrics" lacks specifics - no metric names, no emission points, no alert thresholds.

**Recommendation:** Add a "Definitions" or "Key Formulas" subsection to each README where specialized terminology is introduced.

### Moderate Issue 4: Missing Prerequisites and Cross-References

No level explicitly states what must be understood from prior levels. Level 6 says "Complete level5h checkpoint first" but doesn't say what specific knowledge gates exist. There are no cross-references like "this builds on the sorted-partition invariant from Level 1."

**Recommendation:** Add a "Prerequisites" section to each README listing specific concepts (not just "complete Level N"). Example: "You should be able to explain the three-tier part model (Level 6) and the Bloom skip-index pattern (Level 4)."

### Moderate Issue 5: Levels 5e-5g Are Under-Scaffolded

The difficulty ramp from 5d (read path - well-defined) to 5e (compaction policy - abstract) to 5f (recovery/manifest - very abstract) to 5g (concurrency - vague) is steep. These levels have fewer specific code anchors and more open-ended deliverables. Level 5f asks you to design manifest format, recovery replay, and snapshot semantics with almost no guidance.

**Recommendation:** Either provide more detailed concept explanations in 5e-5g READMEs, or provide reference implementations from other systems (RocksDB, LevelDB) as comparison anchors.

### Minor Issue 6: No Time Estimates

The README suggests "3 sessions/week, 90-120 minutes/session" but individual levels vary wildly in scope. Level 1 (binary search exercises) is maybe 2 hours total. Level 5h (full mini-LSM capstone) could easily take 40+ hours. Level 5a's simulator alone could take 20+ hours for a junior engineer.

**Recommendation:** Add per-level time estimates, even rough ones, so learners can plan realistically.

### Minor Issue 7: No Validation Mechanisms

Pass criteria are goals, not verification methods. "Reads are correct and corruption detection is reliable" (5c) - how does the learner verify this? No test harness, no golden files, no property-based testing guidance.

**Recommendation:** Suggest specific verification approaches per level (e.g., "run your simulator with these 3 input sequences and compare against expected output").

---

## Level-by-Level Quality Summary

| Level | README | Code Accuracy | Labs | Scaffolding | Overall |
|-------|--------|--------------|------|-------------|---------|
| 1 | Excellent | 100% | Good | Adequate | Strong |
| 2 | Good | 100% | Good | Adequate | Strong |
| 3 | Good | 100% | Good | Adequate | Strong |
| 4 | Excellent | 100% | Good | Missing formula | Good |
| 5 | Excellent | 100% | N/A (overview) | Good | Strong |
| 5a | Excellent | 100% | Excellent | Missing definitions | Good |
| 5b | Excellent | 100% | Excellent | Missing WAL details | Good |
| 5c | Good | 100% | Good | Minimal | Fair |
| 5d | Good | 100% | Good | Minimal | Fair |
| 5e | Good | 100% | Good | Minimal | Fair |
| 5f | Good | 100% | Good | Low | Weak |
| 5g | Good | 100% | Moderate | Low | Weak |
| 5h | Very Good | ~100% | Good | No starter code | Fair |
| 6 | Excellent | 100% | High | Good | Strong |
| 7 | Excellent | 100% | Very High | Good | Strong |
| 8 | Good | 100% | Good | Moderate | Good |
| 9 | Good | 100% | Good | Moderate | Good |
| 10 | Excellent | 100% | Good | Low for capstone | Good |

---

## Target Audience Assessment

- **Senior/staff engineers with systems background:** Ready to use as-is. They'll fill in the gaps from experience.
- **Mid-level engineers with Go experience:** Usable with mentorship. Need someone to answer "am I going deep enough?" questions.
- **Junior engineers or those new to storage systems:** Will struggle significantly without worked examples and scaffolding. The jump from Level 4 to Level 5a-5h is especially steep.

---

## Top 5 Recommendations (Priority Order)

1. **Add worked examples** to at least Levels 5a (simulator output), 5b (WAL record binary layout), 7 (heap merge trace), and 10 (capstone outline). Even 10-20 lines each would dramatically improve usability.

2. **Replace generic deliverable templates** with level-specific templates that include the lab instructions, expected output format, and self-assessment checklist.

3. **Add the Bloom FP formula to Level 4** and define "eager/multiplier-threshold/bounded fan-in merge" in Level 5a. Don't make learners hunt for foundational definitions.

4. **Add prerequisite knowledge lists** to each level and cross-reference specific concepts from prior levels.

5. **Provide a mini-LSM starter skeleton** for Level 5h (even just the directory structure with empty files and interface stubs). Building from absolute zero while also learning LSM concepts is two hard problems at once.
