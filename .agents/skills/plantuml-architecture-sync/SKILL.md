---
name: plantuml-architecture-sync
description: >-
  Use this skill when creating, updating, or rendering PlantUML (.puml)
  system design and sequence diagrams in docs/architecture/.
---

# Skill: Synchronize and Render PlantUML Architecture Diagrams

All system design diagrams live in `docs/architecture/` as version-controlled `.puml` files alongside their rendered `.svg` and `.png` outputs.

## Diagram Inventory
1. [`docs/architecture/system-architecture.puml`](../../../docs/architecture/system-architecture.puml) — 6-tier cloud-native architecture with explicit **Role**, **Why**, **Features**, and **Tech** inside each box.
2. [`docs/architecture/data-flow-sequence.puml`](../../../docs/architecture/data-flow-sequence.puml) — End-to-end synchronous fast path (`<15ms P99`) and asynchronous event streaming / OLAP ingestion sequence.
3. [`docs/architecture/scaling-resilience-topology.puml`](../../../docs/architecture/scaling-resilience-topology.puml) — 4-layer zero-repetition scaling (HPA vs KEDA), Resilience4j pipeline, and Multi-AZ HA topology.

## Verified PlantUML Layout & Syntax Rules (Zero Arrow Crossings)
1. **PlantUML `1.2020.02` Compatibility**:
   - Do NOT use `!theme` directives (unsupported in `1.2020.02`); use explicit `skinparam` blocks at the top of the file.
   - Use `""...""` for monospace code formatting inside labels instead of Markdown backticks (backticks render literally in PlantUML).
   - In sequence diagrams, use `par ... else ... end` (not `and`) for parallel fragments.
2. **Preventing Crossing Arrows & Repetition**:
   - Use `top to bottom direction` with `skinparam nodesep 45` and `skinparam ranksep 55`.
   - Order sibling components left-to-right within each tier to match the left-to-right order of their callers/targets in the adjacent tier (e.g., place `L1Cache` above `RiskSvc` and `L2Cache` above `IngestionSvc`) so vertical arrows never form an "X".
   - Avoid repeating identical component boxes across AZs; instead group by architectural layer and explain **Role / Why / Features** inside each component.
3. **Render Command (With `8192` Canvas Limit)**:
   Always pass `-DPLANTUML_LIMIT_SIZE=8192` and run `-tsvg` and `-tpng` as separate commands (passing both flags in one call only outputs the last format):
   ```bash
   plantuml -DPLANTUML_LIMIT_SIZE=8192 -tsvg docs/architecture/*.puml
   plantuml -DPLANTUML_LIMIT_SIZE=8192 -tpng docs/architecture/*.puml
   ```
4. **Visual Inspection**:
   After rendering, read the `.png` files to visually verify there are no crossed lines, overlapping labels, or clipped borders.
