---
title: RevOps Context Graph
slug: revops
---

# Context Graph Example

A CRM / RevOps context graph that captures the full physics of execution — from intelligence capture through enrichment, screening, decision-making, and delivery. This example demonstrates NanoGraph's ability to model complex real-world workflows with typed nodes, edges, and decision traces.

Source files: [`examples/revops/`](https://github.com/omnilake/nanograph/tree/main/examples/revops)

## The trace

The graph captures a three-phase execution cycle. Each phase builds on the previous.

### Phase 1 — Intelligence: Signal surfaces an Opportunity

Jamie's coffee chat produces a Signal. The Signal links to who it's about and what deal it reveals.

```mermaid
graph LR
    Jamie["Jamie Lee (Client)"]
    Coffee["Coffee Chat (Record)"]
    Signal["Hates vendor (Signal)"]
    Priya["Priya Shah (Client)"]
    Opp["Stripe Migration (Opportunity)"]

    Jamie -->|ClientOwnsRecord| Coffee
    Signal -->|SourcedFrom| Coffee
    Signal -->|SignalAffects| Priya
    Signal -->|Surfaced| Opp
    Priya -->|ClientOwnsOpportunity| Opp
```

### Phase 2 — Enrichment & Screening: Agent builds the case

The agent reads the Screening Policy, generates an Enrichment Profile based on its criteria, then the Decision is screened and passes.

```mermaid
graph LR
    Decision["Make Proposal (Decision)"]
    Andrew["Andrew (Actor)"]
    Signal["Hates vendor (Signal)"]
    Priya["Priya Shah (Client)"]
    Opp["Stripe Migration (Opportunity)"]
    Policy["Screening Policy (Policy)"]
    PolicyV1["Screening Policy v1 (Policy)"]
    Action["Build Profile (Action)"]
    Profile["Enrichment Profile (Record)"]

    Decision -->|MadeBy| Andrew
    Decision -->|InformedBy| Signal
    Decision -->|DecisionAffects| Priya
    Decision -->|Targets| Opp
    Decision -->|ScreenedBy| Policy
    Policy -->|Supersedes| PolicyV1
    Decision -->|ResultedIn| Action
    Action -->|TouchedRecord| Profile
    Profile -->|DerivedFrom| Policy
```

### Phase 3 — Execution: Work is assigned, completed, and proven

The Decision generates work. The agent executes it, creating the proposal and advancing the deal.

```mermaid
graph LR
    Decision["Make Proposal (Decision)"]
    Draft["Draft Proposal (ActionItem)"]
    OmniBot["OmniBot (Actor)"]
    SentAction["Sent Proposal (Action)"]
    Proposal["Proposal.pdf (Record)"]
    Opp["Stripe Migration (Opportunity)"]
    Pipeline["Data Pipeline (Project)"]

    Decision -->|DecisionGenerates| Draft
    Draft -->|AssignedTo| OmniBot
    Decision -->|ResultedIn| SentAction
    SentAction -->|Resolves| Draft
    SentAction -->|TouchedRecord| Proposal
    SentAction -->|TouchedOpportunity| Opp
    Opp -->|Contains| Pipeline
```
