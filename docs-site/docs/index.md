# OpenDCB

OpenDCB is an open-source Java toolkit that gives you event storage with
built-in conflict detection (DCB — Dynamic Consistency Boundary) on top of
a database you already run, plus tools to route and relay those events —
for [Axon Framework 5](https://github.com/AxonFramework/AxonFramework)
today, and designed so other event-sourcing frameworks could plug in later
without touching the storage layer.

## The gap it fills

Axon Framework 5 itself is free and open source (Apache 2.0). But DCB-aware
event storage and multi-instance event routing — the pieces you need to
actually run it in production without a single point of failure — are only
shipped as part of **Axon Server**, which is free for a single node and
paid beyond that. OpenDCB fills that gap with a self-hosted alternative
built on databases you already know how to operate.

| | Axon Server (paid tiers) | OpenDCB |
|---|---|---|
| Event storage | Axon Server's own storage engine | Your own PostgreSQL (MySQL/MongoDB planned) |
| Multi-instance routing | Server-managed segment push | Axon's `JdbcTokenStore`, backed by your database |
| Clustering / HA | Yes (paid) | Not provided — use your database's own HA story |
| Hosting | Runs Axon Server as its own service | Just your app + your database |
| Cost | Free single-node, paid beyond that | Apache 2.0, free at any scale |

OpenDCB doesn't try to match Axon Server feature-for-feature — there's no
clustering or Multi-Context here. It targets a specific, common tier:
self-hosted, single-team deployments that don't need enterprise HA yet.

## Who it's for

- Teams building on Axon Framework 5 who want DCB-capable event storage
  without adopting Axon Server.
- Teams who already run PostgreSQL and would rather not run and license a
  separate event-store service.
- Teams that may eventually split a monolith into event-driven
  microservices and want an outbox/relay story ready when they do.

## Who it isn't for

- Teams that need Axon Server's clustering/HA replication or
  Multi-Context features today — that's a genuine reason to use Axon
  Server instead.
- Teams not using an event-sourcing framework at all — OpenDCB's adapters
  are built against Axon Framework 5's real SPIs, not a generic
  persistence library.

Ready to try it? Head to **Getting Started** for a working example in
either a Spring Boot or plain Java project.
