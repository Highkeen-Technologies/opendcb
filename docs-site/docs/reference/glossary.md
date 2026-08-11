# Glossary

**Event Sourcing** — instead of storing only an object's current state,
you store every change (event) that ever happened to it, in order. Current
state is derived by replaying those events.

**Aggregate vs. Entity** — "Aggregate" is the traditional domain-driven-design
term for an object whose state is built from its own events. Axon
Framework 5 uses the term "Entity" (`@EventSourcedEntity`) for the same
idea — this site uses "entity" throughout to match Axon's own terminology.

**DCB (Dynamic Consistency Boundary)** — an alternative to hard-coded
aggregate boundaries, where a single command can validate consistency
against events tagged in a specific way, decided at the point of appending
rather than fixed by which aggregate "owns" an event ahead of time. This is
what makes OpenDCB's conflict-predicate-based `appendAtomically` possible.

**Event Store** — the database that holds every event, in order,
permanently. OpenDCB's `EventStoreStorage` is the interface any database
can implement to serve as one.

**Command vs. Event** — a command is a request to do something ("open this
account") that can be rejected. An event is a record of something that
already happened ("this account was opened") — it can't be un-happened,
only reacted to.

**Saga (Process Manager)** — something that coordinates a business process
spanning multiple steps over time, often across different services,
reacting to events as they arrive and issuing new commands in response.

**Bounded Context** — a boundary around a part of a system with its own
consistent model and language — for example, "orders" and "shipping" might
each be their own bounded context, each with its own database and its own
definition of what an "order" means to them.

**Idempotency** — doing something more than once has the same effect as
doing it once. Important for anything that might be retried or redelivered
(like a message from a broker), so a duplicate doesn't cause a duplicate
effect.

**Outbox Pattern** — a way of reliably publishing events to an external
system (like a message broker) by first writing them durably to your own
database, then having a separate process reliably relay them onward — so a
crash between "wrote to my database" and "published externally" can't lose
or duplicate the message.
