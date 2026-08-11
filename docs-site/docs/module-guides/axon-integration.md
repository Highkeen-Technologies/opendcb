# Axon Integration

Axon Framework is an event-sourcing framework — it handles dispatching
commands, sourcing entities from their event history, and more. Axon
defines its own interface, `EventStorageEngine`, for reading and writing
events, but leaves the actual implementation up to whoever provides one.
`integrations/eventstore-axon` is OpenDCB's implementation of that
interface — the adapter that lets Axon talk to any database OpenDCB
supports.

## What the adapter actually does

The class doing this work, `AbstractDcbEventStorageEngine`, has one job:
translate between Axon's types and OpenDCB's own `StoredEvent`/
`EventStoreStorage`, in both directions. When Axon appends events, this
class converts them into `StoredEvent`s and hands them to
`EventStoreStorage`. When Axon reads events back — to source an entity's
history, for example — this class converts `StoredEvent`s back into the
types Axon expects.

This is deliberately the *only* place in the whole toolkit that imports
anything from `org.axonframework`. Everything below it — the storage
layer — has no idea Axon exists. That's what makes it possible to add
support for a different event-sourcing framework later without touching
storage at all: just one new adapter module, following this same shape.

Payloads (the actual event data) are converted to and from JSON using
Axon's own `Converter` interface, so serialization behaves exactly as it
would in any other Axon Framework application.

## Entities are an Axon concept, not OpenDCB's

If you've looked at the [Quickstart](../getting-started/spring-boot-quickstart.md)
examples, you've seen classes annotated with `@EventSourcedEntity` and
`@EventSourcingHandler`. These belong entirely to Axon Framework itself —
OpenDCB doesn't define or modify them. Once `AbstractDcbEventStorageEngine`
is wired in as your `EventStorageEngine`, you write entities exactly as
Axon's own documentation describes; OpenDCB just supplies the events
underneath.

## A known limitation: no event upcasting yet

Axon Framework has a concept called "upcasting" — translating an old
version of an event's payload into a newer shape, so you can change an
event's structure over time without breaking replay of old events. As of
Axon Framework 5.1.2, there's no released upcaster mechanism to plug into
— the only such code lives in an internal module Axon's own maintainers
have explicitly marked as not meant for release. Because there's nothing
released to integrate with, `AbstractDcbEventStorageEngine` doesn't apply
any upcasting today. In practice: once you publish an event's shape, avoid
changing its structure in a way that isn't backward-compatible. See the
[FAQ](../faq-and-troubleshooting.md) for more, and revisit this page once
Axon ships a released mechanism.
