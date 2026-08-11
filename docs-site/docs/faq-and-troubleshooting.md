# FAQ and Troubleshooting

## Is this free?

Yes. OpenDCB is Apache 2.0, every dependency it pulls in is free, and
`opendcb-conductor-bridge`'s one external server dependency (Conductor OSS)
is free at every tier too. This is a deliberate contrast with Axon
Server, which has paid tiers for things like clustering and its
`JdbcTokenStore` equivalent — OpenDCB exists specifically to fill that gap
without requiring Axon Server at all.

## Do I need Axon Server?

No. OpenDCB's whole storage layer runs against a plain PostgreSQL database
and your own JVM — no Axon Server, ever. `opendcb-axon-spring-boot-routing`
gives you multi-instance scaling via Axon's own free `JdbcTokenStore`
instead.

## Which databases are supported today?

Only PostgreSQL, via `eventstore-postgres`. `eventstore-mysql` and
`eventstore-mongo` are design templates in the roadmap — documented shape
and locking strategy, no working code. See
[Choosing a Storage Provider](setup/choosing-a-storage-provider.md).

## Why isn't my event appending?

Work through these in order:

1. **Is the schema created?** By default OpenDCB creates the `events`/
   `event_tags` tables (or `scheduled_event`/`saga_correlation` for those
   modules) automatically on first connection. If you've set
   `autoCreateSchema=false` anywhere, confirm the tables actually exist.
2. **Is your conflict predicate matching unexpectedly?** `appendAtomically`
   rejects the append (throwing `ConcurrentAppendConflictException`) if
   your predicate matches any event in the checked tail. A predicate that's
   broader than intended will reject appends that should have succeeded.
3. **Is the connection actually pointed at the right database?** Especially
   relevant if you're relying on the Spring Boot starter's fallback to your
   application's primary `DataSource` — check the `INFO` log line it emits
   when it does this, and confirm that's really the database you meant.

## Does this support event upcasting/schema evolution?

No, not yet, and honestly: not because it was skipped, but because Axon
Framework 5.1.2 itself has no released upcaster SPI to integrate with — the
only such code lives in an internal Axon module explicitly marked "not to
be released." `integrations/eventstore-axon` documents this as a known
limitation rather than a bug. In practice, avoid non-backward-compatible
changes to an event's shape once it's published. See
[Axon Integration](module-guides/axon-integration.md#a-known-limitation-no-event-upcasting-yet)
for more.

## What if I need Kafka instead of RabbitMQ?

Not built yet. `outbox-relay-rabbitmq` is the only working transport today;
`outbox-relay-kafka` is on the roadmap but has no code. `outbox-relay-core`'s
`Publisher` interface is transport-agnostic, so implementing your own Kafka
publisher against it today is straightforward if you need it before the
official module exists — follow `RabbitMqPublisher`'s shape (retryable vs.
non-retryable exception handling) as the reference.

## What's next

See [Module and Package Reference](reference/module-and-package-reference.md)
for the status of every module at a glance.
