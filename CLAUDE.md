# OpenDCB

Open-source Java toolkit providing DCB-capable event storage on any backing store, plus event routing, for event-sourcing frameworks — currently Axon Framework 5, designed so other frameworks can be added without touching the storage layer.

Hard rules:
- `eventstore-core` and every `eventstore-<provider>` module must have zero dependency on any specific framework (Axon or otherwise) — see @docs/ARCHITECTURE.md. Framework coupling only lives in `integrations/eventstore-<framework>` (`opendcb-conductor-bridge` is a deliberate, documented exception — it genuinely needs `org.axonframework` for `CommandGateway`, unlike `opendcb-scheduling-core`, which stays framework-agnostic).
- Never depend on anything under `io.axoniq.framework`. Only `org.axonframework` (Apache 2.0) is allowed there.
- Never guess a framework's API signatures — check the real source before implementing against it.
- Package root is `com.highkeen.opendcb`.
- New storage providers must pass the shared contract suite in @docs/TESTING.md before merging.

See @docs/ARCHITECTURE.md (module layout), @docs/CONVENTIONS.md (coding standards), @docs/PROVIDERS.md (adding a provider), @docs/ROADMAP.md (status/priorities).
