# Event Sourcing in Plain English

## Your bank balance vs. your bank statement

Think about how your bank actually tracks your money. It doesn't store a
single number, "$1,347.52", that gets overwritten every time you spend or
earn something. It stores a list: every deposit, every withdrawal, every
transfer, each with a timestamp — a *statement*. Your balance isn't stored
anywhere; it's *computed* by adding up every line on that statement.

This has a few consequences worth noticing:

- **The full history is never lost.** You can ask "what was my balance last
  Tuesday?" and get a real answer, because nothing was ever thrown away.
- **The current state is a derived fact, not a stored one.** "Balance" is
  just "the running total of the statement so far."
- **Mistakes are corrected by adding a new line, not editing an old one.** A
  bank doesn't quietly change a past transaction; it reverses it with
  another transaction. The history stays honest.

Event sourcing applies exactly this idea to application data. Instead of a
database row that gets overwritten (`UPDATE accounts SET balance = 1347.52
WHERE id = ...`), you store a sequence of **events** — facts that happened,
in order — and any "current state" you need is computed by replaying them.

## From bank statement to "event"

In an event-sourced system, `AccountOpened` and `MoneyDeposited` are events
— each one a small, immutable record of something that happened. An
`AccountEntity`'s balance isn't stored directly; it's rebuilt by starting
from zero and replaying every event for that account in order, the same way
your balance is rebuilt by summing your statement.

```java
@EventSourcingHandler
void evolve(MoneyDeposited event) {
    this.balance = this.balance.add(event.amount());
}
```

That one method *is* the "how do I compute current state from history"
logic — nothing more.

## Why bother?

- **A complete audit trail comes for free.** You never have to ask "how did
  we get here?" separately from the data itself — the events *are* the
  answer.
- **You can add new ways of looking at old data.** A new read model, report,
  or projection is just "replay the same events differently" — no backfill
  migration required, since the raw facts were never lost.
- **Debugging a "how did this ever happen" bug becomes tractable.** You can
  replay the exact sequence of events that led to a bad state and watch it
  happen step by step.

The trade-off: you need somewhere durable to store that ordered sequence of
events, and a way to safely append to it without two writers stepping on
each other. That's exactly what `eventstore-core`/`eventstore-postgres`
provide — see [What is DCB](what-is-dcb.md) for the specific problem OpenDCB
solves on top of plain event storage: making sure concurrent writers don't
silently violate a business rule that spans more than one entity.
