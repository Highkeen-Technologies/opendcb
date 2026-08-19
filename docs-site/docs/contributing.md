# Contributing

This site is the **user-facing** documentation — quickstarts, concepts, and
guides for people building on top of OpenDCB. It's deliberately separate
from the repo's internal architecture documentation, which lives in
[`docs/`](https://github.com/Highkeen-Technologies/opendcb/tree/main/docs)
at the repo root and is aimed at contributors working *on* OpenDCB itself:
module dependency rules, coding conventions, how to add a new storage
provider, and the testing standards every module is held to.

If you want to contribute code, start with:

- [`CLAUDE.md`](https://github.com/Highkeen-Technologies/opendcb/blob/main/CLAUDE.md) —
  the hard architectural rules (framework-coupling boundaries, licensing
  constraints) that apply to every change.
- [`docs/ARCHITECTURE.md`](https://github.com/Highkeen-Technologies/opendcb/blob/main/docs/ARCHITECTURE.md) —
  module layout and dependency order.
- [`docs/CONVENTIONS.md`](https://github.com/Highkeen-Technologies/opendcb/blob/main/docs/CONVENTIONS.md) —
  coding standards.
- [`docs/PROVIDERS.md`](https://github.com/Highkeen-Technologies/opendcb/blob/main/docs/PROVIDERS.md) —
  the steps for adding a new storage provider.
- [`docs/TESTING.md`](https://github.com/Highkeen-Technologies/opendcb/blob/main/docs/TESTING.md) —
  the shared contract-test suite every provider must pass.
- [`docs/ROADMAP.md`](https://github.com/Highkeen-Technologies/opendcb/blob/main/docs/ROADMAP.md) —
  current module status and design history.

## Workflow and branch protection

`main` is protected: every change lands via a pull request, and both of
`ci.yml`'s checks — `Architectural boundary checks` and `Build and test
(full reactor)` — must pass before a PR can merge. Direct pushes to `main`
are blocked for everyone, with one narrow, deliberate exception described
below.

**Approving-review requirement is currently relaxed to zero.** OpenDCB is,
per `AUTHORS.md`, a single-maintainer project right now — requiring a
second approving review with nobody else able to give one would block
every PR indefinitely, which defeats the point of the requirement rather
than enforcing anything. This is a deliberate, temporary relaxation, not a
signal that review doesn't matter: both CI checks above still gate every
merge regardless of team size, and `required_approving_review_count`
should be raised back to at least `1` as soon as a second regular
contributor joins the project.

**Branch off `main`, open a PR.** There's no separate development branch —
`main` is always the integration branch. Cut your working branch from
`main`, push it, and open a PR against `main` once `ci.yml` is green
locally (or once you're ready for CI to run it for you).

**Shared local-dev-default version.** Every module builds against two
properties declared once in the root `pom.xml` — `opendcb.version` (the
framework-agnostic module group) and `opendcb-axon.version` (the
Axon-coupled group) — see `docs/ROADMAP.md`'s "Versioning" section for the
full two-group split and why they're independent. There's no
`-SNAPSHOT` suffix anywhere in this scheme: the committed values are
literal, shared version numbers every branch and PR builds against
locally and in CI. They're local-dev defaults only, never what actually
gets published — `release.yml` overrides them with `-D` flags resolved
from the release tag (or `workflow_dispatch` input) at publish time, and
automatically bumps the committed values back to match immediately after
a successful release, so `main` always reflects the last shipped version
rather than drifting stale. No PR should ever exist whose sole purpose is
bumping these two numbers by hand.

**`release.yml` is never triggered by a pull request.** It only runs on a
pushed `v*.*.*` tag or a manually dispatched `workflow_dispatch` run — an
ordinary PR merge into `main` never publishes anything. Its own final
step (bumping the committed versions above) is the one exception to the
no-direct-push rule: it authenticates as the `opendcb-release-bot` GitHub
App — a narrowly-scoped App (`contents: read-and-write`, installed only on
this repository) — rather than the default `GITHUB_TOKEN`, since
`GITHUB_TOKEN` cannot bypass a protected branch under any permissions
setting. This bypass was verified directly against a real ruleset before
being relied on: a plain human push to a protected branch is rejected
(`GH013`), while a push authenticated as the App's installation token
succeeds — proven both on a disposable scratch branch and against `main`
itself, with the test commits and rulesets cleaned up immediately after.

## Contributing to this site instead

This site's source lives under `docs-site/` in the same repository, built
with [MkDocs](https://www.mkdocs.org/) and the
[Material theme](https://squidfunk.github.io/mkdocs-material/). Every page
has an "Edit this page" link (bottom of the page, or in the header on
wide screens) that takes you straight to the GitHub editor for that file.

To preview changes locally:

```bash
pip install -r docs-site/requirements.txt
mkdocs serve -f docs-site/mkdocs.yml
```
