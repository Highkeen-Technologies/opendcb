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
