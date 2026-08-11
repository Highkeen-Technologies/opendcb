# docs-site

Source for OpenDCB's user-facing documentation site (MkDocs + Material),
published to GitHub Pages by `.github/workflows/docs.yml` on every push to
`main` that touches this directory. This is separate from the repo's
`docs/` folder, which covers internal architecture/contributor docs.

## Preview locally

```bash
pip install -r requirements.txt
mkdocs serve -f mkdocs.yml
```

Then open http://127.0.0.1:8000/.
