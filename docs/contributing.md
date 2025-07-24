# Contributions 

This repository is an open-source mirror of our internal monorepo. Contributions via issues and pull requests are welcome.

## Installing Development Dependencies

```bash
pip install -e '.[dev]'
```

## Running Unit Tests

```bash
pip install tox tox-conda
tox -e py310
```

## Releasing

To release docs, run `make release-docs` in the root directory of the repository.
Make sure you have write permissions on the `gh-pages` branch.

We're currently working on releasing the package to an (internal or public)
package index.
