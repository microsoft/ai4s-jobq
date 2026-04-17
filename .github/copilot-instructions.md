# Copilot Instructions for ai4s-jobq

## Build & Test

Tests require [Azurite](https://github.com/Azure/Azurite) (Azure Storage emulator) running locally:

```bash
# Start Azurite (blob on 10000, queue on 10001)
./node_modules/.bin/azurite-blob --skipApiVersionCheck --inMemoryPersistence --blobPort 10000 &
./node_modules/.bin/azurite-queue --skipApiVersionCheck --inMemoryPersistence --queuePort 10001 &

# Install in dev mode and run tests
pip install -e .[dev]
pytest

# Run a single test
pytest tests/test_cli.py::test_name -x

# Run live Azure tests (requires real Azure credentials)
pytest --run-live
```

## Lint & Format

Pre-commit hooks run ruff (lint + format), mypy, and security checks:

```bash
pre-commit run --all-files

# Or individually:
ruff check --fix .
ruff format .
mypy ai4s/ --no-namespace-packages
```

Ruff is configured with 100-char line length, 25+ rule groups (security, bugbear, naming,
async, performance, etc.), and extensive per-file-ignores for intentional patterns.
See `pyproject.toml` `[tool.ruff.lint]` for the full configuration.

Mypy runs with `check_untyped_defs`, `warn_redundant_casts`, `warn_unused_ignores`,
`warn_return_any`, and `no_implicit_reexport` enabled. The pre-commit hook installs
the package itself as a dependency for full type coverage.

## Documentation Lint

Docs use a mix of RST (`docs/*.rst`) and Markdown (`docs/**/*.md`, `README.md`, `CONTRIBUTING.md`),
built with Sphinx + myst_parser. Three doc linters run as pre-commit hooks:

```bash
# Run all doc linters via pre-commit
pre-commit run doc8 --all-files
pre-commit run markdownlint --all-files
pre-commit run vale --all-files

# Or individually:
doc8                                                    # RST only
npx markdownlint-cli '**/*.md' --ignore node_modules --ignore vale-styles  # Markdown only
vale docs/ README.md CONTRIBUTING.md SUPPORT.md         # prose style (MD + RST)
```

**doc8** checks RST structure (line length ≤ 100, whitespace, syntax). Config in `pyproject.toml`
under `[tool.doc8]`.

**markdownlint** checks Markdown formatting. Config in `.markdownlint.json` (MD013 line-length
disabled, MD024 siblings_only). Files excluded in `.markdownlintignore` (CHANGELOG.md,
`ai4s/jobq/data/`).

**Vale** checks prose style using the Google style guide as a base, plus custom rules in
`vale-styles/JobQ/`. Config in `.vale.ini`. Key points:

- Run `vale sync` after cloning to download the Google style package (gitignored under
  `vale-styles/Google/`).
- Custom vocabulary in `vale-styles/config/vocabularies/JobQ/accept.txt` — add new technical
  terms here when Vale flags legitimate project jargon as spelling errors.
- Custom rules: `JobQ.Headings` (sentence-case with acronym exceptions),
  `JobQ.Latin` (prefer "for example" over "e.g.").
- The pre-commit hook runs with `--minAlertLevel error` so warnings don't block commits.
- CHANGELOG.md has `Vale.Repetition` and `JobQ.Headings` disabled (changelog entries
  naturally repeat words and use an all-caps title).
- Noisy rules (passive voice, contractions, parenthetical usage) are disabled — see `.vale.ini`.

When writing or editing docs, prefer plain English over Latin abbreviations ("for example" not
"e.g.", "that is" not "i.e."), use em-dashes without spaces ("word—word" not "word — word"),
and keep headings in sentence case.

## Architecture

**`ai4s.jobq`** is a distributed job queue built on Azure Storage Queues and Azure Service Bus. Users push tasks; workers pull and process them asynchronously.

### Core layers

- **`entities.py`** — Data classes (`Task`, `Response`, `EmptyQueue`, `WorkerCanceled`). Tasks serialize to JSON with versioned schemas.
- **`jobq.py`** — `JobQ` class: the main API. Created via async context managers (`from_storage_queue`, `from_service_bus`, `from_connection_string`, `from_environment`). `JobQFuture` provides awaitable results.
- **`backend/`** — Queue backend implementations behind Protocol classes (`JobQBackend`, `JobQBackendWorker`, `Envelope`) defined in `backend/common.py`. Backends: `storage_queue.py` (Azure Storage Queue) and `servicebus.py` (Azure Service Bus).
- **`work.py`** — Worker-side processing: `Processor` ABC, `ProcessPool` for parallel execution, `ShellCommandProcessor` for CLI-driven tasks.
- **`orchestration/`** — Higher-level coordination: `batch_enqueue`, `launch_workers`, workforce management, multi-region support.
- **`cli.py`** — CLI entry point (`ai4s-jobq`) built with `asyncclick`. Subcommands: push, worker, peek, clear, etc.
- **`track/`** — Optional Dash-based monitoring dashboard (installed via `pip install ai4s-jobq[track]`).

### Key patterns

- **Async-first**: All queue operations are async. Tests use `pytest-asyncio` with `--asyncio-mode=auto` (no need for `@pytest.mark.asyncio`).
- **Async context managers**: `JobQ` instances must be used as `async with` context managers for proper resource cleanup.
- **Protocol-based backends**: `backend/common.py` defines Protocol classes; new backends implement these without inheritance.
- **Namespace package**: `ai4s/` uses `pkgutil.extend_path` — the `ai4s/__init__.py` must not contain regular imports.
- **Environment-driven config**: `JOBQ_STORAGE`, `JOBQ_QUEUE`, `JOBQ_USE_MONTY_JSON`, `JOBQ_DETERMINISTIC_IDS` control runtime behavior.

## Public Repository Policy

This is a **public** Microsoft repository. Never include internal or customer-specific
information in commit messages, PR descriptions, comments, or code:

- No Azure subscription IDs, resource group names, workspace names, or run/job IDs
- No internal hostnames or ingestion endpoints (e.g. `*.in.applicationinsights.azure.com`)
- No specific customer or team names, internal project codenames
- No specific region + SKU combinations that reveal internal infrastructure
- Keep descriptions generic: say "certain GPU SKUs" not "MI200 nodes in westus3"

## Conventions

- Copyright header `# Copyright (c) Microsoft Corporation.` + `# Licensed under the MIT License.` at the top of every source file.
- Python 3.10+ required. Type hints are used throughout; mypy is configured with `strict_optional = true`.
- Logging uses `logging.getLogger("ai4s.jobq")` (or `__name__` in submodules).
- `asyncclick` (not standard `click`) for all CLI commands.
