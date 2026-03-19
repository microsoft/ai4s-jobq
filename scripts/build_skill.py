#!/usr/bin/env python3
"""Build the ai4s-jobq-cli skill directory for packaging.

This script:
1. Builds sphinx docs as markdown using sphinx_markdown_builder
2. Generates SKILL.md from the Click command tree
3. Assembles everything into ai4s/jobq/data/skill/ai4s-jobq-cli/

Run from the repository root:
    python scripts/build_skill.py
"""

import os
import shutil
import subprocess
import sys
import tempfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKILL_DATA_DIR = os.path.join(REPO_ROOT, "ai4s", "jobq", "data", "skill", "ai4s-jobq-cli")
DOC_DIR = os.path.join(REPO_ROOT, "docs")

# Doc pages to include as references, grouped by topic.
# Keys become filenames under references/, values are source doc paths.
REFERENCE_PAGES = {
    "basics": "basics",
    "api": "api",
    "monitoring": "monitoring",
    "dead-lettering": "misc/00_dead-lettering",
    "preemption": "misc/10-preemption",
    "large-objects": "misc/20-large-objects",
    "workforce": "misc/30-workforce",
    "graceful-shutdown": "misc/35-graceful-shutdown",
    "deduplication": "misc/40-deduplication",
}

# Curated index of reference docs, grouped by topic with descriptions.
REFERENCE_INDEX = [
    (
        "Getting Started",
        [
            ("basics", "Choosing a backend, queue specification, pushing and pulling jobs"),
            ("api", "Python API for queueing and running tasks programmatically"),
        ],
    ),
    (
        "Operations",
        [
            ("monitoring", "Monitoring with Azure Application Insights and Grafana"),
            ("workforce", "Managing worker fleets with Azure ML"),
            ("graceful-shutdown", "Graceful shutdown and preemption handling"),
            ("preemption", "Dealing with preempted workers"),
        ],
    ),
    (
        "Advanced Topics",
        [
            ("dead-lettering", "Dead-letter queues for failed messages"),
            ("large-objects", "Handling large payloads via blob storage"),
            ("deduplication", "Duplicate detection with Service Bus"),
        ],
    ),
]


def build_markdown_docs(output_dir):
    """Run sphinx-build with markdown builder."""
    env = os.environ.copy()
    env["SPHINX_MARKDOWN_BUILDER"] = "1"
    subprocess.check_call(
        [sys.executable, "-m", "sphinx", "-b", "markdown", DOC_DIR, output_dir],
        env=env,
    )


def validate_reference_index():
    """Validate that the index and REFERENCE_PAGES are in sync."""
    indexed_names = set()
    errors = []
    for _group_name, entries in REFERENCE_INDEX:
        for ref_name, _desc in entries:
            if ref_name in indexed_names:
                errors.append(f"Duplicate index entry: {ref_name}")
            indexed_names.add(ref_name)
            if ref_name not in REFERENCE_PAGES:
                errors.append(f"Index entry {ref_name!r} not found in REFERENCE_PAGES")

    unindexed = set(REFERENCE_PAGES) - indexed_names
    if unindexed:
        errors.append(f"REFERENCE_PAGES entries not in index: {', '.join(sorted(unindexed))}")

    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


def generate_reference_index(refs_dir):
    """Generate the Documentation References markdown section."""
    lines = [
        "## Documentation References",
        "",
        "Detailed documentation is available in the `references/` directory.",
        "Load these on demand for in-depth information on specific topics.",
        "",
    ]
    for group_name, entries in REFERENCE_INDEX:
        lines.append(f"### {group_name}")
        lines.append("")
        for ref_name, desc in entries:
            ref_file = os.path.join(refs_dir, f"{ref_name}.md")
            if os.path.exists(ref_file):
                lines.append(f"- [{desc}](references/{ref_name}.md)")
        lines.append("")
    return "\n".join(lines)


def generate_skill_md():
    """Generate SKILL.md content from the Click command tree."""
    from ai4s.jobq.cli import main
    from ai4s.jobq.skill_file import _generate_skill_md

    return _generate_skill_md(main)


def main_func():
    # Validate index consistency before doing any work
    validate_reference_index()

    # Clean previous output
    if os.path.exists(SKILL_DATA_DIR):
        shutil.rmtree(SKILL_DATA_DIR)
    os.makedirs(SKILL_DATA_DIR, exist_ok=True)

    # 1. Build markdown docs into a temp dir
    with tempfile.TemporaryDirectory() as tmpdir:
        print("Building markdown docs...")
        build_markdown_docs(tmpdir)

        # 2. Copy selected reference pages
        refs_dir = os.path.join(SKILL_DATA_DIR, "references")
        os.makedirs(refs_dir, exist_ok=True)
        missing = []
        for ref_name, doc_path in sorted(REFERENCE_PAGES.items()):
            src = os.path.join(tmpdir, f"{doc_path}.md")
            if os.path.exists(src):
                dst = os.path.join(refs_dir, f"{ref_name}.md")
                shutil.copy2(src, dst)
                print(f"  {ref_name}.md <- {doc_path}.md")
            else:
                missing.append(f"{ref_name} ({doc_path}.md)")

        if missing:
            print(
                f"\nERROR: {len(missing)} reference doc(s) not found in sphinx output:",
                file=sys.stderr,
            )
            for m in missing:
                print(f"  - {m}", file=sys.stderr)
            sys.exit(1)

    ref_index = generate_reference_index(os.path.join(SKILL_DATA_DIR, "references"))

    # 3. Generate SKILL.md
    print("Generating SKILL.md...")
    content = generate_skill_md()
    content += "\n" + ref_index
    with open(os.path.join(SKILL_DATA_DIR, "SKILL.md"), "w") as f:
        f.write(content)

    total_files = sum(1 for _ in os.listdir(os.path.join(SKILL_DATA_DIR, "references")))
    print(f"\nSkill directory built: {SKILL_DATA_DIR}")
    print(f"  SKILL.md + {total_files} reference files")


if __name__ == "__main__":
    main_func()
