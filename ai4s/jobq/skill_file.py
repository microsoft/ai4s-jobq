# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import os

import asyncclick as click

SKILL_NAME = "ai4s-jobq-cli"

# Extra usage examples for commands where the Click help text alone
# doesn't demonstrate key options.
COMMAND_EXAMPLES = {
    "ai4s-jobq push": [
        "ai4s-jobq $JOBQ push -c 'echo hello'  # push a single command",
        "cat tasks.txt | ai4s-jobq $JOBQ push   # push commands from stdin",
    ],
    "ai4s-jobq worker": [
        "ai4s-jobq $JOBQ worker -n 4 --time-limit 12h  # 4 parallel workers, 12h limit",
    ],
    "ai4s-jobq peek": [
        "ai4s-jobq $JOBQ peek -n 5 --json  # peek at 5 messages as JSON",
    ],
}


def _format_param(param):
    """Format a single Click parameter (option or argument) as a string."""
    if isinstance(param, click.Option):
        decls = ", ".join(f"`{d}`" for d in param.opts + param.secondary_opts)
        parts = [f"  - {decls}"]
        if not param.is_flag:
            type_name = getattr(param.type, "name", None)
            if type_name:
                type_name = type_name.upper()
                if type_name not in ("TEXT", "STRING", "<LAMBDA>"):
                    parts.append(f" `{type_name}`")
        if param.help:
            parts.append(f": {param.help}")
        if param.default is not None and param.default != () and not param.is_flag:
            parts.append(f" (default: `{param.default}`)")
        if param.required:
            parts.append(" **(required)**")
        return "".join(parts)
    elif isinstance(param, click.Argument):
        name = param.human_readable_name
        parts = [f"  - `{name}`"]
        if param.required:
            parts.append(" **(required)**")
        if param.nargs == -1:
            parts.append(" (multiple)")
        return "".join(parts)
    return None


def _is_group(cmd):
    """Check if a command is a group (works with asyncclick patched classes)."""
    return hasattr(cmd, "list_commands") and hasattr(cmd, "get_command")


def _walk_commands(group, prefix="ai4s-jobq"):
    """Yield (full_name, command) tuples by walking the Click command tree."""
    if not _is_group(group):
        return
    ctx = click.Context(group, info_name=prefix)
    for name in sorted(group.list_commands(ctx)):
        cmd = group.get_command(ctx, name)
        if cmd is None or getattr(cmd, "hidden", False):
            continue
        # Skip the copilot-skill group itself from the skill reference
        if name == "copilot-skill":
            continue
        full_name = f"{prefix} {name}"
        yield full_name, cmd
        if _is_group(cmd):
            yield from _walk_commands(cmd, prefix=full_name)


def _generate_skill_md(cli_group):
    """Generate SKILL.md content from the Click command tree."""
    lines = []

    # YAML frontmatter
    lines.append("---")
    lines.append("name: ai4s-jobq-cli")
    lines.append("description: >-")
    lines.append(
        "  Authoritative reference for the ai4s-jobq CLI for managing"
        " distributed job queues on Azure Storage Queues and Azure Service Bus."
    )
    lines.append("---")
    lines.append("")
    lines.append("  When constructing ai4s-jobq commands, you MUST look up the exact option names")
    lines.append("  from this document. Do NOT guess or infer option names from other CLIs.")
    lines.append("  Every option for every command is listed below.")
    lines.append("")

    # Body
    lines.append("# ai4s-jobq CLI Reference")
    lines.append("")
    lines.append("This skill provides a complete reference of all `ai4s-jobq` CLI commands,")
    lines.append("their options, and arguments. Use it to help users construct correct")
    lines.append("`ai4s-jobq` command invocations.")
    lines.append("")
    lines.append(
        "`ai4s-jobq` is a distributed job queue built on Azure Storage Queues and"
        " Azure Service Bus. Users push tasks; workers pull and process them asynchronously."
    )
    lines.append("")
    lines.append("## Queue specification")
    lines.append("")
    lines.append(
        "All commands require a `SERVICE/QUEUE_NAME` argument (or the `JOBQ_STORAGE_QUEUE`"
        " environment variable):"
    )
    lines.append("")
    lines.append("```")
    lines.append("# Azure Storage Queue")
    lines.append("ai4s-jobq mystorageaccount/my-queue <command>")
    lines.append("")
    lines.append("# Azure Service Bus")
    lines.append("ai4s-jobq sb://myservicebus/my-queue <command>")
    lines.append("```")
    lines.append("")
    lines.append("## Tips for agents")
    lines.append("")
    lines.append(
        "**Only use options and arguments documented below.** Do not invent or guess"
        " option names. Every available option for each command is listed explicitly."
    )
    lines.append("")

    for full_name, cmd in _walk_commands(cli_group):
        lines.append(f"## `{full_name}`")
        lines.append("")
        help_text = (cmd.help or cmd.get_short_help_str(limit=300) or "").strip()
        if help_text:
            lines.append(help_text)
            lines.append("")

        # Show subcommands summary for groups
        if _is_group(cmd):
            ctx = click.Context(cmd, info_name=full_name)
            sub_names = cmd.list_commands(ctx)
            visible = [
                n for n in sub_names if not getattr(cmd.get_command(ctx, n), "hidden", False)
            ]
            if visible:
                lines.append("**Subcommands:** " + ", ".join(f"`{n}`" for n in sorted(visible)))
                lines.append("")

        # Options and arguments (filter out help)
        params = [p for p in cmd.params if p.name != "help"]
        formatted_params = [_format_param(p) for p in params]
        formatted_params = [f for f in formatted_params if f]
        if formatted_params:
            lines.append("**Parameters:**")
            lines.append("")
            for formatted in formatted_params:
                lines.append(formatted)
            lines.append("")

        # Usage examples
        examples = COMMAND_EXAMPLES.get(full_name)
        if examples:
            lines.append("**Examples:**")
            lines.append("")
            lines.append("```")
            for ex in examples:
                lines.append(ex)
            lines.append("```")
            lines.append("")

    return "\n".join(lines)


SKILL_DIR = os.path.join(os.path.expanduser("~"), ".copilot", "skills", SKILL_NAME)
DISABLED_STAMP = os.path.join(SKILL_DIR, ".disabled")


@click.group("copilot-skill")
def skill_file_cmd():
    """Manage the GitHub Copilot skill for the ai4s-jobq CLI."""
    pass


@skill_file_cmd.command("install")
def skill_file_install():
    """Install the ai4s-jobq Copilot skill to ~/.copilot/skills/."""
    import logging
    import shutil
    from importlib.resources import files as pkg_files

    from ai4s.jobq import __version__

    LOG = logging.getLogger("ai4s.jobq")

    # Remove disabled marker if present
    if os.path.exists(DISABLED_STAMP):
        os.remove(DISABLED_STAMP)

    # Try to use bundled skill data from package
    bundled = pkg_files("ai4s.jobq.data.skill").joinpath(SKILL_NAME)
    if bundled.is_dir() and bundled.joinpath("SKILL.md").is_file():
        shutil.rmtree(SKILL_DIR, ignore_errors=True)
        shutil.copytree(str(bundled), SKILL_DIR)
        # Write version stamp
        with open(os.path.join(SKILL_DIR, ".jobq-version"), "w") as f:
            f.write(__version__)
        LOG.info(f"Skill installed to {SKILL_DIR}")
    else:
        # Fallback: generate from live Click tree (no docs references)
        LOG.warning(
            "Bundled skill data not found. Generating from live CLI tree. "
            "Run 'python scripts/build_skill.py' to include documentation references."
        )
        from ai4s.jobq.cli import main

        content = _generate_skill_md(main)
        os.makedirs(SKILL_DIR, exist_ok=True)
        with open(os.path.join(SKILL_DIR, "SKILL.md"), "w") as f:
            f.write(content)
        with open(os.path.join(SKILL_DIR, ".jobq-version"), "w") as f:
            f.write(__version__)
        LOG.info(f"Skill file written to {os.path.join(SKILL_DIR, 'SKILL.md')}")

    click.echo(f"Copilot skill installed to {SKILL_DIR}")


@skill_file_cmd.command("clear")
def skill_file_clear():
    """Remove the ai4s-jobq Copilot skill and prevent auto-reinstall."""
    import shutil

    if os.path.exists(SKILL_DIR):
        shutil.rmtree(SKILL_DIR)
    # Leave a disabled marker so auto-install doesn't re-create it
    os.makedirs(SKILL_DIR, exist_ok=True)
    with open(DISABLED_STAMP, "w") as f:
        f.write(
            "Skill disabled by 'ai4s-jobq copilot-skill clear'."
            " Run 'ai4s-jobq copilot-skill install' to re-enable.\n"
        )
    click.echo("Skill removed. Run 'ai4s-jobq copilot-skill install' to re-enable.")


@skill_file_cmd.command("list")
def skill_file_list():
    """List all files that would be installed by the Copilot skill."""
    from importlib.resources import files as pkg_files

    bundled = pkg_files("ai4s.jobq.data.skill").joinpath(SKILL_NAME)
    if not bundled.is_dir():
        click.echo("No bundled skill data found.", err=True)
        raise SystemExit(1)

    def _walk(traversable, prefix=""):
        for item in sorted(traversable.iterdir(), key=lambda x: x.name):
            path = os.path.join(prefix, item.name) if prefix else item.name
            if item.is_file():
                click.echo(path)
            elif item.is_dir():
                _walk(item, path)

    _walk(bundled)
