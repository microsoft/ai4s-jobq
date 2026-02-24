"""Sphinx extension that replaces HTML-producing directives with plain alternatives.

Used when building markdown docs (SPHINX_MARKDOWN_BUILDER=1) so that
directives like ``.. prompt::`` produce clean markdown instead of raw HTML.
"""

from docutils import nodes  # type: ignore[import]
from docutils.parsers.rst import Directive  # type: ignore[import]


class MarkdownPrompt(Directive):
    """Replace ``.. prompt::`` with a plain code-block."""

    optional_arguments = 3  # e.g. bash $ auto
    has_content = True
    option_spec = {"substitutions": lambda x: x}

    def run(self):
        code = "\n".join(self.content)
        node = nodes.literal_block(code, code)
        node["language"] = "bash"
        return [node]


def setup(app):
    app.add_directive("prompt", MarkdownPrompt, override=True)
    return {"version": "0.1", "parallel_read_safe": True}
