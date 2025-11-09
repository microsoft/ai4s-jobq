# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
from typing import List

from ai4s.jobq import __version__ as version

# Use in Sphinx
release = version
rst_prolog = f"""
.. |version| replace:: {version}
"""


project = "AI for Science JobQ"
copyright = "2025, Thijs Vogels, Hannes Schulz, Stephanie Lanius"
author = "Thijs Vogels, Hannes Schulz, Stephanie Lanius"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx_sitemap",
    "sphinx_prompt",
    "sphinx_copybutton",
    "sphinx_markdown_builder",
]

templates_path = ["_templates"]
exclude_patterns: List[str] = []
html_favicon = "_static/favicon.png"

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_logo = "_static/logo.png"
html_css_files = ["custom.css"]
html_theme_options = {
    "logo_only": True,
    "display_version": False,
    "navigation_depth": 2,
}

html_context = {
    "display_github": True,  # Integrate GitHub
    "github_user": "msr-ai4science",
    "github_repo": "ai4s-jobq",
    "github_version": "main",  # or 'master' or any branch
    "conf_py_path": "/docs/",  # Path in the repo to your documentation source files
}

# during CI, we set this to localhost so that we can run pa11y over the page.
html_baseurl = os.environ.get("DOCS_BASEURL", "https://microsoft.github.io/ai4s-jobq/")


def setup(app):
    app.add_js_file("custom.js")
