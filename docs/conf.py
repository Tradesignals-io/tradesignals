"""Sphinx configuration."""
project = "Tradesignals Streaming"
author = "Mac Anderson"
copyright = "2024, Mac Anderson"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_click",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"
