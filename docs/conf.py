# Configuration file for the Sphinx documentation builder.

import os
import sys
sys.path.insert(0, os.path.abspath('../src'))

# -- Project information -----------------------------------------------------
project = 'Conveyor Streaming'
copyright = '2025, Conveyor Team'
author = 'Conveyor Team'
release = '1.0.0'

# -- General configuration ---------------------------------------------------
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    'myst_parser',
    'sphinxext.opengraph',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Specify the master document (without extension)
master_doc = 'index'

# Configure source file suffixes - simplified approach
source_suffix = ['.rst', '.md']

# MyST parser configuration - minimal setup
myst_enable_extensions = [
    "colon_fence",
    "deflist",
]

# -- Options for HTML output -------------------------------------------------
html_theme = 'furo'
html_static_path = ['_static']
html_title = 'Conveyor Streaming Documentation'

html_theme_options = {
    "sidebar_hide_name": True,
    "navigation_with_keys": True,
    "top_of_page_button": "edit",
    "source_repository": "https://github.com/ju-bezdek/conveyor",
    "source_branch": "main",
    "source_directory": "docs/",
}

# -- Extension configuration -------------------------------------------------
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False

# Fixed intersphinx mapping
intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
}

# Todo extension
todo_include_todos = True

# Suppress warnings for missing documents and other common issues
suppress_warnings = ['toc.not_readable', 'ref.any', 'myst.directive_unknown']