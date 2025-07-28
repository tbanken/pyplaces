# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
# import sys
# from pathlib import Path
# sys.path.insert(0, str(Path('../..', 'src').resolve()))


project = 'pyplaces'
copyright = '2025, Theodore Banken'
author = 'Theodore Banken'
release = '0.3.3'
#TODO dynamically get release
# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.autodoc','sphinx_autodoc_typehints','nbsphinx']

templates_path = ['_templates']
exclude_patterns = []

# html_theme_options = {
#     "home_page_in_toc": True,
#     "collapse_navbar": True,
# }
nbsphinx_execute = 'never'

suppress_warnings = [
    'nbsphinx',
]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_book_theme'
html_static_path = ['_static']
