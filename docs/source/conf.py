# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys

sys.path.insert(0, os.path.abspath('../..'))

project = 'windwatts_data'
copyright = '2025, National Renewable Energy Laboratory (USA)'
author = 'Sameer Shaik'
release = '1.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration



extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc']

autoapi_type = 'python'
autoapi_dirs = ['../..']

html_theme = 'alabaster'

# -- Options for EPUB output
epub_show_urls = 'footnote'

html_context = {
    "display_github": True, # Integrate GitHub
    "github_repo": "wind-toolkit/wtk_led_client", # Repo name
    "github_version": "main", # Version
    "conf_py_path": "docs/source/", # Path in the checkout to the docs root
}

add_module_names = False

templates_path = ['_templates']
exclude_patterns = []

language = 'English'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output


html_static_path = ['_static']

repository_url = f"https://github.nrel.gov/wind-toolkit/wtk_led_client"
html_context = {
    "menu_links": [
        (
            '<i class="fa fa-github fa-fw"></i> Source Code',
            repository_url,
        ),
        (
            '<i class="fa fa-book fa-fw"></i> License',
            f"{repository_url}/blob/main/LICENSE",
        ),
    ],
}

