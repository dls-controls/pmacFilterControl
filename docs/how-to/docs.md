# Docs

The documentation uses
[sphinx](https://www.sphinx-doc.org/en/master/index.html) and [MyST](https://myst-parser.readthedocs.io/en/latest/index.html)
for written documentation along with
[doxygen](https://www.doxygen.nl/index.html) and [breathe](https://breathe.readthedocs.io/en/latest/)
to generate reference and integrate them into the docs.


## Building Docs

To generate the API reference docs using doxygen (requires `dot` for UML diagrams):

    $ cd docs/doxygen
    $ doxygen

```{note}
This is also done by running the CMake build.
```

To build and view the documentation:

```bash
$ python -m venv venv
$ source venv/bin/activate
$ cd pmacFilterControl
(venv) $ pip install -r docs/requirements.txt
(venv) $ sphinx-build -b html docs docs/_build/html
```

This will build the docs into `docs/_build/html`. They can be viewed by opening
`index.html` in a web browser. When editing the docs it is useful to get a live preview.
In this case, use `sphinx-autobuild`:

```bash
(venv) $ sphinx-autobuild docs docs/_build/html
...
The HTML pages are in docs/_build/html.
[sphinx-autobuild] Serving on http://127.0.0.1:8000
```

This will build the docs and host them on local web server to view them live
in a web browser. You can edit the files and your browser will automatically
reload with the live changes.

## Writing Docs

The source files are organised under `docs/` in the same structure they appear in the
toctree, which is defined in `docs/sphinx.yaml`. To add content to an existing page,
simply navigate to the markdown file for the page and edit it. To add a new page, create
a markdown file in the appropriate place in the hierarchy and then add it to
`sphinx.yaml`.

## Tables

Tables can be automatically formatted using a [vscode extension](https://marketplace.visualstudio.com/items?itemName=shuworks.vscode-table-formatter)
