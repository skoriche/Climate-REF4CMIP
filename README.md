# CMIP Rapid Evaluation Framework

<!---
Can use start-after and end-before directives in docs, see
https://myst-parser.readthedocs.io/en/latest/syntax/organising_content.html#inserting-other-documents-directly-into-the-current-document
-->

<!--- sec-begin-description -->

Rapid evaluating CMIP data



[![CI](https://github.com/CMIP-REF/cmip-ref/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/CMIP-REF/cmip-ref/actions/workflows/ci.yaml)
[![Coverage](https://codecov.io/gh/CMIP-REF/cmip-ref/branch/main/graph/badge.svg)](https://codecov.io/gh/CMIP-REF/cmip-ref)
[![Docs](https://readthedocs.org/projects/cmip-ref/badge/?version=latest)](https://cmip-ref.readthedocs.io)

**PyPI :**
[![PyPI](https://img.shields.io/pypi/v/cmip-ref.svg)](https://pypi.org/project/cmip-ref/)
[![PyPI: Supported Python versions](https://img.shields.io/pypi/pyversions/cmip-ref.svg)](https://pypi.org/project/cmip-ref/)
[![PyPI install](https://github.com/CMIP-REF/cmip-ref/actions/workflows/install.yaml/badge.svg?branch=main)](https://github.com/CMIP-REF/cmip-ref/actions/workflows/install.yaml)

**Other info :**
[![Licence](https://img.shields.io/github/license/CMIP-REF/cmip-ref.svg)](https://github.com/CMIP-REF/cmip-ref/blob/main/LICENCE)
[![Last Commit](https://img.shields.io/github/last-commit/CMIP-REF/cmip-ref.svg)](https://github.com/CMIP-REF/cmip-ref/commits/main)
[![Contributors](https://img.shields.io/github/contributors/CMIP-REF/cmip-ref.svg)](https://github.com/CMIP-REF/cmip-ref/graphs/contributors)


<!--- sec-end-description -->

Full documentation can be found at:
[cmip-ref.readthedocs.io](https://cmip-ref.readthedocs.io/en/latest/).
We recommend reading the docs there because the internal documentation links
don't render correctly on GitHub's viewer.

## Installation

<!--- sec-begin-installation -->

CMIP Rapid Evaluation Framework can be installed with pip, mamba or conda:

```bash
pip install cmip-ref
mamba install -c conda-forge cmip-ref
conda install -c conda-forge cmip-ref
```


<!--- sec-end-installation -->

### For developers

<!--- sec-begin-installation-dev -->

For development, we rely on [uv](https://docs.astral.sh/uv) for all our
dependency management. To get started, you will need to make sure that uv
is installed
([instructions here](https://docs.astral.sh/uv/getting-started/installation/)).

For all of work, we use our `Makefile`.
You can read the instructions out and run the commands by hand if you wish,
but we generally discourage this because it can be error prone.
In order to create your environment, run `make virtual-environment`.

If there are any issues, the messages from the `Makefile` should guide you
through. If not, please raise an issue in the
[issue tracker](https://github.com/CMIP-REF/cmip-ref/issues).

For the rest of our developer docs, please see [](development-reference).

<!--- sec-end-installation-dev -->
