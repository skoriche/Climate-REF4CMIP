# A Perspective on Python Package Manager Confusion

The ecosystem of python package managers is confusing and continues to evolve. In particular, it is a challenge for the scientist who wants to find a stable way to make their codes run and share their workflows with colleagues.

## Why are there so many python installation tools?

Python ships with `pip` as a package installer which uses the [Python Package Index](https://pypi.org/) (PyPI) repository to install community-built software. It is great for a simple project that only depends on code written in python. However, as your project scales or if you have complex dependencies or need to remove a package, `pip`'s simplicity can lead to chaos as you try to resolve conflicts in package inter-dependencies. Neither does `pip` install non-python software on which a package may depend. For example, the [cf-units](https://github.com/SciTools/cf-units) python package is a wrapper around the C library [UDUNITS-2](https://www.unidata.ucar.edu/software/udunits/). But if you `pip install cf-units`, it will assume you have UDUNITS-2 already installed and not do it for you.

This lackluster situation is a breeding ground for other community tools for installing python packages, their non-python dependencies and handing complex package inter-dependencies. Many alternatives have emerged each with their own use-case and trajectory.

## Conda

[Anaconda, Inc.](https://www.anaconda.com/) provides a distribution of an environment (with or without a GUI interface) that groups many of the common packages that are used in data science. If you download their ~750 Mb distribution installer, you are getting all of these packages in one lump sum along with any system dependencies. They also provide a commandline installer `conda` which is more thorough than `pip`, resolving complex dependencies across various packages, including the non-python ones. You can get just the installer by downloading Miniconda instead. Both are available on the [download](https://www.anaconda.com/download/success) page of their website.

But this distribution is maintained by Anaconda. A community then sprung up to use `conda`'s infrastructure to allow users to build their own recipes for python packages that might have non-python dependencies. This community is known as [conda-forge](https://conda-forge.org/). When you run `conda config --add channels conda-forge` you are telling `conda` to expand its notion of what packages are available to include recipes created and maintained by this community.

And so while a huge step forward in making python software accessible and usable, `conda`'s solve for an environment that satisfies all dependency requirements is slow. This is partially fueled by the sheer number of possible package versions to explore and also the nature of users to want huge monolythic environments that would support all their work. Enter [`mamba`](https://github.com/mamba-org/mamba), a faster, C++-based re-implementation of `conda`'s package manager. The basic contract is that you can use `mamba` as you would `conda` and get almost the exact same environment, only much faster.

In the community as well as this documentation, the references you see to `conda` may refer to any and all of this.

## Non-Conda Alternatives We Use

A few alternatives have emerged that come from the [Rust](https://www.rust-lang.org/) community that provide a python analog to the `cargo` utility. Rust users use `cargo` to install, build, check, run, and even publish their package and are *project*-focused. In place of building single monolithic environments for everything you do, the basic tool usage guides you to start projects with an environment definition that you keep along with each project that you are working on. This makes sharing your work simpler as you also pass along eaxctly what was needed to run your project. These alternatives include:

- [uv](https://docs.astral.sh/uv/) is a replacement for `pip` and other related tools that is 10-100x faster. It also allows for multiple python versions, environments, uploading your package to PyPI and more. The caveat is that it uses PyPI as repository and therefore will not install non-python dependencies. If you are using python-only packages and are willing to adopt this workflow, it is a compelling choice for its speed alone.
- [pixi](https://pixi.sh/latest/) is a fast, modern, and reproducible package manager for many languages (not just python). It is built on top of the `conda` and PyPI ecosystems and defaults to using `conda-forge` as a repositry. When you try to add a package with `pixi`, it will first try to resolve the `conda` dependencies and then resolve the rest using `PyPI` with `uv` under the hood. They also have a nice transition [tutorial](https://pixi.sh/latest/switching_from/conda/) from `conda`.

## What Do We Recommend?

As developers and collaborators with scientists who do not code every day of their career, we understand the importance of using the tools that you know. We are also in the position to have experience with what works better. The REF itself, as it is pure python, uses `uv` throughout. You will find references to features of `uv` in this documentation and so becoming familiar with [basic usage](https://docs.astral.sh/uv/getting-started/) may be helpful. There may be instances where a `conda` installation is preferable, for example on HPC environments where traditional operating system utilities are sometimes lacking.
