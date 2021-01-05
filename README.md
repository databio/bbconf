# bbconf

![Run pytests](https://github.com/databio/bbconf/workflows/Run%20pytests/badge.svg)
[![codecov](https://codecov.io/gh/databio/bbconf/branch/master/graph/badge.svg)](https://codecov.io/gh/databio/bbconf)

*BEDBASE* project configuration package

## What is this?

`bbconf` standardizes reporting of [bedstat](https://github.com/databio/bedstat) and [bedbuncher](https://github.com/databio/bedsbuncher) results. It formalizes a way for these pipelines and downstream tools to communicate -- the produced results can easily and reliably become an
input for the server ([bedhost](https://github.com/databio/bedhost)). The object exposes API for interacting with the results and is backed by a [PostgreSQL](https://www.postgresql.org/) database.

## Installation

Install from [GitHub releases](https://github.com/databio/bbconf/releases) or from PyPI using `pip`:
```
pip install --user bbconf
```

## Usage

- [Usage demonstration](docs/demo.ipynb)
- [Python API documentation](docs/bbc_api.md)
