# bbconf

![Run pytests](https://github.com/databio/bbconf/workflows/Run%20pytests/badge.svg)
[![codecov](https://codecov.io/gh/databio/bbconf/branch/master/graph/badge.svg)](https://codecov.io/gh/databio/bbconf)

*BEDBASE* project configuration package

## What is this?

`bbconf` defines `BedBaseConf` class which is an in-memory representation of the configuration file for the *BEDBASE* project. This is the source of the project-wide constant variables. Additionally it implements multiple convenience methods for interacting with the database backend, i.e. [PostgreSQL](https://www.postgresql.org/)

## Installation

Install from [GitHub releases](https://github.com/databio/bbconf/releases) or from PyPI using `pip`:
```
pip install --user bbconf
```

## Usage

- [Usage demonstration](https://github.com/databio/bbconf/blob/master/docs/demo.ipynb)
- [Python API documentation](https://github.com/databio/bbconf/blob/master/docs/bbc_api.md)
