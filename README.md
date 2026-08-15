<h1 align="center">bbconf</h1>

![Run pytests](https://github.com/databio/bbconf/workflows/Run%20pytests/badge.svg)
[![pypi-badge](https://img.shields.io/pypi/v/bbconf?color=%2334D058)](https://pypi.org/project/bbconf/)
[![pypi-version](https://img.shields.io/pypi/pyversions/bbconf.svg?color=%2334D058)](https://pypi.org/project/bbconf)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Github badge](https://img.shields.io/badge/source-github-354a75?logo=github)](https://github.com/databio/bbconf)
[![coverage](https://coverage-badge.samuelcolvin.workers.dev/databio/bbconf.svg)](https://coverage-badge.samuelcolvin.workers.dev/redirect/databio/bbconf)


*BEDBASE* project configuration package (agent)

## What is this?

`bbconf` is a configuration and data management library for the [BEDbase](https://bedbase.org) platform. It serves as the central backbone for all BEDbase tools and pipelines by:

- Reading and validating YAML configuration files
- Setting up and managing connections to PostgreSQL, Qdrant, S3, and PEPHub
- Loading ML models (Region2Vec, text embedders, sparse encoders, UMAP) used for BED file search
- Providing high-level Python interfaces for querying and managing BED files and BED sets
- Exposing a unified `BedBaseAgent` object that all downstream tools use to interact with the platform

---

**Documentation**: <a href="https://docs.bedbase.org/bedboss" target="_blank">https://docs.bedbase.org/bedboss</a>

**Source Code**: <a href="https://github.com/databio/bbconf" target="_blank">https://github.com/databio/bbconf</a>

---

## Installation

To install `bbclient` use this command: 
```
pip install bbconf
```
or install the latest version from the GitHub repository:
```
pip install git+https://github.com/databio/bbconf.git
```


## Quick start

```python
from bbconf import BedBaseAgent

agent = BedBaseAgent(config="config.yaml")

# Access submodules
agent.bed  # BED file operations
agent.bedset  # BED set operations
agent.objects  # Generic object/file operations

# Get platform statistics
stats = agent.get_stats()
print(stats.bedfiles_number, stats.bedsets_number)
```

## Database migrations

`bbconf` uses [Alembic](https://alembic.sqlalchemy.org/) to version the database
schema. The migration scripts live in `bbconf/alembic`, and `alembic.ini` (repo
root) is used for local CLI work. The first (baseline) revision is
`8b0b706d0827`; it reproduces exactly the schema that `Base.metadata.create_all()`
builds, including the `pg_trgm` extension and the trigram / partial / expression
indexes.

### Creating a new revision

After changing the models in `bbconf/db_utils.py`:

```bash
alembic revision --autogenerate -m "Describe your change"
```

Review the generated file. Alembic cannot autogenerate a few constructs used by
bbconf — the `pg_trgm` extension and expression-based indexes may need a manual
`op.execute(...)` — so always check the diff before committing.

### Applying migrations

```bash
alembic upgrade head      # upgrade to the latest revision
alembic downgrade -1      # roll back one revision
alembic current           # show the DB's current revision
```

### Running migrations automatically

To upgrade the database to `head` automatically when `bbconf` starts, set
`run_migrations: true` under the `database` section of the config file:

```yaml
database:
  host: localhost
  port: 5432
  user: postgres
  password: docker
  database: bedbase
  run_migrations: true
```

> **Note:** enable this only after the database has been stamped/upgraded to a
> known revision. Turning it on against an un-stamped existing database will fail
> on startup, because the baseline revision creates tables that already exist.
