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

`pip install bbconf` does not include the search models. Add the `local` extra to load them in-process (`pip install "bbconf[local]"`), or use embedding servers (below).


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

## Embedding servers

By default bbconf loads the text, sparse, and region2vec models named in `path` into memory. To get embeddings from HTTP servers instead, add an `embedding_server` block. Each URL that is set moves that one model to a server; the others stay local. A blank value or an unset `$VAR` counts as unset.

```yaml
embedding_server:
  text_url: http://localhost:8080     # dense text model (path.text2vec)
  sparse_url: http://localhost:8081   # sparse model (path.sparse_model)
  region_url: http://localhost:8082   # region model (path.region2vec)
  api_key: $EMBEDDING_API_KEY         # sent as a Bearer token
```

The text servers speak the [Hugging Face Text Embeddings Inference (TEI)](https://huggingface.co/docs/text-embeddings-inference) API (`POST /embed`, `POST /embed_sparse`, `GET /info`). The region server speaks this API:

- `GET /info` returns JSON with `api_version` (`"1"`), `model_id`, and `embedding_dim`.
- `POST /embed_bed?pooling=mean` takes a raw BED file as the body (plain, or gzipped with `Content-Type: application/gzip`) and returns `{"embeddings": [[...]]}`: one vector, the float32 mean of the region vectors, the same as `np.mean(Region2VecExModel.encode(regions), axis=0)`.
- Errors return a non-2xx status with `{"error": "...", "error_type": "..."}`.

At startup bbconf calls each server's `/info` and stops if the server's model is not the one named in `path`, or if its vector size differs from the Qdrant collection's. If a server can't be reached, bbconf logs a warning and starts anyway.

## Database migrations

`bbconf` uses [Alembic](https://alembic.sqlalchemy.org/) to version the database
schema. The migration scripts live in `bbconf/alembic`, and `alembic.ini` (repo
root) is used for local CLI work. The first (baseline) revision is
`8b0b706d0827`; it reproduces exactly the schema that `Base.metadata.create_all()`
builds, including the `pg_trgm` extension and the trigram / partial / expression
indexes.

To update schema for desirable database, use different database url in `alembic.ini`, 
otherwise run test database

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
