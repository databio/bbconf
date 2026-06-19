"""Encoder Protocols and local (in-process) wrapper implementations.

This package defines the abstract contracts (``DenseEncoder``,
``SparseEncoder``, ``RegionEncoder``) that ``bbconf`` uses to talk to
embedding models, along with local wrappers that adapt third-party
libraries (``fastembed``, ``sentence_transformers``, ``geniml``) to those
Protocols.

A future phase will add ``Remote*Encoder`` implementations backed by an
HTTP embeddings service; call sites in ``bbconf`` depend only on the
Protocol surface, so swapping in a remote implementation requires no
changes to consumers.
"""

from bbconf.encoders.base import (
    DenseEncoder,
    RegionEncoder,
    SparseEncoder,
    SparseTensorLike,
)
from bbconf.encoders.local import (
    LocalDenseEncoder,
    LocalRegionEncoder,
    LocalSparseEncoder,
)

__all__ = [
    "DenseEncoder",
    "SparseEncoder",
    "RegionEncoder",
    "SparseTensorLike",
    "LocalDenseEncoder",
    "LocalSparseEncoder",
    "LocalRegionEncoder",
]
