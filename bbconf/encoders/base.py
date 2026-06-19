"""Protocol contracts for encoder implementations.

These Protocols (PEP 544, structural typing) describe exactly the method
surface that ``bbconf`` uses on its encoder attributes. They are defined
here so that both in-process wrappers (``bbconf.encoders.local``) and
future remote / HTTP-backed implementations can satisfy the same
contract without requiring changes at call sites.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Protocol

import numpy as np

if TYPE_CHECKING:
    import torch
    from gtars.models import RegionSet as GRegionSet


class DenseEncoder(Protocol):
    """Contract for dense text embedding encoders.

    Matches the surface of ``fastembed.TextEmbedding`` that ``bbconf``
    actually uses:

    - ``embed`` is called on single strings (yielding an iterable the
      caller materialises with ``list(...)``).
    - ``get_embedding_size`` is called to size vectors when creating
      Qdrant collections.
    """

    def embed(self, text: str | list[str]) -> Iterable[np.ndarray]:
        ...

    def get_embedding_size(self, model: str) -> int:
        ...


class SparseTensorLike(Protocol):
    """Minimal Protocol describing the sparse-tensor result that
    ``SparseEncoder.encode`` returns.

    Corresponds to the subset of ``torch.Tensor`` sparse API that
    ``bbconf`` invokes: ``.coalesce()`` returning a sparse tensor whose
    ``.indices()`` (shape ``[1, N]``) and ``.values()`` yield torch
    tensors convertible to Python lists.
    """

    def coalesce(self) -> "SparseTensorLike":
        ...

    def indices(self) -> "torch.Tensor":
        ...

    def values(self) -> "torch.Tensor":
        ...


class SparseEncoder(Protocol):
    """Contract for sparse text embedding encoders.

    Matches the surface of ``sentence_transformers.SparseEncoder`` that
    ``bbconf`` uses: a single ``encode(text)`` call returning a sparse
    tensor on which ``.coalesce()``, ``.indices()`` and ``.values()``
    are valid.
    """

    def encode(self, text: str) -> SparseTensorLike:
        ...


class RegionEncoder(Protocol):
    """Contract for genomic region set encoders.

    Matches the surface of ``geniml.region2vec.Region2VecExModel``:
    ``encode`` takes a ``RegionSet`` and returns a 2D array of
    per-region embeddings. Callers average over ``axis=0`` to get a
    single file-level embedding.
    """

    def encode(self, region_set: "GRegionSet") -> np.ndarray:
        ...
