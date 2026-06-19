"""Local (in-process) encoder wrappers.

Thin adapters that own a third-party encoder instance and expose only
the method surface defined by the ``bbconf.encoders.base`` Protocols.

These wrappers intentionally keep the expensive third-party imports at
module top-level: loading ``fastembed``, ``sentence_transformers`` and
``geniml`` is exactly the cost that Phase 3 (``Remote*Encoder``) will
eliminate for deployments that don't want in-process models.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

import numpy as np
from fastembed import TextEmbedding
from geniml.region2vec.main import Region2VecExModel
from sentence_transformers import SparseEncoder as STSparseEncoder

from bbconf.encoders.base import SparseTensorLike

if TYPE_CHECKING:
    from gtars.models import RegionSet as GRegionSet


class LocalDenseEncoder:
    """In-process dense text encoder backed by ``fastembed.TextEmbedding``."""

    def __init__(self, model_name: str):
        self._impl = TextEmbedding(model_name)
        self._model_name = model_name

    def embed(self, text: str | list[str]) -> Iterable[np.ndarray]:
        return self._impl.embed(text)

    def get_embedding_size(self, model: str) -> int:
        return int(self._impl.get_embedding_size(model))


class LocalSparseEncoder:
    """In-process sparse text encoder backed by
    ``sentence_transformers.SparseEncoder``."""

    def __init__(self, model_name: str):
        self._impl = STSparseEncoder(model_name)
        self._model_name = model_name

    def encode(self, text: str) -> SparseTensorLike:
        return self._impl.encode(text)


class LocalRegionEncoder:
    """In-process region set encoder backed by
    ``geniml.region2vec.Region2VecExModel``.

    Exposes the Protocol surface (``encode``) and additionally
    delegates unknown attribute access to the wrapped impl. The
    ``__getattr__`` fall-through is a Phase-2 stopgap: ``geniml.search.
    query2vec.BED2Vec`` currently does ``isinstance(model,
    Region2VecExModel)``, so call sites in ``BedBaseConfig`` pass
    ``region_encoder._impl`` directly to ``BED2Vec``. Phase 3 will
    replace that integration with a Protocol-aware alternative and this
    delegation can be removed.
    """

    def __init__(self, model_name: str):
        self._impl = Region2VecExModel(model_name)
        self._model_name = model_name

    def encode(self, region_set: "GRegionSet") -> np.ndarray:
        return self._impl.encode(region_set)

    def __getattr__(self, name: str) -> Any:
        # Only called if normal attribute lookup fails; delegate to impl.
        return getattr(self._impl, name)
