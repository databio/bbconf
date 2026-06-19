"""Tests for the bbconf.encoders package.

These tests use mocks so they don't hit the network or download models.
They verify the Protocol contract shape and that the Local* wrappers
delegate to their wrapped third-party impl.
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest


def test_protocols_importable():
    from bbconf.encoders import (
        DenseEncoder,
        LocalDenseEncoder,
        LocalRegionEncoder,
        LocalSparseEncoder,
        RegionEncoder,
        SparseEncoder,
        SparseTensorLike,
    )

    # Protocols are typing.Protocol subclasses (regular classes at runtime).
    assert DenseEncoder is not None
    assert SparseEncoder is not None
    assert RegionEncoder is not None
    assert SparseTensorLike is not None
    # Wrappers are regular classes.
    assert isinstance(LocalDenseEncoder, type)
    assert isinstance(LocalSparseEncoder, type)
    assert isinstance(LocalRegionEncoder, type)


def test_local_dense_encoder_delegates():
    with patch("bbconf.encoders.local.TextEmbedding") as mock_te:
        impl = MagicMock()
        impl.embed.return_value = iter([np.array([1.0, 2.0, 3.0])])
        impl.get_embedding_size.return_value = 384
        mock_te.return_value = impl

        from bbconf.encoders import LocalDenseEncoder

        enc = LocalDenseEncoder("fake-model")
        mock_te.assert_called_once_with("fake-model")

        out = list(enc.embed("hello"))
        assert len(out) == 1
        assert out[0].tolist() == [1.0, 2.0, 3.0]
        impl.embed.assert_called_once_with("hello")

        assert enc.get_embedding_size("fake-model") == 384
        impl.get_embedding_size.assert_called_once_with("fake-model")


def test_local_dense_encoder_satisfies_protocol():
    """Structural check: LocalDenseEncoder exposes DenseEncoder's surface."""
    from bbconf.encoders import LocalDenseEncoder

    assert callable(getattr(LocalDenseEncoder, "embed", None))
    assert callable(getattr(LocalDenseEncoder, "get_embedding_size", None))


def test_local_sparse_encoder_delegates():
    with patch("bbconf.encoders.local.STSparseEncoder") as mock_se:
        impl = MagicMock()
        sparse_result = MagicMock()
        impl.encode.return_value = sparse_result
        mock_se.return_value = impl

        from bbconf.encoders import LocalSparseEncoder

        enc = LocalSparseEncoder("fake-sparse-model")
        mock_se.assert_called_once_with("fake-sparse-model")

        result = enc.encode("some text")
        impl.encode.assert_called_once_with("some text")
        assert result is sparse_result


def test_local_sparse_encoder_satisfies_protocol():
    from bbconf.encoders import LocalSparseEncoder

    assert callable(getattr(LocalSparseEncoder, "encode", None))


def test_local_region_encoder_delegates():
    with patch("bbconf.encoders.local.Region2VecExModel") as mock_r2v:
        impl = MagicMock()
        impl.encode.return_value = np.array([[1.0, 2.0], [3.0, 4.0]])
        mock_r2v.return_value = impl

        from bbconf.encoders import LocalRegionEncoder

        enc = LocalRegionEncoder("fake-region-model")
        mock_r2v.assert_called_once_with("fake-region-model")

        region_set = MagicMock()
        arr = enc.encode(region_set)
        impl.encode.assert_called_once_with(region_set)
        assert arr.shape == (2, 2)


def test_local_region_encoder_getattr_delegates_to_impl():
    """Phase-2 stopgap: unknown attributes pass through to the wrapped
    Region2VecExModel so geniml.search.query2vec.BED2Vec's internal
    attribute access (if any) still resolves."""
    with patch("bbconf.encoders.local.Region2VecExModel") as mock_r2v:
        impl = MagicMock()
        impl.custom_attr = "delegated-value"
        mock_r2v.return_value = impl

        from bbconf.encoders import LocalRegionEncoder

        enc = LocalRegionEncoder("fake-region-model")
        assert enc.custom_attr == "delegated-value"


def test_local_region_encoder_satisfies_protocol():
    from bbconf.encoders import LocalRegionEncoder

    assert callable(getattr(LocalRegionEncoder, "encode", None))
