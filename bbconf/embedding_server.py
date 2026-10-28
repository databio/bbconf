"""
Clients for embedding models served over HTTP, used instead of loading the models
in-process (see the `embedding_server` config block).

- Text (dense and sparse): the Hugging Face Text Embeddings Inference (TEI) API,
  `POST /embed`, `POST /embed_sparse`, `GET /info`.
- Regions: the region embedding API, `POST /embed_bed`, `GET /info`.

Nothing here imports torch, fastembed, or sentence-transformers.
"""

import logging
import os
import tempfile

import httpx
import numpy as np
from gtars.models import RegionSet as GRegionSet

from bbconf.const import PKG_NAME
from bbconf.exceptions import EmbeddingServerError

_LOGGER = logging.getLogger(PKG_NAME)

TIMEOUT = 60.0


class _EmbeddingServer:
    """One model served at one base URL."""

    kind = "embedding"

    def __init__(
        self,
        url: str,
        api_key: str | None = None,
        transport: httpx.BaseTransport | None = None,
    ):
        self.url = url.rstrip("/")
        self.dim: int | None = None  # known after a successful check()
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._client = httpx.Client(
            base_url=self.url, headers=headers, timeout=TIMEOUT, transport=transport
        )

    def _request(self, method: str, path: str, **kwargs):
        try:
            response = self._client.request(method, path, **kwargs)
        except httpx.HTTPError as e:
            raise EmbeddingServerError(
                f"{self.kind} server at {self.url} is unreachable: {e}"
            ) from e
        if response.is_error:
            try:
                body = response.json()
                detail = f"{body.get('error_type')}: {body.get('error')}"
            except Exception:
                detail = response.text[:200]
            raise EmbeddingServerError(
                f"{self.kind} server at {self.url}{path} returned "
                f"{response.status_code} ({detail})"
            )
        return response.json()

    def _probe(self) -> tuple[dict, int | None]:
        """Return the server's /info and the size of the vectors it returns."""
        raise NotImplementedError

    def check(self, expected_model: str | None) -> None:
        """
        Compare the served model with the configured one, and learn the vector size.

        Raises EmbeddingServerError if the server serves another model. If the
        server can not be reached, logs a warning and returns.
        """
        try:
            info, self.dim = self._probe()
        except EmbeddingServerError as e:
            _LOGGER.warning(f"Skipping startup check: {e}")
            return
        if str(info.get("api_version", "1")).split(".")[0] != "1":
            raise EmbeddingServerError(
                f"{self.kind} server at {self.url} speaks API version "
                f"{info.get('api_version')}, expected 1"
            )
        served = (info.get("model_id"), info.get("served_model_name"))
        if expected_model and expected_model not in served:
            raise EmbeddingServerError(
                f"{self.kind} server at {self.url} serves '{info.get('model_id')}', "
                f"but the config names '{expected_model}'"
            )
        size = f" ({self.dim}-d)" if self.dim else ""
        _LOGGER.info(
            f"{self.kind} server at {self.url} serves '{info.get('model_id')}'{size}"
        )


class TextEmbeddingServer(_EmbeddingServer):
    """
    A TEI server. `embed` matches fastembed's `TextEmbedding.embed`, and `forward`
    matches geniml's query2vec, so either can be used where the local model was.
    """

    kind = "text embedding"

    def embed(self, text: str | list[str]) -> list[np.ndarray]:
        inputs = [text] if isinstance(text, str) else list(text)
        vectors = self._request(
            "POST", "/embed", json={"inputs": inputs, "truncate": True}
        )
        return [np.asarray(v, dtype=np.float32) for v in vectors]

    def forward(self, query: str) -> np.ndarray:
        return self.embed(query)[0]

    def embed_sparse(self, text: str) -> tuple[list[int], list[float]]:
        """Sparse (SPLADE) vector of one text, as (indices, values)."""
        result = self._request(
            "POST", "/embed_sparse", json={"inputs": [text], "truncate": True}
        )
        return [t["index"] for t in result[0]], [t["value"] for t in result[0]]

    def _probe(self) -> tuple[dict, int | None]:
        info = self._request("GET", "/info")
        if "splade" in str(info.get("model_type")):
            # sparse vectors have no fixed size
            return info, None
        return info, len(self.forward("dimension probe"))


class RegionEmbeddingServer(_EmbeddingServer):
    """
    A region embedding API server. `forward` returns what geniml's `BED2Vec`
    returns locally: the float32 mean of the region vectors of a BED file.
    """

    kind = "region embedding"

    def forward(self, query: str | GRegionSet) -> np.ndarray:
        """
        Args:
            query: Path to a BED file (plain or gzipped), or a gtars RegionSet.
        """
        if isinstance(query, GRegionSet):
            with tempfile.TemporaryDirectory(prefix="bbconf-") as tmp:
                path = os.path.join(tmp, "regions.bed.gz")
                query.to_bed_gz(path)
                return self.forward(path)
        with open(query, "rb") as f:
            body = f.read()
        content_type = "application/gzip" if body[:2] == b"\x1f\x8b" else "text/plain"
        result = self._request(
            "POST",
            "/embed_bed",
            params={"pooling": "mean"},
            content=body,
            headers={"Content-Type": content_type},
        )
        return np.asarray(result["embeddings"][0], dtype=np.float32)

    def _probe(self) -> tuple[dict, int | None]:
        info = self._request("GET", "/info")
        return info, info.get("embedding_dim")
