"""
Tests for the embedding server clients (bbconf.embedding_server), with fake servers
(httpx.MockTransport), so no live server, database, or model is needed.
"""

import gzip
import json
import os
import subprocess
import sys
from types import SimpleNamespace

import httpx
import numpy as np
import pytest
from gtars.models import RegionSet as GRegionSet

from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from bbconf.config_parser.models import ConfigEmbeddingServer
from bbconf.embedding_server import RegionEmbeddingServer, TextEmbeddingServer
from bbconf.exceptions import EmbeddingServerError
from bbconf.modules.bedfiles import BedAgentBedFile

from .conftest import DATA_PATH

BED_FILE = os.path.join(DATA_PATH, "files", "bbad85f21962bb8d972444f7f9a3a932.bed.gz")
TEXT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
REGION_MODEL = "databio/r2v-encode-hg38"


def fake_tei(model_id=TEXT_MODEL, pooling="mean", dim=4):
    """A TEI server. Records requests in `seen`."""
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/info":
            info = {
                "model_id": model_id,
                "model_type": {"embedding": {"pooling": pooling}},
            }
            return httpx.Response(200, json=info)
        inputs = json.loads(request.content)["inputs"]
        if request.url.path == "/embed":
            return httpx.Response(200, json=[[0.5] * dim for _ in inputs])
        if request.url.path == "/embed_sparse":
            sparse = [{"index": 7, "value": 0.25}, {"index": 42, "value": 1.5}]
            return httpx.Response(200, json=[sparse for _ in inputs])
        return httpx.Response(
            404, json={"error": "no route", "error_type": "Validation"}
        )

    return httpx.MockTransport(handler), seen


def fake_region_server(model_id=REGION_MODEL, dim=3):
    """A region embedding API server. Records requests in `seen`."""
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/info":
            return httpx.Response(
                200,
                json={"api_version": "1", "model_id": model_id, "embedding_dim": dim},
            )
        if request.url.path == "/embed_bed":
            if not request.content:
                return httpx.Response(
                    400, json={"error": "no regions", "error_type": "Empty"}
                )
            body = {
                "embeddings": [[0.1] * dim],
                "n_regions": 1,
                "n_tokens": 1,
                "n_unmatched": 0,
            }
            return httpx.Response(200, json=body)
        return httpx.Response(404)

    return httpx.MockTransport(handler), seen


def unreachable(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("connection refused", request=request)


def test_text_embed_and_sparse():
    transport, seen = fake_tei()
    server = TextEmbeddingServer("http://tei/", api_key="secret", transport=transport)

    vectors = list(server.embed("a query"))  # used like fastembed's embed()
    assert len(vectors) == 1 and vectors[0].dtype == np.float32
    assert server.forward("a query").shape == (4,)
    assert json.loads(seen[0].content) == {"inputs": ["a query"], "truncate": True}
    assert seen[0].headers["Authorization"] == "Bearer secret"

    assert server.embed_sparse("a query") == ([7, 42], [0.25, 1.5])
    assert seen[-1].url.path == "/embed_sparse"


def test_region_embed_file_and_region_set():
    transport, seen = fake_region_server()
    server = RegionEmbeddingServer("http://regions", transport=transport)

    vector = server.forward(BED_FILE)
    assert vector.dtype == np.float32 and vector.shape == (3,)
    request = seen[-1]
    assert request.url.path == "/embed_bed"
    assert request.url.params["pooling"] == "mean"
    assert request.headers["Content-Type"] == "application/gzip"
    assert "Authorization" not in request.headers

    # a RegionSet is sent as the same regions, gzipped
    server.forward(GRegionSet(BED_FILE))
    sent = gzip.decompress(seen[-1].content).decode().splitlines()
    assert len(sent) == len(gzip.open(BED_FILE, "rt").read().splitlines())


def test_error_body_is_reported(tmp_path):
    transport, _ = fake_region_server()
    server = RegionEmbeddingServer("http://regions", transport=transport)
    empty = tmp_path / "empty.bed"
    empty.write_text("")
    with pytest.raises(EmbeddingServerError, match="400 .*Empty: no regions"):
        server.forward(str(empty))


def test_check_learns_dim():
    transport, _ = fake_tei(dim=4)
    text = TextEmbeddingServer("http://tei", transport=transport)
    text.check(TEXT_MODEL)
    assert text.dim == 4

    transport, _ = fake_tei(model_id="prithivida/Splade_PP_en_v2", pooling="splade")
    sparse = TextEmbeddingServer("http://tei", transport=transport)
    sparse.check("prithivida/Splade_PP_en_v2")
    assert sparse.dim is None

    transport, _ = fake_region_server(dim=100)
    region = RegionEmbeddingServer("http://regions", transport=transport)
    region.check(REGION_MODEL)
    assert region.dim == 100


def test_check_model_mismatch_raises():
    transport, _ = fake_tei(model_id="BAAI/bge-small-en-v1.5")
    with pytest.raises(EmbeddingServerError, match="bge-small"):
        TextEmbeddingServer("http://tei", transport=transport).check(TEXT_MODEL)

    transport, _ = fake_region_server(model_id="databio/other-model")
    with pytest.raises(EmbeddingServerError, match="other-model"):
        RegionEmbeddingServer("http://regions", transport=transport).check(REGION_MODEL)


def test_check_unreachable_warns(caplog):
    server = TextEmbeddingServer(
        "http://down", transport=httpx.MockTransport(unreachable)
    )
    server.check(TEXT_MODEL)  # does not raise
    assert server.dim is None
    assert "unreachable" in caplog.text
    with pytest.raises(EmbeddingServerError, match="unreachable"):
        server.embed("a query")


def test_qdrant_dim_mismatch_raises():
    transport, _ = fake_region_server(dim=3)
    region = RegionEmbeddingServer("http://regions", transport=transport)
    region.check(REGION_MODEL)
    collection = SimpleNamespace(
        config=SimpleNamespace(
            params=SimpleNamespace(vectors=SimpleNamespace(size=100))
        )
    )
    cfg = object.__new__(BedBaseConfig)
    cfg.dense_encoder, cfg.r2v_encoder = None, region
    cfg.qdrant_client = SimpleNamespace(
        get_collection=lambda collection_name: collection
    )
    cfg._config = SimpleNamespace(
        qdrant=SimpleNamespace(
            hybrid_collection="h", text_collection="t", file_collection="bedbase"
        )
    )
    with pytest.raises(EmbeddingServerError, match="3-d .* 100-d"):
        cfg._check_server_dims()


def test_bedfile_uses_servers():
    region_transport, _ = fake_region_server()
    tei_transport, _ = fake_tei()
    bed_agent = object.__new__(BedAgentBedFile)
    bed_agent.config = SimpleNamespace(
        qdrant_file_backend=object(),
        r2v_encoder=RegionEmbeddingServer("http://regions", transport=region_transport),
        sparse_encoder=TextEmbeddingServer("http://tei", transport=tei_transport),
    )
    assert bed_agent._embed_file(BED_FILE).shape == (1, 3)
    sparse = bed_agent._embed_sparse("a query")
    assert sparse.indices == [7, 42] and sparse.values == [0.25, 1.5]


def test_config_blanks_are_unset():
    cfg = ConfigEmbeddingServer(
        text_url="",
        sparse_url="$SPARSE_URL",
        region_url="http://regions",
        api_key="$KEY",
    )
    assert cfg.text_url is None and cfg.sparse_url is None and cfg.api_key is None
    assert cfg.region_url == "http://regions"
    assert ConfigEmbeddingServer().region_url is None


def test_remote_mode_imports_no_model_libraries():
    code = """
import sys
from bbconf.bbagent import BedBaseAgent
from bbconf.embedding_server import RegionEmbeddingServer, TextEmbeddingServer
TextEmbeddingServer("http://tei"), RegionEmbeddingServer("http://regions")
heavy = ("torch", "sentence_transformers", "geniml.region2vec")
print([m for m in heavy if m in sys.modules])
"""
    out = subprocess.run(
        [sys.executable, "-W", "ignore", "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == "[]"
