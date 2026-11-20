import pytest

from bbconf.exceptions import AnalysisFileNotFoundError
from bbconf.models.base_models import AnalysisFileArtifact

from .conftest import SERVICE_UNAVAILABLE
from .utils import ContextManagerDBTesting

UPLOAD_TARGET = "bbconf.config_parser.bedbaseconfig.BedBaseConfig.upload_s3"
DELETE_TARGET = "bbconf.config_parser.bedbaseconfig.BedBaseConfig.delete_s3"


def _artifact(**overrides) -> AnalysisFileArtifact:
    values = dict(
        path="/local/openSignalMatrix_hg38.txt.gz",
        name="openSignalMatrix",
        file_type="openSignalMatrix",
        genome="hg38",
        description="Open signal matrix for hg38",
        tags=["reference", "hg38"],
        file_size=12345,
        checksum="a" * 64,
    )
    values.update(overrides)
    return AnalysisFileArtifact(**values)


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
class Test_AnalysisFile_Agent:
    def test_add(self, bbagent_obj, mocker):
        upload_mock = mocker.patch(UPLOAD_TARGET, return_value=True)
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=False):
            result = bbagent_obj.analysis_files.add(_artifact())

            assert upload_mock.called
            assert result.count == 1
            row = result.results[0]
            assert row.id is not None
            assert row.name == "openSignalMatrix"
            assert row.genome == "hg38"
            assert row.tags == ["reference", "hg38"]
            assert row.checksum == "a" * 64
            assert row.file_path == "analysis_files/openSignalMatrix_hg38.txt.gz"

    def test_list_and_filters(self, bbagent_obj, mocker):
        mocker.patch(UPLOAD_TARGET, return_value=True)
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=False):
            bbagent_obj.analysis_files.add(
                [
                    _artifact(),
                    _artifact(
                        path="/local/openSignalMatrix_mm10.txt.gz",
                        genome="mm10",
                        tags=["reference", "mm10"],
                        checksum="b" * 64,
                    ),
                    _artifact(
                        path="/local/some_model.pt",
                        name="some_model",
                        file_type="model",
                        genome=None,
                        tags=["model"],
                        checksum="c" * 64,
                    ),
                ]
            )

            assert bbagent_obj.analysis_files.list().count == 3
            assert (
                bbagent_obj.analysis_files.list(file_type="openSignalMatrix").count == 2
            )
            assert bbagent_obj.analysis_files.list(genome="mm10").count == 1
            assert bbagent_obj.analysis_files.list(tag="model").count == 1
            assert bbagent_obj.analysis_files.list(genome="hg19").count == 0

    def test_get_and_get_by_name(self, bbagent_obj, mocker):
        mocker.patch(UPLOAD_TARGET, return_value=True)
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=False):
            added = bbagent_obj.analysis_files.add(_artifact()).results[0]

            by_id = bbagent_obj.analysis_files.get(added.id)
            assert by_id.name == "openSignalMatrix"

            by_name = bbagent_obj.analysis_files.get_by_name(
                "openSignalMatrix", genome="hg38"
            )
            assert by_name.id == added.id

            by_filename = bbagent_obj.analysis_files.get_by_filename(
                "openSignalMatrix_hg38.txt.gz"
            )
            assert by_filename.id == added.id

    def test_get_missing_raises(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=False):
            with pytest.raises(AnalysisFileNotFoundError):
                bbagent_obj.analysis_files.get(999999)
            with pytest.raises(AnalysisFileNotFoundError):
                bbagent_obj.analysis_files.get_by_name("does-not-exist")

    def test_delete(self, bbagent_obj, mocker):
        mocker.patch(UPLOAD_TARGET, return_value=True)
        delete_mock = mocker.patch(DELETE_TARGET, return_value=True)
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=False):
            added = bbagent_obj.analysis_files.add(_artifact()).results[0]

            bbagent_obj.analysis_files.delete(added.id)
            assert delete_mock.called
            assert bbagent_obj.analysis_files.list().count == 0
            with pytest.raises(AnalysisFileNotFoundError):
                bbagent_obj.analysis_files.get(added.id)

    def test_delete_by_checksum(self, bbagent_obj, mocker):
        mocker.patch(UPLOAD_TARGET, return_value=True)
        delete_mock = mocker.patch(DELETE_TARGET, return_value=True)
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=False):
            bbagent_obj.analysis_files.add(_artifact())

            bbagent_obj.analysis_files.delete_by_checksum("a" * 64)
            assert delete_mock.called
            assert bbagent_obj.analysis_files.list().count == 0
            with pytest.raises(AnalysisFileNotFoundError):
                bbagent_obj.analysis_files.delete_by_checksum("a" * 64)
