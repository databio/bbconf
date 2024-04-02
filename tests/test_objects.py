from bbconf.exceptions import BEDFileNotFoundError, MissingThumbnailError
import pytest

from .utils import ContextManagerDBTesting

from .utils import BED_TEST_ID


class TestObjects:
    def test_object_path(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            result = bbagent_obj.objects.get_object_uri(
                "bed", BED_TEST_ID, "bed_file", "http"
            )

            assert isinstance(result, str)

    def test_object_path_error(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            with pytest.raises(BEDFileNotFoundError):
                bbagent_obj.objects.get_object_uri("bed", "not_f", "bed_file", "http")

    def test_object_path_thumbnail(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            result = bbagent_obj.objects.get_thumbnail_uri(
                "bed", BED_TEST_ID, "chrombins", "http"
            )
            assert isinstance(result, str)

    def test_object_path_thumbnail_error(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            with pytest.raises(MissingThumbnailError):
                bbagent_obj.objects.get_thumbnail_uri(
                    "bed", BED_TEST_ID, "bed_file", "http"
                )

    def test_object_metadata(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            result = bbagent_obj.objects.get_drs_metadata(
                "bed", BED_TEST_ID, "bed_file", "localhost"
            )
            assert result is not None


@pytest.mark.skip("Used to visualize the schema")
def test_create_schema_graph(bbagent_obj):
    bbagent_obj.config.db_engine.create_schema_graph()
