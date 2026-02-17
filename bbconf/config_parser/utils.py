import logging

import yacman
from pephubclient.helpers import MessageHandler as m
from pydantic_core._pydantic_core import ValidationError

from bbconf.config_parser.models import ConfigFile
from bbconf.const import PKG_NAME
from bbconf.helpers import get_bedbase_cfg

_LOGGER = logging.getLogger(PKG_NAME)


def config_analyzer(config_path: str) -> bool:
    """
    Read configuration file and insert default values if not set.

    Args:
        config_path: Configuration file path.

    Returns:
        True if the config is valid, False otherwise.

    Raises:
        raise_missing_key: If config key is missing.
    """
    config_path = get_bedbase_cfg(config_path)

    _LOGGER.info(f"Analyzing the configuration file {config_path}...")

    _config = yacman.YAMLConfigManager(filepath=config_path).exp

    config_dict = {}
    for field_name, annotation in ConfigFile.model_fields.items():
        try:
            config_dict[field_name] = annotation.annotation(**_config.get(field_name))
        except TypeError:
            if annotation.is_required():
                _LOGGER.error(
                    f"`Config info: {field_name}` Field is not set in the configuration file or missing."
                )
            else:
                _LOGGER.info(
                    f"Config info: `{field_name}` Field is not set in the configuration file. Using default value."
                )
            try:
                config_dict[field_name] = None
            except ValidationError as e:
                _LOGGER.error(
                    f"Error in provided configuration file. Section: `{field_name}` missing values :: \n {e}"
                )
                return False
        except ValidationError as e:
            _LOGGER.error(
                f"Error in provided configuration file. Section: `{field_name}` missing values :: \n {e}"
            )
            return False

    m.print_success("Configuration file is valid! ")

    return True
