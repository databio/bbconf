import yacman
from pephubclient.helpers import MessageHandler as m
from pydantic_core._pydantic_core import ValidationError

from bbconf.config_parser.models import ConfigFile
from bbconf.exceptions import BedBaseConfError
from bbconf.helpers import get_bedbase_cfg


def config_analyzer(config_path: str) -> bool:
    """
    Read configuration file and insert default values if not set

    :param config_path: configuration file path
    :return: None
    :raises: raise_missing_key (if config key is missing)
    """
    config_path = get_bedbase_cfg(config_path)

    print(f"Analyzing the configuration file {config_path}...")

    _config = yacman.YAMLConfigManager(filepath=config_path).exp

    config_dict = {}
    for field_name, annotation in ConfigFile.model_fields.items():
        try:
            config_dict[field_name] = annotation.annotation(**_config.get(field_name))
        except TypeError:
            if annotation.is_required():
                print(
                    str(
                        BedBaseConfError(
                            f"`Config info: {field_name}` Field is not set in the configuration file or missing. "
                        )
                    )
                )
            else:
                print(
                    f"Config info: `{field_name}` Field is not set in the configuration file. Using default value."
                )
            try:
                config_dict[field_name] = None
            except ValidationError as e:
                print(
                    str(
                        BedBaseConfError(
                            f"Error in provided configuration file. Section: `{field_name}` missing values :: \n {e}"
                        )
                    )
                )
                return False
        except ValidationError as e:
            print(
                str(
                    BedBaseConfError(
                        f"Error in provided configuration file. Section: `{field_name}` missing values :: \n {e}"
                    )
                )
            )
            return False

    m.print_success("Configuration file is valid! ")

    return True
