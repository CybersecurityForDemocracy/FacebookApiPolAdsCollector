"""Code to load the ad renderer configuration."""

__author__ = 'Tobias Lauinger'
__email__ = 'lauinger@nyu.edu'

from configparser import ConfigParser, ExtendedInterpolation
from datetime import timedelta, date
from json import loads
from logging import getLogger
from pathlib import Path
from re import compile, Pattern
from typing import List, Optional, Set

DEFAULT_CONFIGURATION_LOCATION = Path(__file__).parent / 'defaults.ini'
INTERNAL_SECTION_NAME = 'INTERNAL-META'
INTERNAL_CONFIGURATION_META = 'configuration_meta'

log = getLogger(__name__)


class ConfigConverters:

    @staticmethod
    def _get_str_or_none(from_input: Optional[str]) -> Optional[str]:
        if from_input is None or len(from_input.strip()) == 0 or from_input.strip().lower() == 'none':
            return None
        return from_input

    @staticmethod
    def _get_pattern(from_input: Optional[str]) -> Optional[Pattern]:
        if from_input is None:
            return None
        return compile(from_input)

    @staticmethod
    def _get_path(from_input: Optional[str]) -> Optional[Path]:
        if from_input is None:
            return None
        return Path(from_input)

    @staticmethod
    def _get_path_or_none(from_input: Optional[str]) -> Optional[Path]:
        return ConfigConverters._get_path(ConfigConverters._get_str_or_none(from_input))

    @staticmethod
    def _get_list(from_input: Optional[str]) -> Optional[List]:
        if from_input is None:
            return None
        return loads(from_input)

    @staticmethod
    def _get_list_or_none(from_input: Optional[str]) -> Optional[List]:
        return ConfigConverters._get_list(ConfigConverters._get_str_or_none(from_input))

    @staticmethod
    def _get_dict(from_input: Optional[str]) -> Optional[dict]:
        if from_input is None:
            return None
        return loads(from_input)

    @staticmethod
    def _get_set(from_input: Optional[str]) -> Optional[Set]:
        if from_input is None:
            return None
        return set(loads(from_input))  # assuming JSON is a list

    @staticmethod
    def _get_timedelta_from_seconds(from_input: Optional[str]) -> Optional[timedelta]:
        if from_input is None:
            return None
        return timedelta(seconds=float(from_input))

    @staticmethod
    def _get_timedelta_from_minutes(from_input: Optional[str]) -> Optional[timedelta]:
        if from_input is None:
            return None
        return timedelta(minutes=float(from_input))

    @staticmethod
    def _get_timedelta_from_hours(from_input: Optional[str]) -> Optional[timedelta]:
        if from_input is None:
            return None
        return timedelta(hours=float(from_input))

    @staticmethod
    def _get_timedelta_from_days(from_input: Optional[str]) -> Optional[timedelta]:
        if from_input is None:
            return None
        return timedelta(days=float(from_input))

    @staticmethod
    def _get_date(from_input: Optional[str]) -> Optional[date]:
        if from_input is None:
            return None
        return date.fromisoformat(from_input)  # expects a date in ISO format

    @staticmethod
    def _get_date_or_none(from_input: Optional[str]) -> Optional[date]:
        return ConfigConverters._get_date(ConfigConverters._get_str_or_none(from_input))

    @staticmethod
    def get_converters() -> dict:
        return {
            'date': ConfigConverters._get_date,
            'date_or_none': ConfigConverters._get_date_or_none,
            'dict': ConfigConverters._get_dict,
            'list': ConfigConverters._get_list,
            'list_or_none': ConfigConverters._get_list_or_none,
            'Pattern': ConfigConverters._get_pattern,
            'Path': ConfigConverters._get_path,
            'Path_or_none': ConfigConverters._get_path_or_none,
            'set': ConfigConverters._get_set,
            'str_or_none': ConfigConverters._get_str_or_none,
            'timedelta_from_days': ConfigConverters._get_timedelta_from_days,
            'timedelta_from_hours': ConfigConverters._get_timedelta_from_hours,
            'timedelta_from_minutes': ConfigConverters._get_timedelta_from_minutes,
            'timedelta_from_seconds': ConfigConverters._get_timedelta_from_seconds,
        }


def load_config(manual_custom_configuration_path: Path) -> ConfigParser:
    parser = ConfigParser(converters=ConfigConverters.get_converters(), interpolation=ExtendedInterpolation())
    with open(DEFAULT_CONFIGURATION_LOCATION, 'r') as f_defaults:
        parser.read_file(f_defaults)
    config_meta = ['- default configuration version "{}"'.format(parser['DEFAULT']['version'])]

    custom_configuration_file_path = manual_custom_configuration_path
    custom_configuration_location = 'location defined in method parameter'
    if len(parser.read((custom_configuration_file_path,))) > 0:
        custom_version = parser.get('CUSTOM', 'version', fallback='NOT SPECIFIED')
        config_meta.append('- loaded custom configuration version "{}" from file "{}" ({})'.format(
            custom_version, custom_configuration_file_path, custom_configuration_location))
    else:
        config_meta.append('- no custom configuration loaded (expected in file "{}")'.format(
            custom_configuration_file_path))

    secrets_file_path = parser['Config'].getPath('secrets_location')
    if len(parser.read((secrets_file_path,))) > 0:
        secrets_version = parser.get('SECRETS', 'version', fallback='NOT SPECIFIED')
        config_meta.append('- loaded secrets version "{}" from file "{}"'.format(secrets_version, secrets_file_path))
    else:
        config_meta.append('- no secrets file loaded (expected in file "{}")'.format(secrets_file_path))

    parser.add_section(INTERNAL_SECTION_NAME)
    parser.set(INTERNAL_SECTION_NAME, INTERNAL_CONFIGURATION_META, '\n'.join(config_meta))
    return parser


def log_config(config: ConfigParser) -> None:
    config_meta = config.get(INTERNAL_SECTION_NAME, INTERNAL_CONFIGURATION_META,
                             fallback='(detailed configuration information not available)')
    log.info("Configuring crawler:\n%s", config_meta)
