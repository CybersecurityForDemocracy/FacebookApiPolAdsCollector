from __future__ import annotations

"""Start up a browser with Selenium."""

__author__ = 'Tobias Lauinger'
__email__ = 'lauinger@nyu.edu'

from configparser import ConfigParser
from contextlib import contextmanager
from render_ad.docker_context import AbstractContainerConnectionData, AbstractDockerContextFactory
from logging import DEBUG, getLogger
from selenium.webdriver import Remote
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.remote_connection import ChromeRemoteConnection
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from tenacity import before_sleep_log, Retrying, stop_after_attempt, wait_fixed
from typing import Generator, Optional

log = getLogger(__name__)


class DockerSeleniumBrowserContextFactory(AbstractDockerContextFactory[Remote]):
    """Run a Docker container with Chrome and Selenium. Each instance of this class can only start a single
    browser/container at a time, but multiple containers could be active at the same time when run by different
    instances, and a single instance can run multiple containers sequentially."""

    def __init__(self, config: ConfigParser):
        config_section = config['adsnapshots']
        super().__init__(
            config=config,
            docker_image=config_section['browser_docker_image'],
            container_ready_min_delay_seconds=config_section.getfloat('browser_ready_min_delay_seconds'),
            container_error_next_batch_delay_seconds=config_section.getfloat(
                'browser_error_next_batch_delay_seconds'),
            container_port=config_section.getint('selenium_port_container'),
        )
        self._docker_container_environment_variables = config_section.getdict('docker_container_environment_variables')
        self._docker_container_shm_size = config_section['docker_container_shm_size']
        self._browser_arguments = config_section.getlist('browser_arguments')
        self._browser_preferences = config_section.getdict('browser_preferences')
        self._browser_window_height = config_section.getint('browser_window_height')
        self._browser_window_width = config_section.getint('browser_window_width')
        self._browser_ready_max_attempts = config_section.getint('browser_ready_max_attempts')
        self._browser_ready_retry_delay_seconds = config_section.getfloat('browser_ready_retry_delay_seconds')
        self._page_load_timeout_seconds = config_section.getfloat('page_load_timeout_seconds')
        self._active = False
        self._proxy_string = None

    @contextmanager
    def web_browser(self, proxy_container: Optional[str] = None, proxy_string: Optional[str] = None) \
            -> Generator[Remote, None, None]:
        """Use either proxy_container or proxy_string (or none)."""
        if self._active:
            raise ValueError('A browser instance is already running, cannot start a second one')
        self._proxy_string = proxy_string

        log_message = 'browser with Selenium'
        with super()._new_container(log_message, container_providing_network=proxy_container) as browser_connection:
            log.info("Connected to %s", log_message)
            try:
                yield browser_connection
            finally:
                self._active = False
                self._proxy_string = None

    def _get_run_parameters(self, container_connection: AbstractContainerConnectionData) -> dict:
        parameters = super()._get_run_parameters(container_connection)
        parameters['environment'] = self._docker_container_environment_variables.copy()
        parameters['shm_size'] = self._docker_container_shm_size
        return parameters

    def _get_container_connection(self, container_connection: AbstractContainerConnectionData) -> Remote:
        for attempt in Retrying(before_sleep=before_sleep_log(log, DEBUG),
                                stop=stop_after_attempt(self._browser_ready_max_attempts),
                                wait=wait_fixed(self._browser_ready_retry_delay_seconds)):
            with attempt:
                return self._get_headless_chrome_driver(container_connection.get_container_connection_string())

    def _get_headless_chrome_driver(self, connection_string: str) -> Remote:
        chrome_options = Options()
        if self._proxy_string is not None:
            chrome_options.add_argument('--proxy-server={}'.format(self._proxy_string))
        for argument in self._browser_arguments:
            chrome_options.add_argument(argument)
        chrome_options.add_experimental_option("prefs", self._browser_preferences)
        desired_capabilities = DesiredCapabilities.CHROME
        desired_capabilities['goog:loggingPrefs'] = {'browser': 'SEVERE'}
        desired_capabilities['pageLoadStrategy'] = 'normal'
        command_executor = ChromeRemoteConnection("http://{connection_string}/wd/hub".format(
            connection_string=connection_string))
        driver = Remote(command_executor, desired_capabilities=desired_capabilities, options=chrome_options)
        driver.set_window_size(self._browser_window_width, self._browser_window_height)
        driver.set_page_load_timeout(self._page_load_timeout_seconds)
        return driver
