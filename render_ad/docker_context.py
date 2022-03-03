from __future__ import annotations

"""Common code to start up Docker containers as a context manager."""

__author__ = 'Tobias Lauinger'
__email__ = 'lauinger@nyu.edu'

from abc import ABC, abstractmethod
from contextlib import closing, contextmanager
from docker import from_env
from docker.errors import DockerException
from docker.models.containers import Container
from logging import getLogger
from random import random
from struct import pack
from socket import AF_INET, socket, SOCK_STREAM, SO_LINGER, SOL_SOCKET
from time import monotonic, sleep
from typing import Generator, Generic, Optional, TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from configparser import ConfigParser

DEFAULT_RUN_PARAMETERS = {
    'auto_remove': True,
    'detach': True,
    'remove': True,
}
HOST_INTERFACE = '127.0.0.1'
CONTAINER_CONNECTION_STRING_TEMPLATE = "{host}:{port}"

ContainerConnection = TypeVar('ContainerConnection')

log = getLogger(__name__)


def get_random(prefix: str) -> str:
    return "{prefix}{digits}".format(prefix=prefix, digits=int(random() * 10000000000))


def build_container_connection_from_configuration(config: 'ConfigParser', container_port: int) \
        -> AbstractContainerConnectionData:
    config_section = config['AbstractDockerContextFactory']
    connection_type = config_section['container_connection_type']
    if connection_type == 'host_port':
        return HostContainerConnectionData(container_port)
    elif connection_type == 'docker_network':
        network_name = config_section['docker_network_name']
        return DockerNetworkContainerConnectionData(container_port, network_name)
    else:
        raise ValueError("Unsupported container connection type '{}'".format(connection_type))


class ContainerStartFailedException(Exception):
    """The requested Docker container did not start properly."""

    def __init__(self, wait_before_next_batch_seconds: float, *args):
        """Arguments after the mandatory parameter will be passed to the superclass. This can be used to set an error
        message: ContainerStartFailedException(3.0, 'the crawler must wait')"""
        super().__init__(*args)
        self.wait_before_next_batch_seconds = wait_before_next_batch_seconds


class AbstractContainerConnectionData(ABC):

    def __init__(self, container_port: int):
        self._container_port = container_port
        self._container_name = get_random('render_ad')

    def get_container_name(self) -> str:
        """Returns the name of the container."""
        return self._container_name

    def get_docker_run_parameters(self) -> dict:
        """Returns the parameters to the Docker run command needed to set up the connection in the container."""
        return {
            'name': self._container_name,
        }

    def get_container_connection_string(self) -> str:
        """Returns the connection string (host/ip:port) that can be used to connect to the container."""
        ...


class HostContainerConnectionData(AbstractContainerConnectionData):

    def __init__(self, container_port: int):
        super().__init__(container_port)
        self._host_interface = HOST_INTERFACE
        self._host_port = self._find_available_tcp_port()

    def _find_available_tcp_port(self) -> int:
        with closing(socket(AF_INET, SOCK_STREAM)) as s:
            s.bind((self._host_interface, 0))
            s.setsockopt(SOL_SOCKET, SO_LINGER, pack('ii', 1, 0))
            return s.getsockname()[1]

    def get_docker_run_parameters(self) -> dict:
        parameters = super().get_docker_run_parameters()
        parameters['ports'] = {
            self._container_port: (self._host_interface, self._host_port),
        }
        return parameters

    def get_container_connection_string(self) -> str:
        return CONTAINER_CONNECTION_STRING_TEMPLATE.format(host=self._host_interface, port=self._host_port)


class DockerNetworkContainerConnectionData(AbstractContainerConnectionData):

    def __init__(self, container_port: int, docker_network: str):
        super().__init__(container_port)
        self._docker_network = docker_network

    def get_docker_run_parameters(self) -> dict:
        parameters = super().get_docker_run_parameters()
        parameters['network'] = self._docker_network
        return parameters

    def get_container_connection_string(self) -> str:
        return CONTAINER_CONNECTION_STRING_TEMPLATE.format(host=self._container_name, port=self._container_port)


class ParentDockerNetworkContainerConnectionData(AbstractContainerConnectionData):

    def __init__(self, container_port: int, parent_container_name: str):
        super().__init__(container_port)
        self._parent_container_name = parent_container_name

    def get_docker_run_parameters(self) -> dict:
        parameters = super().get_docker_run_parameters()
        parameters['network'] = "container:{parent_name}".format(parent_name=self._parent_container_name)
        return parameters

    def get_container_connection_string(self) -> str:
        return CONTAINER_CONNECTION_STRING_TEMPLATE.format(host=self._parent_container_name, port=self._container_port)


class AbstractDockerContextFactory(ABC, Generic[ContainerConnection]):
    """Run Docker containers with a context manager."""

    def __init__(self, config: 'ConfigParser', docker_image: str,
                 container_ready_min_delay_seconds: float, container_error_next_batch_delay_seconds: float,
                 container_port: int):
        self._config = config
        self._docker_image = docker_image
        self._container_ready_min_delay_seconds = container_ready_min_delay_seconds
        self._container_error_next_batch_delay_seconds = container_error_next_batch_delay_seconds
        self._container_port = container_port

        self._docker = from_env()

    @contextmanager
    def _new_container(self, error_log_message: str, container_providing_network: Optional[str] = None) \
            -> Generator[ContainerConnection, None, None]:
        before = monotonic()
        if container_providing_network is not None:
            container_connection = ParentDockerNetworkContainerConnectionData(self._container_port,
                                                                              container_providing_network)
        else:
            container_connection = build_container_connection_from_configuration(self._config, self._container_port)
        try:
            container = self._docker.containers.run(self._docker_image,
                                                    **self._get_run_parameters(container_connection))
        except DockerException:
            log.info("Could not start Docker container", exc_info=True)
            raise ContainerStartFailedException(self._container_error_next_batch_delay_seconds)
        try:
            yield self._set_up_container_and_get_connection(container, container_connection, error_log_message)
        finally:
            container.kill()
            log.debug("Stopped Docker container '%s'", container.id)

    def _get_run_parameters(self, container_connection: AbstractContainerConnectionData) -> dict:
        """To be overwritten in subclasses."""
        result = DEFAULT_RUN_PARAMETERS.copy()
        result.update(container_connection.get_docker_run_parameters())
        return result

    def _set_up_container_and_get_connection(self, container: Container,
                                             container_connection: AbstractContainerConnectionData,
                                             error_log_message: str) -> ContainerConnection:
        log.debug("Started Docker container '%s' ('%s'), waiting %s seconds before connecting",
                  container_connection.get_container_name(), container.id,
                  self._container_ready_min_delay_seconds)
        # container takes time to start up, reduce retry delays when attempting to connect in subclasses
        sleep(self._container_ready_min_delay_seconds)
        try:
            connection = self._get_container_connection(container_connection)
        except Exception:
            log.info("Could not connect to %s in Docker container", error_log_message, exc_info=True)
            raise ContainerStartFailedException(self._container_error_next_batch_delay_seconds)
        return connection

    @abstractmethod
    def _get_container_connection(self, container_connection: AbstractContainerConnectionData) -> ContainerConnection:
        """To be implemented in subclasses. Establishes a connection with the newly started container, and returns the
        connection object for use by client code. Should retry internally in case the container is not ready yet; if the
        code raises an exception, the container will be terminated and the exception will propagate to client code."""
        ...
