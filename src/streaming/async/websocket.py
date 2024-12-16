import abc
from typing import Callable, Protocol, TypeVar

from attr import define

WebSocketApp = TypeVar("WebSocketApp", bound="WebSocketClientProtocol")



@define
class WebSocketClient(abc.ABC):
    """Abstract base class for an async WebSocket client."""

    url: str
    _ws: Any

    def __init__(self, url: str):
        """Initialize the WebSocket client.

        Args:
            url (str): The WebSocket URL to connect to.
        """
        self.url = url
        self._ws = None

    @abc.abstractmethod
    async def connect(self):
        """Establish a connection to the WebSocket server."""
        pass

    @abc.abstractmethod
    async def send(self, message: str):
        """Send a message to the WebSocket server.

        Args:
            message (str): The message to send.
        """
        pass

    @abc.abstractmethod
    async def receive(self) -> str:
        """Receive a message from the WebSocket server.

        Returns:
            str: The received message.
        """
        pass

    @abc.abstractmethod
    async def close(self):
        """Close the WebSocket connection."""
        pass

