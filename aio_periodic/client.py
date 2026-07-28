"""Provide client-role connections to Periodic servers."""

from typing import List, Optional

from .types.utils import TYPE_CLIENT
from .types.base_client import (BaseClient, BaseCluster, MessageCallbackFunc,
                                OnConnectedFunc, OnDisconnectedFunc)


class Client(BaseClient):
    """Submit jobs to a Periodic server using the client protocol role.

    ``TYPE_CLIENT`` is fixed here so callers cannot accidentally register a
    job-submitting client as another protocol participant.
    """

    def __init__(
        self,
        message_callback: Optional[MessageCallbackFunc] = None,
        on_connected: Optional[OnConnectedFunc] = None,
        on_disconnected: Optional[OnDisconnectedFunc] = None,
        client_name: Optional[str] = None,
        client_token: Optional[str] = None,
    ) -> None:
        """Initialize a client-role connection before it is connected.

        Args:
            message_callback: Handler for server-initiated messages.
            on_connected: Coroutine invoked after the connection handshake.
            on_disconnected: Coroutine invoked after the connection closes.
            client_name: Optional name included in the client registration.
            client_token: Optional token included in the client registration.
        """
        super().__init__(TYPE_CLIENT, message_callback, on_connected,
                         on_disconnected, client_name, client_token)


class ClientCluster(BaseCluster):
    """Route client requests across multiple Periodic server entrypoints.

    The base cluster uses ``Client`` instances so every selected endpoint has
    the same client-role registration and callback behavior.
    """

    def __init__(
        self,
        entrypoints: List[str],
        message_callback: Optional[MessageCallbackFunc] = None,
        on_connected: Optional[OnConnectedFunc] = None,
        on_disconnected: Optional[OnDisconnectedFunc] = None,
        client_name: Optional[str] = None,
        client_token: Optional[str] = None,
    ) -> None:
        """Initialize a cluster of client-role connections.

        Args:
            entrypoints: Server endpoints available to the cluster.
            message_callback: Handler for server-initiated messages.
            on_connected: Coroutine invoked after an endpoint connects.
            on_disconnected: Coroutine invoked after an endpoint disconnects.
            client_name: Optional name included in each client registration.
            client_token: Optional token included in each client registration.
        """
        super().__init__(Client,
                         entrypoints,
                         message_callback=message_callback,
                         on_connected=on_connected,
                         on_disconnected=on_disconnected,
                         client_name=client_name,
                         client_token=client_token)
