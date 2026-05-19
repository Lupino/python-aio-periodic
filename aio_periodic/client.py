from typing import List, Optional

from .types.utils import TYPE_CLIENT
from .types.base_client import (BaseClient, BaseCluster, MessageCallbackFunc,
                                OnConnectedFunc, OnDisconnectedFunc)


class Client(BaseClient):
    """
    Standard client for submitting jobs to the Periodic server.
    Fixed with TYPE_CLIENT identification.
    """

    def __init__(
        self,
        message_callback: Optional[MessageCallbackFunc] = None,
        on_connected: Optional[OnConnectedFunc] = None,
        on_disconnected: Optional[OnDisconnectedFunc] = None,
    ) -> None:
        """
        Initialize the Client.
        Passes extra arguments (like callbacks) to the BaseClient.
        """
        super().__init__(TYPE_CLIENT, message_callback, on_connected,
                         on_disconnected)


class ClientCluster(BaseCluster):
    """
    Cluster manager for Client instances.
    Handles sharding or failover across multiple Periodic servers.
    """

    def __init__(
        self,
        entrypoints: List[str],
        message_callback: Optional[MessageCallbackFunc] = None,
        on_connected: Optional[OnConnectedFunc] = None,
        on_disconnected: Optional[OnDisconnectedFunc] = None,
    ) -> None:
        super().__init__(Client,
                         entrypoints,
                         message_callback=message_callback,
                         on_connected=on_connected,
                         on_disconnected=on_disconnected)
