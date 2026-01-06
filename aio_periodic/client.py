from typing import Any
from .types.utils import TYPE_CLIENT
from .types.base_client import BaseClient, BaseCluster


class Client(BaseClient):
    """
    Standard client for submitting jobs to the Periodic server.
    Fixed with TYPE_CLIENT identification.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize the Client.
        Passes extra arguments (like callbacks) to the BaseClient.
        """
        super().__init__(TYPE_CLIENT, *args, **kwargs)


class ClientCluster(BaseCluster):
    """
    Cluster manager for Client instances.
    Handles sharding or failover across multiple Periodic servers.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(Client, *args, **kwargs)
