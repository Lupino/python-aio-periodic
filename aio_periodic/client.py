from .types.utils import TYPE_CLIENT
from .types.base_client import BaseClient, BaseCluster
from typing import Any


class Client(BaseClient):

    def __init__(self) -> None:
        BaseClient.__init__(self, TYPE_CLIENT)


class ClientCluster(BaseCluster):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        BaseCluster.__init__(self, Client, *args, **kwargs)
