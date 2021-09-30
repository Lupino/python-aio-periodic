from .types.utils import TYPE_CLIENT
from .types.base_client import BaseClient, BaseCluster


class Client(BaseClient):
    def __init__(self):
        BaseClient.__init__(self, TYPE_CLIENT)


class ClientCluster(BaseCluster):
    def __init__(self, *args, **kwargs):
        BaseCluster.__init__(self, Client, *args, **kwargs)
