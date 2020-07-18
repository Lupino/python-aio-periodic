from .types.utils import TYPE_CLIENT
from .types.base_client import BaseClient

class Client(BaseClient):

    def __init__(self, loop=None):
        BaseClient.__init__(self, TYPE_CLIENT, loop)
