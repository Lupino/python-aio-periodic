import asyncio
import logging
import hashlib
from typing import Tuple, cast

# Import cryptography components
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey

logger = logging.getLogger('aio_periodic.transport')

class Transport:
    def __init__(self, entrypoint: str) -> None:
        self.entrypoint = entrypoint

    async def get(self) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Establish underlying TCP or Unix Domain Socket connection"""
        logger.info(f"Connecting to: {self.entrypoint}")
        try:
            if '://' in self.entrypoint:
                protocol, addr = self.entrypoint.split('://')
            else:
                protocol, addr = 'tcp', self.entrypoint

            if protocol == 'unix':
                return await asyncio.open_unix_connection(addr)
            else:
                host, port = addr.split(':')
                return await asyncio.open_connection(host, int(port))
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise

class RSAStreamReader:
    """Wrapper for StreamReader providing RSA-OAEP decryption"""
    def __init__(self, reader: asyncio.StreamReader, private_key: RSAPrivateKey):
        self.reader = reader
        self.private_key = private_key
        # RSA ciphertext length matches the key modulus size in bytes
        self.block_size: int = private_key.key_size // 8

    async def read(self, n: int = -1) -> bytes:
        """Read and decrypt data. n defaults to block_size"""
        try:
            target_size = self.block_size
            # RSA decryption requires reading the exact full ciphertext block
            encrypted_data = await self.reader.readexactly(target_size)

            if not encrypted_data:
                return b''

            # Perform OAEP decryption (using SHA-256)
            decrypted = self.private_key.decrypt(
                encrypted_data,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            return cast(bytes, decrypted)
        except (asyncio.IncompleteReadError, Exception) as e:
            logger.debug(f"Read/Decrypt error: {e}")
            return b''

class RSAStreamWriter:
    """Wrapper for StreamWriter providing RSA-OAEP encryption"""
    def __init__(self, writer: asyncio.StreamWriter, public_key: RSAPublicKey):
        self.writer = writer
        self.public_key = public_key
        # OAEP SHA-256 overhead: 2 * 32 (hash) + 2 = 66 bytes
        self.max_plaintext: int = (public_key.key_size // 8) - 66

    def write(self, data: bytes) -> None:
        """Encrypt data in chunks and write to the stream"""
        if not data:
            return
        try:
            for i in range(0, len(data), self.max_plaintext):
                chunk = data[i : i + self.max_plaintext]
                encrypted = self.public_key.encrypt(
                    chunk,
                    padding.OAEP(
                        mgf=padding.MGF1(algorithm=hashes.SHA256()),
                        algorithm=hashes.SHA256(),
                        label=None
                    )
                )
                self.writer.write(encrypted)
        except Exception as e:
            logger.error(f"Encryption error: {e}")

    async def drain(self) -> None:
        await self.writer.drain()

    def close(self) -> None:
        self.writer.close()

    async def wait_closed(self) -> None:
        await self.writer.wait_closed()

class RSATransport:
    """Transport layer handling key loading and handshake logic"""
    def __init__(self, transport: Transport, private_key_path: str, server_pub_key_path: str):
        self.transport = transport
        self.private_key: RSAPrivateKey
        self.server_public_key: RSAPublicKey
        self._load_keys(private_key_path, server_pub_key_path)

    def _load_keys(self, priv_path: str, pub_path: str) -> None:
        """Load PEM keys and perform type narrowing for Mypy compatibility"""
        # Load private key
        with open(priv_path, "rb") as f:
            priv_raw = serialization.load_pem_private_key(f.read(), password=None)
            if not isinstance(priv_raw, rsa.RSAPrivateKey):
                raise TypeError(f"Key at {priv_path} is not an RSA Private Key")
            self.private_key = priv_raw

        # Load server public key
        with open(pub_path, "rb") as f:
            pub_raw = serialization.load_pem_public_key(f.read())
            if not isinstance(pub_raw, rsa.RSAPublicKey):
                raise TypeError(f"Key at {pub_path} is not an RSA Public Key")
            self.server_public_key = pub_raw

    async def get(self) -> Tuple[RSAStreamReader, RSAStreamWriter]:
        """Establish an encrypted transport channel"""
        reader, writer = await self.transport.get()

        rsa_reader = RSAStreamReader(reader, self.private_key)
        rsa_writer = RSAStreamWriter(writer, self.server_public_key)

        # Fingerprint calculation: Use PKCS#1 DER format with SHA-256
        # This ensures compatibility with components expecting PKCS#1 fingerprints
        my_pubkey = self.private_key.public_key()
        pub_bytes_pkcs1 = my_pubkey.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.PKCS1
        )
        fingerprint = hashlib.sha256(pub_bytes_pkcs1).digest()

        # Send 32-byte fingerprint
        rsa_writer.write(fingerprint)
        await rsa_writer.drain()

        # Optional: Add protocol handshake here if the server sends a confirmation
        await rsa_reader.read()

        return rsa_reader, rsa_writer
