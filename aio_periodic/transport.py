import asyncio
import logging
import hashlib
import os
import struct
from enum import IntEnum
from typing import Tuple
from functools import partial

# Import cryptography components
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, \
    RSAPublicKey
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger('aio_periodic.transport')

# Constants matching Haskell configuration
AES_KEY_SIZE = 32
AES_IV_SIZE = 16
OAEP_OVERHEAD = 66
MAX_PACKET_SIZE = 100 * 1024 * 1024  # Safety: Max packet size 100MB


class RSAMode(IntEnum):
    """Matches Haskell: data RSAMode = Plain | RSA | AES"""
    Plain = 0
    RSA = 1
    AES = 2


class BaseTransport(object):

    async def get(self) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        raise NotImplementedError('must be implemented by subclass')


class Transport(BaseTransport):

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


class SecureStreamReader(asyncio.StreamReader):
    """
    Unified Reader that handles Plain, RSA, or AES decryption.
    Implements internal buffering to support readexactly() with block-based
    crypto.
    """

    def __init__(self,
                 reader: asyncio.StreamReader,
                 private_key: RSAPrivateKey,
                 mode: RSAMode,
                 session_key: bytes = b''):
        # Initialize parent to set up internals
        super().__init__()

        self.reader = reader
        self.private_key = private_key
        self.mode = mode
        self.session_key = session_key
        self.rsa_block_size: int = private_key.key_size // 8
        self._loop = asyncio.get_running_loop()

        # Internal buffer to store decrypted data that hasn't been consumed yet
        self._decrypted_buffer = bytearray()

    async def read(self, n: int = -1) -> bytes:
        if self.mode == RSAMode.Plain:
            return await self.reader.read(n)

        try:
            # 1. If we have buffered data, return it first
            if self._decrypted_buffer:
                if n == -1:
                    data = bytes(self._decrypted_buffer)
                    self._decrypted_buffer = bytearray()
                    return data
                else:
                    available = min(len(self._decrypted_buffer), n)
                    data = self._decrypted_buffer[:available]
                    del self._decrypted_buffer[:available]
                    return bytes(data)

            # 2. If buffer is empty, fetch one new chunk
            # Note: We do not loop here; read() typically returns whatever is
            # available
            if self.mode == RSAMode.RSA:
                return await self._read_rsa()

            # RSAMode.AES
            return await self._read_aes()

        except Exception as e:
            logger.error(f"Secure Read Error ({self.mode.name}): {e}")
            self._exception = e
            raise

    async def readexactly(self, n: int) -> bytes:
        """
        Read exactly n bytes.
        Buffers decrypted packets until n bytes are available.
        """
        if self.mode == RSAMode.Plain:
            return await self.reader.readexactly(n)

        while len(self._decrypted_buffer) < n:
            try:
                # Fetch next logical chunk
                if self.mode == RSAMode.RSA:
                    chunk = await self._read_rsa()
                elif self.mode == RSAMode.AES:
                    chunk = await self._read_aes()
            except asyncio.IncompleteReadError:
                chunk = b''
            except Exception as e:
                logger.error(
                    f"Secure ReadExactly Error ({self.mode.name}): {e}")
                self._exception = e
                raise

            # Handle EOF
            if not chunk:
                partial_data = bytes(self._decrypted_buffer)
                self._decrypted_buffer = bytearray()
                raise asyncio.IncompleteReadError(partial_data, n)

            self._decrypted_buffer.extend(chunk)

        # Extract exactly n bytes
        data = self._decrypted_buffer[:n]
        del self._decrypted_buffer[:n]
        return bytes(data)

    async def _read_rsa(self) -> bytes:
        # RSA decryption requires reading the exact full ciphertext block
        encrypted_data = await self.reader.readexactly(self.rsa_block_size)
        if not encrypted_data:
            return b''

        return await self._loop.run_in_executor(
            None,
            partial(
                self.private_key.decrypt, encrypted_data,
                padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()),
                             algorithm=hashes.SHA256(),
                             label=None)))

    async def _read_aes(self) -> bytes:
        # 1. Read Packet Length (8 bytes Int64 - BigEndian)
        len_bytes = await self.reader.readexactly(8)
        pkt_len = struct.unpack('>Q', len_bytes)[0]

        if pkt_len > MAX_PACKET_SIZE:
            raise ValueError(f"Packet length {pkt_len} exceeds max allowed.")

        # 2. Read Payload (IV + Ciphertext)
        payload = await self.reader.readexactly(pkt_len)

        mv = memoryview(payload)
        iv = mv[:AES_IV_SIZE].tobytes()
        ciphertext = mv[AES_IV_SIZE:].tobytes()

        # 3. Decrypt
        def _decrypt_sync() -> bytes:
            cipher = Cipher(algorithms.AES(self.session_key),
                            modes.CTR(iv),
                            backend=default_backend())
            decryptor = cipher.decryptor()
            return decryptor.update(ciphertext) + decryptor.finalize()

        return await self._loop.run_in_executor(None, _decrypt_sync)


class SecureStreamWriter(asyncio.StreamWriter):
    """
    Unified Writer that handles Plain, RSA, or AES encryption.
    """

    def __init__(self,
                 writer: asyncio.StreamWriter,
                 public_key: RSAPublicKey,
                 mode: RSAMode,
                 session_key: bytes = b''):
        self.writer = writer
        self.public_key = public_key
        self.mode = mode
        self.session_key = session_key
        self.rsa_max_plaintext: int = (public_key.key_size //
                                       8) - OAEP_OVERHEAD
        self._transport = writer.transport
        self._loop = asyncio.get_running_loop()

    def write(self, data: bytes) -> None:
        if not data:
            return

        try:
            if self.mode == RSAMode.Plain:
                self.writer.write(data)
            elif self.mode == RSAMode.RSA:
                self._write_rsa(data)
            elif self.mode == RSAMode.AES:
                self._write_aes(data)
        except Exception as e:
            logger.error(f"Secure Write Error ({self.mode.name}): {e}")
            raise

    def _write_rsa(self, data: bytes) -> None:
        for i in range(0, len(data), self.rsa_max_plaintext):
            chunk = data[i:i + self.rsa_max_plaintext]
            encrypted = self.public_key.encrypt(
                chunk,
                padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()),
                             algorithm=hashes.SHA256(),
                             label=None))
            self.writer.write(encrypted)

    def _write_aes(self, data: bytes) -> None:
        iv = os.urandom(AES_IV_SIZE)

        cipher = Cipher(algorithms.AES(self.session_key),
                        modes.CTR(iv),
                        backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(data) + encryptor.finalize()

        payload_len = len(iv) + len(ciphertext)
        len_header = struct.pack('>Q', payload_len)

        self.writer.write(len_header)
        self.writer.write(iv)
        self.writer.write(ciphertext)

    async def drain(self) -> None:
        await self.writer.drain()

    def close(self) -> None:
        self.writer.close()

    async def wait_closed(self) -> None:
        await self.writer.wait_closed()


class RSATransport(BaseTransport):
    """
    Transport layer handling key loading, handshake, mode negotiation.
    """

    def __init__(self,
                 transport: Transport,
                 private_key_path: str,
                 server_pub_key_path: str,
                 mode: RSAMode = RSAMode.AES):
        self.transport = transport
        self.mode = mode
        self.private_key: RSAPrivateKey
        self.server_public_key: RSAPublicKey
        self._load_keys(private_key_path, server_pub_key_path)

    def _load_keys(self, priv_path: str, pub_path: str) -> None:
        with open(priv_path, "rb") as f:
            self.private_key = serialization.load_pem_private_key(
                f.read(), password=None)  # type: ignore

        with open(pub_path, "rb") as f:
            self.server_public_key = serialization.load_pem_public_key(
                f.read())  # type: ignore

    def _get_fingerprint(self, key: RSAPublicKey) -> bytes:
        pub_bytes = key.public_bytes(encoding=serialization.Encoding.DER,
                                     format=serialization.PublicFormat.PKCS1)
        return hashlib.sha256(pub_bytes).digest()

    async def _send_oaep(self, writer: asyncio.StreamWriter,
                         data: bytes) -> None:
        max_size = (self.server_public_key.key_size // 8) - OAEP_OVERHEAD
        if len(data) > max_size:
            raise ValueError("Handshake data too large for RSA key")

        loop = asyncio.get_running_loop()
        encrypted = await loop.run_in_executor(
            None,
            partial(
                self.server_public_key.encrypt, data,
                padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()),
                             algorithm=hashes.SHA256(),
                             label=None)))
        writer.write(encrypted)
        await writer.drain()

    async def _recv_oaep(self, reader: asyncio.StreamReader) -> bytes:
        block_size = self.private_key.key_size // 8
        encrypted_data = await reader.readexactly(block_size)

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            partial(
                self.private_key.decrypt, encrypted_data,
                padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()),
                             algorithm=hashes.SHA256(),
                             label=None)))

    async def get(self) -> Tuple[SecureStreamReader, SecureStreamWriter]:
        raw_reader, raw_writer = await self.transport.get()

        try:
            # 1. Send our public key fingerprint
            my_pub = self.private_key.public_key()
            my_fp = self._get_fingerprint(my_pub)
            await self._send_oaep(raw_writer, my_fp)

            # 2. Receive Server's fingerprint verification
            server_fp_incoming = await self._recv_oaep(raw_reader)

            # 3. Verify Server Identity
            expected_server_fp = self._get_fingerprint(self.server_public_key)
            if server_fp_incoming != expected_server_fp:
                raise ConnectionError("Server fingerprint mismatch!")

            logger.info("RSA Handshake successful. Server verified.")

            # 4. Mode Negotiation
            mode_byte = struct.pack('B', self.mode.value)
            await self._send_oaep(raw_writer, mode_byte)

            # 5. Key Exchange
            session_key = b''
            if self.mode == RSAMode.AES:
                session_key = os.urandom(AES_KEY_SIZE)
                await self._send_oaep(raw_writer, session_key)
                logger.info("AES Session Key established.")

            secure_reader = SecureStreamReader(raw_reader, self.private_key,
                                               self.mode, session_key)
            secure_writer = SecureStreamWriter(raw_writer,
                                               self.server_public_key,
                                               self.mode, session_key)

            return secure_reader, secure_writer

        except Exception as e:
            raw_writer.close()
            await raw_writer.wait_closed()
            raise e
