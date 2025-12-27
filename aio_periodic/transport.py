import asyncio
import logging
import hashlib
import os
import struct
from enum import IntEnum
from typing import Tuple, cast, Optional

# Import cryptography components
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger('aio_periodic.transport')

# Constants matching Haskell configuration
AES_KEY_SIZE = 32
AES_IV_SIZE = 16
OAEP_OVERHEAD = 66  # SHA256 OAEP padding overhead

class RSAMode(IntEnum):
    """Matches Haskell: data RSAMode = Plain | RSA | AES"""
    Plain = 0
    RSA = 1
    AES = 2

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

class SecureStreamReader:
    """
    Unified Reader that handles Plain, RSA, or AES decryption based on negotiated mode.
    Matches Haskell recvData logic.
    """
    def __init__(self, reader: asyncio.StreamReader, private_key: RSAPrivateKey, mode: RSAMode, session_key: bytes = b''):
        self.reader = reader
        self.private_key = private_key
        self.mode = mode
        self.session_key = session_key
        # RSA ciphertext length matches key modulus size (e.g., 256 bytes for 2048-bit key)
        self.rsa_block_size: int = private_key.key_size // 8

    async def read(self, n: int = -1) -> bytes:
        try:
            if self.mode == RSAMode.Plain:
                return await self.reader.read(n)

            elif self.mode == RSAMode.RSA:
                return await self._read_rsa()

            elif self.mode == RSAMode.AES:
                return await self._read_aes()
        except Exception as e:
            logger.error(f"Secure Read Error ({self.mode.name}): {e}")
            raise

    async def _read_rsa(self) -> bytes:
        # RSA decryption requires reading the exact full ciphertext block
        encrypted_data = await self.reader.readexactly(self.rsa_block_size)
        if not encrypted_data:
            return b''

        return self.private_key.decrypt(
            encrypted_data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

    async def _read_aes(self) -> bytes:
        # Matches Haskell recvDataAes:
        # 1. Read Packet Length (8 bytes Int64 - BigEndian standard for Haskell Data.Binary)
        len_bytes = await self.reader.readexactly(8)
        pkt_len = struct.unpack('>Q', len_bytes)[0]

        # 2. Read Payload (IV + Ciphertext)
        payload = await self.reader.readexactly(pkt_len)

        # 3. Split IV and Ciphertext
        iv = payload[:AES_IV_SIZE]
        ciphertext = payload[AES_IV_SIZE:]

        # 4. Decrypt (AES-CTR)
        cipher = Cipher(algorithms.AES(self.session_key), modes.CTR(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        return decryptor.update(ciphertext) + decryptor.finalize()

class SecureStreamWriter:
    """
    Unified Writer that handles Plain, RSA, or AES encryption based on negotiated mode.
    Matches Haskell sendData logic.
    """
    def __init__(self, writer: asyncio.StreamWriter, public_key: RSAPublicKey, mode: RSAMode, session_key: bytes = b''):
        self.writer = writer
        self.public_key = public_key
        self.mode = mode
        self.session_key = session_key
        self.rsa_max_plaintext: int = (public_key.key_size // 8) - OAEP_OVERHEAD

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
        # Chunk data if it exceeds RSA block size limits
        for i in range(0, len(data), self.rsa_max_plaintext):
            chunk = data[i : i + self.rsa_max_plaintext]
            encrypted = self.public_key.encrypt(
                chunk,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            self.writer.write(encrypted)

    def _write_aes(self, data: bytes) -> None:
        # Matches Haskell sendDataAes:
        # 1. Generate IV
        iv = os.urandom(AES_IV_SIZE)

        # 2. Encrypt (AES-CTR)
        cipher = Cipher(algorithms.AES(self.session_key), modes.CTR(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(data) + encryptor.finalize()

        # 3. Construct Packet: [IV][Ciphertext]
        payload = iv + ciphertext

        # 4. Length Header (8 bytes BigEndian)
        # Haskell: encode (fromIntegral (BS.length payload) :: Int)
        len_header = struct.pack('>Q', len(payload))

        self.writer.write(len_header + payload)

    async def drain(self) -> None:
        await self.writer.drain()

    def close(self) -> None:
        self.writer.close()

    async def wait_closed(self) -> None:
        await self.writer.wait_closed()

class RSATransport:
    """
    Transport layer handling key loading, handshake, mode negotiation, and session setup.
    """
    def __init__(self, transport: Transport, private_key_path: str, server_pub_key_path: str, mode: RSAMode = RSAMode.AES):
        self.transport = transport
        self.mode = mode
        self.private_key: RSAPrivateKey
        self.server_public_key: RSAPublicKey
        self._load_keys(private_key_path, server_pub_key_path)

    def _load_keys(self, priv_path: str, pub_path: str) -> None:
        with open(priv_path, "rb") as f:
            self.private_key = serialization.load_pem_private_key(f.read(), password=None) # type: ignore

        with open(pub_path, "rb") as f:
            self.server_public_key = serialization.load_pem_public_key(f.read()) # type: ignore

    def _get_fingerprint(self, key: RSAPublicKey) -> bytes:
        """Calculate SHA256 fingerprint of DER encoded PKCS#1 public key"""
        # Ensure we are hashing the Public Key in PKCS1 format to match Haskell 'Data.ASN1' behavior usually
        # implied by Crypto.PubKey.RSA structure hashing in the provided context.
        pub_bytes = key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.PKCS1
        )
        return hashlib.sha256(pub_bytes).digest()

    async def _send_oaep(self, writer: asyncio.StreamWriter, data: bytes) -> None:
        """Helper to send initial handshake data using raw OAEP (Client -> Server)"""
        # Similar to _write_rsa but strictly for the handshake phase using raw writer
        max_size = (self.server_public_key.key_size // 8) - OAEP_OVERHEAD
        if len(data) > max_size:
             raise ValueError("Handshake data too large for RSA key")

        encrypted = self.server_public_key.encrypt(
            data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        writer.write(encrypted)
        await writer.drain()

    async def _recv_oaep(self, reader: asyncio.StreamReader) -> bytes:
        """Helper to recv handshake data using raw OAEP (Server -> Client)"""
        block_size = self.private_key.key_size // 8
        encrypted_data = await reader.readexactly(block_size)
        return self.private_key.decrypt(
            encrypted_data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

    async def get(self) -> Tuple[SecureStreamReader, SecureStreamWriter]:
        """Establish connection and perform RSA Handshake + Mode Negotiation"""
        # 1. Connect
        raw_reader, raw_writer = await self.transport.get()

        try:
            # --- Handshake: Identity Verification ---
            # Match Haskell: clientHandshake

            # Step 1: Send our public key fingerprint (Encrypted with Server PubKey)
            my_pub = self.private_key.public_key()
            my_fp = self._get_fingerprint(my_pub)
            await self._send_oaep(raw_writer, my_fp)

            # Step 2: Receive Server's fingerprint verification
            server_fp_incoming = await self._recv_oaep(raw_reader)

            # Verify Server Identity
            expected_server_fp = self._get_fingerprint(self.server_public_key)
            if server_fp_incoming != expected_server_fp:
                raise ConnectionError("Server fingerprint mismatch! Possible Man-in-the-Middle.")

            logger.info("RSA Handshake successful. Server verified.")

            # --- Mode Negotiation ---
            # Match Haskell: newTP -> Mode Negotiation

            # Client sends the configured mode to Server (Encrypted)
            # Haskell uses Data.Binary Generic for the Enum.
            # Typically 0=Plain, 1=RSA, 2=AES. We send it as a single byte.
            mode_byte = struct.pack('B', self.mode.value)
            await self._send_oaep(raw_writer, mode_byte)

            # --- Key Exchange ---
            session_key = b''
            if self.mode == RSAMode.AES:
                # Client generates key and sends it encrypted with Server's PubKey
                session_key = os.urandom(AES_KEY_SIZE)
                await self._send_oaep(raw_writer, session_key)
                logger.info("AES Session Key established.")

            # Return wrapped streams
            secure_reader = SecureStreamReader(raw_reader, self.private_key, self.mode, session_key)
            secure_writer = SecureStreamWriter(raw_writer, self.server_public_key, self.mode, session_key)

            return secure_reader, secure_writer

        except Exception as e:
            raw_writer.close()
            await raw_writer.wait_closed()
            raise e
