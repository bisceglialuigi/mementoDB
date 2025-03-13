import struct
import hashlib


class Header:
    TIMESTAMP_SIZE = 8
    KEY_SIZE = 4
    VALUE_SIZE = 4
    SIZE = TIMESTAMP_SIZE + KEY_SIZE + VALUE_SIZE

    def __init__(self, header_bytes):
        if len(header_bytes) != self.SIZE:
            raise ValueError(f"Invalid header size. Expected {self.SIZE} bytes, got {len(header_bytes)}")

        self.timestamp, self.key_size, self.value_size = struct.unpack("QII", header_bytes)

    def get_timestamp(self):
        return self.timestamp

    def get_key_size(self):
        return self.key_size

    def get_value_size(self):
        return self.value_size


class Payload:
    CHECKSUM_SIZE = 32

    def __init__(self, key, value):
        self.key = key
        self.key_bytes = self.key.encode()
        self.value = value
        self.value_bytes = self.value.encode()
        self.checksum = self.calculate_checksum()
        self.size = len(self.key_bytes) + len(self.value_bytes) + self.CHECKSUM_SIZE

    @classmethod
    def from_bytes(cls, header, payload_bytes):
        key_bytes = payload_bytes[:header.get_key_size()]
        key = key_bytes.decode()
        value_bytes = payload_bytes[header.get_key_size():header.get_key_size() + header.get_value_size()]
        value = value_bytes.decode()
        checksum = payload_bytes[header.get_key_size() + header.get_value_size():]

        instance = cls(key, value)
        instance.checksum = checksum  # Overwrite calculated checksum with stored one
        return instance

    def get_key(self):
        return self.key

    def get_key_bytes(self):
        return self.key_bytes

    def get_value(self):
        return self.value

    def get_value_bytes(self):
        return self.value_bytes

    def get_size(self):
        return self.size

    def get_checksum(self):
        return self.checksum

    def calculate_checksum(self):
        data = self.key_bytes + self.value_bytes
        return hashlib.sha256(data).digest()

    def is_data_integrity_ok(self):
        return self.checksum == self.calculate_checksum()