import time
import struct
import os

class Memento:
    def __init__(self):
        self.keydir = {}            #   { key:   (offset, value_size) }
        self._load_index()          #   reconstruct keydir from existing file
        print(self.keydir)

    def _load_index(self):
        if not os.path.exists("file.log"):
            return

        log_file = open("file.log", "rb")

        offset = 0
        while True:
            # read the header (first 16 bytes from the current pos, TIMESTAMP (8 bytes) | KEY_SIZE (4 bytes) | VALUE_SIZE (4 bytes))
            header = log_file.read(16)

            if not header:
                break

            timestamp, key_size, value_size = struct.unpack("QII", header)

            # retrieve the key
            key = log_file.read(key_size).decode()
            # retrieve and discard value
            log_file.read(value_size)

            # insert into keydir
            self.keydir[key] = (offset, value_size)

            # update the offset for next fetch
            offset += 16 + key_size + value_size


    def put(self, key, value):
        # create HEADER to store in log file as TIMESTAMP | KEY_SIZE | VALUE_SIZE
        timestamp = int(time.time())
        key_bytes = key.encode()
        value_bytes = value.encode()
        header = struct.pack("QII", timestamp, len(key_bytes), len(value_bytes))

        # get offset from file
        log_file = open("file.log", "ab")
        offset = log_file.tell()

        # store in append in log_file: HEADER | KEY | VALUE
        log_file.write(header + key_bytes + value_bytes)

        # update key dictionary with
        self.keydir[key] = (offset, len(value_bytes))

    def get(self, key):
        if key not in self.keydir:
            return None

        offset, value_size = self.keydir[key]

        # open the file
        log_file = open("file.log", "rb")

        # the reading starts at OFFSET (in bytes) + HEADERS BYTES + KEY BYTES, what remains are the VALUE BYTES
        reading_offset = offset + 16 + len(key)
        # seek from the reading offset
        log_file.seek(reading_offset)

        # fetch, after the offset, the VALUE BYTES
        value = log_file.read(value_size).decode()

        return value

    def delete(self, key):
        if key in self.keydir:
            self.put(key, "__tombstone__")
            del self.keydir[key]



memento_db = Memento()

memento_db.put("key1", "first value")
value = memento_db.get("key1")
print(value)

memento_db.put("key2", "second value")
value = memento_db.get("key2")
print(value)

memento_db.put("key1", "first value overridden")
value = memento_db.get("key1")
print(value)

memento_db.delete("key1")
value = memento_db.get("key1")
print(value)