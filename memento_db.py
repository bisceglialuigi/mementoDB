import time
import struct
import os
import glob
import hashlib


class MementoDb:
    MAX_FILE_SIZE = 2 * 1024    # 2 KB
    CHECKSUM_SIZE = 32
    FIRST_FILE_NAME = "file-1.log"
    FILE_NAME_PATTERN = "file-*.log"
    TOMBSTONE = "__tombstone__"


    def __init__(self):
        #   { key:   (segment_file_name, offset, value_size) }
        self.dictionary = {}
        self.current_file_path = self._get_latest_log_file()
        #   reconstruct keydir from existing file
        self._reload_index_from_disk()
        print(self.dictionary)


    def _get_latest_log_file(self):
        log_files = sorted(glob.glob(self.FILE_NAME_PATTERN))
        if log_files:
            return log_files[-1]
        return self.FIRST_FILE_NAME


    def _rotate_segment_file(self):
        file_size = 0
        if os.path.exists(self.current_file_path):
            file_size = os.path.getsize(self.current_file_path)

        if file_size >= self.MAX_FILE_SIZE:
            file_number = int(self.current_file_path.split("-")[1].split(".")[0])
            self.current_file_path = f"file-{file_number + 1}.log"


    def _calculate_checksum(self, key_bytes, value_bytes):
        data = key_bytes + value_bytes
        return hashlib.sha256(data).digest()


    def _reload_index_from_disk(self):
        if not os.path.exists(self.current_file_path):
            return

        log_files = sorted(glob.glob(self.FILE_NAME_PATTERN))

        for log_file_path in log_files:
            with open(log_file_path, "rb") as log_file:
                offset = 0
                while True:
                    # read the header (first 16 bytes from the current pos, TIMESTAMP (8 bytes) | KEY_SIZE (4 bytes) | VALUE_SIZE (4 bytes))
                    header_bytes = log_file.read(Header.SIZE)

                    if not header_bytes:
                        break

                    header = Header(header_bytes)

                    # retrieve the key
                    offset += Header.SIZE
                    log_file.seek(offset)
                    key_bytes = log_file.read(header.get_key_size())
                    key = key_bytes.decode()

                    # retrieve the value
                    offset += header.get_key_size()
                    log_file.seek(offset)
                    value_bytes = log_file.read(header.get_value_size())
                    value = value_bytes.decode()

                    # retrieve the checksum
                    offset += header.get_value_size()
                    log_file.seek(offset)
                    fetched_checksum = log_file.read(self.CHECKSUM_SIZE)

                    checksum = self._calculate_checksum(key_bytes, value_bytes)

                    offset += self.CHECKSUM_SIZE
                    log_file.seek(offset)

                    if fetched_checksum != checksum:
                        print(f"Key {key} has corrupted data, not restoring it.")
                        continue

                    # do not restore keys marked with tombstone marker
                    if value != self.TOMBSTONE:
                        # insert into keydir
                        self.dictionary[key] = (log_file_path, offset, header.get_value_size())


    def put(self, key, value):

        self._rotate_segment_file()

        # create HEADER to store in log file as TIMESTAMP | KEY_SIZE | VALUE_SIZE
        timestamp = int(time.time())
        key_bytes = key.encode()
        value_bytes = value.encode()
        header = struct.pack("QII", timestamp, len(key_bytes), len(value_bytes))


        with open(self.current_file_path, "ab") as log_file:
            # get offset from file
            offset = log_file.tell()

            checksum = self._calculate_checksum(key_bytes, value_bytes)

            # store in append in log_file: HEADER | KEY | VALUE | CHECKSUM
            log_file.write(header + key_bytes + value_bytes + checksum)

        # update key dictionary with
        self.dictionary[key] = (self.current_file_path, offset, len(value_bytes))

    def get(self, key):
        if key not in self.dictionary:
            return None

        # fetch the segment where the data are stored, the offset and the size of the value
        segment_log_file, offset, value_size = self.dictionary[key]

        # open the file
        with open(segment_log_file, "rb") as log_file:
            # the reading starts at OFFSET (in bytes) + HEADERS BYTES + KEY BYTES, what remains are the VALUE BYTES
            reading_offset = offset + Header.SIZE
            log_file.seek(reading_offset)
            # right after the header, it is stored the key
            key_bytes = log_file.read(len(key))

            reading_offset += len(key)
            log_file.seek(reading_offset)
            value_bytes = log_file.read(value_size)

            reading_offset += len(value_bytes.decode())
            log_file.seek(reading_offset)
            fetched_checksum = log_file.read(self.CHECKSUM_SIZE)

            checksum = self._calculate_checksum(key_bytes, value_bytes)
            if fetched_checksum != checksum:
                raise ValueError(f"Data corrupted for key {key}")

        value = value_bytes.decode()
        return value

    def delete(self, key):
        if key in self.dictionary:
            self.put(key, self.TOMBSTONE)
            del self.dictionary[key]



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