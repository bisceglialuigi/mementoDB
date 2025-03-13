import time
import struct
import os
import glob

from models import Header, Payload


class MementoDb:
    MAX_FILE_SIZE = 2 * 1024    # 2 KB
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


    def _reload_index_from_disk(self):
        if not os.path.exists(self.current_file_path):
            return

        log_files = sorted(glob.glob(self.FILE_NAME_PATTERN))

        for log_file_path in log_files:
            with open(log_file_path, "rb") as log_file:
                offset = 0
                while True:
                    # read the header
                    header_bytes = log_file.read(Header.SIZE)

                    if not header_bytes:
                        break

                    header = Header(header_bytes)

                    # right after the header, it is stored the payload
                    offset += Header.SIZE
                    log_file.seek(offset)
                    payload_bytes = log_file.read(header.get_key_size() + header.get_value_size() + Payload.CHECKSUM_SIZE)
                    payload = Payload.from_bytes(header, payload_bytes)

                    if not payload.is_data_integrity_ok():
                        print(f"Key {payload.get_key()} has corrupted data, not restoring it.")
                        continue

                    # do not restore keys marked with tombstone marker
                    if payload.get_value() != self.TOMBSTONE:
                        # insert into keydir
                        self.dictionary[payload.get_key()] = (log_file_path, offset, header.get_value_size())

                    offset += payload.get_size()
                    log_file.seek(offset)


    def put(self, key, value):

        self._rotate_segment_file()

        payload = Payload(key, value)

        # create HEADER to store in log file as TIMESTAMP | KEY_SIZE | VALUE_SIZE
        timestamp = int(time.time())

        header = struct.pack("QII", timestamp, len(payload.get_key_bytes()), len(payload.get_value_bytes()))


        with open(self.current_file_path, "ab") as log_file:
            # get offset from file
            offset = log_file.tell()

            # store in append in log_file: HEADER | KEY | VALUE | CHECKSUM
            log_file.write(header + payload.get_key_bytes() + payload.get_value_bytes() + payload.get_checksum())

        # update key dictionary with
        self.dictionary[key] = (self.current_file_path, offset, len(payload.get_value_bytes()))

    def get(self, key):
        if key not in self.dictionary:
            return None

        # fetch the segment where the data are stored, the offset and the size of the value
        segment_log_file, offset, value_size = self.dictionary[key]

        # open the file
        with open(segment_log_file, "rb") as log_file:
            # the reading starts at OFFSET (in bytes) + HEADERS BYTES + KEY BYTES, what remains are the VALUE BYTES
            reading_offset = offset
            log_file.seek(reading_offset)
            header_bytes = log_file.read(Header.SIZE)
            header = Header(header_bytes)

            # right after the header, it is stored the payload
            reading_offset += Header.SIZE
            log_file.seek(reading_offset)
            payload_bytes = log_file.read(header.get_key_size() + header.get_value_size() + Payload.CHECKSUM_SIZE)
            payload = Payload.from_bytes(header, payload_bytes)

            if not payload.is_data_integrity_ok():
                raise ValueError(f"Data corrupted for key {key}")

        return payload.get_value()

    def delete(self, key):
        if key in self.dictionary:
            self.put(key, self.TOMBSTONE)
            del self.dictionary[key]