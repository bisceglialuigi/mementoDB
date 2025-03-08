import time
import struct
import os
import glob

class MementoDb:
    MAX_FILE_SIZE = 2 * 1024    # 2 KB
    TOMBSTONE = "__tombstone__"
    HEADER_SIZE = 16

    def __init__(self):
        #   { key:   (segment_file_name, offset, value_size) }
        self.dictionary = {}
        self.current_file_path = self._get_latest_log_file()
        #   reconstruct keydir from existing file
        self._load_index()
        print(self.dictionary)


    def _get_latest_log_file(self):
        log_files = sorted(glob.glob("file-*.log"))
        if log_files:
            return log_files[-1]
        return "file-1.log"


    def _rotate_segment_file(self):
        file_size = 0
        if os.path.exists(self.current_file_path):
            file_size = os.path.getsize(self.current_file_path)

        if file_size >= self.MAX_FILE_SIZE:
            file_number = int(self.current_file_path.split("-")[1].split(".")[0])
            self.current_file_path = f"file-{file_number + 1}.log"


    def _load_index(self):
        if not os.path.exists(self.current_file_path):
            return

        log_files = sorted(glob.glob("file-*.log"))

        for log_file_path in log_files:
            with open(log_file_path, "rb") as log_file:
                offset = 0
                while True:
                    # read the header (first 16 bytes from the current pos, TIMESTAMP (8 bytes) | KEY_SIZE (4 bytes) | VALUE_SIZE (4 bytes))
                    header = log_file.read(self.HEADER_SIZE)

                    if not header:
                        break

                    timestamp, key_size, value_size = struct.unpack("QII", header)

                    # retrieve the key
                    key = log_file.read(key_size).decode()
                    # retrieve the value
                    value = log_file.read(value_size)

                    # do not restore keys marked with tombstone marker
                    if value != self.TOMBSTONE:
                        # insert into keydir
                        self.dictionary[key] = (log_file_path, offset, value_size)

                        # update the offset for next fetch
                        offset += self.HEADER_SIZE + key_size + value_size


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

            # store in append in log_file: HEADER | KEY | VALUE
            log_file.write(header + key_bytes + value_bytes)

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
            reading_offset = offset + self.HEADER_SIZE + len(key)
            # seek from the reading offset
            log_file.seek(reading_offset)

            # fetch, after the offset, the VALUE BYTES
            value = log_file.read(value_size).decode()

        return value

    def delete(self, key):
        if key in self.dictionary:
            self.put(key, self.TOMBSTONE)
            del self.dictionary[key]



memento_db = MementoDb()

memento_db.put("key1", "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
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