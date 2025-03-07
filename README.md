# MementoDB

MementoDB is a **toy project** designed for learning about database internals. It is inspired by the **Bitcask** key-value store and implements a simple **append-only** log-based storage engine.

## Features
- **Append-Only Storage:** New writes are appended to a log file (`file.log`).
- **In-Memory Index:** Keeps track of key locations for fast lookups.
- **Fast Reads:** Values are retrieved using an in-memory index for quick access.
- **Basic Deletes:** Implements soft deletes using a "tombstone" marker.
- **Persistence:** Data is stored on disk and loaded into memory on startup.

## Time Complexity
| Operation      | Time Complexity | Explanation |
|---------------|---------------|-------------|
| **Put**       | **O(1)**      | Append to file + update dictionary |
| **Get**       | **O(1)**      | Dictionary lookup + file seek/read |
| **Delete**    | **O(1)**      | Append tombstone + remove from dictionary |
| **Startup (_load_index)** | **O(n)** | Scan entire log file to rebuild index |
| **Compaction (Future Feature)** | **O(n)** | Read and rewrite only latest key-value pairs |

## How It Works
1. **Writes (`put`)**
   - Each key-value pair is written to `file.log` with a header containing a timestamp, key size, and value size.
   - The in-memory index (`keydir`) tracks the file offset for each key.

2. **Reads (`get`)**
   - The in-memory index is used to find the offset of a key in the log file.
   - The file is read at the correct position to retrieve the value.

3. **Deletes (`delete`)**
   - A "tombstone" marker (`__tombstone__`) is written to indicate deletion.
   - The key is removed from the in-memory index.

## Critical Issues & Planned Solutions
While the current implementation provides **O(1) performance** for inserts, reads, and deletes, there are some **critical issues** that will be solved in future updates:

### **1. Log Growth (Unbounded File Size)**
- Since writes and deletes **append** new data instead of updating in place, the log file grows indefinitely.
- **Planned Solution:** **Log Compaction** – periodically rewrite the log file to keep only the latest versions of each key.

### **2. No Concurrency Support**
- The database is **not safe** for multiple processes or threads.
- **Planned Solution:** Implement **file locking and transaction support**.

### **3. Startup Time (O(n) Index Rebuild)**
- Every time the database starts, it **scans the entire log file** to rebuild the in-memory index.
- **Planned Solution:** Implement **checkpointing** or **hint files** to store index snapshots.

### **4. No Configuration Options**
- File paths, max file size, and other settings are hardcoded.
- **Planned Solution:** Allow **user-defined settings** for storage paths, compaction frequency, etc.

### **5. No Tests Yet**
- There are currently **no automated tests** to ensure correctness.
- **Planned Solution:** Write **unit tests** and **integration tests** to verify correctness and stability.

## TODO
- **Log Compaction**: Merge old log files to remove stale data and reduce file size.
- **Concurrency Support**: Implement file locking for multiple readers/writers.
- **Configuration Options**: Allow users to configure storage paths, max file size, etc.
- **Performance Improvements**: Use memory-mapped files or buffering for efficiency.
- **Error Handling**: Improve handling of file I/O errors.
- **Write Tests**: Implement unit tests to validate core functionalities.

## Disclaimer
This project is **not intended for production use**. It is a simple implementation meant for educational purposes to understand database storage engines and indexing techniques.
