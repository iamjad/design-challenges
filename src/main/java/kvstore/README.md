# KV Store
This basic KV Store implementation uses sequential writes and random access reads. This is a simple Key Value Store that supports String-based keys and Values. The goal is to show how open-source Key Value data stores provide Superfast reads and writes while maintaining terabytes of data.

## Disk Writes 
This implementation stores data in HFile. The key and value are always appended to the HFile in HFileDataRow Format. Appending to the file ensures Sequential Writes, which increases disk performance. 

## Reads
For faster reads, the offset of the row is stored in the in-memory HashMap with the key of the HBase. To Perform a read, It reads the offsets from the hashmap and then uses RandomAccessFile.seek(offset) to read the starting byte of the row. This avoids any scanning or indeterministic iteration of the large file. The index is also flushed to a file. For every put and delete.

## Updates

Updating key results in an offset update in the in-memory hashmap so the older values are ignored in the File, and reading the value of the key will always map to the latest value. During the file compaction process, the older values are removed from the HFile.

## Deletes
When a key is deleted, it is not immediately removed from the file but marked for deletion using a tombstone flag. The index points to a new row in the HFile with tombstone = true. The idea is to remove the deleted key rows from the file during the file compaction operation. Reading keys after the delete operation results in null value. 




Java File Operations
https://www.codejava.net/java-se/file-io/java-io-how-to-use-randomaccess-file-java-io-package
