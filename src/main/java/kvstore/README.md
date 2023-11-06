# KV Store
This is a basic KV Store implementation using sequential writes and ramdom access reads. This is a simple Key Value Store that supports String based keys and Values. The goal is to show how open source Key Value datastores provide Superfast reads and writes while maintaining terabytes of data.

## Writes 
This implementation stores data in HFile. the key and value are always appended to the HFile in HFileDataRow Format. Appending to the file ensures Sequential Writes which increases Disk Performance. 

## Reads
For faster reads, the offset of the row is stored in the in-momery HashMap with key of the HBase. To Perform a read, It reads the offsets from hashmap and then uses RandomAccessFile.seek(offset) to read starting byte of the row. This avoids any scanning or inderministic iteration of the large file.

## Deletes
When a key is deleted, it is not immediately removed from the file but a new value of the key is stored and during the file compaction operation, it is removed from the file. Reading after the delete operation result in key not found.

## Updates

Updating a key results in an offset update in the in-memory hashmap so the older values are ignored in the File and reading the value of the key will always map to latest value. During file compaction process, the older values are removed from the HFile.

Java File Operations
https://www.codejava.net/java-se/file-io/java-io-how-to-use-randomaccess-file-java-io-package
