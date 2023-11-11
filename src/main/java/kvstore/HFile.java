package kvstore;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/***
 * HFile is a file format used by HBase to store and retrieve both Data and Index.
 * The Max size of the HFile is 4MB.
 * HFile only allows sequential writes and random reads.
 */
public class HFile {

    public HashMap<String, Long> index;
    HFileDataRow hFileDataRow = new HFileDataRow();
    public static final String hFileDirectory = "/tmp/hfile";
//    public static final int MAX_HFILE_SIZE = 4 * 1024 * 1024; // 4MB

    public HFile() {
        try {
            index = readIndexHFileRowFromDisk();
        } catch (IOException e) {
            throw new RuntimeException("can't read from the HFile" + e);
        }
    }

    public String getKeyValue(String key) {
        if(!index.containsKey(key)) {
            return null;
        }
        long keyOffset = index.get(key);
        try {
            return readHFileRowFromDisk(keyOffset, false);
        } catch (IOException e) {
            throw new RuntimeException("can't read from the HFile" + e);
        }
    }

    /***
     * This is a basic rudimentary implementation and can be enhanced.
     * Buffering incoming data in the memory and writing to disk after reaching a threshold. The reads can check for data in the memory first before going to disk.
     * This implementation appends to the file for each write and determine if index needs to be flushed to the file.
     * @param key
     * @param value
     */
    void appendData(String key, String value, boolean tombstone) {

        byte[] rowBytes = hFileDataRow.createHFileRow(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), false);
        long keyOffset = 0;
        try {
            keyOffset = writeHFileRowToDisk(rowBytes, false);
            index.put(key, keyOffset);
            // Flush the index
            ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES);
            offsetBuffer.putLong(keyOffset);
            byte[] indexBytes = hFileDataRow.createIndexFileRow(key.getBytes(StandardCharsets.UTF_8), offsetBuffer.array());
            writeHFileRowToDisk(indexBytes, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(String key) {
        if (index.containsKey(key)) {
            appendData(key, "", true);
        }
    }

    private String getCurrentHFile(boolean indexFile) throws IOException {
        String filePath = hFileDirectory + "/hfile1" + (indexFile ? "_index" : "") + ".hfile";
        File dir = new File(hFileDirectory);
        File file = new File(filePath);
        if(!dir.isDirectory()) {
            dir.mkdirs();
        }
        if (!file.exists()) {
            file.createNewFile();
        }
        return filePath;
    }

    private long writeHFileRowToDisk(byte[] rowBytes, boolean indexFile) throws IOException {
        return hFileDataRow.writeHFileRowToDisk(rowBytes, getCurrentHFile(indexFile));
    }

    private String readHFileRowFromDisk(long offset, boolean indexFile) throws IOException {
        return hFileDataRow.readHFileRow(offset, getCurrentHFile(indexFile));
    }

    private HashMap<String, Long> readIndexHFileRowFromDisk() throws IOException {
        return hFileDataRow.loadIndexHFile(getCurrentHFile(true));
    }
}
