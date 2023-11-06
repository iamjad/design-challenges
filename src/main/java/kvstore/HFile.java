package kvstore;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/***
 * HFile is a file format used by HBase to store and retrieve both Data and Index.
 * The Max size of the HFile is 4MB.
 * HFile only allows sequential writes and random reads.
 */
public class HFile {

    public HashMap<String, Long> index = new HashMap<>();
    HFileDataRow hFileDataRow = new HFileDataRow();
    public static final String hFileDirectory = "/tmp/hfile";
    public static final int MAX_HFILE_SIZE = 4 * 1024 * 1024; // 4MB

    public String getKeyValue(String key) {
        long keyOffset = index.get(key);
        try {
            return readHFileRowFromDisk(keyOffset);
        } catch (IOException e) {
            throw new RuntimeException("can't read from the HFile" + e);
        }
    }

    void appendData(String key, String value) {
        byte[] rowBytes = hFileDataRow.createHFileRow(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
        long keyOffset = writeHFileRowToDisk(rowBytes);
        index.put(key, keyOffset);
    }

    HashMap<String, String> loadIndex() {
        return null;
    }

    void commitIndexToDisk(HashMap<String, String> updatedIndex) {

    }

    private String getCurrentHFile() throws IOException {
        String filePath = hFileDirectory + "/hfile1.hfile";
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

    private long writeHFileRowToDisk(byte[] rowBytes) {
        long offset = 0;

        RandomAccessFile ramdonAccessFile = null;
        try {
            ramdonAccessFile = new RandomAccessFile(getCurrentHFile(), "rw");
        } catch (IOException e) {
            throw new RuntimeException("can not create HFile" + e);
        }

        try {
            offset = ramdonAccessFile.length();
            System.out.println("start offset: " + offset);
            ramdonAccessFile.seek(ramdonAccessFile.length());
            ramdonAccessFile.write(rowBytes);
            System.out.println("last offset: " + ramdonAccessFile.length());

            return offset;
        } catch (IOException e) {
            throw new RuntimeException("can't write to the HFile" + e);
        } finally {
            try {
                ramdonAccessFile.close();
            } catch (IOException e) {
                throw new RuntimeException("can't close the HFile" + e);
            }
        }
    }

    private String readHFileRowFromDisk(long offset) throws IOException {
        return hFileDataRow.readHFileRow(offset, getCurrentHFile());
    }
}
