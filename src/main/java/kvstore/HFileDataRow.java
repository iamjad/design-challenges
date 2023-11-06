package kvstore;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class HFileDataRow {

    private static final long serialVersionUID = 1L;

    // epoch + KeySize + ValueSize + key + value
    public byte[] createHFileRow(byte[] key, byte[] value) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + 4 + key.length + value.length + 1);
        buffer.putLong(System.currentTimeMillis());
        buffer.putInt(key.length);
        buffer.putInt(value.length);
        buffer.put(key);
        buffer.put(value);
        buffer.put("\n".getBytes());
        return buffer.array();
    }

    public String readHFileRow(long offset, String HFilePath) {
        RandomAccessFile ramdonAccessFile = null;
        try {
            ramdonAccessFile = new RandomAccessFile(HFilePath, "r");
        } catch (IOException e) {
            throw new RuntimeException("can not create HFile" + e);
        }
        try {
            ramdonAccessFile.seek(offset);
            long epoch = ramdonAccessFile.readLong();
            int keyLength = ramdonAccessFile.readInt();
            int valueLength = ramdonAccessFile.readInt();
            byte[] keyBytes = new byte[keyLength];
            byte[] valueBytes = new byte[valueLength];

            ramdonAccessFile.read(keyBytes, 0, keyLength);
            ramdonAccessFile.read(valueBytes, 0, valueLength);
            System.out.print("epoch: " + epoch + " keyLength: " + keyLength + " valueLength: " + valueLength + " key: " + new String(keyBytes) + " value: " + new String(valueBytes) + "\n");
            return new String(valueBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
