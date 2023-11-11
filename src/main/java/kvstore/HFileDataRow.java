package kvstore;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class HFileDataRow {

    private static final long serialVersionUID = 1L;

    // epoch + KeySize + ValueSize + key + value
    public byte[] createHFileRow(byte[] key, byte[] value, boolean tombstone) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + 4 + key.length + 1 + value.length + 1);
        buffer.putLong(System.currentTimeMillis());
        buffer.putInt(key.length);
        buffer.putInt(value.length);
        buffer.put(key);
        buffer.put(tombstone ? (byte) 1 : (byte) 0);
        buffer.put(value);
        buffer.put("\n".getBytes());
        return buffer.array();
    }

    public byte[] createIndexFileRow(byte[] key, byte[] offset) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + key.length + offset.length + 1);
        buffer.putInt(key.length);
        buffer.putInt(offset.length);
        buffer.put(key);
        buffer.put(offset);
        buffer.put("\n".getBytes());
        return buffer.array();
    }

    public HashMap<String, Long> loadIndexHFile(String HFilePath) {

        HashMap<String, Long> index = new HashMap<>();
        RandomAccessFile randonAccessFile = null;
        try {
            randonAccessFile = new RandomAccessFile(HFilePath, "r");
        } catch (IOException e) {
            throw new RuntimeException("can not create HFile" + e);
        }

        try {
            randonAccessFile.seek(0);
            while (randonAccessFile.getFilePointer() < randonAccessFile.length()) {
                int keyLength = randonAccessFile.readInt();
                int offsetLength = randonAccessFile.readInt();
                byte[] keyBytes = new byte[keyLength];
                byte[] offsetByte = new byte[offsetLength];

                randonAccessFile.read(keyBytes, 0, keyLength);
                randonAccessFile.read(offsetByte, 0, offsetLength);
                long offset = ByteBuffer.wrap(offsetByte).getLong();
                index.put(new String(keyBytes), offset);
                randonAccessFile.readByte(); //reading \n
//                System.out.print("keyLength: " + keyLength + " offsetLength: " + offsetLength + " key: " + new String(keyBytes) + " offset: " + offset + "\n");
            }
            return index;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            Byte tomestone = ramdonAccessFile.readByte(); //reading tombstone byte
            ramdonAccessFile.read(valueBytes, 0, valueLength);
            //System.out.print("epoch: " + epoch + " keyLength: " + keyLength + " valueLength: " + valueLength + " key: " + new String(keyBytes) + " value: " + new String(valueBytes) + "\n");
            if (tomestone == 1) {
                return null; // row is marked for deleted.
            }
            return new String(valueBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long writeHFileRowToDisk(byte[] rowBytes, String HFilePath) {
        long offset = 0;

        RandomAccessFile ramdonAccessFile = null;
        try {
            ramdonAccessFile = new RandomAccessFile(HFilePath, "rw");
        } catch (IOException e) {
            throw new RuntimeException("can not create HFile" + e);
        }

        try {
            offset = ramdonAccessFile.length();
            ramdonAccessFile.seek(ramdonAccessFile.length());
            ramdonAccessFile.write(rowBytes);
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
}
