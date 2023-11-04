package org.designchallenge;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class UniqueIdGenerator {

    // Starting EPOCH 2023-09-01 00:00:00 to gain 41 years.
    // The max year until this id repeats itself is after 2092-08-31 23:59:59
    public static final long START_EPOCH = 1697860800000L;
    // Machine Id across the datacenters
    public static final int MACHINE_BITS = 10;
    // Total 4096 numbers generated in one millisecond.
    public static final int COUNTER_BITS = 12;

    private static int nodeMax = (int) Math.pow(2, MACHINE_BITS) - 1;
    private static int nodeId = new Random().nextInt(nodeMax - 1);

    // threadsafe counter for counting up to 4096 numbers
    private AtomicInteger counter = new AtomicInteger(0);
    private static int counterMax = (int) Math.pow(2, COUNTER_BITS) - 1;

    public static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    public static final int BASE62 = ALPHABET.length();

    /**
     * Generate Unique 64-bit Numeric Identified based on 41-bit Timestamp, 10-bit Machine Id and 12-bit Integer Counter
     * @return 64-bit long Id
     */
    public synchronized long generateUniqueId() {
        long snowflakeId = (System.currentTimeMillis() - START_EPOCH) << MACHINE_BITS + COUNTER_BITS;
        snowflakeId |= nodeId << COUNTER_BITS;
        snowflakeId |= counter.incrementAndGet() % counterMax;;
        return snowflakeId;
    }

    /**
     * Generate alphanumeric Base62 Id given a long unique Id.
     * Id character set  [a-z][A-Z][0-9].
     * The length of the Id depends on the size of the input identifier.
     * 16-digit Snowflake Id will generate 9 character long character
     * @param uniqueId long
     * @return Base62 Alphanumeric Id
     */
    public String generateShortURLId(long uniqueId) {
        StringBuilder stringBuilder = new StringBuilder(); // default capacity 16
        while (uniqueId > 0) {
            int remainder = (int) (uniqueId % BASE62);
            System.out.println(uniqueId + " " + remainder + " " + ALPHABET.charAt(remainder));
            stringBuilder.append(ALPHABET.charAt(remainder));
            uniqueId = uniqueId / BASE62;
        }
        return stringBuilder.reverse().toString();
    }
}
