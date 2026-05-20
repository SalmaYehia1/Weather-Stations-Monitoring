package com.central.bitcask.serde;

import com.central.bitcask.model.HintEntry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class HintDecoder {

    /**
     * Unmarshals an index metadata chunk back into a functional pointer instance.
     */
    public static HintEntry decode(ByteBuffer bufferedIndexStream) {
        long timestamp = bufferedIndexStream.getLong();
        int keySize = bufferedIndexStream.getInt();
        int valueSize = bufferedIndexStream.getInt();
        long valueOffset = bufferedIndexStream.getLong();

        byte[] rawKeyBuffer = new byte[keySize];
        bufferedIndexStream.get(rawKeyBuffer);
        String key = new String(rawKeyBuffer, StandardCharsets.UTF_8);

        return new HintEntry(timestamp, valueSize, valueOffset, key);
    }
}