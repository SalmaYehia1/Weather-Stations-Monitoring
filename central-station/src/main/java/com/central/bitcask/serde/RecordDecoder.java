package com.central.bitcask.serde;

import com.central.bitcask.model.Record;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RecordDecoder {

    /**
     * Decodes a raw storage segment byte block back into a structured memory entity.
     */
    public static Record decode(ByteBuffer bufferedStream) {
        // Unpack fixed system dimensions
        long timestamp = bufferedStream.getLong();
        int keySize = bufferedStream.getInt();
        int valueSize = bufferedStream.getInt();

        // Reconstruct string coordinates safely
        byte[] rawKeyBuffer = new byte[keySize];
        bufferedStream.get(rawKeyBuffer);
        String decodedKey = new String(rawKeyBuffer, StandardCharsets.UTF_8);

        // Map payload metrics allocations
        byte[] rawValueBuffer = new byte[valueSize];
        bufferedStream.get(rawValueBuffer);

        return new Record(timestamp, decodedKey, rawValueBuffer);
    }
}