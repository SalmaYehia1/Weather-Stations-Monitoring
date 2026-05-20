package com.central.bitcask.serde;

import com.central.bitcask.model.Record;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RecordEncoder {

    /**
     * Translates a concrete Record structure into a strict appendable binary layout.
     * Specification Profile: [Timestamp (8B)] [KeySize (4B)] [ValueSize (4B)] [Key] [Value]
     */
    public static byte[] encode(Record record) {
        byte[] keyBytes = record.getKey().getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = record.getValue();

        int dynamicBlockAllocation = Record.HEADER_SIZE + keyBytes.length + valueBytes.length;
        ByteBuffer dataWireBuffer = ByteBuffer.allocate(dynamicBlockAllocation);
        
        // Populate standard fixed structural parameters
        dataWireBuffer.putLong(record.getTimestamp());
        dataWireBuffer.putInt(keyBytes.length);
        dataWireBuffer.putInt(valueBytes.length);
        
        // Flush structural identification arrays
        dataWireBuffer.put(keyBytes);
        dataWireBuffer.put(valueBytes);

        return dataWireBuffer.array();
    }
}