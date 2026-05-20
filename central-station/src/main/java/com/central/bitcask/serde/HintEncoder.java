package com.central.bitcask.serde;

import com.central.bitcask.model.HintEntry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class HintEncoder {

    /**
     * Marshals an index snapshot item into an optimized indexing log format.
     * Specification Profile: [Timestamp (8B)] [KeySize (4B)] [ValueSize (4B)] [ValueOffset (8B)] [Key]
     */
    public static byte[] encode(HintEntry trackingToken) {
        byte[] keyBytes = trackingToken.getKey().getBytes(StandardCharsets.UTF_8);
        
        int exactBlockAllocation = HintEntry.STATIC_HEADER_SIZE + keyBytes.length;
        ByteBuffer indexWireBuffer = ByteBuffer.allocate(exactBlockAllocation);
        
        // Serialization of spatial indexing parameters
        indexWireBuffer.putLong(trackingToken.getTimestamp());
        indexWireBuffer.putInt(keyBytes.length);
        indexWireBuffer.putInt(trackingToken.getValueSize());
        indexWireBuffer.putLong(trackingToken.getValueOffset());
        
        // Append identifying device key tag
        indexWireBuffer.put(keyBytes);

        return indexWireBuffer.array();
    }
}