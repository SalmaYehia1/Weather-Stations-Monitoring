package com.central.bitcask.model;

import com.central.bitcask.constant.StorageConstants;

public class HintEntry {
    //8 + 4 + 4 + 8; 
    public static final int STATIC_HEADER_SIZE = StorageConstants.HINT_HEADER_SIZE;
    
    
    private final long timestamp;
    private final int valueSize;
    private final long valueOffset;
    private final String key;

    public HintEntry(long timestamp, int valueSize, long valueOffset, String key) {
        this.timestamp = timestamp;
        this.valueSize = valueSize;
        this.valueOffset = valueOffset;
        this.key = key;
    }

    public long getTimestamp() { return timestamp; }
    public int getValueSize() { return valueSize; }
    public long getValueOffset() { return valueOffset; }
    public String getKey() { return key; }
}