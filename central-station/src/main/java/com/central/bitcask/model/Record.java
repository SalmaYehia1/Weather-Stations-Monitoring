package com.central.bitcask.model;

import com.central.bitcask.constant.StorageConstants;

public class Record {
    // Layout fields: Timestamp(8) + KeySize(4) + ValueSize(4)
    public static final int HEADER_SIZE = StorageConstants.RECORD_HEADER_SIZE; 
    
    private final long timestamp;
    private final String key;
    private final byte[] value;

    public Record(String key, byte[] value) {
        this.timestamp = System.currentTimeMillis();
        this.key = key;
        this.value = value;
    }

    public Record(long timestamp, String key, byte[] value) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public long getTimestamp() { return timestamp; }
    public String getKey() { return key; }
    public byte[] getValue() { return value; }
}