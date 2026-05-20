package com.central.bitcask.index;

public class KeyDirEntry {
    private final String fileId;
    private final int valueSize;
    private final long valueOffset;
    private final long timestamp;

    public KeyDirEntry(String fileId, int valueSize, long valueOffset, long timestamp) {
        this.fileId = fileId;
        this.valueSize = valueSize;
        this.valueOffset = valueOffset;
        this.timestamp = timestamp;
    }

    public String getFileId() { return fileId; }
    public int getValueSize() { return valueSize; }
    public long getValueOffset() { return valueOffset; }
    public long getTimestamp() { return timestamp; }
}