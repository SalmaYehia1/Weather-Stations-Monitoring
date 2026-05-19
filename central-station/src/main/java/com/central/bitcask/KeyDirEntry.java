package com.central.bitcask;

/**
 * Represents one entry in the in-memory KeyDir index.
 *
 * The KeyDir is a HashMap<String, KeyDirEntry> that maps every known key
 * to the exact location of its *latest* value on disk:
 *
 *   key  →  { fileId, offset, timestamp, valueSize, keySize }
 *
 * A GET therefore never scans a file; it does a single seeked read.
 */
public class KeyDirEntry {

    /** Logical name of the data file (without extension), e.g. "1716000000000" */
    private String fileId;

    /** Byte offset of the *start of the record header* inside that file */
    private long offset;

    /** Unix timestamp (seconds) stored in the record header */
    private long timestamp;

    /** Length of the raw value payload in bytes */
    private int valueSize;

    /** Length of the raw key payload in bytes */
    private int keySize;

    public KeyDirEntry(String fileId, long offset, long timestamp,
                       int valueSize, int keySize) {
        this.fileId    = fileId;
        this.offset    = offset;
        this.timestamp = timestamp;
        this.valueSize = valueSize;
        this.keySize   = keySize;
    }

    // ── Getters ──────────────────────────────────────────────────────────────

    public String getFileId()    { return fileId;    }
    public long   getOffset()    { return offset;    }
    public long   getTimestamp() { return timestamp; }
    public int    getValueSize() { return valueSize; }
    public int    getKeySize()   { return keySize;   }

    // ── Setters (used by Compactor to redirect pointers) ────────────────────

    public void setFileId(String fileId)    { this.fileId    = fileId;    }
    public void setOffset(long offset)      { this.offset    = offset;    }

    @Override
    public String toString() {
        return String.format(
            "KeyDirEntry{fileId=%s, offset=%d, timestamp=%d, valueSize=%d, keySize=%d}",
            fileId, offset, timestamp, valueSize, keySize);
    }
}
