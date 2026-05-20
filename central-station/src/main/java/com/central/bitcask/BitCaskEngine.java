package com.central.bitcask;

import com.central.bitcask.model.Record;
import com.central.bitcask.serde.RecordEncoder;
import com.central.bitcask.index.KeyDir;
import com.central.bitcask.index.KeyDirEntry;
import com.central.bitcask.storage.FileManager;
import com.central.bitcask.storage.RecoveryManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class BitCaskEngine {
    // Everything writes continuously to this single segment file name
    private static final String DATA_FILE_ID = "active_data";

    private final KeyDir keyDir;
    private final FileManager fileManager;
    private final RandomAccessFile activeRaf;

    public BitCaskEngine(File dataDir) throws IOException {
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        this.keyDir = new KeyDir();
        this.fileManager = new FileManager(dataDir);
        
        // 1. Rebuild the RAM index on cold start scan
        RecoveryManager recoveryManager = new RecoveryManager(dataDir, fileManager);
        recoveryManager.rebuildIndex(this.keyDir);

        // 2. Open our permanent single active appending file channel
        this.activeRaf = fileManager.openActiveFile(DATA_FILE_ID);
    }

    public synchronized void put(String key, byte[] value) throws IOException {
        Record record = new Record(key, value);
        byte[] serializedBytes = RecordEncoder.encode(record);
        
        // Direct append to the unchanging file handle
        long baseWriteOffset = fileManager.appendToActive(activeRaf, serializedBytes);
        long valueOffset = baseWriteOffset + Record.HEADER_SIZE + key.getBytes().length;

        KeyDirEntry indexPointer = new KeyDirEntry(
            DATA_FILE_ID, 
            value.length, 
            valueOffset, 
            record.getTimestamp()
        );
        
        keyDir.put(key, indexPointer);
    }

    public byte[] get(String key) throws IOException {
        KeyDirEntry indexPointer = keyDir.get(key);
        if (indexPointer == null) {
            return null; // Cache miss
        }

        return fileManager.readFromSegment(
            indexPointer.getFileId(), 
            indexPointer.getValueOffset(), 
            indexPointer.getValueSize()
        );
    }
    
    public synchronized void close() throws IOException {
        if (activeRaf != null) {
            activeRaf.close();
        }
        fileManager.closeAll();
    }
    public void printIndexState() {
    this.keyDir.printDiagnosticSnapshot();
}
}