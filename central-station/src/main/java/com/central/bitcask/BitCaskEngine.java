package com.central.bitcask;

import com.central.bitcask.model.Record;
import com.central.bitcask.serde.RecordEncoder;
import com.central.bitcask.index.KeyDir;
import com.central.bitcask.index.KeyDirEntry;
import com.central.bitcask.storage.BitcaskCompactor;
import com.central.bitcask.storage.FileManager;
import com.central.bitcask.storage.RecoveryManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BitCaskEngine {
    private static final String DATA_FILE_ID = "active_data";

    private final KeyDir keyDir;
    private final FileManager fileManager;
    private final RandomAccessFile activeRaf;
    private final ScheduledExecutorService compactionScheduler;  

    public BitCaskEngine(File dataDir) throws IOException {
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        this.keyDir = new KeyDir();
        this.fileManager = new FileManager(dataDir);

        // 1. Rebuild RAM index on cold start
        RecoveryManager recoveryManager = new RecoveryManager(dataDir, fileManager);
        recoveryManager.rebuildIndex(this.keyDir);

        // 2. Open active file channel
        this.activeRaf = fileManager.openActiveFile(DATA_FILE_ID);

        // 3. Schedule compaction every 30 seconds  ← NEW
        BitcaskCompactor compactor = new BitcaskCompactor(dataDir, fileManager);
        this.compactionScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bitcask-compactor");  // thread name
            t.setDaemon(true);
            return t;
        });

        this.compactionScheduler.scheduleAtFixedRate(() -> {
            try {
                System.out.println("[COMPACTION] Starting scheduled compaction...");
                compactor.runCompaction(keyDir);
            } catch (IOException e) {
                System.err.println("[COMPACTION] Failed: " + e.getMessage());
            }
        }, 30, 30, TimeUnit.SECONDS);  // first run after 30s, then every 30s

        System.out.println("[COMPACTION] Scheduler started — runs every 30 seconds.");
    }

    public synchronized void put(String key, byte[] value) throws IOException {
        Record record = new Record(key, value);
        byte[] serializedBytes = RecordEncoder.encode(record);

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
            return null;
        }

        return fileManager.readFromSegment(
            indexPointer.getFileId(),
            indexPointer.getValueOffset(),
            indexPointer.getValueSize()
        );
    }

    public synchronized void close() throws IOException {
        compactionScheduler.shutdown();  // ← stop compaction on close
        if (activeRaf != null) {
            activeRaf.close();
        }
        fileManager.closeAll();
    }

    public void printIndexState() {
        this.keyDir.printDiagnosticSnapshot();
    }
}