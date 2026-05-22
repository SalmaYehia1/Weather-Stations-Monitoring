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
import java.util.concurrent.atomic.AtomicReference;

public class BitCaskEngine {

    private final File dataDir;
    private final KeyDir keyDir;
    private final FileManager fileManager;
    private final AtomicReference<String> currentActiveFileId;
    private volatile RandomAccessFile activeRaf;
    private final ScheduledExecutorService compactionScheduler;

    public BitCaskEngine(File dataDir) throws IOException {
        this.dataDir = dataDir;
        
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        this.keyDir = new KeyDir();
        this.fileManager = new FileManager(dataDir);
        this.currentActiveFileId = new AtomicReference<>("active_data");

        // STEP 1: Rebuild RAM index from hint files on cold start
        System.out.println("[ENGINE] Recovering index from storage...");
        RecoveryManager recoveryManager = new RecoveryManager(dataDir, fileManager);
        recoveryManager.rebuildIndex(this.keyDir);

        // STEP 2: Open initial active file for writing
        this.activeRaf = fileManager.openActiveFile(currentActiveFileId.get());
        System.out.println("[ENGINE] Opened active file: " + currentActiveFileId.get() + ".bin");

        // STEP 3: Schedule automatic compaction
        BitcaskCompactor compactor = new BitcaskCompactor(dataDir, fileManager);
        this.compactionScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bitcask-compactor");
            t.setDaemon(true);
            return t;
        });

        this.compactionScheduler.scheduleAtFixedRate(() -> {
            try {
                System.out.println("[COMPACTION] Starting scheduled compaction...");
                
                // Pass the CURRENT active file ID to compactor
                String newActiveFileId = compactor.runCompaction(
                    currentActiveFileId.get(), 
                    keyDir
                );
                
                // CRITICAL: Switch to new active file after successful compaction
                if (newActiveFileId != null && !newActiveFileId.equals(currentActiveFileId.get())) {
                    rotateActiveFile(newActiveFileId);
                }
            } catch (IOException e) {
                System.err.println("[COMPACTION] Failed: " + e.getMessage());
                e.printStackTrace();
            }
        }, 30, 30, TimeUnit.SECONDS);  // Start after 30s, repeat every 30s

        System.out.println("[ENGINE] Compaction scheduler started — runs every 30 seconds.");
    }

    /**
     * Atomically rotate to a new active file.
     * This is called when compaction completes and a new active file is ready.
     */
    private synchronized void rotateActiveFile(String newActiveFileId) throws IOException {
        String oldFileId = currentActiveFileId.getAndSet(newActiveFileId);
        
        System.out.println("[ENGINE] Rotating active file:");
        System.out.println("  Old: " + oldFileId + ".bin");
        System.out.println("  New: " + newActiveFileId + ".bin");

        // Close old RAF
        if (activeRaf != null) {
            try {
                activeRaf.close();
                System.out.println("[ENGINE] Closed old active file: " + oldFileId + ".bin");
            } catch (IOException e) {
                System.err.println("[ENGINE] Error closing old file: " + e.getMessage());
            }
        }

        // Open new RAF
        try {
            this.activeRaf = fileManager.openActiveFile(newActiveFileId);
            System.out.println("[ENGINE] Opened new active file: " + newActiveFileId + ".bin");
        } catch (IOException e) {
            System.err.println("[ENGINE] CRITICAL - Failed to open new active file!");
            throw e;
        }
    }

    /**
     * Write a key-value pair to the current active file
     */
    public synchronized void put(String key, byte[] value) throws IOException {
        // Verify activeRaf is still valid
        if (activeRaf == null) {
            throw new IOException("Active file is closed - engine not initialized properly");
        }

        Record record = new Record(key, value);
        byte[] serializedBytes = RecordEncoder.encode(record);

        long baseWriteOffset = fileManager.appendToActive(activeRaf, serializedBytes);
        long valueOffset = baseWriteOffset + Record.HEADER_SIZE + key.getBytes().length;

        // Create index pointer to CURRENT active file
        KeyDirEntry indexPointer = new KeyDirEntry(
            currentActiveFileId.get(),
            value.length,
            valueOffset,
            record.getTimestamp()
        );

        keyDir.put(key, indexPointer);
    }

    /**
     * Read a value by key from the index.
     * Will find the record in whichever file it's stored in.
     */
    public byte[] get(String key) throws IOException {
        KeyDirEntry indexPointer = keyDir.get(key);
        if (indexPointer == null) {
            return null;
        }

        // Read from the file specified in the index pointer (could be old or new file)
        return fileManager.readFromSegment(
            indexPointer.getFileId(),
            indexPointer.getValueOffset(),
            indexPointer.getValueSize()
        );
    }

    /**
     * Shutdown the engine gracefully
     */
    public synchronized void close() throws IOException {
        System.out.println("[ENGINE] Shutting down...");
        
        // Stop compaction scheduler
        compactionScheduler.shutdown();
        try {
            if (!compactionScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                compactionScheduler.shutdownNow();
                System.out.println("[ENGINE] Compactor forcefully terminated");
            }
        } catch (InterruptedException e) {
            compactionScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Close active RAF
        if (activeRaf != null) {
            activeRaf.close();
            System.out.println("[ENGINE] Closed active file");
        }
        
        // Close all read file descriptors
        fileManager.closeAll();
        System.out.println("[ENGINE] Shutdown complete");
    }

    /**
     * Print diagnostic info about current index state
     */
    public void printIndexState() {
        System.out.println("\n[DIAGNOSTIC] Current active file: " + currentActiveFileId.get() + ".bin");
        this.keyDir.printDiagnosticSnapshot();
    }

    /**
     * Get the KeyDir for advanced operations
     */
    public KeyDir getKeyDir() {
        return this.keyDir;
    }

    /**
     * Get the current active file ID being written to
     */
    public String getCurrentActiveFileId() {
        return currentActiveFileId.get();
    }
}