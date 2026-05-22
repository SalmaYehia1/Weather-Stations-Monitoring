package com.central.bitcask.storage;

import com.central.bitcask.constant.StorageConstants;
import com.central.bitcask.model.Record;
import com.central.bitcask.model.HintEntry;
import com.central.bitcask.index.KeyDir;
import com.central.bitcask.index.KeyDirEntry;
import com.central.bitcask.serde.RecordEncoder;
import com.central.bitcask.serde.HintEncoder;
import com.central.bitcask.serde.RecordDecoder;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class BitcaskCompactor {

    private final File dataDir;
    private final FileManager fileManager;

    public BitcaskCompactor(File dataDir, FileManager fileManager) {
        this.dataDir = dataDir;
        this.fileManager = fileManager;
    }

    /**
     * MAIN COMPACTION FLOW:
     * 1. Take the current active file being written to
     * 2. Generate a hint file from it (index it)
     * 3. Create a new empty active file
     * 4. Return new active file ID for BitCaskEngine to use
     * 
     * This allows BitCask to continue writing to the new file while
     * the old file has a hint index created for fast recovery.
     */
    public synchronized String runCompaction(String currentActiveFileId, KeyDir activeKeyDir) throws IOException {
        if (!dataDir.exists() || !dataDir.isDirectory()) {
            System.err.println("[COMPACTION ERROR] Data folder not found.");
            return null;
        }

        System.out.println("==================================================");
        System.out.println("BITCASK COMPACTION TRACE");
        System.out.println("==================================================");
        System.out.println("[COMPACTION] Current active file: " + currentActiveFileId + ".bin");

        // STEP 1: Get the current active file
        File currentActiveFile = new File(dataDir, currentActiveFileId + StorageConstants.BIN_FILE_EXTENSION);
        
        if (!currentActiveFile.exists()) {
            System.err.println("[COMPACTION ERROR] Active file not found: " + currentActiveFile.getName());
            return null;
        }

        long fileSize = currentActiveFile.length();
        System.out.printf("[COMPACTION] Active file size: %d bytes%n", fileSize);

        // STEP 2: Read all records from the current active file
        System.out.println("[COMPACTION] Reading records from active file...");
        List<Record> allRecords = readRecordsFromActiveFile(currentActiveFile);
        System.out.printf("[COMPACTION] Found %d records%n", allRecords.size());

        // STEP 3: Create hint file for the current active file (for fast recovery later)
        String hintFileName = "hint_" + currentActiveFile.getName();
        File hintFile = new File(dataDir, hintFileName);
        
        File tempHintFile = new File(dataDir, StorageConstants.TEMP_PREFIX + hintFileName);
        System.out.println("[COMPACTION] Creating hint file: " + hintFileName);
        
        createHintFileFromRecords(allRecords, tempHintFile, currentActiveFile);
        
        // Atomically rename temp hint to final name
        if (!tempHintFile.renameTo(hintFile)) {
            System.err.println("[COMPACTION ERROR] Failed to finalize hint file.");
            return null;
        }
        System.out.println("[COMPACTION] Hint file created successfully.");

        // STEP 4: Create NEW active file for future writes
        int nextSegmentId = getNextSegmentNumber();
        String newActiveFileId = String.format("active_data_%03d", nextSegmentId);
        String newActiveFileName = newActiveFileId + StorageConstants.BIN_FILE_EXTENSION;
        File newActiveFile = new File(dataDir, newActiveFileName);

        System.out.println("[COMPACTION] Creating new active file: " + newActiveFileName);
        
        // Create the new empty active file
        if (!newActiveFile.createNewFile()) {
            System.err.println("[COMPACTION ERROR] Failed to create new active file.");
            return null;
        }

        System.out.println("[COMPACTION] New active file created: " + newActiveFileId);

        // STEP 5: Update KeyDir - all records still point to current file (not new file)
        // The new writes will automatically go to newActiveFileId
        System.out.println("[COMPACTION] Updating index pointers...");
        
        long trackingOffset = 0;
        for (Record r : allRecords) {
            byte[] encodedBytes = RecordEncoder.encode(r);
            byte[] keyBytes = r.getKey().getBytes(StorageConstants.DEFAULT_CHARSET);
            long valueOffset = trackingOffset + StorageConstants.RECORD_HEADER_SIZE + keyBytes.length;
            
            KeyDirEntry indexPointer = new KeyDirEntry(
                currentActiveFileId,  // Old records still point to current file
                r.getValue().length,
                valueOffset,
                r.getTimestamp()
            );
            activeKeyDir.put(r.getKey(), indexPointer);
            
            trackingOffset += encodedBytes.length;
        }

        System.out.println("[COMPACTION SUCCESS] Compaction complete.");
        System.out.printf("   Source file: %s.bin (%d bytes)%n", currentActiveFileId, fileSize);
        System.out.printf("   Hint file: %s%n", hintFileName);
        System.out.printf("   New active file: %s%n", newActiveFileId);
        System.out.println("==================================================\n");

        // RETURN the new active file ID for engine to use
        return newActiveFileId;
    }

    /**
     * Read all records from a data file
     */
    private List<Record> readRecordsFromActiveFile(File dataFile) throws IOException {
        List<Record> records = new ArrayList<>();
        
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(dataFile)))) {
            while (true) {
                try {
                    // Read header
                    byte[] headerBytes = new byte[StorageConstants.RECORD_HEADER_SIZE];
                    dis.readFully(headerBytes);
                    ByteBuffer headerBuf = ByteBuffer.wrap(headerBytes);

                    long timestamp = headerBuf.getLong();
                    int keySize = headerBuf.getInt();
                    int valueSize = headerBuf.getInt();

                    // Read key
                    byte[] keyBytes = new byte[keySize];
                    dis.readFully(keyBytes);
                    String key = new String(keyBytes, StorageConstants.DEFAULT_CHARSET);

                    // Read value
                    byte[] valueBytes = new byte[valueSize];
                    dis.readFully(valueBytes);

                    records.add(new Record(timestamp, key, valueBytes));
                } catch (EOFException e) {
                    break;
                }
            }
        }
        
        return records;
    }

    /**
     * Create hint file from records.
     * Hint file contains: [Timestamp][KeySize][ValueSize][ValueOffset][Key]
     * This allows quick recovery without scanning the data file.
     */
    private void createHintFileFromRecords(List<Record> records, File hintOutput, File dataFile) throws IOException {
        try (FileOutputStream hintFos = new FileOutputStream(hintOutput);
             RandomAccessFile dataRaf = new RandomAccessFile(dataFile, "r")) {

            long currentOffset = 0;
            
            for (Record r : records) {
                // Calculate where the value starts in the data file
                byte[] keyBytes = r.getKey().getBytes(StorageConstants.DEFAULT_CHARSET);
                long valueOffset = currentOffset + StorageConstants.RECORD_HEADER_SIZE + keyBytes.length;
                
                // Create and write hint entry
                HintEntry hint = new HintEntry(
                    r.getTimestamp(),
                    r.getValue().length,
                    valueOffset,
                    r.getKey()
                );
                
                byte[] rawHintBytes = HintEncoder.encode(hint);
                hintFos.write(rawHintBytes);
                
                // Track offset for next record
                byte[] encodedRecord = RecordEncoder.encode(r);
                currentOffset += encodedRecord.length;
            }
        }
    }

    /**
     * Get the next segment number based on existing active_data_XXX files
     */
    private int getNextSegmentNumber() {
        int max = 0;
        File[] files = dataDir.listFiles((dir, name) ->
            name.startsWith("active_data_") &&
            name.endsWith(StorageConstants.BIN_FILE_EXTENSION) &&
            !name.startsWith(StorageConstants.ARCHIVE_PREFIX) &&
            !name.startsWith(StorageConstants.TEMP_PREFIX)
        );
        
        if (files == null) return 1;
        
        for (File f : files) {
            String name = f.getName();
            int underscore = name.lastIndexOf('_');
            int dot = name.lastIndexOf('.');
            if (underscore == -1 || dot == -1) continue;
            try {
                int number = Integer.parseInt(name.substring(underscore + 1, dot));
                max = Math.max(max, number);
            } catch (NumberFormatException ignored) {}
        }
        return max + 1;
    }
}