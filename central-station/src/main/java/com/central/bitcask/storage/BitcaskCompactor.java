package com.central.bitcask.storage;

import com.central.bitcask.constant.StorageConstants;
import com.central.bitcask.model.Record;
import com.central.bitcask.model.HintEntry;
import com.central.bitcask.index.KeyDir;
import com.central.bitcask.index.KeyDirEntry;
import com.central.bitcask.serde.RecordEncoder;
import com.central.bitcask.serde.HintEncoder;

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

    public synchronized void runCompaction(KeyDir activeKeyDir) throws IOException {
        if (!dataDir.exists() || !dataDir.isDirectory()) {
            System.err.println("[COMPACTION ERROR] Data folder not found.");
            return;
        }

        File[] binFiles = dataDir.listFiles((dir, name) -> 
            name.endsWith(StorageConstants.BIN_FILE_EXTENSION) && 
            !name.startsWith(StorageConstants.TEMP_PREFIX) && 
            !name.startsWith(StorageConstants.ARCHIVE_PREFIX) &&
            !name.startsWith("hint_")
        );
        
        if (binFiles == null || binFiles.length == 0) {
            System.out.println("[COMPACTION] No log segments found to compact.");
            return;
        }

        Arrays.sort(binFiles, Comparator.comparing(File::getName));

        System.out.println("==================================================");
        System.out.println("BITCASK LIVE COMPACTION TRACE RUNNER");
        System.out.println("==================================================");

        Map<String, Record> latestRecords = new LinkedHashMap<>();
        long totalInputSize = 0;

        for (File file : binFiles) {
            totalInputSize += file.length();
            List<Record> parsedRecords = parseFileLogs(file);

            for (Record r : parsedRecords) {
                Record existing = latestRecords.get(r.getKey());
                if (existing == null || r.getTimestamp() >= existing.getTimestamp()) {
                    latestRecords.put(r.getKey(), r);
                }
            }
        }

        int nextSegmentId = getNextSegmentNumber(binFiles);
        String finalDataName = String.format("active_data_%03d" + StorageConstants.BIN_FILE_EXTENSION, nextSegmentId);
        String finalHintName = String.format("hint_active_data_%03d" + StorageConstants.BIN_FILE_EXTENSION, nextSegmentId);

        File tempDataOutput = new File(dataDir, StorageConstants.TEMP_PREFIX + finalDataName);
        File tempHintOutput = new File(dataDir, StorageConstants.TEMP_PREFIX + finalHintName);
        
        File finalDataOutput = new File(dataDir, finalDataName);
        File finalHintOutput = new File(dataDir, finalHintName);

        List<Record> survivors = new ArrayList<>(latestRecords.values());
        
        // Simultaneously write compacted records and hint files
        writeCompactedAssets(survivors, tempDataOutput, tempHintOutput);

        // Atomically transition assets from working status to production status
        boolean dataRenamed = tempDataOutput.renameTo(finalDataOutput);
        boolean hintRenamed = tempHintOutput.renameTo(finalHintOutput);
        
        if (!dataRenamed || !hintRenamed) {
            System.err.println("[COMPACTION ERROR] Failed to finalize compacted file pairs.");
            return;
        }

        // Clean up obsolete data logs and their matching old hint files
        for (File oldFile : binFiles) {
            File archivedDataFile = new File(dataDir, StorageConstants.ARCHIVE_PREFIX + oldFile.getName());
            oldFile.renameTo(archivedDataFile);
            
            String oldHintName = "hint_" + oldFile.getName();
            File oldHintFile = new File(dataDir, oldHintName);
            if (oldHintFile.exists()) {
                File archivedHintFile = new File(dataDir, StorageConstants.ARCHIVE_PREFIX + oldHintName);
                oldHintFile.renameTo(archivedHintFile);
            }
        }

        // Atomically update Memory Index Map pointers
        activeKeyDir.clear();
        String activeFileId = finalDataName.replace(StorageConstants.BIN_FILE_EXTENSION, "");
        long trackingOffset = 0;

        for (Record r : survivors) {
            byte[] encodedBytes = RecordEncoder.encode(r);
            long valueOffset = trackingOffset + StorageConstants.RECORD_HEADER_SIZE + r.getKey().getBytes(StorageConstants.DEFAULT_CHARSET).length;
            
            KeyDirEntry freshPointer = new KeyDirEntry(activeFileId, r.getValue().length, valueOffset, r.getTimestamp());
            activeKeyDir.put(r.getKey(), freshPointer);

            trackingOffset += encodedBytes.length;
        }

        System.out.println("[COMPACTION SUCCESS] Merged log compression complete.");
        System.out.printf("   Optimized Space Boundaries: %d Bytes -> %d Bytes%n", totalInputSize, finalDataOutput.length());
        System.out.println("==================================================\n");
    }

    private List<Record> parseFileLogs(File file) throws IOException {
        List<Record> records = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            while (true) {
                try {
                    byte[] headerBytes = new byte[StorageConstants.RECORD_HEADER_SIZE];
                    dis.readFully(headerBytes);
                    ByteBuffer headerBuf = ByteBuffer.wrap(headerBytes);

                    long timestamp = headerBuf.getLong();
                    int keySize = headerBuf.getInt();
                    int valueSize = headerBuf.getInt();

                    byte[] keyBytes = new byte[keySize];
                    dis.readFully(keyBytes);
                    String key = new String(keyBytes, StorageConstants.DEFAULT_CHARSET);

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

    private void writeCompactedAssets(List<Record> records, File dataOutput, File hintOutput) throws IOException {
        try (FileOutputStream dataFos = new FileOutputStream(dataOutput);
             FileOutputStream hintFos = new FileOutputStream(hintOutput)) {
             
            long currentOffset = 0;
            for (Record r : records) {
                byte[] rawRecordBytes = RecordEncoder.encode(r);
                dataFos.write(rawRecordBytes);
                
                byte[] keyBytes = r.getKey().getBytes(StorageConstants.DEFAULT_CHARSET);
                long valueOffset = currentOffset + StorageConstants.RECORD_HEADER_SIZE + keyBytes.length;
                
                    HintEntry hint = new HintEntry(r.getTimestamp(), r.getValue().length, valueOffset, r.getKey());                
                    byte[] rawHintBytes = HintEncoder.encode(hint);
                hintFos.write(rawHintBytes);
                
                currentOffset += rawRecordBytes.length;
            }
        }
    }

    private int getNextSegmentNumber(File[] files) {
        int max = 0;
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