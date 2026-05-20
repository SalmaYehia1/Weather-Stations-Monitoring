package com.central.bitcask.storage;

import com.central.bitcask.constant.StorageConstants;
import com.central.bitcask.model.Record;
import com.central.bitcask.model.HintEntry;
import com.central.bitcask.serde.RecordDecoder;
import com.central.bitcask.serde.HintDecoder;
import com.central.bitcask.index.KeyDir;
import com.central.bitcask.index.KeyDirEntry;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class RecoveryManager {
    private final File dataDir;
    private final FileManager fileManager;

    public RecoveryManager(File dataDir, FileManager fileManager) {
        this.dataDir = dataDir;
        this.fileManager = fileManager;
    }

    public void rebuildIndex(KeyDir keyDir) throws IOException {
        File[] files = dataDir.listFiles((dir, name) -> 
            name.endsWith(StorageConstants.BIN_FILE_EXTENSION) && 
            !name.startsWith(StorageConstants.TEMP_PREFIX) && 
            !name.startsWith(StorageConstants.ARCHIVE_PREFIX) &&
            !name.startsWith("hint_")
        );
        
        if (files == null) return;

        for (File file : files) {
            String fileId = file.getName().replace(StorageConstants.BIN_FILE_EXTENSION, "");
            String matchingHintName = "hint_" + file.getName();
            File hintFile = new File(dataDir, matchingHintName);

            if (hintFile.exists()) {
                // FAST PATH: Rebuild using the dedicated hint index file
                System.out.println("[RECOVERY] Optimized Index Boot using: " + matchingHintName);
                buildIndexFromHint(hintFile, fileId, keyDir);
            } else {
                // SLOW FALLBACK PATH: Run sequential log tracking scan
                System.out.println("[RECOVERY] Fast index missing. Scanning data log file: " + file.getName());
                buildIndexFromDataLog(fileId, keyDir);
            }
        }
    }

    private void buildIndexFromHint(File hintFile, String fileId, KeyDir keyDir) throws IOException {
        try (RandomAccessFile hintRaf = new RandomAccessFile(hintFile, "r")) {
            long fileLength = hintRaf.length();
            long currentOffset = 0;

            while (currentOffset < fileLength) {
                hintRaf.seek(currentOffset);

                byte[] headerBytes = new byte[StorageConstants.HINT_HEADER_SIZE];
                if (hintRaf.read(headerBytes) < StorageConstants.HINT_HEADER_SIZE) break;

                ByteBuffer headerBuf = ByteBuffer.wrap(headerBytes);
                long timestamp = headerBuf.getLong();
                int keySize = headerBuf.getInt();
                int valueSize = headerBuf.getInt();
                long valueOffset = headerBuf.getLong();

                byte[] keyBytes = new byte[keySize];
                hintRaf.readFully(keyBytes);

                ByteBuffer decodeBuf = ByteBuffer.allocate(StorageConstants.HINT_HEADER_SIZE + keySize);
                decodeBuf.put(headerBytes);
                decodeBuf.put(keyBytes);
                decodeBuf.flip();

                HintEntry hint = HintDecoder.decode(decodeBuf);
                KeyDirEntry indexPointer = new KeyDirEntry(fileId, hint.getValueSize(), hint.getValueOffset(), hint.getTimestamp());
                keyDir.put(hint.getKey(), indexPointer);

                currentOffset += StorageConstants.HINT_HEADER_SIZE + keySize;
            }
        }
    }

    private void buildIndexFromDataLog(String fileId, KeyDir keyDir) throws IOException {
        try (RandomAccessFile scanRaf = fileManager.openActiveFile(fileId)) {
            long fileLength = scanRaf.length();
            long currentOffset = 0;

            while (currentOffset < fileLength) {
                scanRaf.seek(currentOffset);

                byte[] headerBytes = new byte[StorageConstants.RECORD_HEADER_SIZE];
                if (scanRaf.read(headerBytes) < StorageConstants.RECORD_HEADER_SIZE) break;

                ByteBuffer headerBuf = ByteBuffer.wrap(headerBytes);
                long timestamp = headerBuf.getLong();
                int keySize = headerBuf.getInt();
                int valueSize = headerBuf.getInt();

                byte[] keyBytes = new byte[keySize];
                scanRaf.readFully(keyBytes);
                
                long valueOffset = currentOffset + StorageConstants.RECORD_HEADER_SIZE + keySize;
                
                ByteBuffer decodeBuf = ByteBuffer.allocate(StorageConstants.RECORD_HEADER_SIZE + keySize + valueSize);
                decodeBuf.put(headerBytes);
                decodeBuf.put(keyBytes);
                
                byte[] dummyValue = new byte[valueSize];
                scanRaf.readFully(dummyValue);
                decodeBuf.put(dummyValue);
                decodeBuf.flip();

                Record record = RecordDecoder.decode(decodeBuf);
                KeyDirEntry indexPointer = new KeyDirEntry(fileId, valueSize, valueOffset, record.getTimestamp());
                keyDir.put(record.getKey(), indexPointer);

                currentOffset += StorageConstants.RECORD_HEADER_SIZE + keySize + valueSize;
            }
        }
    }
}