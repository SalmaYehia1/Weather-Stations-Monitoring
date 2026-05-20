package com.central.bitcask.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentHashMap;

public class FileManager {
    private final File dataDir;
    private final ConcurrentHashMap<String, RandomAccessFile> readDescriptorsCache;

    public FileManager(File dataDir) {
        this.dataDir = dataDir;
        this.readDescriptorsCache = new ConcurrentHashMap<>();
    }

    public RandomAccessFile openActiveFile(String fileId) throws IOException {
        File file = new File(dataDir, fileId + ".bin");
        return new RandomAccessFile(file, "rw");
    }

    public synchronized long appendToActive(RandomAccessFile activeRaf, byte[] serializedRecord) throws IOException {
        long writeOffset = activeRaf.length();
        activeRaf.seek(writeOffset);
        activeRaf.write(serializedRecord);
        activeRaf.getChannel().force(false); // Flush completely to system disk hardware
        return writeOffset;
    }

    public byte[] readFromSegment(String fileId, long offset, int size) throws IOException {
        RandomAccessFile readerRaf = readDescriptorsCache.computeIfAbsent(fileId, id -> {
            try {
                File file = new File(dataDir, id + ".bin");
                return new RandomAccessFile(file, "r");
            } catch (IOException e) {
                throw new RuntimeException("Failed to open read-only descriptor: " + id, e);
            }
        });

        synchronized (readerRaf) {
            readerRaf.seek(offset);
            byte[] valueBytes = new byte[size];
            readerRaf.readFully(valueBytes);
            return valueBytes;
        }
    }

    public void closeAll() {
        for (String fileId : readDescriptorsCache.keySet()) {
            RandomAccessFile raf = readDescriptorsCache.remove(fileId);
            if (raf != null) {
                try { raf.close(); } catch (IOException ignored) {}
            }
        }
    }
}