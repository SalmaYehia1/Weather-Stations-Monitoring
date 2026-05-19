package com.central.bitcask;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.*;

public class BitCaskEngine implements Closeable {

    static final String DATA_EXTENSION = ".bin";
    static final int    HEADER_SIZE    = 12;

    private final Path                                   dataDir;
    private final ConcurrentHashMap<String, KeyDirEntry> keyDir;
    private final ReentrantReadWriteLock                 rwLock;

    private RandomAccessFile activeFile;
    private String           activeFileId;
    private long             activeFileOffset;

    // ── Constructor ──────────────────────────────────────────────────────────

    public BitCaskEngine(String dataDirPath) throws IOException {
        this.dataDir = Paths.get(dataDirPath);
        this.keyDir  = new ConcurrentHashMap<>();
        this.rwLock  = new ReentrantReadWriteLock();

        Files.createDirectories(dataDir);
        rebuildKeyDir();
        openNewActiveFile();
    }

    // ── Public API ───────────────────────────────────────────────────────────

    public void put(String key, String value) throws IOException {
        byte[] keyBytes   = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        int    timestamp  = (int) (System.currentTimeMillis() / 1000);

        rwLock.writeLock().lock();
        try {
            long recordOffset = activeFileOffset;

            ByteBuffer hdr = ByteBuffer.allocate(HEADER_SIZE);
            hdr.putInt(timestamp);
            hdr.putInt(keyBytes.length);
            hdr.putInt(valueBytes.length);
            activeFile.write(hdr.array());
            activeFile.write(keyBytes);
            activeFile.write(valueBytes);

            activeFileOffset += HEADER_SIZE + keyBytes.length + valueBytes.length;

            keyDir.put(key, new KeyDirEntry(
                    activeFileId, recordOffset, timestamp,
                    valueBytes.length, keyBytes.length));
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public String get(String key) throws IOException {
        rwLock.readLock().lock();
        try {
            KeyDirEntry entry = keyDir.get(key);
            if (entry == null) return null;

            Path filePath = dataDir.resolve(entry.getFileId() + DATA_EXTENSION);
            try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
                long valueStart = entry.getOffset() + HEADER_SIZE + entry.getKeySize();
                raf.seek(valueStart);
                byte[] valueBytes = new byte[entry.getValueSize()];
                raf.readFully(valueBytes);
                return new String(valueBytes, StandardCharsets.UTF_8);
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public Set<String> listKeys() {
        return Collections.unmodifiableSet(keyDir.keySet());
    }

    public Map<String, String> getAll() throws IOException {
        Set<String> keys;
        rwLock.readLock().lock();
        try {
            keys = new HashSet<>(keyDir.keySet());
        } finally {
            rwLock.readLock().unlock();
        }

        Map<String, String> result = new LinkedHashMap<>();
        for (String key : keys) {
            String value = get(key);
            if (value != null) result.put(key, value);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        rwLock.writeLock().lock();
        try {
            if (activeFile != null) {
                flushHintFileForActive();
                activeFile.close();
                activeFile = null;
                System.out.println("[BITCASK] Closed cleanly.");
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // ── Active-file management ────────────────────────────────────────────────

    private void openNewActiveFile() throws IOException {
        activeFileId     = String.valueOf(System.currentTimeMillis());
        Path filePath    = dataDir.resolve(activeFileId + DATA_EXTENSION);
        activeFile       = new RandomAccessFile(filePath.toFile(), "rw");
        activeFileOffset = 0;
        System.out.println("[BITCASK] Active file opened: " + filePath.getFileName());
    }

    private void flushHintFileForActive() throws IOException {
        Map<String, KeyDirEntry> activeEntries = new HashMap<>();
        for (Map.Entry<String, KeyDirEntry> e : keyDir.entrySet()) {
            if (activeFileId.equals(e.getValue().getFileId()))
                activeEntries.put(e.getKey(), e.getValue());
        }
        if (!activeEntries.isEmpty())
            HintFile.write(dataDir, activeFileId, activeEntries);
    }

    // ── Startup: KeyDir rebuild ───────────────────────────────────────────────

    private void rebuildKeyDir() throws IOException {
        List<Path> dataFiles = new ArrayList<>();
        try (DirectoryStream<Path> ds =
                     Files.newDirectoryStream(dataDir, "*" + DATA_EXTENSION)) {
            for (Path p : ds) dataFiles.add(p);
        }
        dataFiles.sort(Comparator.comparing(p -> p.getFileName().toString()));

        for (Path dataFile : dataFiles) {
            String id       = fileId(dataFile);
            Path   hintPath = dataDir.resolve(id + HintFile.EXTENSION);

            if (Files.exists(hintPath)) {
                keyDir.putAll(HintFile.read(hintPath, id));
            } else {
                rebuildFromDataFile(dataFile, id);
            }
        }
        System.out.printf("[BITCASK] KeyDir ready: %d key(s).%n", keyDir.size());
    }

    private void rebuildFromDataFile(Path dataFile, String id) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "r")) {
            long offset = 0;
            long length = raf.length();

            while (offset + HEADER_SIZE <= length) {
                byte[]     hdrBytes = new byte[HEADER_SIZE];
                raf.readFully(hdrBytes);
                ByteBuffer hdr      = ByteBuffer.wrap(hdrBytes);

                int timestamp = hdr.getInt();
                int keySize   = hdr.getInt();
                int valueSize = hdr.getInt();

                if (offset + HEADER_SIZE + keySize + valueSize > length) break;

                byte[] keyBytes = new byte[keySize];
                raf.readFully(keyBytes);
                raf.skipBytes(valueSize);

                String key = new String(keyBytes, StandardCharsets.UTF_8);
                keyDir.put(key, new KeyDirEntry(id, offset, timestamp, valueSize, keySize));

                offset += HEADER_SIZE + keySize + valueSize;
            }
        }
        System.out.println("[BITCASK] Scanned (no hint): " + dataFile.getFileName());
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private static String fileId(Path path) {
        return path.getFileName().toString().replace(DATA_EXTENSION, "");
    }
}