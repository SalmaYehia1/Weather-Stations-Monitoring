package com.central.bitcask;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * HintFile — a compact side-car written alongside every *closed* data segment.
 *
 * Problem it solves
 * -----------------
 * On startup (or crash recovery) BitCask must rebuild the in-memory KeyDir.
 * Without hint files it would have to scan every data file byte-by-byte.
 * Hint files store just enough information to rebuild the KeyDir in a single
 * sequential pass — no value bytes are stored, so they are ~10× smaller.
 *
 * Hint record wire format (fixed 20-byte header + variable key)
 * ─────────────────────────────────────────────────────────────
 *   [4B  timestamp ] — Unix epoch seconds
 *   [4B  keySize   ] — length of key in bytes
 *   [4B  valueSize ] — length of value in bytes (needed to rebuild KeyDirEntry)
 *   [8B  offset    ] — byte offset of the full record in the *data* file
 *   [?B  key       ] — raw key bytes
 */
public class HintFile {

    public static final String EXTENSION   = ".hint";
    static final int           HEADER_SIZE = 20; // 4+4+4+8

    // ── Writing ──────────────────────────────────────────────────────────────

    /**
     * Writes a hint file for {@code fileId} using only the entries in
     * {@code fileEntries} (already filtered to this file by the caller).
     *
     * @param dataDir      directory that holds all segment files
     * @param fileId       logical file name without extension
     * @param fileEntries  key → KeyDirEntry map for entries that live in fileId
     */
    public static void write(Path dataDir,
                             String fileId,
                             Map<String, KeyDirEntry> fileEntries) throws IOException {

        Path hintPath = dataDir.resolve(fileId + EXTENSION);

        try (FileOutputStream fos = new FileOutputStream(hintPath.toFile())) {

            for (Map.Entry<String, KeyDirEntry> e : fileEntries.entrySet()) {
                byte[]       keyBytes = e.getKey().getBytes(StandardCharsets.UTF_8);
                KeyDirEntry  kde      = e.getValue();

                ByteBuffer hdr = ByteBuffer.allocate(HEADER_SIZE);
                hdr.putInt ((int) kde.getTimestamp());
                hdr.putInt (keyBytes.length);
                hdr.putInt (kde.getValueSize());
                hdr.putLong(kde.getOffset());

                fos.write(hdr.array());
                fos.write(keyBytes);
            }
            fos.flush();
        }

        System.out.printf("[HINT] Written %s (%d entries)%n",
                hintPath.getFileName(), fileEntries.size());
    }

    // ── Reading ──────────────────────────────────────────────────────────────

    /**
     * Reads a hint file and reconstructs KeyDirEntry objects for {@code fileId}.
     *
     * @param hintPath path to the .hint file
     * @param fileId   the data file these hints belong to
     * @return map of key → KeyDirEntry (ready to be merged into the global KeyDir)
     */
    public static Map<String, KeyDirEntry> read(Path hintPath,
                                                String fileId) throws IOException {
        Map<String, KeyDirEntry> entries = new HashMap<>();

        try (RandomAccessFile raf = new RandomAccessFile(hintPath.toFile(), "r")) {

            while (raf.getFilePointer() < raf.length()) {

                byte[] hdrBytes = new byte[HEADER_SIZE];
                raf.readFully(hdrBytes);
                ByteBuffer hdr = ByteBuffer.wrap(hdrBytes);

                int  timestamp = hdr.getInt();
                int  keySize   = hdr.getInt();
                int  valueSize = hdr.getInt();
                long offset    = hdr.getLong();

                byte[] keyBytes = new byte[keySize];
                raf.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                entries.put(key, new KeyDirEntry(fileId, offset, timestamp, valueSize, keySize));
            }
        }

        System.out.printf("[HINT] Read %s (%d entries)%n",
                hintPath.getFileName(), entries.size());
        return entries;
    }
}
