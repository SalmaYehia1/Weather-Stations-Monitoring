package com.weather;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

/**
 * Generalized Bitcask Compactor
 * <p>
 * FEATURES:
 * ---------------------------------------------------------
 * 1. Automatically scans a folder for ALL .bin files
 * 2. Sorts files so older segments are processed first
 * 3. Keeps ONLY the newest version of every key
 * 4. Uses long timestamps (8 bytes)
 * 5. Uses streaming (DataInputStream) instead of loading
 * entire files into RAM
 * 6. Produces a compacted output segment
 * 7. Verifies output by re-reading compacted file
 * <p>
 * RECORD FORMAT:
 * ---------------------------------------------------------
 * [8B timestamp]
 * [4B keySize]
 * [4B valueSize]
 * [key bytes]
 * [value bytes]
 * <p>
 * TOTAL HEADER = 12 bytes
 * <p>
 * NOTES:
 * ---------------------------------------------------------
 * - No tombstones/deletion handling
 * - No checksum/CRC
 * - Assumes files are not corrupted
 * - Suitable for educational Bitcask implementation
 */
public class BitcaskCompactor {

    // Folder containing all .bin segment files
    private static final String DATA_FOLDER = "data";

    public static void main(String[] args) throws IOException {

        File folder = new File(DATA_FOLDER);

        if (!folder.exists() || !folder.isDirectory()) {
            System.out.println("Data folder not found: " + folder.getAbsolutePath());
            return;
        }

        // Get all .bin files
        File[] binFiles = folder.listFiles((dir, name) -> name.endsWith(".bin") && !name.startsWith("#") && !name.startsWith("$"));
        ;

        if (binFiles == null || binFiles.length == 0) {
            System.out.println("No .bin files found.");
            return;
        }

        // Sort files alphabetically
        // Example:
        // segment_001.bin
        // segment_002.bin
        // segment_003.bin
        Arrays.sort(binFiles, Comparator.comparing(File::getName));

        System.out.println("==================================================");
        System.out.println("BITCASK COMPACTION");
        System.out.println("==================================================");

        System.out.println("\nInput files:");
        long totalInputSize = 0;

        for (File f : binFiles) {
            System.out.printf("  %-30s %10d bytes%n", f.getName(), f.length());

            totalInputSize += f.length();
        }

        // =====================================================
        // COMPACTION
        // =====================================================

        // Keeps latest version of each key
        Map<String, Record> latestRecords = new LinkedHashMap<>();

        System.out.println("\n==================================================");
        System.out.println("PARSING SEGMENTS");
        System.out.println("==================================================");

        for (File file : binFiles) {

            System.out.println("\nReading: " + file.getName());

            List<Record> records = parseFile(file);

            for (Record r : records) {

                System.out.printf("  key=%-20s value=%-20s ts=%d%n", r.key, r.value, r.timestamp);

                Record existing = latestRecords.get(r.key);

                // Keep newest timestamp
                if (existing == null || r.timestamp >= existing.timestamp) {
                    latestRecords.put(r.key, r);
                }
            }
        }

        // =====================================================
        // SURVIVORS
        // =====================================================

        List<Record> survivors = new ArrayList<>(latestRecords.values());

        System.out.println("\n==================================================");
        System.out.println("FINAL SURVIVING RECORDS");
        System.out.println("==================================================");

        for (Record r : survivors) {
            System.out.printf("  key=%-20s value=%-20s ts=%d%n", r.key, r.value, r.timestamp);
        }

        // =====================================================
        // WRITE COMPACTED FILE
        // =====================================================

        int nextSegment = getNextSegmentNumber(binFiles);

        String finalName = String.format("bitcask_test_data_data_file_%03d.bin", nextSegment);

        String tempName = "#" + finalName;

        File tempOutput = new File(folder, tempName);

        File finalOutput = new File(folder, finalName);

// Write temporary compacted file
        writeCompactedFile(survivors, tempOutput);

// Rename after successful completion
        boolean renamed = tempOutput.renameTo(finalOutput);

        for (File oldFile : binFiles) {

            File compactedOldFile = new File(
                    folder,
                    "$" + oldFile.getName()
            );

            boolean oldRenamed =
                    oldFile.renameTo(compactedOldFile);

            if (!oldRenamed) {
                System.out.println(
                        "Warning: Failed to mark old file as compacted: "
                                + oldFile.getName()
                );
            }
        }

        if (!renamed) {
            System.out.println("Failed to finalize compacted file.");
            return;
        }

        // =====================================================
        // RESULTS
        // =====================================================

        System.out.println("\n==================================================");
        System.out.println("COMPACTION RESULT");
        System.out.println("==================================================");

        System.out.println("Input size  : " + totalInputSize + " bytes");
        System.out.println("Output size : " + finalOutput.length() + " bytes");

        double saved = 100.0 * (1.0 - ((double) finalOutput.length() / totalInputSize));

        System.out.printf("Space saved : %.2f%%%n", saved);

        System.out.println("Keys kept   : " + survivors.size());

        System.out.println("\nCompacted file written to:");
        System.out.println(finalOutput.getAbsolutePath());

        // =====================================================
        // VERIFICATION
        // =====================================================

        System.out.println("\n==================================================");
        System.out.println("VERIFYING OUTPUT");
        System.out.println("==================================================");

        List<Record> verified = parseFile(finalOutput);

        for (Record r : verified) {
            System.out.printf("  key=%-20s value=%-20s ts=%d%n", r.key, r.value, r.timestamp);
        }

        System.out.println("\nVerification complete.");
        System.out.println("Successfully read back " + verified.size() + " records.");
    }

    static int getNextSegmentNumber(File[] files) {

        int max = 0;

        for (File f : files) {

            String name = f.getName();

            // Example:
            // bitcask_test_data_data_file_001.bin

            int underscore = name.lastIndexOf('_');
            int dot = name.lastIndexOf('.');

            if (underscore == -1 || dot == -1) continue;

            try {

                int number = Integer.parseInt(name.substring(underscore + 1, dot));

                max = Math.max(max, number);

            } catch (NumberFormatException ignored) {
            }
        }

        return max + 1;
    }

    /**
     * Parses a single .bin segment file.
     * <p>
     * FORMAT:
     * [8B timestamp]
     * [4B keySize]
     * [4B valueSize]
     * [key]
     * [value]
     */
    static List<Record> parseFile(File file) throws IOException {

        List<Record> records = new ArrayList<>();

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {

            while (true) {

                try {

                    // =================================================
                    // READ HEADER
                    // =================================================

                    long timestamp = Integer.toUnsignedLong(dis.readInt());

                    int keySize = dis.readInt();

                    int valueSize = dis.readInt();

                    // Basic validation
                    if (keySize <= 0 || valueSize < 0) {
                        System.out.println("Invalid record sizes encountered.");
                        break;
                    }

                    // =================================================
                    // READ KEY
                    // =================================================

                    byte[] keyBytes = new byte[keySize];
                    dis.readFully(keyBytes);

                    // =================================================
                    // READ VALUE
                    // =================================================

                    byte[] valueBytes = new byte[valueSize];
                    dis.readFully(valueBytes);

                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    String value = new String(valueBytes, StandardCharsets.UTF_8);

                    records.add(new Record(timestamp, key, value, file.getName()));

                } catch (EOFException eof) {
                    // End of file reached normally
                    break;
                }
            }
        }

        return records;
    }

    /**
     * Writes compacted records into a new .bin file.
     */
    static void writeCompactedFile(List<Record> records, File output) throws IOException {

        try (FileOutputStream fos = new FileOutputStream(output)) {

            for (Record r : records) {

                byte[] keyBytes = r.key.getBytes(StandardCharsets.UTF_8);

                byte[] valueBytes = r.value.getBytes(StandardCharsets.UTF_8);

                ByteBuffer buffer = ByteBuffer.allocate(12 + keyBytes.length + valueBytes.length);

                // =================================================
                // HEADER
                // =================================================

                buffer.putInt((int) r.timestamp);

                buffer.putInt(keyBytes.length);

                buffer.putInt(valueBytes.length);

                // =================================================
                // DATA
                // =================================================

                buffer.put(keyBytes);

                buffer.put(valueBytes);

                fos.write(buffer.array());
            }
        }
    }

    /**
     * Represents a parsed Bitcask record.
     */
    static class Record {

        final long timestamp;

        final String key;

        final String value;

        final String sourceFile;

        Record(long timestamp, String key, String value, String sourceFile) {
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
            this.sourceFile = sourceFile;
        }
    }
}