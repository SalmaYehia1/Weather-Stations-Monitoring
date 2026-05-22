package com.central.bitcask.index;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class KeyDir {
    // Keeps track of the latest memory pointer entries for each weather station
    private final ConcurrentHashMap<String, KeyDirEntry> map;

    public KeyDir() {
        this.map = new ConcurrentHashMap<>();
    }

    public void put(String key, KeyDirEntry entry) {
        this.map.put(key, entry);
    }

    public KeyDirEntry get(String key) {
        return this.map.get(key);
    }

    public void clear() {
        this.map.clear();
    }

    /**
     * Exposes a delegate key set view of active entries.
     * This fixes the loop error in the API Server.
     */
    public Set<String> keySet() {
        return this.map.keySet();
    }

    public void printDiagnosticSnapshot() {
        System.out.println("[DIAGNOSTIC] Total active indexing keys in RAM: " + map.size());
        map.forEach((k, v) -> System.out.printf("  -> Key: %s | File: %s.bin | Offset: %d%n", k, v.getFileId(), v.getValueOffset()));
    }
}