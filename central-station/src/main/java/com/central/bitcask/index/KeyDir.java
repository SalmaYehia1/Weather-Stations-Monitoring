package com.central.bitcask.index;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Collection;

public class KeyDir {
    private final ConcurrentHashMap<String, KeyDirEntry> map = new ConcurrentHashMap<>();

    public void put(String key, KeyDirEntry entry) {
        map.put(key, entry);
    }

    public KeyDirEntry get(String key) {
        return map.get(key);
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public Collection<String> allKeys() {
        return map.keySet();
    }
    
    public void clear() {
        map.clear();
    }
    public void printDiagnosticSnapshot() {
    System.out.println("\n===== 🗂️ BITCASK KEYDIR IN-MEMORY INDEX SNAPSHOT =====");
    if (map.isEmpty()) {
        System.out.println("(The memory index is completely empty)");
    } else {
        map.forEach((key, entry) -> {
            System.out.printf("🔑 Key: %-15s -> [File: %s.bin | ValueSize: %d bytes | ValueOffset: %d | Timestamp: %d]%n",
                key, entry.getFileId(), entry.getValueSize(), entry.getValueOffset(), entry.getTimestamp());
        });
    }
    System.out.println("======================================================\n");
}
}