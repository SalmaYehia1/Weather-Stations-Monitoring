package com.central.bitcask.constant;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class StorageConstants {

    // Suppress default constructor to prevent instantiation
    private StorageConstants() {}
    public static final String DATA_DIR_NAME = "data";
    public static final String ACTIVE_FILE_NAME = "active_data";
    public static final String BIN_FILE_EXTENSION = ".bin";
    public static final String ARCHIVE_PREFIX = "$";   //OLD SEGMENET 
    public static final String TEMP_PREFIX = "#";       //TEMP FILES DURING COMPACTION


    public static final int RECORD_HEADER_SIZE = 8 + 4 + 4; // 16 Bytes
    public static final int HINT_HEADER_SIZE = 8 + 4 + 4 + 8; // 28 Bytes


    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
}