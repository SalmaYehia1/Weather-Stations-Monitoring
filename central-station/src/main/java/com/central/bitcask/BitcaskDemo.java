package com.central.bitcask;

import com.central.bitcask.constant.StorageConstants;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * Standalone BitCask smoke test — no Kafka. Seeds keys, reads them back, then exits.
 * Run from central-station/ so data/ is created in the right place.
 */
public class BitcaskDemo {

    public static void main(String[] args) throws Exception {
        File dataDir = new File(StorageConstants.DATA_DIR_NAME);
        BitCaskEngine engine = new BitCaskEngine(dataDir);

        String[] keys = {"station_id:1", "station_id:2", "station_id:3"};
        for (String key : keys) {
            String json = String.format(
                    "{\"station_id\":%s,\"s_no\":1,\"weather\":{\"humidity\":75}}",
                    key.substring("station_id:".length())
            );
            engine.put(key, json.getBytes(StandardCharsets.UTF_8));
            System.out.println("[PUT] " + key);
        }

        engine.printIndexState();

        for (String key : keys) {
            byte[] val = engine.get(key);
            System.out.println("[GET] " + key + " -> " + (val == null ? "null" : new String(val, StandardCharsets.UTF_8)));
        }

        engine.close();
        System.out.println("[DONE] BitCask demo finished. data/ is ready for BitcaskServer or re-run.");
    }
}
