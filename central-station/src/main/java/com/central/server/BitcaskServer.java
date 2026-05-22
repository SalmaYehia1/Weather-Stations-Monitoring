package com.central.server;

import com.central.bitcask.BitCaskEngine;
import com.central.bitcask.constant.StorageConstants;
import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class BitcaskServer {

    public static void main(String[] args) {
        System.out.println("=== Launching Independent BitCask API Server ===");
        
        try {
            // Instantiate the exact same data directory to share the active Bitcask segment files
            File storageDir = new File(StorageConstants.DATA_DIR_NAME);
            BitCaskEngine engine = new BitCaskEngine(storageDir);

            // Print internal state on startup
            engine.printIndexState();

            // Bind the REST endpoints onto a high-performance HTTP engine
            HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);

            // Endpoint 1: Fetch single key value -> /get?key=station_id:1
            server.createContext("/get", exchange -> {
                String query = exchange.getRequestURI().getQuery();
                String key = (query != null && query.startsWith("key=")) ? query.substring(4) : null;

                if (key == null) {
                    String resp = "Missing key parameter";
                    exchange.sendResponseHeaders(400, resp.length());
                    try (OutputStream os = exchange.getResponseBody()) { os.write(resp.getBytes()); }
                    return;
                }

                byte[] valBytes = engine.get(key);
                if (valBytes == null) {
                    String resp = "KEY_NOT_FOUND";
                    exchange.sendResponseHeaders(404, resp.length());
                    try (OutputStream os = exchange.getResponseBody()) { os.write(resp.getBytes()); }
                } else {
                    exchange.sendResponseHeaders(200, valBytes.length);
                    try (OutputStream os = exchange.getResponseBody()) { os.write(valBytes); }
                }
            });

            // Endpoint 2: Fetch all entries -> /getAll
            server.createContext("/getAll", exchange -> {
                try {
                    StringBuilder sb = new StringBuilder();
                    // Compiles perfectly using the new custom KeyDir delegator loop structure
                    for (String key : engine.getKeyDir().keySet()) {
                        byte[] valBytes = engine.get(key);
                        if (valBytes != null) {
                            String valueStr = new String(valBytes, StorageConstants.DEFAULT_CHARSET)
                                                .replace("\n", "").replace("\r", "");
                            sb.append(key).append(",").append(valueStr).append("\n");
                        }
                    }
                    byte[] respBytes = sb.toString().getBytes();
                    exchange.sendResponseHeaders(200, respBytes.length);
                    try (OutputStream os = exchange.getResponseBody()) { os.write(respBytes); }
                } catch (Exception e) {
                    String resp = "Server Error";
                    exchange.sendResponseHeaders(500, resp.length());
                }
            });

            // Handle mass benchmarking clients simultaneously using scalable virtual threads
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.start();
            System.out.println("[API SERVER] Listening on port 8080. Awaiting client scripts...");

        } catch (Exception e) {
            System.err.println("[FATAL] API Server failed to launch: " + e.getMessage());
            e.printStackTrace();
        }
    }
}