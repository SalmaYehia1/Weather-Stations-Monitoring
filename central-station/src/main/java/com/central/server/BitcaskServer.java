package com.central.server;

import com.central.bitcask.BitCaskEngine;
import com.central.bitcask.constant.StorageConstants;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class BitcaskServer {
    private final HttpServer server;

    public BitcaskServer(int port, BitCaskEngine engine) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

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
                exchange.sendResponseHeaders(500, 0);
            }
        });

        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    }

    public void start() {
        server.start();
        System.out.println("[API SERVER] Embedded server listening on port 8080 smoothly.");
    }
    
    public void stop() {
        server.stop(0);
    }
}