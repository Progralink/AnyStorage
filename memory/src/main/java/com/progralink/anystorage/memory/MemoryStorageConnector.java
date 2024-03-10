package com.progralink.anystorage.memory;

import com.progralink.anystorage.api.AbstractStorageConnector;
import com.progralink.anystorage.api.StorageSession;
import com.progralink.anystorage.api.options.Options;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStorageConnector extends AbstractStorageConnector {
    protected Map<String, Map<String, byte[]>> allDataMaps = new ConcurrentHashMap<>();

    @Override
    public String getTypeLabel() {
        return "Memory (Heap)";
    }

    @Override
    public boolean canHandle(String connectionString) {
        return connectionString.equals("mem") || connectionString.startsWith("mem:");
    }

    @Override
    public StorageSession connect(String name, String connectionString, Options options) throws IOException {
        if (!canHandle(connectionString)) {
            throw new IllegalArgumentException("connectionString");
        }

        String namespace = "";
        if (connectionString.length() > 4) {
            namespace = connectionString.substring(4);
        }
        Map<String, byte[]> dataMap = allDataMaps.computeIfAbsent(namespace, n -> new ConcurrentHashMap<>());
        return new MemoryStorageSession(name, dataMap, options);
    }

}
