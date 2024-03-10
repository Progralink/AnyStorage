package com.progralink.anystorage.api;

import com.progralink.anystorage.api.options.Options;

import java.io.IOException;
import java.util.*;

public class StorageConnectors implements Iterable<StorageConnector> {
    private Collection<StorageConnector> connectors;

    public StorageConnectors() {
        connectors = new LinkedList<>();
        ServiceLoader<StorageConnector> loader = ServiceLoader.load(StorageConnector.class);
        Iterator<StorageConnector> iterator = loader.iterator();
        while(iterator.hasNext()) {
            connectors.add(iterator.next());
        }
    }

    public StorageConnectors(Collection<StorageConnector> connectors) {
        this.connectors = connectors;
    }

    public Optional<StorageSession> provide(String name, String connectionString) throws IOException {
        return provide(name, connectionString, Options.DEFAULTS);
    }

    public Optional<StorageSession> provide(String name, String connectionString, Options options) throws IOException {
        for (StorageConnector connector : connectors) {
            if (connector.canHandle(connectionString)) {
                return Optional.ofNullable(connector.connect(name, connectionString, options));
            }
        }
        return Optional.empty();
    }

    @Override
    public Iterator<StorageConnector> iterator() {
        return connectors.iterator();
    }
}
