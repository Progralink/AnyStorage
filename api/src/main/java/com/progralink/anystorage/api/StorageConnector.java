package com.progralink.anystorage.api;

import com.progralink.anystorage.api.options.Options;

import java.io.IOException;

public interface StorageConnector {
    String getTypeLabel();
    boolean canHandle(String connectionString);

    StorageSession connect(String name, String connectionString, Options options) throws IOException;

    default StorageSession connect(String name, String connectionString) throws IOException {
        return connect(name, connectionString, Options.DEFAULTS);
    }

    default StorageSession connect(String connectionString) throws IOException {
        return connect(connectionString, connectionString, Options.DEFAULTS);
    }
}
