package com.progralink.anystorage.filesystem;

import com.progralink.anystorage.api.StorageConnector;
import com.progralink.anystorage.api.StorageSession;
import com.progralink.anystorage.api.options.Options;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileSystemStorageConnector implements StorageConnector {
    @Override
    public String getTypeLabel() {
        return "File System";
    }

    @Override
    public boolean canHandle(String connectionString) {
        return connectionString.startsWith("file://") || connectionString.startsWith("/") || (connectionString.length() > 2 && connectionString.charAt(1) == ':') || connectionString.startsWith("\\\\?\\");
    }

    @Override
    public StorageSession connect(String name, String connectionString, Options options) {
        Path path;
        if (connectionString.startsWith("file://")) {
            try {
                path = Paths.get(new URI(connectionString));
            } catch (URISyntaxException e) {
                throw new IllegalStateException(e);
            }
        } else {
            path = Paths.get(connectionString);
        }
        return new FileSystemStorageSession(name, path, options);
    }
}
