package com.progralink.anystorage.filesystem;

import com.progralink.anystorage.api.StorageConnector;
import com.progralink.anystorage.api.StorageSession;
import com.progralink.anystorage.testsuite.AbstractStorageTestSuite;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileSystemStorageTest extends AbstractStorageTestSuite {
    private Path tempPath;

    @Override
    protected String getDefaultConnectionString() throws IOException {
        if (tempPath == null) {
            tempPath = Files.createTempDirectory("fs-storage-test");
        }
        return tempPath.toUri().toString();
    }

    @Override
    protected StorageConnector provideConnector() {
        return new FileSystemStorageConnector();
    }

    @Override
    protected void cleanup(StorageSession session) throws IOException {
        super.cleanup(session);
        try {
            FileSystemUtils.deleteDirectory(
                Paths.get(new URI(connectionString))
            );
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }
}
