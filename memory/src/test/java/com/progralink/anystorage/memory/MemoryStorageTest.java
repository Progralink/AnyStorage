package com.progralink.anystorage.memory;

import com.progralink.anystorage.api.StorageConnector;
import com.progralink.anystorage.testsuite.AbstractStorageTestSuite;

import java.io.IOException;

public class MemoryStorageTest extends AbstractStorageTestSuite {
    @Override
    protected String getDefaultConnectionString() throws IOException {
        return "mem:test";
    }

    @Override
    protected StorageConnector provideConnector() {
        return new MemoryStorageConnector();
    }
}
