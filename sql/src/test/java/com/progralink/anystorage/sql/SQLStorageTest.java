package com.progralink.anystorage.sql;

import com.progralink.anystorage.api.StorageConnector;
import com.progralink.anystorage.testsuite.AbstractStorageTestSuite;

import java.io.IOException;

public class SQLStorageTest extends AbstractStorageTestSuite {
    @Override
    protected String getDefaultConnectionString() throws IOException {
        return "jdbc:h2:mem:storage-test-db"; //test another DB with env/property com.progralink.anystorage.sql.SqlStorageTest.URL
    }

    @Override
    protected StorageConnector provideConnector() {
        return new SQLStorageConnector();
    }

}
