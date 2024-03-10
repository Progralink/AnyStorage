package com.progralink.anystorage.aws.s3;

import com.progralink.anystorage.api.StorageConnector;
import com.progralink.anystorage.testsuite.AbstractStorageTestSuite;

import java.io.IOException;

public class S3StorageTest extends AbstractStorageTestSuite {

    @Override
    protected String getDefaultConnectionString() throws IOException {
        return null; //use property/env: com.progralink.anystorage.aws.s3.URL=s3://...
    }

    @Override
    protected StorageConnector provideConnector() {
        return new S3StorageConnector();
    }
}
