package com.progralink.anystorage.aws.s3;

import com.progralink.anystorage.api.StorageConnector;
import com.progralink.anystorage.api.StorageSession;
import com.progralink.anystorage.testsuite.AbstractStorageTestSuite;
import software.amazon.awssdk.services.s3.model.StorageClass;

import java.io.IOException;

public class S3StorageTest extends AbstractStorageTestSuite {
    StorageClass storageClass = null;

    @Override
    protected String getDefaultConnectionString() throws IOException {
        return null; //use property/env: com.progralink.anystorage.aws.s3.URL=s3://...
    }

    @Override
    protected StorageConnector provideConnector() {
        return new S3StorageConnector();
    }

    @Override
    protected synchronized StorageSession startSession() throws IOException {
        S3StorageSession session = (S3StorageSession)super.startSession();
        session.setDefaultStorageClass(storageClass);
        return session;
    }
}
