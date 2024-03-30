package com.progralink.anystorage.smb;

import com.progralink.anystorage.api.StorageConnector;
import com.progralink.anystorage.testsuite.AbstractStorageTestSuite;

import java.io.IOException;

public class SMBStorageTest extends AbstractStorageTestSuite {
    @Override
    protected String getDefaultConnectionString() throws IOException {
        return null; //use property/env: com.progralink.anystorage.smb.SMBStorageTest.URL=smb://tester:testIt!3@nas/test/dir
    }

    @Override
    protected StorageConnector provideConnector() {
        return new SMBStorageConnector();
    }
}
