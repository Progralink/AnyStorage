package com.progralink.anystorage.smb;

import com.progralink.anystorage.api.StorageConnector;
import com.progralink.anystorage.testsuite.AbstractStorageTestSuite;

import java.io.IOException;

public class SMBStorageTest extends AbstractStorageTestSuite {
    @Override
    protected String getDefaultConnectionString() throws IOException {
        return null; //use property/env: com.progralink.anystorage.smb.SmbStorageTest.URL=smb://tester:testIt!3@bignas/test/dir
    }

    @Override
    protected StorageConnector provideConnector() {
        return new SMBStorageConnector();
    }
}
