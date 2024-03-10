package com.progralink.anystorage.api.multistorage;

import com.progralink.anystorage.api.StorageSession;

public class StorageSessionHandler {
    private StorageSession storageSession;
    private boolean allowRead;
    private boolean allowWrite;

    public StorageSessionHandler(StorageSession storageSession, boolean allowRead, boolean allowWrite) {
        this.storageSession = storageSession;
        this.allowRead = allowRead;
        this.allowWrite = allowWrite;
    }

    public StorageSession getStorageSession() {
        return storageSession;
    }

    public boolean isAllowRead() {
        return allowRead;
    }

    public boolean isAllowWrite() {
        return allowWrite;
    }
}
