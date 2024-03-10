package com.progralink.anystorage.api;

import com.progralink.anystorage.api.options.Options;

public abstract class AbstractStorageSession implements StorageSession {
    private String name;
    private Options options;
    protected StorageResource rootResource;

    protected AbstractStorageSession(String name, Options options) {
        this.name = name;
        this.options = options;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Options getOptions() {
        return options;
    }

    @Override
    public StorageResource getRootResource() {
        return rootResource;
    }

    @Override
    public String toString() {
        return getName();
    }
}
