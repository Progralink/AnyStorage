package com.progralink.anystorage.memory;

import com.progralink.anystorage.api.AbstractStorageSession;
import com.progralink.anystorage.api.options.Option;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.WriteOption;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStorageSession extends AbstractStorageSession {
    private Map<String, byte[]> dataMap;

    public MemoryStorageSession() {
        this("mem@" + UUID.randomUUID(), Options.DEFAULTS);
    }

    public MemoryStorageSession(String name, Options options) {
        super(name, options);
        this.dataMap = new ConcurrentHashMap<>();
        this.rootResource = new MemoryStorageResource(this);
    }

    MemoryStorageSession(String name, Map<String, byte[]> dataMap, Options options) {
        super(name, options);
        this.dataMap = dataMap;
        this.rootResource = new MemoryStorageResource(this);
    }

    Map<String, byte[]> getDataMap() {
        return dataMap;
    }

    @Override
    public boolean isDirectoryless() {
        return true;
    }

    @Override
    public boolean isSupported(Option<?> option) {
        return option == WriteOption.ATOMIC || option == WriteOption.APPEND || option == WriteOption.CREATE_NEW;
    }
}
