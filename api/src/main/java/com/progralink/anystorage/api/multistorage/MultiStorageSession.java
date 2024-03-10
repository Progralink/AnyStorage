package com.progralink.anystorage.api.multistorage;

import com.progralink.anystorage.api.StorageResource;
import com.progralink.anystorage.api.StorageSession;
import com.progralink.anystorage.api.options.Option;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.ReadOption;
import com.progralink.anystorage.api.options.WriteOption;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MultiStorageSession implements StorageSession, Iterable<StorageSessionHandler> {
    private String name;
    private Options options;
    private MultiStorageResource rootResource;
    private List<StorageSessionHandler> handlers;

    public MultiStorageSession(String name, Options options, StorageSessionHandler... handlers) {
        this(name, options, Arrays.asList(handlers));
    }

    public MultiStorageSession(String name, Options options, List<StorageSessionHandler> handlers) {
        this.name = name;
        this.options = options;
        this.handlers = handlers;
        this.rootResource = new MultiStorageResource(this);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public StorageResource getRootResource() {
        return rootResource;
    }

    @Override
    public Options getOptions() {
        return options;
    }

    @Override
    public boolean isDirectoryless() {
        for (StorageSessionHandler handler : handlers) {
            if (handler.getStorageSession().isDirectoryless()) {
                return true;
            }
        }
        return false;
    }

    public List<StorageSessionHandler> getHandlers() {
        return handlers;
    }

    @Override
    public boolean isSupported(Option<?> option) {
        return isSupportedByAny(option);
    }

    public boolean isSupportedByAny(Option<?> option) {
        for (StorageSessionHandler handler : handlers) {
            if (option instanceof ReadOption && !handler.isAllowRead()) {
                continue;
            }
            if (option instanceof WriteOption && !handler.isAllowWrite()) {
                continue;
            }
            if (handler.getStorageSession().isSupported(option)) {
                return true;
            }
        }

        return false;
    }

    public boolean isSupportedByAll(Option<?> option) {
        for (StorageSessionHandler handler : handlers) {
            if (!handler.getStorageSession().isSupported(option)) {
                return false;
            }
        }

        return !handlers.isEmpty();
    }

    @Override
    public Iterator<StorageSessionHandler> iterator() {
        return handlers.iterator();
    }
}
