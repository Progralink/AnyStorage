package com.progralink.anystorage.api;

import com.progralink.anystorage.api.exceptions.NotFoundException;
import com.progralink.anystorage.api.options.Option;
import com.progralink.anystorage.api.options.Options;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;

public interface StorageSession extends Closeable {
    String getName();
    StorageResource getRootResource();

    default StorageResource getResource(String path) throws IOException {
        return getRootResource().resolve(path);
    }

    Options getOptions();

    boolean isDirectoryless();

    boolean isSupported(Option<?> option);

    default void clear() throws IOException {
        getRootResource().deleteDeep(false);
    }

    default void close() throws IOException {
    }
}
