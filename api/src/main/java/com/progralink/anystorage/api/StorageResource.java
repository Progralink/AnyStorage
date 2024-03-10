package com.progralink.anystorage.api;

import com.progralink.anystorage.api.options.DeleteOption;
import com.progralink.anystorage.api.options.ReadOption;
import com.progralink.anystorage.api.options.WriteOption;
import com.progralink.jinout.streams.IOStreams;
import com.progralink.jinout.streams.input.LengthAwareInputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface StorageResource extends Iterable<StorageResource> {
    StorageSession getSession();

    StorageResource getParent() throws IOException;

    default boolean hasParent() throws IOException {
        return getParent() != null;
    }

    default StorageResource getLocalRoot() throws IOException {
        return getSession().getRootResource();
    }

    default String getLocalPath() throws IOException {
        StorageResource parent = getParent();
        if (parent != null) {
            return parent.getLocalPath() + "/" + getName();
        }
        return "";
    }

    default String getParentLocalPath() throws IOException {
        StorageResource parent = getParent();
        if (parent != null) {
            return parent.getLocalPath();
        }
        return null;
    }

    boolean isFile() throws IOException;
    default boolean isDirectory() throws IOException {
        return hasChildren();
    }
    boolean exists() throws IOException;

    default boolean hasChildren() throws IOException {
        return children().findAny().isPresent();
    }
    
    String getName() throws IOException;
    long getSize(ReadOption<?>... options) throws IOException;

    Instant getTimeCreated() throws IOException;
    Instant getTimeLastModified() throws IOException;
    Instant getTimeLastAccess() throws IOException;

    default byte[] readFully(ReadOption<?>... options) throws IOException {
        try (InputStream inputStream = openRead(options)) {
            return IOStreams.readFully(inputStream);
        }
    }

    InputStream openRead(ReadOption<?>... options) throws IOException;
    OutputStream openWrite(WriteOption<?>... options) throws IOException;

    long write(InputStream source, WriteOption<?>... options) throws IOException;

    default void write(byte[] data, WriteOption<?>... options) throws IOException {
        write(data, 0, data.length, options);
    }

    default void write(byte[] data, int offset, int length, WriteOption<?>... options) throws IOException {
        write(new LengthAwareInputStream(new ByteArrayInputStream(data, offset, length), length), options);
    }

    default void write(Path filePath, WriteOption<?>... options) throws IOException {
        try (InputStream inputStream = Files.newInputStream(filePath)) {
            long size = Files.size(filePath);
            write(new LengthAwareInputStream(inputStream, size), options);
        }
    }

    default void write(File file, WriteOption<?>... options) throws IOException {
        write(file.toPath(), options);
    }


    Stream<StorageResource> children() throws IOException;

    Collection<String> childrenNames() throws IOException;

    default Stream<StorageResource> childrenFiles() throws IOException {
        return children().filter(resource -> {
            try {
                return resource.isFile();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }


    default Stream<StorageResource> childrenDirectories() throws IOException {
        return children().filter(resource -> {
            try {
                return resource.isDirectory();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    StorageResource child(String name) throws IOException;

    default List<String> listChildrenNames() throws IOException {
        try {
            return children().map(r -> {
                try {
                    return r.getName();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).collect(Collectors.toList());
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    default List<String> listChildrenFileNames() throws IOException {
        try {
            return children().filter(r -> {
                try {
                    return r.isFile();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).map(r -> {
                try {
                    return r.getName();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).collect(Collectors.toList());
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    default List<String> listChildrenDirectoryNames() throws IOException {
        try {
            return children().filter(r -> {
                try {
                    return r.isDirectory();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).map(r -> {
                try {
                    return r.getName();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).collect(Collectors.toList());
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    boolean delete(DeleteOption<?>... options) throws IOException;

    default boolean deleteDeep(boolean includeSelf, DeleteOption<?>... options) throws IOException {
        AtomicBoolean childrenDeleted = new AtomicBoolean(false);
        try {
            if (hasChildren()) {
                children().forEach(child -> {
                    try {
                        child.deleteDeep(true, options);
                        childrenDeleted.set(true);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

        if (includeSelf) {
            return delete(options);
        }
        return childrenDeleted.get();
    }

    default boolean isEmpty() throws IOException {
        if (exists()) {
            if (isFile()) {
                return getSize() == 0;
            }
            if (isDirectory()) {
                return children().findAny().isPresent();
            }
            return false;
        }
        return true;
    }

    @Override
    default Iterator<StorageResource> iterator() {
        try {
            return children().iterator();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    default StorageResource resolve(String subpath) throws IOException {
        if (subpath == null || subpath.isEmpty()) {
            return this;
        }
        if (subpath.equals("..") || subpath.startsWith("../")) {
            return getParent().resolve(subpath.substring(Math.min(3, subpath.length())));
        }
        if (subpath.startsWith("/")) {
            return getLocalRoot().resolve(subpath.substring(1));
        }

        int i = subpath.indexOf('/');
        int j = subpath.indexOf('\\');
        int tokenEnd;
        if (i != -1 && j != -1) {
            tokenEnd = Math.min(i, j);
        } else if (i != -1) {
            tokenEnd = i;
        } else if (j != -1) {
            tokenEnd = j;
        } else {
            tokenEnd = subpath.length();
        }

        String name = subpath.substring(0, tokenEnd);
        StorageResource resource;
        if (!name.isEmpty()) {
            resource = child(name);
        } else {
            resource = this;
        }
        if (tokenEnd != subpath.length()) {
            return resource.resolve(subpath.substring(tokenEnd + 1));
        }
        return resource;
    }
}
