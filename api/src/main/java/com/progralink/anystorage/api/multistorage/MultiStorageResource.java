package com.progralink.anystorage.api.multistorage;

import com.progralink.anystorage.api.options.DeleteOption;
import com.progralink.jinout.streams.IOStreams;
import com.progralink.jinout.streams.input.LengthAwareInputStream;
import com.progralink.anystorage.api.StorageResource;
import com.progralink.anystorage.api.StorageSession;
import com.progralink.anystorage.api.exceptions.NotFoundException;
import com.progralink.anystorage.api.options.ReadOption;
import com.progralink.anystorage.api.options.WriteOption;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultiStorageResource implements StorageResource {
    private MultiStorageResource parent;
    private MultiStorageSession session;
    private String name;
    private String path;

    public MultiStorageResource(MultiStorageResource parent, String name) throws IOException {
        this.parent = parent;
        this.session = (MultiStorageSession) parent.getSession();
        this.name = name;
        this.path = parent.getLocalPath() + "/" + name;
    }

    public MultiStorageResource(MultiStorageSession session) {
        this.name = "";
        this.session = session;
    }

    private Iterable<StorageResource> getInnerResources(boolean write) throws IOException {
        List<StorageResource> innerResources = new LinkedList<>();
        for (StorageSessionHandler handler : session) {
            if ((write && handler.isAllowWrite()) || (!write && handler.isAllowRead())) {
                innerResources.add(handler.getStorageSession().getResource(path));
            }
        }
        return innerResources;
    }

    @Override
    public StorageSession getSession() {
        return session;
    }

    @Override
    public StorageResource getParent() throws IOException {
        return parent;
    }

    @Override
    public boolean isFile() throws IOException {
        for (StorageResource resource : getInnerResources(false)) {
            if (resource.isFile()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isDirectory() throws IOException {
        for (StorageResource resource : getInnerResources(false)) {
            if (resource.isDirectory()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean exists() throws IOException {
        for (StorageResource resource : getInnerResources(false)) {
            if (resource.exists()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getName() throws IOException {
        return name;
    }

    @Override
    public long getSize(ReadOption<?>... options) throws IOException {
        for (StorageResource resource : getInnerResources(false)) {
            if (resource.exists()) {
                return resource.getSize();
            }
        }
        return 0;
    }

    @Override
    public Instant getTimeCreated() throws IOException {
        for (StorageResource resource : getInnerResources(false)) {
            Instant t = resource.getTimeCreated();
            if (t != null) {
                return t;
            }
        }
        return null;
    }

    @Override
    public Instant getTimeLastModified() throws IOException {
        for (StorageResource resource : getInnerResources(false)) {
            Instant t = resource.getTimeLastModified();
            if (t != null) {
                return t;
            }
        }
        return null;
    }

    @Override
    public Instant getTimeLastAccess() throws IOException {
        for (StorageResource resource : getInnerResources(false)) {
            Instant t = resource.getTimeLastAccess();
            if (t != null) {
                return t;
            }
        }
        return null;
    }

    @Override
    public InputStream openRead(ReadOption<?>... options) throws IOException {
        for (StorageResource resource : getInnerResources(false)) {
            if (resource.exists()) {
                return resource.openRead(options);
            }
        }
        throw new NotFoundException();
    }

    @Override
    public OutputStream openWrite(WriteOption<?>... options) throws IOException {
        List<OutputStream> outputStreams = new LinkedList<>();
        for (StorageResource resource : getInnerResources(true)) {
            outputStreams.add(resource.openWrite(options));
        }

        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                for (OutputStream outputStream : outputStreams) {
                    outputStream.write(b);
                }
            }

            @Override
            public void write(byte[] b) throws IOException {
                for (OutputStream outputStream : outputStreams) {
                    outputStream.write(b);
                }
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                for (OutputStream outputStream : outputStreams) {
                    outputStream.write(b, off, len);
                }
            }

            @Override
            public void flush() throws IOException {
                for (OutputStream outputStream : outputStreams) {
                    outputStream.flush();
                }
            }

            @Override
            public void close() throws IOException {
                for (OutputStream outputStream : outputStreams) {
                    outputStream.close();
                }
            }
        };
    }

    @Override
    public void write(byte[] data, int offset, int length, WriteOption<?>... options) throws IOException {
        for (StorageResource resource : getInnerResources(true)) {
            resource.write(data, offset, length, options);
        }
    }

    @Override
    public long write(InputStream source, WriteOption<?>... options) throws IOException {
        //TODO: multithreaded write
        Path tempFile = Files.createTempFile("multistorage-temp", "tmp");
        long length;
        try {
            try (OutputStream outputStream = Files.newOutputStream(tempFile)) {
                length = IOStreams.transfer(source, outputStream);
            }
            for (StorageResource resource : getInnerResources(true)) {
                try (InputStream inputStream = Files.newInputStream(tempFile)) {
                    InputStream source2 = new LengthAwareInputStream(inputStream, length);
                    resource.write(source2, options);
                }
            }
        } finally {
            Files.deleteIfExists(tempFile);
        }
        return length;
    }

    @Override
    public Stream<StorageResource> children() throws IOException {
        return childrenNames().stream().map(name -> {
            try {
                return new MultiStorageResource(this, name);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public List<String> childrenNames() throws IOException {
        Set<String> names = new LinkedHashSet<>();
        for (StorageResource resource : getInnerResources(false)) {
            for (StorageResource child : resource.children().collect(Collectors.toList())) {
                names.add(child.getName());
            }
        }
        return new ArrayList<>(names);
    }

    @Override
    public StorageResource child(String name) throws IOException {
        return new MultiStorageResource(this, name);
    }

    @Override
    public boolean delete(DeleteOption<?>... options) throws IOException {
        boolean found = false;
        for (StorageResource resource : getInnerResources(true)) {
            if (resource.delete()) {
                found = true;
            }
        }
        return found;
    }
}
