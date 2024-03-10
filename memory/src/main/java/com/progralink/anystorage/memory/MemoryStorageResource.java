package com.progralink.anystorage.memory;

import com.progralink.anystorage.api.AbstractStorageResource;
import com.progralink.anystorage.api.StorageResource;
import com.progralink.anystorage.api.exceptions.AlreadyExistsException;
import com.progralink.anystorage.api.exceptions.NotEmptyDirectoryException;
import com.progralink.anystorage.api.exceptions.NotFoundException;
import com.progralink.anystorage.api.options.DeleteOption;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.ReadOption;
import com.progralink.anystorage.api.options.WriteOption;

import java.io.*;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

public class MemoryStorageResource extends AbstractStorageResource {
    private String name = "";

    protected MemoryStorageResource(MemoryStorageSession session) {
        super(session);
    }

    protected MemoryStorageResource(MemoryStorageResource parent, String name) {
        super(parent);
        this.name = name;
    }

    @Override
    public MemoryStorageSession getSession() {
        return (MemoryStorageSession) super.getSession();
    }

    protected byte[] getData() throws IOException {
        return getSession().getDataMap().get(getLocalPath());
    }

    @Override
    protected InputStream openInputStream(Options options) throws Exception {
        byte[] data = getData();
        if (data == null) {
            throw new NotFoundException();
        }
        return new ByteArrayInputStream(data);
    }

    @Override
    protected OutputStream openOutputStream(Options options) throws Exception {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                byte[] written = this.toByteArray();
                byte[] target = written;

                if (WriteOption.APPEND.isEnabled(options)) {
                    byte[] data = getData();
                    if (data != null) {
                        target = new byte[data.length + written.length];
                        System.arraycopy(data, 0, target, 0, data.length);
                        System.arraycopy(written, 0, target, data.length, written.length);
                    }
                }

                if (WriteOption.CREATE_NEW.isEnabled(options)) {
                    if (getSession().getDataMap().putIfAbsent(getLocalPath(), target) != null) {
                        throw new AlreadyExistsException();
                    }
                } else {
                    getSession().getDataMap().put(getLocalPath(), target);
                }
                super.close();
            }
        };
    }

    @Override
    public boolean isFile() throws IOException {
        return getData() != null;
    }

    @Override
    public boolean exists() throws IOException {
        return getData() != null;
    }

    @Override
    public String getName() throws IOException {
        return name;
    }

    @Override
    public long getSize(ReadOption<?>... options) throws IOException {
        byte[] data = getData();
        if (data != null) {
            return data.length;
        }
        throw new NotFoundException();
    }

    @Override
    public Stream<StorageResource> children() throws IOException {
        Set<String> childrenNames = new LinkedHashSet<>();
        String localPath = getLocalPath();
        for (String path : getSession().getDataMap().keySet()) {
            if (path.startsWith(localPath + "/")) {
                String restOfThePath = path.substring(localPath.length() + 1);
                if (restOfThePath.contains("/")) {
                    childrenNames.add(restOfThePath.substring(0, restOfThePath.indexOf('/')));
                } else {
                    childrenNames.add(restOfThePath);
                }
            }
        }

        return childrenNames.stream().map(name -> {
            try {
                return child(name);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public StorageResource child(String name) throws IOException {
        return new MemoryStorageResource(this, name);
    }

    @Override
    public boolean delete(DeleteOption<?>... options) throws IOException {
        if (children().findAny().isPresent()) {
            throw new NotEmptyDirectoryException();
        }
        return getSession().getDataMap().remove(getLocalPath()) != null;
    }
}
