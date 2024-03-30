package com.progralink.anystorage.api;

import com.progralink.anystorage.api.exceptions.AlreadyExistsException;
import com.progralink.anystorage.api.exceptions.NotEmptyDirectoryException;
import com.progralink.anystorage.api.exceptions.NotFoundException;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.ReadOption;
import com.progralink.anystorage.api.options.WriteOption;
import com.progralink.jinout.streams.IOStreams;

import java.io.*;
import java.net.URLEncoder;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractStorageResource implements StorageResource {
    protected StorageSession session;
    protected StorageResource parent;

    protected AbstractStorageResource(StorageSession session) {
        this.session = session;
    }

    protected AbstractStorageResource(StorageResource parent) {
        this.session = parent.getSession();
        this.parent = parent;
    }

    @Override
    public StorageSession getSession() {
        return session;
    }

    @Override
    public StorageResource getParent() {
        return parent;
    }

    @Override
    public Instant getTimeCreated() throws IOException {
        return null;
    }

    @Override
    public Instant getTimeLastModified() throws IOException {
        return null;
    }

    @Override
    public Instant getTimeLastAccess() throws IOException {
        return null;
    }

    @Override
    public long getSize(ReadOption<?>... options) throws IOException {
        return IOStreams.consume(openRead(options));
    }

    @Override
    public InputStream openRead(ReadOption<?>... options) throws IOException {
        try {
            return openInputStream(Options.merge(session.getOptions(), options));
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    protected abstract InputStream openInputStream(Options options) throws Exception;

    @Override
    public OutputStream openWrite(WriteOption<?>... options) throws IOException {
        try {
            Options allOptions = Options.merge(session.getOptions(), options);
            if (WriteOption.ATOMIC.isEnabled(allOptions)) {
                throw new UnsupportedOperationException("ATOMIC option not supported for openWrite(), use write() instead");
            }
            if (WriteOption.CREATE_NEW.isEnabled(allOptions) && exists()) {
                throw new AlreadyExistsException();
            }
            return openOutputStream(allOptions);
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    protected abstract OutputStream openOutputStream(Options options) throws Exception;

    @Override
    public long write(InputStream source, WriteOption<?>... options) throws IOException {
        try {
            Options allOptions = Options.merge(session.getOptions(), options);
            if (WriteOption.ATOMIC.isEnabled(allOptions) && WriteOption.APPEND.isEnabled(allOptions)) {
                throw new UnsupportedOperationException("Cannot mix CREATE_NEW with APPEND");
            }
            return writeStream(source, Options.merge(session.getOptions(), options));
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    protected long writeStream(InputStream source, Options options) throws Exception {
        try (OutputStream outputStream = openOutputStream(options.without(WriteOption.ATOMIC))) {
            return IOStreams.transfer(source, outputStream);
        }
    }

    protected IOException translateException(Exception e) {
        if (e == null) {
            return new IOException("Unspecified general exception");
        }
        if (e instanceof UncheckedIOException) {
            e = ((UncheckedIOException)e).getCause();
        }
        if (e instanceof NoSuchFileException || e instanceof FileNotFoundException) {
            return new NotFoundException();
        }
        if (e instanceof FileAlreadyExistsException) {
            return new AlreadyExistsException();
        }
        if (e instanceof DirectoryNotEmptyException) {
            return new NotEmptyDirectoryException();
        }
        String msg = e.getMessage();
        if (msg != null) {
            if (msg.contains("cannot find the file specified") || msg.contains("not found")) {
                return new NotFoundException();
            }
        }
        if (e instanceof IOException) {
            return (IOException) e;
        }
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        return new IOException(e);
    }

    protected String urlencode(String name) {
        try {
            return URLEncoder.encode(name, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new Error(e);
        }
    }

    protected String resolveChild(String url, String name) {
        if (url.endsWith("/")) {
            return url + urlencode(name);
        }
        return url + "/" + urlencode(name);
    }

    @Override
    public Stream<StorageResource> children() throws IOException {
        return childrenNames().stream().map(name -> {
            try {
                return child(name);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public Collection<String> childrenNames() throws IOException {
        return children().map(resource -> {
            try {
                return resource.getName();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        try {
            return getSession().getName() + ":" + getLocalPath();
        } catch (IOException e) {
            return super.toString();
        }
    }
}
