package com.progralink.anystorage.smb;

import com.progralink.anystorage.api.AbstractStorageResource;
import com.progralink.anystorage.api.StorageResource;
import com.progralink.anystorage.api.exceptions.AlreadyExistsException;
import com.progralink.anystorage.api.exceptions.NotFoundException;
import com.progralink.anystorage.api.options.DeleteOption;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.ReadOption;
import com.progralink.anystorage.api.options.WriteOption;
import com.progralink.jinout.iterators.Iterators;
import com.progralink.jinout.streams.IOStreams;
import jcifs.CIFSException;
import jcifs.CloseableIterator;
import jcifs.SmbResource;
import jcifs.smb.SmbException;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

public class SMBStorageResource extends AbstractStorageResource {
    private String url;
    private SmbResource resource;

    SMBStorageResource(SMBStorageSession session, String url) {
        super(session);
        this.url = url;
    }

    SMBStorageResource(SMBStorageResource parent, String url, SmbResource resource) {
        super(parent);
        this.url = url;
        this.resource = resource;
    }

    private SmbResource getSmbResource() throws IOException {
        if (resource == null) {
            try {
                resource = ((SMBStorageSession) getSession()).getSmbContext().get(url);
            } catch (CIFSException e) {
                throw translateException(e);
            }
        }
        return resource;
    }

    @Override
    public boolean isFile() throws IOException {
        try {
            return getSmbResource().isFile();
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public boolean isDirectory() throws IOException {
        try {
            return getSmbResource().isDirectory();
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public boolean exists() throws IOException {
        try {
            return getSmbResource().exists();
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public String getName() throws IOException {
        return name(getSmbResource());
    }

    @Override
    public long getSize(ReadOption<?>... options) throws IOException {
        try {
            return getSmbResource().length();
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public Instant getTimeCreated() throws IOException {
        try {
            return Instant.ofEpochMilli(getSmbResource().createTime());
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public Instant getTimeLastModified() throws IOException {
        try {
            return Instant.ofEpochMilli(getSmbResource().lastModified());
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public Instant getTimeLastAccess() throws IOException {
        try {
            return Instant.ofEpochMilli(getSmbResource().lastAccess());
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    protected InputStream openInputStream(Options options) throws Exception {
        return getSmbResource().openInputStream();
    }

    @Override
    protected OutputStream openOutputStream(Options options) throws Exception {
        return new FilterOutputStream(getSmbResource().openOutputStream()) {
            @Override
            public void close() throws IOException {
                super.close();
                postWrite(options);
            }
        };
    }

    @Override
    protected long writeStream(InputStream source, Options options) throws Exception {
        if (!WriteOption.ATOMIC.isEnabled(options)) {
            return super.writeStream(source, options);
        }

        boolean overwrite = !WriteOption.CREATE_NEW.isEnabled(options);

        SmbResource targetResource = getSmbResource();

        SmbResource parentResource = ((SMBStorageResource) getParent()).getSmbResource();
        if (!parentResource.exists()) {
            parentResource.mkdirs();
        }

        long length;
        SmbResource tempResource = parentResource.resolve(".~" + getName() + "." + UUID.randomUUID() + ".TMP");
        tempResource.createNewFile();
        try {
            try (OutputStream out = tempResource.openOutputStream()) {
                length = IOStreams.transfer(source, out);
            }

            if (!overwrite && exists()) {
                throw new AlreadyExistsException();
            }
            applyOptionalTimes(tempResource, options);
            tempResource.renameTo(targetResource, overwrite);
            postWrite(options);
            return length;
        } finally {
            try {
                if (tempResource.exists()) {
                    tempResource.delete();
                }
            } catch (IOException ignore) { }
        }
    }

    protected void postWrite(Options options) throws IOException {
        applyOptionalTimes(getSmbResource(), options);
    }

    protected void applyOptionalTimes(SmbResource smbResource, Options options) throws CIFSException {
        Instant creationTime = options.getInstant(WriteOption.Name.CREATION_TIME);
        Instant modificationTime = options.getInstant(WriteOption.Name.LAST_MODIFIED_TIME);
        if (creationTime != null) {
            smbResource.setCreateTime(creationTime.toEpochMilli());
        }
        if (modificationTime != null) {
            smbResource.setLastModified(modificationTime.toEpochMilli());
        }
    }

    @Override
    public Stream<StorageResource> children() throws IOException {
        try {
            CloseableIterator<SmbResource> children = getSmbResource().children();
            return Iterators.asStream(children)
                    .filter(smbResource -> !smbResource.getName().startsWith(".~")) //skip temporary upload files
                    .filter(smbResource -> !smbResource.getName().startsWith("IPC$")) //TODO: only for full and only for share name
                    .map(smbResource -> new SMBStorageResource(this, resolveChild(url, name(smbResource)), smbResource));
        } catch (Exception e) {
            if (e.getMessage().contains("The directory name is invalid.")) {
                return Stream.empty();
            }
            throw translateException(e);
        }
    }

    @Override
    public StorageResource child(String name) throws IOException {
        return new SMBStorageResource(this, resolveChild(url, name), getSmbResource().resolve(name + "/"));
    }

    @Override
    public boolean delete(DeleteOption<?>... options) throws IOException {
        try {
            getSmbResource().delete();
            return true;
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SMBStorageResource that = (SMBStorageResource) o;
        return Objects.equals(url, that.url);
    }

    @Override
    public int hashCode() {
        return url.hashCode();
    }

    @Override
    public String toString() {
        return url;
    }

    @Override
    protected IOException translateException(Exception e) {
        if (e instanceof SmbException) {
            long status = ((SmbException) e).getNtStatus();
            if (status == -1073741772) {
                return new NotFoundException(e);
            }
            if (status == -1073741771) {
                return new AlreadyExistsException(e);
            }
        }
        return super.translateException(e);
    }

    private static String name(SmbResource smbResource) {
        String name = smbResource.getName();
        if (name.endsWith("/")) {
            return name.substring(0, name.length() - 1);
        }
        return name;
    }
}
