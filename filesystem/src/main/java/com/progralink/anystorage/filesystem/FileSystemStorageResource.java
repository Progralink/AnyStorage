package com.progralink.anystorage.filesystem;

import com.progralink.anystorage.api.AbstractStorageResource;
import com.progralink.anystorage.api.StorageResource;
import com.progralink.anystorage.api.exceptions.AlreadyExistsException;
import com.progralink.anystorage.api.exceptions.NotFoundException;
import com.progralink.anystorage.api.options.DeleteOption;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.ReadOption;
import com.progralink.anystorage.api.options.WriteOption;
import com.progralink.jinout.streams.IOStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

public class FileSystemStorageResource extends AbstractStorageResource {
    private static final long FILE_LOCK_RECURRENT_TIMEOUT_MILLIS = 10000;

    private final Path path;

    public FileSystemStorageResource(FileSystemStorageSession session, Path path) {
        super(session);
        this.path = path;
    }

    public FileSystemStorageResource(FileSystemStorageResource parent, Path path) {
        super(parent);
        this.path = path;
    }

    @Override
    public boolean isFile() throws IOException {
        try {
            return Files.isRegularFile(path);
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public boolean isDirectory() throws IOException {
        try {
            return Files.isDirectory(path);
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public boolean exists() throws IOException {
        try {
            return Files.exists(path);
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public String getName() throws IOException {
        try {
            if (path.getFileName() != null) {
                return path.getFileName().toString();
            }
            return "";
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public long getSize(ReadOption<?>... options) throws IOException {
        try {
            return Files.size(path);
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    public Instant getTimeCreated() throws IOException {
        try {
            BasicFileAttributes fileAttributes = Files.readAttributes(path, BasicFileAttributes.class);
            return fileAttributes.creationTime().toInstant();
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public Instant getTimeLastModified() throws IOException {
        try {
            return Files.getLastModifiedTime(path).toInstant();
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public Instant getTimeLastAccess() throws IOException {
        try {
            BasicFileAttributes fileAttributes = Files.readAttributes(path, BasicFileAttributes.class);
            return fileAttributes.lastAccessTime().toInstant();
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    protected InputStream openInputStream(Options options) throws IOException {
        try {
            return Files.newInputStream(path);
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    protected OutputStream openOutputStream(Options options) throws IOException {
        Files.createDirectories(path.getParent());

        List<StandardOpenOption> openOptions = new LinkedList<>();
        if (WriteOption.CREATE_NEW.isEnabled(options)) {
            openOptions.add(StandardOpenOption.CREATE_NEW);
        }
        else if (WriteOption.APPEND.isEnabled(options)) {
            openOptions.add(StandardOpenOption.APPEND);
        }

        try {
            return Files.newOutputStream(path);
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    protected long writeStream(InputStream source, Options options) throws Exception {
        Files.createDirectories(path.getParent());

        if (!WriteOption.ATOMIC.isEnabled(options)) {
            return super.writeStream(source, options);
        }

        boolean overwrite = !options.getBoolean(WriteOption.CREATE_NEW.getName());

        Path targetFilePath = path;
        FileBasedLock lockFile = null;
        if (!overwrite && Files.exists(targetFilePath)) {
            throw new AlreadyExistsException();
        }

        Path parentPath = targetFilePath.getParent();
        if (!Files.exists(parentPath)) {
            Files.createDirectories(parentPath);
        }

        Path tempFilePath = parentPath.resolve(".~" + targetFilePath.getFileName() + "." + UUID.randomUUID() + ".TMP");
        try {
            try (OutputStream out = Files.newOutputStream(tempFilePath, StandardOpenOption.CREATE_NEW)) {
                IOStreams.transfer(source, out);
            }

            lockFile = new FileBasedLock(targetFilePath.resolveSibling(".~" + targetFilePath.getFileName() + ".LOCK"));
            if (!lockFile.acquire(FILE_LOCK_RECURRENT_TIMEOUT_MILLIS, () -> overwrite || !Files.exists(targetFilePath))) {
                throw new AlreadyExistsException();
            }

            try {
                if (!overwrite && Files.exists(targetFilePath)) {
                    throw new AlreadyExistsException();
                }

                applyOptionalTimes(tempFilePath, options);

                try {
                    if (overwrite) {
                        Files.move(tempFilePath, targetFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
                    } else {
                        Files.move(tempFilePath, targetFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.COPY_ATTRIBUTES);
                    }
                } catch (AccessDeniedException | AtomicMoveNotSupportedException | UnsupportedOperationException e) {
                    if (overwrite) {
                        try {
                            Files.move(tempFilePath, targetFilePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
                        } catch (AccessDeniedException | UnsupportedOperationException e2) {
                            Files.move(tempFilePath, targetFilePath, StandardCopyOption.REPLACE_EXISTING);
                            applyOptionalTimes(targetFilePath, options);
                        }
                    } else {
                        if (Files.exists(targetFilePath)) {
                            throw new AlreadyExistsException();
                        }

                        try {
                            Files.move(tempFilePath, targetFilePath, StandardCopyOption.COPY_ATTRIBUTES);
                        } catch (AccessDeniedException | UnsupportedOperationException e3) {
                            Files.move(tempFilePath, targetFilePath);
                            applyOptionalTimes(targetFilePath, options);
                        }
                    }
                }
            } catch (FileAlreadyExistsException e) {
                throw new AlreadyExistsException(e);
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            Files.deleteIfExists(tempFilePath);
            if (lockFile != null) {
                try {
                    lockFile.release();
                } catch (IOException ignore) { }
            }
        }
        return 0;
    }

    protected void applyOptionalTimes(Path path, Options options) {
        Instant creationTime = options.getInstant(WriteOption.Name.CREATION_TIME);
        Instant lastModifiedTime = options.getInstant(WriteOption.Name.LAST_MODIFIED_TIME);
        Instant lastAccessTime = options.getInstant(WriteOption.Name.LAST_ACCESS_TIME);
        if (creationTime != null || lastModifiedTime != null || lastAccessTime != null) {
            try {
                BasicFileAttributeView attributesView = Files.getFileAttributeView(path, BasicFileAttributeView.class);
                if (attributesView != null) {
                    attributesView.setTimes(
                            creationTime != null ? FileTime.from(creationTime) : null,
                            lastModifiedTime != null ? FileTime.from(lastModifiedTime) : null,
                            lastAccessTime != null ? FileTime.from(lastAccessTime) : null
                    );
                }
            } catch (IOException e) {
                //TODO: log
            }
        }
    }

    @Override
    public Stream<StorageResource> children() throws IOException {
        try {
            return Files.list(path)
                    .filter(p -> !p.getFileName().toString().startsWith(".~"))
                    .map(p -> new FileSystemStorageResource(this, p));
        } catch (Exception e) {
            try {
                throw translateException(e);
            } catch (NotFoundException nfe) {
                return Stream.empty();
            }
        }
    }

    @Override
    public StorageResource child(String name) throws IOException {
        try {
            Path childPath = path.resolve(name);
            if (!childPath.startsWith(path)) {
                throw new IllegalArgumentException("Insecure name");
            }
            return new FileSystemStorageResource(this, childPath);
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public boolean delete(DeleteOption<?>... options) throws IOException {
        return Files.deleteIfExists(path);
    }

    @Override
    public boolean deleteDeep(boolean includeSelf, DeleteOption<?>... options) throws IOException {
        try {
            if (!includeSelf) {
                return FileSystemUtils.deleteDirectory(path, false);
            }

            try {
                return delete(options);
            } catch (DirectoryNotEmptyException e) {
                return FileSystemUtils.deleteDirectory(path, includeSelf);
            }
        } catch (Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileSystemStorageResource that = (FileSystemStorageResource) o;
        return Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }

    @Override
    public String toString() {
        return path.toString();
    }

}
