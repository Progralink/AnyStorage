package com.progralink.anystorage.filesystem;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.BooleanSupplier;

public class FileBasedLock implements AutoCloseable {
    private final Path lockFilePath;
    private FileChannel fileChannel;
    private FileLock fileLock;
    private volatile boolean acquired = false;
    private Thread lockThread;

    public FileBasedLock(Path lockFilePath) {
        this.lockFilePath = lockFilePath;
    }

    public Path getLockFilePath() {
        return lockFilePath;
    }

    public boolean exists() {
        return Files.exists(lockFilePath);
    }

    public boolean isAcquired() {
        return acquired;
    }

    public boolean tryAcquire(long lockRecurrentTimeoutMillis) throws IOException {
        if (!acquired) {
            try {
                fileChannel = FileChannel.open(lockFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SYNC, StandardOpenOption.DELETE_ON_CLOSE);
                if (fileChannel.size() >= Long.BYTES) {
                    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                    if (fileChannel.read(buffer) == Long.BYTES) {
                        long currentDeadlineEpochMillis = buffer.getLong();
                        long currentTimeoutMillis = currentDeadlineEpochMillis - System.currentTimeMillis();
                        if (currentTimeoutMillis > 0) {
                            fileChannel.close();
                            return false;
                        }
                    }
                }

                try {
                    fileLock = fileChannel.lock(0, Long.MAX_VALUE, true);
                } catch (OverlappingFileLockException e) {
                    fileChannel.close();
                    return false;
                }

                fileChannel.truncate(0);
                fileChannel.write(ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis() + lockRecurrentTimeoutMillis));
                acquired = true;

                lockThread = new Thread(() -> {
                    while (acquired) {
                        try {
                            if (Thread.interrupted()) {
                                throw new InterruptedException();
                            }

                            if (fileChannel != null) {
                                fileChannel.write(ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis() + lockRecurrentTimeoutMillis), 0);
                            }

                            Thread.sleep(lockRecurrentTimeoutMillis / 2);

                        } catch (ClosedChannelException | InterruptedException ignore) {
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                });
                lockThread.start();
            } catch (AccessDeniedException e) {
                return false;
            }
        }
        return acquired;
    }

    public void acquire(long lockRecurrentTimeoutMillis) throws IOException, InterruptedException {
        acquire(lockRecurrentTimeoutMillis, null);
    }

    public boolean acquire(long lockRecurrentTimeoutMillis, BooleanSupplier precondition) throws IOException, InterruptedException {
        long waitTime = 0;

        do {
            if (precondition != null && !precondition.getAsBoolean()) {
                return false;
            }

            if (waitTime == 0) {
                waitTime = 5;
            } else {
                Thread.sleep(waitTime);
                waitTime *= 2;
            }
        } while (!tryAcquire(lockRecurrentTimeoutMillis));

        return true;
    }

    public void release() throws IOException {
        if (isAcquired()) {
            lockThread.interrupt();
            lockThread = null;

            if (fileChannel != null) {
                if (fileLock != null) {
                    fileChannel.truncate(0);
                    fileLock.close();
                }
                fileChannel.close();
            }
        }
        acquired = false;
    }

    @Override
    public void close() throws Exception {
        release();
    }

    @Override
    public String toString() {
        return (acquired ? "[ACQUIRED] " : "") + lockFilePath;
    }
}
