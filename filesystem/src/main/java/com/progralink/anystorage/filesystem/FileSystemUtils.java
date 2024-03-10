package com.progralink.anystorage.filesystem;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicBoolean;

public final class FileSystemUtils {
    private FileSystemUtils() { }

    public static boolean deleteDirectory(Path path) throws IOException {
        return deleteDirectory(path, true);
    }

    public static boolean deleteDirectory(Path path, boolean includeSelf) throws IOException {
        try {
            AtomicBoolean deleted = new AtomicBoolean(false);
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    deleted.set(true);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (includeSelf || !dir.equals(path)) {
                        Files.delete(dir);
                        deleted.set(true);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
            return deleted.get();
        } catch (NoSuchFileException ignore) {
            return false;
        }
    }
}
