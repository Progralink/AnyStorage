package com.progralink.anystorage.filesystem;

import com.progralink.anystorage.api.AbstractStorageSession;
import com.progralink.anystorage.api.options.Option;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.WriteOption;

import java.nio.file.Path;

public class FileSystemStorageSession extends AbstractStorageSession {

    public FileSystemStorageSession(Path path) {
        this(path.toString(), path, Options.DEFAULTS);
    }

    public FileSystemStorageSession(String name, Path path, Options options) {
        super(name, options);
        this.rootResource = new FileSystemStorageResource(this, path);
    }

    @Override
    public boolean isDirectoryless() {
        return false;
    }

    @Override
    public boolean isSupported(Option<?> option) {
        return option == WriteOption.ATOMIC ||
                option == WriteOption.APPEND ||
                option == WriteOption.CREATE_NEW ||
                WriteOption.Name.CREATION_TIME.equals(option.getName()) ||
                WriteOption.Name.LAST_MODIFIED_TIME.equals(option.getName()) ||
                WriteOption.Name.LAST_ACCESS_TIME.equals(option.getName());
    }
}
