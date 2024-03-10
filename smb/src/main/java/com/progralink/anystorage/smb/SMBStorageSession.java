package com.progralink.anystorage.smb;

import com.progralink.anystorage.api.AbstractStorageSession;
import com.progralink.anystorage.api.options.Option;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.WriteOption;
import jcifs.CIFSContext;

import java.io.IOException;

public class SMBStorageSession extends AbstractStorageSession {

    private CIFSContext smbContext;

    SMBStorageSession(String name, CIFSContext smbContext, String url, Options options) {
        super(name, options);
        this.smbContext = smbContext;
        this.rootResource = new SMBStorageResource(this, url);
    }

    public CIFSContext getSmbContext() {
        return smbContext;
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
                WriteOption.Name.LAST_MODIFIED_TIME.equals(option.getName());
    }

    @Override
    public void close() throws IOException {
        smbContext.close();
    }
}
