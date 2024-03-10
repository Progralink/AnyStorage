package com.progralink.anystorage.smb;

import com.progralink.anystorage.api.AbstractStorageConnector;
import com.progralink.anystorage.api.StorageSession;
import com.progralink.anystorage.api.credentials.BasicCredentials;
import com.progralink.anystorage.api.options.Options;
import jcifs.CIFSContext;
import jcifs.Credentials;
import jcifs.config.PropertyConfiguration;
import jcifs.context.BaseContext;
import jcifs.smb.NtlmPasswordAuthenticator;

import java.io.IOException;
import java.net.URI;

public class SMBStorageConnector extends AbstractStorageConnector {

    @Override
    public String getTypeLabel() {
        return "SMB/CIFS";
    }

    @Override
    public boolean canHandle(String connectionString) {
        return connectionString.startsWith("//") || connectionString.startsWith("smb://") || connectionString.startsWith("cifs://");
    }

    @Override
    public StorageSession connect(String name, String connectionString, Options options) throws IOException {
        URI uri = URI.create(connectionString);
        BasicCredentials credentials = BasicCredentials.fromURI(uri);
        return connect(name, uri.getHost(), uri.getPath(), new NtlmPasswordAuthenticator(credentials.getDomain(), credentials.getUsername(), credentials.getPassword()), options);
    }

    public StorageSession connect(String name, String host, String path, String username, String password, Options options) throws IOException {
        return connect(name, host, path, new NtlmPasswordAuthenticator(username, password), options);
    }

    public StorageSession connect(String name, String host, String path, String domain, String username, String password, Options options) throws IOException {
        return connect(name, host, path, new NtlmPasswordAuthenticator(domain, username, password), options);
    }

    public StorageSession connect(String name, String host, String path, Credentials credentials, Options options) throws IOException {
        CIFSContext context = new BaseContext(new PropertyConfiguration(System.getProperties())).withCredentials(credentials);
        String url = "smb://" + host + (path.startsWith("/") ? path : "/" + path);
        if (!url.endsWith("/")) {
            url += "/";
        }
        return new SMBStorageSession(name, context, url, options);
    }
}
