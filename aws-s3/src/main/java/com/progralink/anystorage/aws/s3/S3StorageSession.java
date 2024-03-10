package com.progralink.anystorage.aws.s3;

import com.progralink.anystorage.api.AbstractStorageSession;
import com.progralink.anystorage.api.exceptions.NotFoundException;
import com.progralink.anystorage.api.options.DeleteOption;
import com.progralink.anystorage.api.options.Option;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.WriteOption;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;

public class S3StorageSession extends AbstractStorageSession {
    private S3Client client;
    private String bucket;
    private String rootPath;

    S3StorageSession(String name, S3Client client, String bucket, String rootPath, Options options) {
        super(name, options);
        this.client = client;
        this.bucket = bucket;

        if (rootPath == null || rootPath.isEmpty()) {
            this.rootPath = "";
        } else {
            this.rootPath = rootPath;
        }
        if (!this.rootPath.endsWith("/")) {
            this.rootPath += "/";
        }
        this.rootResource = new S3StorageResource(this, this.rootPath);
    }

    public S3Client getClient() {
        return client;
    }

    public String getBucket() {
        return bucket;
    }

    public String getRootPath() {
        return rootPath;
    }

    @Override
    public boolean isDirectoryless() {
        return true;
    }

    @Override
    public boolean isSupported(Option<?> option) {
        return option == WriteOption.ATOMIC || option == WriteOption.APPEND || option == WriteOption.CREATE_NEW || option == DeleteOption.REMOVE_HISTORY;
    }

    protected IOException translateException(Exception e) {
        if (e instanceof NoSuchKeyException) {
            return new NotFoundException(e);
        }
        return new IOException(e);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
