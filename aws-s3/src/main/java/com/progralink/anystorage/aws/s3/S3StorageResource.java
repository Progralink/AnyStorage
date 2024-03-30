package com.progralink.anystorage.aws.s3;

import com.progralink.anystorage.api.AbstractStorageResource;
import com.progralink.anystorage.api.StorageResource;
import com.progralink.anystorage.api.exceptions.AlreadyExistsException;
import com.progralink.anystorage.api.exceptions.NotFoundException;
import com.progralink.anystorage.api.options.DeleteOption;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.ReadOption;
import com.progralink.anystorage.api.options.WriteOption;
import com.progralink.jinout.streams.IOStreams;
import com.progralink.jinout.streams.input.LengthAwareInputStream;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

public class S3StorageResource extends AbstractStorageResource {
    private static final String CACHE_CONTROL_NO_CACHE = "no-cache";


    private String path;

    S3StorageResource(S3StorageSession session, String path) {
        super(session);
        this.path = path;
    }

    S3StorageResource(S3StorageResource parent, String path) {
        super(parent);
        this.path = path;
    }

    @Override
    public S3StorageSession getSession() {
        return (S3StorageSession) super.getSession();
    }

    @Override
    protected InputStream openInputStream(Options options) throws Exception {
        ResponseInputStream<GetObjectResponse> responseInputStream = getSession().getClient().getObject(
                prepareGetObjectRequest(options)
        );
        GetObjectResponse response = responseInputStream.response();
        InputStream inputStream = responseInputStream;
        if (response != null && response.contentLength() != null) {
            inputStream = new LengthAwareInputStream(responseInputStream, response.contentLength());
        }
        return inputStream;
    }

    @Override
    protected OutputStream openOutputStream(Options options) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected long writeStream(InputStream source, Options options) throws Exception {
        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(getSession().getBucket())
                .key(path)
                .cacheControl(CACHE_CONTROL_NO_CACHE)
                .expires(Instant.now());

        String storageClass = options.getString(S3WriteOption.Name.S3_STORAGE_CLASS);
        if (storageClass.isEmpty()) {
            if (getSession().getDefaultStorageClass() != null) {
                storageClass = getSession().getDefaultStorageClass().toString();
            }
            if (!storageClass.isEmpty()) {
                requestBuilder.storageClass(storageClass);
            }
        }

        byte[] checksum = options.get(WriteOption.Name.CHECKSUM_SHA256);
        if (checksum != null) {
            requestBuilder.checksumSHA256(
                    Base64.getEncoder().encodeToString(checksum)
            );
        }

        boolean overwrite = !WriteOption.CREATE_NEW.isEnabled(options);
        if (!overwrite && exists()) {
            throw new AlreadyExistsException();
        }

        Long length = options.getLong(WriteOption.Name.CONTENT_LENGTH);
        if (length == null) {
            length = IOStreams.getRemainingByteLength(source);
        }
        if (length == null) {
            throw new IllegalStateException("Unknown content length");
        }

        RequestBody requestBody = RequestBody.fromInputStream(
                new BufferedInputStream(source), length   //without wrapping with BufferedInputStream it was hanging very often!
        );

        PutObjectResponse response = getSession().getClient().putObject(requestBuilder.build(), requestBody);

        if (!overwrite) {
            String putVersionId = response.versionId();
            ObjectVersion oldestVersion = getOldestVersion();
            if (!Objects.equals(putVersionId, oldestVersion.versionId())) {
                try {
                    getSession().getClient().deleteObject(
                            DeleteObjectRequest.builder()
                                    .bucket(getSession().getBucket())
                                    .key(path)
                                    .versionId(putVersionId)
                                    .build()
                    );
                } catch (S3Exception ignore) {
                    //ignoring because it might be locked
                    //and even if failed, when reading with ReadOption.OLDEST_VERSION option still will retrieve valid version
                }

                throw new AlreadyExistsException();
            }
        }
        return length;
    }

    @Override
    public boolean isFile() throws IOException {
        return exists();
    }

    @Override
    public boolean exists() throws IOException {
        try {
            getObjectHead();
            return true;
        } catch (NoSuchKeyException noSuchKeyException) {
            return false;
        } catch (S3Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public String getName() throws IOException {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    @Override
    public long getSize(ReadOption<?>... options) throws IOException {
        try {
            for (ReadOption option : options) {
                if (option == ReadOption.OLDEST_VERSION) {
                    Long size = getOldestVersion().size();
                    if (size == null) {
                        throw new NotFoundException();
                    }
                }
            }
            
            Long size = getLatestVersion().size();
            if (size == null) {
                throw new NotFoundException();
            }
            return size;
        } catch (NoSuchKeyException e) {
            throw new NotFoundException();
        } catch (S3Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public byte[] readFully(ReadOption<?>... options) throws IOException {
        ResponseBytes<GetObjectResponse> responseBytes = getSession().getClient().getObjectAsBytes(
                prepareGetObjectRequest(Options.merge(getSession().getOptions(), options))
        );
        return responseBytes.asByteArrayUnsafe();
    }

    @Override
    public void write(byte[] data, int offset, int length, WriteOption<?>... options) throws IOException {
        List<WriteOption> optionList = new LinkedList<>(Arrays.asList(options));
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(data, offset, length);
            byte[] checksumSHA256 = messageDigest.digest();
            optionList.add(WriteOption.ofChecksumSHA256(checksumSHA256));
            optionList.add(WriteOption.ofContentLength(length));
        } catch (NoSuchAlgorithmException e) {
            throw new Error(e);
        }

        super.write(data, offset, length, optionList.toArray(new WriteOption[optionList.size()]));
    }

    @Override
    public Stream<StorageResource> children() throws IOException {
        return childrenNames().stream().map(this::child);
    }

    @Override
    public Collection<String> childrenNames() throws IOException {
        final String prefix;
        if (!path.endsWith("/")) {
            prefix = path + "/";
        } else {
            prefix = path;
        }

        Set<String> names = new LinkedHashSet<>();
        ListObjectsV2Request request = ListObjectsV2Request.builder()
            .bucket(getSession().getBucket())
            .prefix(prefix)
            //.delimiter("/")   //TODO: now it is inefficient (when there are subdirectories), but with delimiter it skips (virtual) directories
            .build();

        ListObjectsV2Iterable response = getSession().getClient().listObjectsV2Paginator(request);
        for (ListObjectsV2Response page : response) {
            page.contents().forEach((S3Object object) -> {
                String key = object.key();
                String name;
                int i = key.indexOf('/', prefix.length());
                if (i != -1) {
                    name = key.substring(prefix.length(), i);
                } else {
                    name = key.substring(prefix.length());
                }
                names.add(name);
            });
        }

        return names;
    }

    @Override
    public StorageResource child(String name) {
        String childPath;
        if (path.endsWith("/")) {
            childPath = path + name;
        } else {
            childPath = path + "/" + name;
        }
        return new S3StorageResource(this, childPath);
    }

    @Override
    public boolean delete(DeleteOption<?>... options) throws IOException {
        Collection<ObjectIdentifier> identifiers = new LinkedList<>();
        if (Arrays.asList(options).contains(DeleteOption.REMOVE_HISTORY)) {
            ListObjectVersionsResponse listObjectVersionsResponse = getAllVersions();
            if (listObjectVersionsResponse != null) {
                for (ObjectVersion version : listObjectVersionsResponse.versions()) {
                    identifiers.add(ObjectIdentifier.builder().key(path).versionId(version.versionId()).build());
                }
                for (DeleteMarkerEntry deleteMarkerEntry : listObjectVersionsResponse.deleteMarkers()) {
                    identifiers.add(ObjectIdentifier.builder().key(path).versionId(deleteMarkerEntry.versionId()).build());
                }
            }
        }
        identifiers.add(ObjectIdentifier.builder().key(path).build());

        try {
            DeleteObjectsResponse resonse = getSession().getClient().deleteObjects(
                    DeleteObjectsRequest.builder()
                            .bucket(getSession().getBucket())
                            .delete(Delete.builder().objects(identifiers).build())
                            .bypassGovernanceRetention(true)
                            .build()
            );
            return !resonse.deleted().isEmpty();
        } catch (S3Exception e) {
            throw translateException(e);
        }
    }

    protected HeadObjectResponse getObjectHead() {
        return getSession().getClient().headObject(
                HeadObjectRequest.builder()
                        .bucket(getSession().getBucket())
                        .key(path)
                        .build()
        );
    }

    protected GetObjectRequest prepareGetObjectRequest(Options options) throws NotFoundException {
        GetObjectRequest.Builder builder = GetObjectRequest.builder()
                .bucket(getSession().getBucket())
                .key(path)
                .responseCacheControl(CACHE_CONTROL_NO_CACHE)
                .responseExpires(Instant.now());

        if (ReadOption.OLDEST_VERSION.isEnabled(options)) {
            ObjectVersion firstVersion = getOldestVersion();
            String versionId = firstVersion.versionId();
            builder.versionId(versionId);
        }

        return builder.build();
    }

    protected ObjectVersion getOldestVersion() throws NotFoundException {
        ObjectVersion oldestVersion = null;
        ListObjectVersionsResponse listObjectVersionsResponse = getAllVersions();
        if (listObjectVersionsResponse == null) {
            return null;
        }

        for (ObjectVersion ov : listObjectVersionsResponse.versions()) {
            if (ov.key().equals(path)) {
                if (oldestVersion == null || ov.lastModified().compareTo(oldestVersion.lastModified()) <= 0) {
                    oldestVersion = ov;
                }
            }
        }

        if (oldestVersion == null) {
            throw new NotFoundException();
        }

        return oldestVersion;
    }

    protected ListObjectVersionsResponse getAllVersions() {
        try {
            return getSession().getClient().listObjectVersions(
                    ListObjectVersionsRequest.builder()
                            .bucket(getSession().getBucket())
                            .prefix(path)
                            .build()
            );
        } catch (NoSuchKeyException e) {
            return null;
        }
    }

    protected ObjectVersion getLatestVersion() {
        try {
            HeadObjectResponse head = getObjectHead();

            return ObjectVersion.builder()
                    .key(path)
                    .versionId(head.versionId())
                    .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                    .checksumAlgorithmWithStrings(head.checksumSHA256())
                    .isLatest(true)
                    .eTag(head.eTag())
                    .size(head.contentLength())
                    .lastModified(head.lastModified())
                    .build();
        } catch (NoSuchKeyException e) {
            return null;
        }
    }
}
