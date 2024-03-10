package com.progralink.anystorage.aws.s3;

import com.progralink.anystorage.api.AbstractStorageConnector;
import com.progralink.anystorage.api.StorageSession;
import com.progralink.anystorage.api.credentials.BasicCredentials;
import com.progralink.anystorage.api.options.Options;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;
import java.net.URISyntaxException;

public class S3StorageConnector extends AbstractStorageConnector {
    @Override
    public String getTypeLabel() {
        return "AWS S3";
    }

    @Override
    public boolean canHandle(String connectionString) {
        if (connectionString.startsWith("arn:")) {
            try {
                ARN arn = ARN.parse(connectionString);
                if ("s3".equals(arn.getService())) {
                    return true;
                }
            } catch (IllegalArgumentException ignore) {
                return false;
            }
        }
        return connectionString.equals("s3") || connectionString.startsWith("s3://") || (connectionString.startsWith("https://s3-") && connectionString.contains(".amazonaws.com"));
    }

    @Override
    public StorageSession connect(String name, String connectionString, Options options) {
        if (!canHandle(connectionString)) {
            throw new IllegalArgumentException("connectionString");
        }

        String region = options.getString("AWS_DEFAULT_REGION");
        if (region.isEmpty()) {
            region = options.getString("AWS_REGION");
        }

        S3ClientBuilder builder = S3Client.builder();

        String bucket = "storage";
        String rootPath = null;

        BasicCredentials credentials = null;

        if (connectionString.startsWith("s3")) {
            if (connectionString.length() > 5) {
                // s3://my-bucket/test=hello-world
                URI s3uri = URI.create(connectionString);
                credentials = BasicCredentials.fromURI(s3uri);
                bucket = s3uri.getHost();
                rootPath = s3uri.getPath();
                if (rootPath.startsWith("/")) {
                    rootPath = rootPath.substring(1);
                }
            }
        }
        else if (connectionString.startsWith("arn:")) {
                // arn:aws:s3:::my-transactional-bucket

                ARN arn = ARN.parse(connectionString);
                if (!arn.getRegion().isEmpty()) {
                    builder.useArnRegion(true);
                }
                if (!arn.getResourceId().isEmpty()) {
                    bucket = arn.getResourceId();
                }
        } else {
            URI uri = URI.create(connectionString);
            credentials = BasicCredentials.fromURI(uri);

            String host = uri.getHost();
            if (host.startsWith("s3-")) {
                region = host.substring(3);
            } else {
                region = host;
            }

            if (region.endsWith(".amazonaws.com")) {
                region = region.substring(0, region.length() - ".amazonaws.com".length());
            }

            String path = uri.getPath();
            if (path.length() > 1) {
                int bucketEnd = path.indexOf('/', 1);
                bucket = path.substring(1, bucketEnd);

                if (path.length() > bucketEnd + 1) {
                    rootPath = path.substring(bucketEnd + 1);
                }
            }


            if (name == null || name.isEmpty()) {
                try {
                    name = new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment()).toString();
                } catch (URISyntaxException ignore) {
                    name = "s3";
                }
            }
        }

        if (credentials != null) {
            AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(
                    credentials.getUsername(),
                    credentials.getPassword()
            );
            builder.credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials));
        }

        if (region != null && !region.isEmpty()) {
            builder.region(Region.of(region));
        }

        S3Client client = builder.build();
        return new S3StorageSession(name, client, bucket, rootPath, options);
    }
}
