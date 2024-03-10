package com.progralink.anystorage.aws.s3;

//https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html
public class ARN {
    private String partition = "";
    private String region = "";
    private String service = "";
    private String accountId = "";
    private String resourceId = "";
    private String resourceType = "";

    public ARN() {
    }

    public ARN(String partition, String region, String service, String accountId, String resourceId, String resourceType) {
        this.partition = partition;
        this.region = region;
        this.service = service;
        this.accountId = accountId;
        this.resourceId = resourceId;
        this.resourceType = resourceType;
    }

    public String getPartition() {
        return partition;
    }

    public String getRegion() {
        return region;
    }

    public String getService() {
        return service;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getResourceId() {
        return resourceId;
    }

    public String getResourceType() {
        return resourceType;
    }

    public static ARN parse(String arn) {
        if (!arn.startsWith("arn")) {
            throw new IllegalArgumentException("arn");
        }

        ARN parsed = new ARN();
        String[] parts = arn.split(":");
        if (parts.length < 6) {
            throw new IllegalArgumentException("arn");
        }
        parsed.partition = parts[1];
        parsed.service = parts[2];
        parsed.region = parts[3];
        parsed.accountId = parts[4];
        if (parts.length == 6) {
            String resource = parts[5];
            if (resource.contains("/")) {
                parsed.resourceType = resource.substring(0, resource.indexOf('/'));
                parsed.resourceId = resource.substring(resource.indexOf('/') + 1);
            } else {
                parsed.resourceId = resource;
            }
        } else {
            parsed.resourceType = parts[5];
            parsed.resourceId = parts[6];
        }
        return parsed;
    }
}
