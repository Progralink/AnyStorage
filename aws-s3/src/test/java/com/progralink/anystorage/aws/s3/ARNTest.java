package com.progralink.anystorage.aws.s3;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ARNTest {

    @Test
    void testIAMexample() {
        ARN arn = ARN.parse("arn:aws:iam::123456789012:user/johndoe");
        assertEquals("aws", arn.getPartition());
        assertEquals("iam", arn.getService());
        assertEquals("", arn.getRegion());
        assertEquals("123456789012", arn.getAccountId());
        assertEquals("user", arn.getResourceType());
        assertEquals("johndoe", arn.getResourceId());
    }

    @Test
    void testSNSexample() {
        ARN arn = ARN.parse("arn:aws:sns:us-east-1:123456789012:example-sns-topic-name");
        assertEquals("aws", arn.getPartition());
        assertEquals("us-east-1", arn.getRegion());
        assertEquals("123456789012", arn.getAccountId());
        assertEquals("", arn.getResourceType());
        assertEquals("example-sns-topic-name", arn.getResourceId());
    }

}
