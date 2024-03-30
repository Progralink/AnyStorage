package com.progralink.anystorage.aws.s3;

import com.progralink.anystorage.api.options.WriteOption;
import software.amazon.awssdk.services.s3.model.StorageClass;

public class S3WriteOption<T> extends WriteOption<T> {
    public static final class Name {
        private Name() {
        }

        public static final String S3_STORAGE_CLASS = "AWS_S3_STORAGE_CLASS";
    }

    public S3WriteOption(String name, T value) {
        super(name, value);
    }


    public static S3WriteOption<StorageClass> ofStorageClass(StorageClass storageClass) {
        return new S3WriteOption<>(Name.S3_STORAGE_CLASS, storageClass);
    }
}
