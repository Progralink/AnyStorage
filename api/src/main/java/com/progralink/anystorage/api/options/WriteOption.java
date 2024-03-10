package com.progralink.anystorage.api.options;

import java.time.Instant;

public class WriteOption<T> extends Option<T> {
    public static final class Name {
        private Name() { }

        public static final String ATOMIC = "ATOMIC";
        public static final String CREATE_NEW = "CREATE_NEW";
        public static final String APPEND = "APPEND";
        public static final String CREATION_TIME = "CREATION_TIME";
        public static final String LAST_MODIFIED_TIME = "LAST_MODIFIED_TIME";
        public static final String LAST_ACCESS_TIME = "LAST_ACCESS_TIME";
        public static final String EXPIRATION_TIME = "EXPIRATION_TIME";
        public static final String CHECKSUM_SHA256 = "CHECKSUM_SHA256";
        public static final String CONTENT_LENGTH = "CONTENT_LENGTH";
    }


    public static final WriteOption<Boolean> ATOMIC = new WriteOption<>(Name.ATOMIC, true);
    public static final WriteOption<Boolean> CREATE_NEW = new WriteOption<>(Name.CREATE_NEW, true);
    public static final WriteOption<Boolean> APPEND = new WriteOption<>(Name.APPEND, true);

    public static WriteOption<Instant> ofCreationTime(Instant value) { return new WriteOption<>(Name.CREATION_TIME, value); }
    public static WriteOption<Instant> ofLastModifiedTime(Instant value) { return new WriteOption<>(Name.LAST_MODIFIED_TIME, value); }
    public static WriteOption<Instant> ofLastAccessTime(Instant value) { return new WriteOption<>(Name.LAST_ACCESS_TIME, value); }
    public static WriteOption<Instant> ofExpirationTime(Instant value) { return new WriteOption<>(Name.EXPIRATION_TIME, value); }
    public static WriteOption<byte[]> ofChecksumSHA256(byte[] value) { return new WriteOption<>(Name.CHECKSUM_SHA256, value); }
    public static WriteOption<Long> ofContentLength(long value) { return new WriteOption<>(Name.CONTENT_LENGTH, value); }


    public WriteOption(String name, T value) {
        super(name, value);
    }
}
