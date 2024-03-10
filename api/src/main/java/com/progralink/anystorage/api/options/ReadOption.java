package com.progralink.anystorage.api.options;

public class ReadOption<T> extends Option<T> {
    public static final class Name {
        private Name() { }

        public static final String OLDEST_VERSION = "OLDEST_VERSION";
    }


    public static final ReadOption<Boolean> OLDEST_VERSION = new ReadOption<>(Name.OLDEST_VERSION, true);


    public ReadOption(String name, T value) {
        super(name, value);
    }
}
