package com.progralink.anystorage.api.options;

public class DeleteOption<T> extends Option<T> {
    public static final class Name {
        private Name() { }

        public static final String REMOVE_HISTORY = "REMOVE_HISTORY";
    }


    public static final DeleteOption<Boolean> REMOVE_HISTORY = new DeleteOption<>(Name.REMOVE_HISTORY, true);


    public DeleteOption(String name, T value) {
        super(name, value);
    }
}
