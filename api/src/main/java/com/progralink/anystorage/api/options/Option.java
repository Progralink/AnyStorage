package com.progralink.anystorage.api.options;

import java.util.Locale;

public abstract class Option<T> {
    String name;
    T value;

    public Option(String name, T value) {
        this.name = name;
        this.value = value;
    }

    public String getName(){
        return name;
    }

    public T getValue() {
        return value;
    }

    public T from(Options options) {
        T value = options.get(getName());
        return value;
    }

    public boolean isEnabled(Options options) {
        return isValueEnabled(options.get(getName()));
    }

    public Options set(Options options, T value) {
        return options.with(getName(), value);
    }

    public Options unset(Options options) {
        return options.without(getName());
    }

    static boolean isValueEnabled(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        String s = value.toString().toLowerCase(Locale.ROOT);
        return !s.isEmpty() && !s.equals("false") && !s.equals("0") && !s.equals("no");
    }
}
