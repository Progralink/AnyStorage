package com.progralink.anystorage.api.options;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

public interface Options {
    <T> T get(String name);


    Options DEFAULTS = HierarchicalOptions.DEFAULTS;

    Options NONE = new HierarchicalOptions((Options) null);


    default boolean getBoolean(String name) {
        return Option.isValueEnabled(get(name));
    }

    default String getString(String name) {
        Object value = get(name);
        if (value != null) {
            return value.toString();
        }
        return "";
    }

    default Instant getInstant(String name) {
        Object value = get(name);
        if (value != null) {
            if (value instanceof Instant) {
                return (Instant) value;
            }
            String s = value.toString();
            return Instant.parse(s);
        }
        return null;
    }

    default Long getLong(String name) {
        Object value = get(name);
        if (value != null) {
            if (value instanceof Long) {
                return (Long) value;
            }
            String s = value.toString();
            return Long.parseLong(s);
        }
        return null;
    }


    default Options with(String name, Object value) {
        return new HierarchicalOptions(this).with(name, value);
    }

    default <T> Options with(Option<T> option, T value) {
        return with(option.getName(), value);
    }

    default Options without(String name) {
        return new HierarchicalOptions(this).without(name);
    }

    default <T> Options without(Option<T> option) {
        return without(option.getName());
    }

    static Options of(String optionName, Object value) {
        return new HierarchicalOptions(Collections.singletonMap(optionName, value));
    }

    static Options of(Map<String, Object> map) {
        return new HierarchicalOptions(map);
    }

    static Options merge(Options baseOptions, Options overrides) {
        if (baseOptions == null) {
            return overrides;
        }
        if (overrides == null) {
            return baseOptions;
        }

        return new Options() {
            @Override
            public <T> T get(String name) {
                T value = overrides.get(name);
                if (value == null) {
                    return baseOptions.get(name);
                }
                return value;
            }
        };
    }

    static Options merge(Options baseOptions, Option<?>... options) {
        if (options.length == 0) {
            return baseOptions;
        }

        Options result = baseOptions;
        for (Option<?> option : options) {
            result = result.with(option.getName(), option.getValue());
        }
        return result;
    }
}
