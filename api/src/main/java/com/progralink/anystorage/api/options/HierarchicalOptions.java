package com.progralink.anystorage.api.options;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class HierarchicalOptions implements Options {
    public static final HierarchicalOptions DEFAULTS = new HierarchicalOptions(new HierarchicalOptions(System.getenv()), System.getProperties());

    private Options parent;
    private Map<String, Object> map;

    HierarchicalOptions() { }

    public HierarchicalOptions(Map<String, ?> map) {
        this(null, map);
    }

    public HierarchicalOptions(Properties properties) {
        this(null, properties);
    }

    public HierarchicalOptions(Options parent) {
        this(parent, Collections.emptyMap());
    }

    @SuppressWarnings("unchecked")
    public HierarchicalOptions(Options parent, Map<String, ?> map) {
        this.parent = parent;
        this.map = (Map<String, Object>)map;
    }

    @SuppressWarnings("unchecked")
    public HierarchicalOptions(Options parent, Properties properties) {
        this.parent = parent;
        map = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            map.put(entry.getKey().toString(), entry.getValue());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String name) {
        if (!map.containsKey(name)) {
            if (parent != null) {
                return parent.get(name);
            }
            return null;
        }
        return (T)map.get(name);
    }

    @Override
    public HierarchicalOptions with(String name, Object value) {
        HierarchicalOptions nextOptions = new HierarchicalOptions();
        nextOptions.parent = this;
        nextOptions.map = Collections.singletonMap(name, value);
        return nextOptions;
    }

    @Override
    public HierarchicalOptions without(String name) {
        HierarchicalOptions nextOptions = new HierarchicalOptions();
        nextOptions.parent = this;
        nextOptions.map = Collections.singletonMap(name, null);
        return nextOptions;
    }
}
