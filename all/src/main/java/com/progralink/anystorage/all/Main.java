package com.progralink.anystorage.all;

import com.progralink.anystorage.api.StorageConnector;
import com.progralink.anystorage.api.StorageConnectors;

public class Main {
    public static void main(String[] args) {
        for (StorageConnector connector : new StorageConnectors()) {
            System.out.println(connector.getTypeLabel());
        }
    }
}