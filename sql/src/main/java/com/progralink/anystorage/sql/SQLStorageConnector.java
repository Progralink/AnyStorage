package com.progralink.anystorage.sql;

import com.progralink.anystorage.api.AbstractStorageConnector;
import com.progralink.anystorage.api.StorageSession;
import com.progralink.anystorage.api.options.Options;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SQLStorageConnector extends AbstractStorageConnector {
    @Override
    public String getTypeLabel() {
        return "SQL";
    }

    @Override
    public boolean canHandle(String connectionString) {
        return connectionString.startsWith("jdbc:");
    }

    @Override
    public StorageSession connect(String name, String connectionString, Options options) throws IOException {
        try {
            Connection connection = DriverManager.getConnection(connectionString);
            return new SQLStorageSession(name, options, connection);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
