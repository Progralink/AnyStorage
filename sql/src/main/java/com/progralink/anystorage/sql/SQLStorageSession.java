package com.progralink.anystorage.sql;

import com.progralink.anystorage.api.AbstractStorageSession;
import com.progralink.anystorage.api.options.Option;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.WriteOption;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLStorageSession extends AbstractStorageSession {
    static final String TABLE_NAME = "storage";
    static final String COLUMN_PATH = "path";
    static final String COLUMN_PARENT = "parent";
    static final String COLUMN_SIZE = "size";
    static final String COLUMN_DATA = "data";

    private Connection connection;

    public SQLStorageSession(String name, Options options, Connection connection) throws IOException {
        super(name, options);
        this.connection = connection;
        this.rootResource = new SQLStorageResource(this);

        executeSQL("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" + COLUMN_PATH + " VARCHAR(32767) PRIMARY KEY, " + COLUMN_PARENT + " VARCHAR(32767), " + COLUMN_SIZE + " LONG, " + COLUMN_DATA + " BLOB)");
        executeSQL("CREATE INDEX IF NOT EXISTS " + COLUMN_PARENT + "_idx ON " + TABLE_NAME + "(" + COLUMN_PARENT + ")");
    }

    private void executeSQL(String sql) throws IOException {
        try {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean isDirectoryless() {
        return true;
    }

    @Override
    public boolean isSupported(Option<?> option) {
        return option == WriteOption.ATOMIC || option == WriteOption.CREATE_NEW || option.getName().equals(WriteOption.Name.CONTENT_LENGTH);
    }
}
