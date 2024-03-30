package com.progralink.anystorage.sql;

import com.progralink.anystorage.api.AbstractStorageResource;
import com.progralink.anystorage.api.StorageResource;
import com.progralink.anystorage.api.exceptions.AlreadyExistsException;
import com.progralink.anystorage.api.exceptions.NotEmptyDirectoryException;
import com.progralink.anystorage.api.exceptions.NotFoundException;
import com.progralink.anystorage.api.options.DeleteOption;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.ReadOption;
import com.progralink.anystorage.api.options.WriteOption;
import com.progralink.jinout.streams.IOStreams;
import com.progralink.jinout.streams.input.LengthAwareInputStream;
import com.progralink.jinout.streams.input.PositionAwareInputStream;
import com.progralink.jinout.streams.output.PositionAwareOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;

import static com.progralink.anystorage.sql.SQLStorageSession.*;

public class SQLStorageResource extends AbstractStorageResource {

    private String name;

    SQLStorageResource(SQLStorageSession session) {
        super(session);
        this.name = "";
    }

    SQLStorageResource(SQLStorageResource parent, String name) {
        super(parent);
        this.name = name;
    }

    @Override
    protected InputStream openInputStream(Options options) throws Exception {
        try (PreparedStatement stmt = prepareSqlStatement("SELECT "+COLUMN_SIZE+","+COLUMN_DATA+" FROM "+TABLE_NAME+" WHERE "+COLUMN_PATH+"=?")) {
            stmt.setString(1, getLocalPath());
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                long size = rs.getLong(COLUMN_SIZE);
                boolean noSize = rs.wasNull();
                if (noSize) {
                    Blob blob = rs.getBlob(COLUMN_DATA);
                    return blob.getBinaryStream();
                } else {
                    Blob blob = rs.getBlob(COLUMN_DATA);
                    return new LengthAwareInputStream(blob.getBinaryStream(), size);
                }
            }
            throw new NotFoundException();
        } catch (SQLException e) {
            throw translateException(e);
        }
    }

    @Override
    protected OutputStream openOutputStream(Options options) throws Exception {
        Path tempFile = Files.createTempFile("storage-sql-blob", ".tmp");
        return new PositionAwareOutputStream(Files.newOutputStream(tempFile)) {
            @Override
            public void close() throws IOException {
                super.close();

                long length = getPosition();
                try {
                    try (InputStream inputStream = Files.newInputStream(tempFile, StandardOpenOption.DELETE_ON_CLOSE)) {
                        writeStream(new LengthAwareInputStream(inputStream, length), options);
                    }
                } catch (SQLException e) {
                    throw translateException(e);
                } finally {
                    Files.deleteIfExists(tempFile);
                }
            }
        };
    }

    @Override
    protected long writeStream(InputStream source, Options options) throws SQLException, IOException {
        String sql;
        boolean createNew = WriteOption.CREATE_NEW.isEnabled(options);
        boolean exists = exists();
        if (createNew && exists) {
            throw new AlreadyExistsException();
        }

        Long length = options.getLong(WriteOption.Name.CONTENT_LENGTH);
        if (length == null) {
            length = IOStreams.getRemainingByteLength(source);
        }

        //TODO: possibility of concurrent insertions - need to be handled, but be aware that source stream will be consumed after first SQL execution
        //unfortunately not every SQL database supports UPSERT clause

        boolean updateMode = false;
        if (createNew || !exists) {
            sql = "INSERT INTO "+TABLE_NAME+" ("+COLUMN_PATH+","+COLUMN_PARENT+","+COLUMN_SIZE+","+COLUMN_DATA+") VALUES (?,?,?,?)";
        } else {
            sql = "UPDATE "+TABLE_NAME+" SET "+COLUMN_PATH+"=?,"+COLUMN_PARENT+"=?,"+COLUMN_SIZE+"=?,"+COLUMN_DATA+"=? WHERE "+COLUMN_PATH+"=?";
            updateMode = true;
        }

        try (PreparedStatement stmt = prepareSqlStatement(sql)) {
            stmt.setString(1, getLocalPath());
            stmt.setString(2, getParentLocalPath());
            if (length != null) {
                stmt.setLong(3, length);
                stmt.setBlob(4, source, length);
            } else {
                source = new PositionAwareInputStream(source);
                stmt.setNull(3, Types.BIGINT);
                stmt.setBlob(4, source);
            }
            if (updateMode) {
                stmt.setString(5, getLocalPath());
            }

            try {
                boolean success = stmt.executeUpdate() > 0;
                if (!success) {
                    throw new IOException("Unable to update");
                }
            } catch (SQLException e) {
                throw translateException(e);
            }
        }

        if (length == null) {
            return ((PositionAwareInputStream)source).getPosition();
        }
        return length;
    }

    @Override
    public boolean isFile() throws IOException {
        return exists();
    }

    @Override
    public boolean exists() throws IOException {
        try (PreparedStatement stmt = prepareSqlStatement("SELECT "+COLUMN_PARENT+" FROM "+TABLE_NAME+" WHERE "+COLUMN_PATH+"=?")) {
            stmt.setString(1, getLocalPath());
            ResultSet rs = stmt.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            throw translateException(e);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getSize(ReadOption<?>... options) throws IOException {
        try (PreparedStatement stmt = prepareSqlStatement("SELECT "+COLUMN_SIZE+" FROM "+TABLE_NAME+" WHERE "+COLUMN_PATH+" = ?")) {
            stmt.setString(1, getLocalPath());
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                long length = rs.getLong(COLUMN_SIZE);
                if (rs.wasNull()) {
                    try (InputStream inputStream = openRead(options)) {
                        return IOStreams.consume(inputStream);
                    }
                }
                return length;
            }
        } catch (SQLException e) {
            throw translateException(e);
        }
        throw new NotFoundException();
    }

    @Override
    public Collection<String> childrenNames() throws IOException {
        try (PreparedStatement stmt = getSession().getConnection().prepareStatement("SELECT "+COLUMN_PATH+" FROM "+TABLE_NAME+" WHERE "+COLUMN_PARENT+"=?")) {
            Collection<String> names = new LinkedList<>();
            stmt.setString(1, getLocalPath());
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String childPath = rs.getString(COLUMN_PATH);
                String childName = childPath.substring(childPath.lastIndexOf('/') + 1);
                names.add(childName);
            }

            if (names.isEmpty()) {
                //paths in-between (virtual directories) were not reflected
                //TODO: inefficient, better refactor it
                names = new LinkedHashSet<>();
                String thisPath = getLocalPath();
                rs = getSession().getConnection().createStatement().executeQuery("SELECT DISTINCT "+COLUMN_PARENT+" FROM "+TABLE_NAME);
                while (rs.next()) {
                    String thatPath = rs.getString(COLUMN_PARENT);
                    if (thatPath.startsWith(thisPath + "/")) {
                        String childName = thatPath.substring(thisPath.length() + 1);
                        int i = childName.indexOf('/');
                        if (i > -1) {
                            childName = childName.substring(0, i);
                        }
                        names.add(childName);
                    }
                }
            }

            return names;
        } catch (SQLException e) {
            throw translateException(e);
        }
    }

    @Override
    public StorageResource child(String name) {
        return new SQLStorageResource(this, name);
    }

    @Override
    public boolean delete(DeleteOption<?>... options) throws IOException {
        if (hasChildren()) {
            throw new NotEmptyDirectoryException();
        }

        try (PreparedStatement stmt = prepareSqlStatement("DELETE FROM "+TABLE_NAME+" WHERE "+COLUMN_PATH+"=?")) {
            stmt.setString(1, getLocalPath());
            return stmt.executeUpdate() > 0;
        } catch (SQLException e) {
            throw translateException(e);
        }
    }

    protected PreparedStatement prepareSqlStatement(String sql) throws SQLException {
        return getSession().getConnection().prepareStatement(sql);
    }

    @Override
    protected IOException translateException(Exception e) {
        if (e instanceof SQLException) {
            String message = e.getMessage();
            if (message.contains("Unique index or primary key violation")) {
                return new AlreadyExistsException();
            }
        }

        return super.translateException(e);
    }

    @Override
    public SQLStorageSession getSession() {
        return (SQLStorageSession) super.getSession();
    }
}
