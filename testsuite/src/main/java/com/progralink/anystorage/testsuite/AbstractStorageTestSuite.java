package com.progralink.anystorage.testsuite;

import com.progralink.anystorage.api.StorageConnector;
import com.progralink.anystorage.api.StorageResource;
import com.progralink.anystorage.api.StorageSession;
import com.progralink.anystorage.api.exceptions.AlreadyExistsException;
import com.progralink.anystorage.api.options.DeleteOption;
import com.progralink.anystorage.api.options.Options;
import com.progralink.anystorage.api.options.ReadOption;
import com.progralink.anystorage.api.options.WriteOption;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractStorageTestSuite {
    protected StorageConnector connector;
    protected StorageSession session;
    protected static String connectionString = null;

    @BeforeEach
    void setUp() throws IOException {
        session = startSession();
        if (session != null) {
            //session.clear();
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        if (session != null) {
            cleanup(session);
        }
    }

    protected abstract String getDefaultConnectionString() throws IOException;

    protected synchronized String getConnectionString() throws IOException {
        String optionName = this.getClass().getName() + ".URL";
        if (connectionString == null) {
            connectionString = getConnectionString(optionName);
            if (connectionString == null || connectionString.isEmpty()) {
                connectionString = getDefaultConnectionString();
                if (connectionString == null || connectionString.isEmpty()) {
                    System.err.println(optionName + " not specified!");
                    connectionString = "";
                }
            }
        }
        return connectionString;
    }

    protected boolean isTestSkipped() throws IOException {
        String cstr = getConnectionString();
        return cstr == null || cstr.isEmpty();
    }

    protected String getConnectionString(String optionName) {
        Options options = Options.DEFAULTS;
        return options.getString(optionName);
    }

    protected abstract StorageConnector provideConnector();

    protected synchronized StorageSession startSession() throws IOException {
        if (connector == null) {
            connector = provideConnector();
        }
        return connector.connect(getConnectionString());
    }
    protected void cleanup(StorageSession session) throws IOException {
        session.close();
    }

    @Test
    @DisabledIf("isTestSkipped")
    void testStandardOperations() throws IOException {
        session.getRootResource().child("hello").write("Hello!".getBytes(UTF_8), WriteOption.ATOMIC);
        session.getRootResource().child("world").write("World 1!".getBytes(UTF_8), WriteOption.ATOMIC);
        assertThrows(AlreadyExistsException.class, () ->
                session.getResource("/hello").write("Hello?".getBytes(UTF_8), WriteOption.CREATE_NEW, WriteOption.ATOMIC)
        );
        session.getResource("/world").write(new ByteArrayInputStream("World 2!".getBytes(UTF_8)), WriteOption.ATOMIC);
        assertEquals("Hello!", new String(session.getResource("/hello").readFully(ReadOption.OLDEST_VERSION)));
        assertEquals("World 2!", new String(session.getResource("/world").readFully()));

        session.getResource("/world").delete();
        assertTrue(session.getResource("/hello").exists());
        assertFalse(session.getResource("/world").exists());
        session.getRootResource().deleteDeep(true);
        assertFalse(session.getResource("/world").exists());
    }

    @Test
    @DisabledIf("isTestSkipped")
    void testDeepStructure() throws IOException {
        session.getResource("/authdb/users/john/name.txt").write("John Doe".getBytes(UTF_8));
        session.getResource("/authdb/users/tom/name.txt").write("Tom Doe".getBytes(UTF_8));

        assertEquals(1, session.getResource("/authdb/users/john").childrenNames().size());
        assertEquals("name.txt", session.getResource("/authdb/users/john").childrenNames().iterator().next());
        assertEquals(1, session.getResource("/authdb/users/tom").childrenNames().size());
        assertEquals("name.txt", session.getResource("/authdb/users/tom").childrenNames().iterator().next());

        List<String> names = new ArrayList<>(session.getResource("/authdb/users").childrenNames());
        Collections.sort(names);
        assertEquals(2, names.size());
        assertEquals("john", names.get(0));
        assertEquals("tom", names.get(1));

        assertTrue(session.getResource("/authdb/users").deleteDeep(false));
        if (!session.isDirectoryless()) {
            assertTrue(session.getResource("/authdb/users").exists());
            assertEquals(0, (int) session.getResource("/authdb/users").children().count());
            assertTrue(session.getResource("/authdb/users").delete());
            assertFalse(session.getResource("/authdb/users").exists());
        }
    }

    @Test
    @DisabledIf("isTestSkipped")
    void testWriteOnceReadManyPreventOverwriteInMultipleThreads() throws IOException, InterruptedException {
        String resourcePath = "/target3";
        session.getResource(resourcePath).delete(DeleteOption.REMOVE_HISTORY);

        AtomicReference<String> winnerContent = new AtomicReference<>();
        int threadCount = 10;
        List<Thread> threads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            threads.add(createThreadForImmutableTest(resourcePath, i, winnerContent));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        try (StorageSession localSession = startSession()) {
            StorageResource resource = localSession.getResource(resourcePath);
            String storedContent = new String(resource.readFully(ReadOption.OLDEST_VERSION));
            assertNotNull(storedContent);
            assertNotNull(winnerContent.get());
            assertEquals(storedContent, winnerContent.get());
        }
    }

    private Thread createThreadForImmutableTest(String resourcePath, int threadNumber, AtomicReference<String> winnerContent) {
        return new Thread(() -> {
            String content = "[" + threadNumber + "]:" + Thread.currentThread().toString();
            try (StorageSession localSession = startSession()) {
                StorageResource resource = localSession.getResource(resourcePath);
                resource.write(content.getBytes(UTF_8), WriteOption.ATOMIC, WriteOption.CREATE_NEW);
                winnerContent.set(content);
            } catch (AlreadyExistsException ignore) {
                //another thread was first
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
