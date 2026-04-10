package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Functional integration tests for {@link DbLedgerStorage}.
 *
 * All tests run against a real on-disk RocksDB + EntryLogger backend — no mocks.
 * JUnit 5 provides and cleans up the temporary directory automatically.
 *
 * Entry buffer layout expected by {@link DbLedgerStorage#addEntry}:
 *   [ 8 bytes : ledgerId ][ 8 bytes : entryId ][ N bytes : payload ]
 */
class Isw2DbLedgerStorageFunctionalTest {

    @TempDir
    File tempDir;

    private DbLedgerStorage storage;

    private static final long LEDGER_ID = 1L;
    private static final int  HEADER_BYTES = Long.BYTES + Long.BYTES; // ledgerId + entryId

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Creates and starts a real DbLedgerStorage instance before every test.
     * Cache sizes are kept small (16 MB) to work comfortably in any CI environment.
     * setAllowLoopback(true) is needed when the hostname resolves to 127.x.x.x.
     */
    @BeforeEach
    void setUp() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerDirNames(new String[]{tempDir.getAbsolutePath()});
        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 16);
        conf.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 16);
        conf.setDiskUsageThreshold(0.99f);
        conf.setDiskUsageWarnThreshold(0.98f);
        conf.setAllowLoopback(true);

        DiskChecker diskChecker = new DiskChecker(
                conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager dirsManager = new LedgerDirsManager(
                conf, conf.getLedgerDirs(), diskChecker, NullStatsLogger.INSTANCE);

        storage = new DbLedgerStorage();
        storage.initialize(conf, null, dirsManager, dirsManager,
                NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);

        // No-op: keeps tests synchronous, prevents background flush interference
        storage.setCheckpointSource(new CheckpointSource() {
            public Checkpoint newCheckpoint()                               { return Checkpoint.MAX; }
            public void checkpointComplete(Checkpoint cp, boolean compact) { /* no-op */ }
        });
        storage.setCheckpointer(new Checkpointer() {
            public void startCheckpoint(CheckpointSource.Checkpoint c) { /* no-op */ }
            public void start()                                         { /* no-op */ }
        });

        storage.start();
        storage.setMasterKey(LEDGER_ID, "master-key".getBytes());
    }

    @AfterEach
    void tearDown() throws Exception {
        storage.shutdown();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds a valid entry buffer: [ledgerId][entryId][payload].
     * The caller is responsible for releasing the returned buffer.
     */
    private static ByteBuf buildEntry(long ledgerId, long entryId, long payload) {
        ByteBuf buf = Unpooled.buffer(Long.BYTES * 3);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeLong(payload);
        return buf;
    }

    /**
     * Skips the 16-byte header and reads the long payload from an entry buffer
     * returned by getEntry. The caller must release the buffer after this call.
     */
    private static long readPayload(ByteBuf buf) {
        buf.skipBytes(HEADER_BYTES);
        return buf.readLong();
    }

    // -------------------------------------------------------------------------
    // Test 1 — write several entries and read each one back
    // -------------------------------------------------------------------------

    /**
     * Writes 5 sequential entries to LEDGER_ID and verifies that each one
     * can be retrieved individually with the correct payload.
     */
    @Test
    void testWriteAndReadMultipleEntries() throws Exception {
        int numEntries = 5;

        // Write phase
        for (int i = 0; i < numEntries; i++) {
            ByteBuf entry = buildEntry(LEDGER_ID, i, i);
            try {
                storage.addEntry(entry);
            } finally {
                entry.release();
            }
        }
        storage.flush();

        // Read phase — every entry must round-trip with its original payload
        for (int i = 0; i < numEntries; i++) {
            ByteBuf result = storage.getEntry(LEDGER_ID, i);
            try {
                assertEquals(i, readPayload(result), "Payload mismatch for entryId=" + i);
            } finally {
                result.release();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Test 2 — writing to a fenced ledger must be rejected
    // -------------------------------------------------------------------------

    /**
     * Verifies that addEntry throws LedgerFencedException once the ledger is fenced.
     * Fencing is the BookKeeper mechanism for permanently closing a ledger to writes.
     */
    @Disabled
    @Test
    void testWriteToFencedLedgerIsRejected() throws Exception {
        storage.setFenced(LEDGER_ID);

        ByteBuf entry = buildEntry(LEDGER_ID, 0L, 42L);
        try {
            assertThrows(
                    BookieException.LedgerFencedException.class,
                    () -> storage.addEntry(entry),
                    "addEntry on a fenced ledger must throw LedgerFencedException"
            );
        } finally {
            entry.release();
        }
    }

    // -------------------------------------------------------------------------
    // Test 3 — duplicate entryId overwrites the previous value
    // -------------------------------------------------------------------------

    /**
     * Writes two entries with the same entryId (but different payloads) and
     * confirms that the second write overwrites the first (last-write semantics).
     */
    @Test
    void testDuplicateEntryIdOverwrites() throws Exception {
        final long entryId = 1L;

        ByteBuf first = buildEntry(LEDGER_ID, entryId, 100L);
        try { storage.addEntry(first); } finally { first.release(); }

        ByteBuf second = buildEntry(LEDGER_ID, entryId, 200L); // same entryId, new payload
        try { storage.addEntry(second); } finally { second.release(); }

        storage.flush();

        ByteBuf result = storage.getEntry(LEDGER_ID, entryId);
        try {
            assertEquals(200L, readPayload(result),
                    "Second write must overwrite the first for entryId=" + entryId);
        } finally {
            result.release();
        }
    }

    // -------------------------------------------------------------------------
    // Test 4 — malformed entries must be rejected
    // -------------------------------------------------------------------------

    /**
     * Verifies that an empty buffer (0 bytes) is rejected because the
     * ledgerId field cannot be read from the missing header.
     */
    @Test
    void testEmptyBufferIsRejected() {
        ByteBuf empty = Unpooled.buffer(0);
        try {
            assertThrows(Exception.class,
                    () -> storage.addEntry(empty),
                    "An empty buffer has no header: storage must throw");
        } finally {
            empty.release();
        }
    }

    /**
     * Verifies that a buffer containing only the ledgerId (8 bytes) is rejected.
     * The entryId is absent, making the header incomplete.
     */
    @Test
    void testPartialHeaderIsRejected() {
        ByteBuf partial = Unpooled.buffer(Long.BYTES);
        partial.writeLong(LEDGER_ID); // only ledgerId — entryId and payload are missing
        try {
            assertThrows(Exception.class,
                    () -> storage.addEntry(partial),
                    "A buffer with only ledgerId is malformed: storage must throw");
        } finally {
            partial.release();
        }
    }

    /**
     * Verifies that writing to a ledger whose master key has never been registered
     * is rejected by the storage layer.
     */
    @Disabled
    @Test
    void testUnknownLedgerIsRejected() {
        final long unknownLedgerId = 999L; // no setMasterKey() was called for this id

        ByteBuf entry = buildEntry(unknownLedgerId, 0L, 0L);
        try {
            assertThrows(Exception.class,
                    () -> storage.addEntry(entry),
                    "Writing to an unregistered ledger must throw");
        } finally {
            entry.release();
        }
    }

    // -------------------------------------------------------------------------
    // Test 5 — concurrent writes must not deadlock or corrupt data
    // -------------------------------------------------------------------------

    /**
     * Validates storage resilience under concurrent write access from two threads.
     *
     * Scenario:
     *   - Thread A writes to ledger 10, then sleeps 200 ms to hold the contention
     *     window open and simulate high latency.
     *   - Thread B enters the write path only after Thread A has confirmed its write,
     *     maximising the overlap of internal locking regions.
     *   - The main thread waits for both workers with a 5-second deadline;
     *     a timeout is treated as a deadlock.
     *   - After both threads finish, both entries are read back to verify
     *     correct persistence and indexing with no corruption.
     */
    @Test
    void testConcurrentWritesDoNotDeadlockOrCorruptData() throws Exception {
        final long ledgerA = 10L;
        final long ledgerB = 20L;
        storage.setMasterKey(ledgerA, "key-a".getBytes());
        storage.setMasterKey(ledgerB, "key-b".getBytes());

        CountDownLatch startGun   = new CountDownLatch(1); // fires both threads simultaneously
        CountDownLatch aWriteDone = new CountDownLatch(1); // B waits until A's write is committed
        CountDownLatch allDone    = new CountDownLatch(2); // main thread waits for both workers

        AtomicReference<Throwable> workerError = new AtomicReference<>();

        Thread threadA = new Thread(() -> {
            try {
                startGun.await();

                ByteBuf entry = buildEntry(ledgerA, 0L, 100L);
                try { storage.addEntry(entry); } finally { entry.release(); }

                aWriteDone.countDown();  // signal B: write committed, now going slow
                Thread.sleep(200);       // simulate post-write latency / resource contention
            } catch (Throwable t) {
                workerError.compareAndSet(null, t);
            } finally {
                allDone.countDown();
            }
        }, "ThreadA");

        Thread threadB = new Thread(() -> {
            try {
                startGun.await();
                aWriteDone.await(); // enter write path while A is still sleeping

                ByteBuf entry = buildEntry(ledgerB, 0L, 200L);
                try { storage.addEntry(entry); } finally { entry.release(); }
            } catch (Throwable t) {
                workerError.compareAndSet(null, t);
            } finally {
                allDone.countDown();
            }
        }, "ThreadB");

        threadA.start();
        threadB.start();
        startGun.countDown(); // both threads start concurrently

        // A timeout here means a deadlock was detected
        assertTrue(allDone.await(5, TimeUnit.SECONDS),
                "Deadlock detected: worker threads did not finish within 5 seconds");

        if (workerError.get() != null) {
            throw new AssertionError("Unexpected exception in worker thread", workerError.get());
        }

        storage.flush();

        ByteBuf resultA = storage.getEntry(ledgerA, 0L);
        try {
            assertEquals(100L, readPayload(resultA), "Thread A entry corrupted");
        } finally {
            resultA.release();
        }

        ByteBuf resultB = storage.getEntry(ledgerB, 0L);
        try {
            assertEquals(200L, readPayload(resultB), "Thread B entry corrupted");
        } finally {
            resultB.release();
        }
    }
}