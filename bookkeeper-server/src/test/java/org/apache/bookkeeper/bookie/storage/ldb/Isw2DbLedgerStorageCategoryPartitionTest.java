package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.stream.Stream;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.io.TempDir;

/**
 * Category-Partition tests for {@link DbLedgerStorage#getEntry(long, long)}.
 *
 * The operation under test is getEntry(ledgerId, entryId).
 * A successful retrieval requires the storage to locate an entry that
 * references BOTH identifiers simultaneously.
 *
 * Two ledgers are pre-populated in setUp():
 *   - Ledger 0L  → entries 0-5  (the "known" ledger)
 *   - Ledger 1L  → entries 1-10 (used to generate a mismatched entryId)
 *
 * Category partition table (from specification):
 * ┌─────────────────────────────────┬──────────────────────────────────────────┬─────────┐
 * │ LedgerId                        │ EntryId                                  │ Output  │
 * ├─────────────────────────────────┼──────────────────────────────────────────┼─────────┤
 * │ Valid in storage        [0L]    │ Valid in storage, matching ledger   [0L] │ Success │
 * │ Valid in storage        [0L]    │ Valid in storage,NOT matching ledger[10L]│ Failure │
 * │ Valid but not in storage[3L]    │ Valid in storage                    [0L] │ Failure │
 * │ Malformed               [-1L]   │ Valid in storage                    [0L] │ Failure │
 * │ Valid in storage        [0L]    │ Malformed                          [-1L] │ Failure │
 * └─────────────────────────────────┴──────────────────────────────────────────┴─────────┘
 *
 * All tests run against a real on-disk RocksDB + EntryLogger backend — no mocks.
 *
 * Entry buffer layout expected by addEntry:
 *   [ 8 bytes : ledgerId ][ 8 bytes : entryId ][ 8 bytes : payload ]
 */
@DisplayName("DbLedgerStorage — Category Partition: getEntry(ledgerId, entryId)")
public class Isw2DbLedgerStorageCategoryPartitionTest {

    @TempDir
    File tempDir;

    private DbLedgerStorage storage;

    // Ledger IDs used across tests
    private static final long LEDGER_IN_STORAGE     = 0L;  // populated in setUp
    private static final long LEDGER_OTHER          = 1L;  // populated in setUp (different range)
    private static final long LEDGER_NOT_IN_STORAGE = 3L;  // never registered
    private static final long LEDGER_MALFORMED      = -1L; // invalid / negative id

    // Entry IDs used across tests
    private static final long ENTRY_MATCHING        = 0L;  // exists under LEDGER_IN_STORAGE
    private static final long ENTRY_FROM_OTHER      = 10L; // exists only under LEDGER_OTHER
    private static final long ENTRY_MALFORMED       = -1L; // invalid / negative id

    private static final int HEADER_BYTES = Long.BYTES + Long.BYTES;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Boots a real DbLedgerStorage and pre-populates two ledgers so that every
     * parameterised case has a consistent, known state to query against.
     *
     * Ledger 0L → entries 0-5   (entryId range that belongs to this ledger)
     * Ledger 1L → entries 1-10  (provides an entryId that does NOT belong to ledger 0L)
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

        storage.setCheckpointSource(new CheckpointSource() {
            public Checkpoint newCheckpoint()                               { return Checkpoint.MAX; }
            public void checkpointComplete(Checkpoint cp, boolean compact) { /* no-op */ }
        });
        storage.setCheckpointer(new Checkpointer() {
            public void startCheckpoint(CheckpointSource.Checkpoint c) { /* no-op */ }
            public void start()                                         { /* no-op */ }
        });

        storage.start();

        // Populate ledger 0L with entries 0-5
        storage.setMasterKey(LEDGER_IN_STORAGE, "key-0".getBytes());
        for (long i = 0; i <= 5; i++) {
            ByteBuf entry = buildEntry(LEDGER_IN_STORAGE, i, i * 10);
            try { storage.addEntry(entry); } finally { entry.release(); }
        }

        // Populate ledger 1L with entries 1-10 (entryId=10 is only reachable via this ledger)
        storage.setMasterKey(LEDGER_OTHER, "key-1".getBytes());
        for (long i = 1; i <= 10; i++) {
            ByteBuf entry = buildEntry(LEDGER_OTHER, i, i * 10);
            try { storage.addEntry(entry); } finally { entry.release(); }
        }

        storage.flush();
    }

    @AfterEach
    void tearDown() throws Exception {
        storage.shutdown();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ByteBuf buildEntry(long ledgerId, long entryId, long payload) {
        ByteBuf buf = Unpooled.buffer(Long.BYTES * 3);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeLong(payload);
        return buf;
    }

    // -------------------------------------------------------------------------
    // Category-Partition parameterised test
    // -------------------------------------------------------------------------

    /**
     * Provides one {@link Arguments} row per cell in the category-partition table.
     *
     * Each row contains:
     *   1. testName   – human-readable label shown in the test report
     *   2. ledgerId   – ledger identifier passed to getEntry
     *   3. entryId    – entry identifier passed to getEntry
     *   4. expectSuccess – true  → getEntry must return a non-null ByteBuf
     *                      false → getEntry must throw an exception
     */
    static Stream<Arguments> categoryPartitionCases() {
        return Stream.of(
                // ── Row 1 ────────────────────────────────────────────────────────────────
                // LedgerId: valid and in storage [0L]
                // EntryId:  valid and in storage, MATCHES the ledger [0L]
                // Expected: Success — the entry is found
                Arguments.of(
                        "LedgerId=valid[0L], EntryId=valid+matching[0L] → Success",
                        LEDGER_IN_STORAGE,  // 0L
                        ENTRY_MATCHING,     // 0L
                        true
                ),

                // ── Row 2 ────────────────────────────────────────────────────────────────
                // LedgerId: valid and in storage [0L]
                // EntryId:  valid in storage but belongs to a DIFFERENT ledger [10L]
                // Expected: Failure — entryId 10L does not exist under ledger 0L
                Arguments.of(
                        "LedgerId=valid[0L], EntryId=valid+mismatched[10L] → Failure",
                        LEDGER_IN_STORAGE,  // 0L
                        ENTRY_FROM_OTHER,   // 10L — exists only under ledger 1L
                        false
                ),

                // ── Row 3 ────────────────────────────────────────────────────────────────
                // LedgerId: valid format but NOT registered in storage [3L]
                // EntryId:  valid and in storage [0L]
                // Expected: Failure — ledger 3L has no master key and no entries
                Arguments.of(
                        "LedgerId=valid+notInStorage[3L], EntryId=valid[0L] → Failure",
                        LEDGER_NOT_IN_STORAGE,  // 3L — never registered
                        ENTRY_MATCHING,         // 0L
                        false
                ),

                // ── Row 4 ────────────────────────────────────────────────────────────────
                // LedgerId: malformed / invalid [-1L]
                // EntryId:  valid and in storage [0L]
                // Expected: Failure — a negative ledgerId is structurally invalid
                Arguments.of(
                        "LedgerId=malformed[-1L], EntryId=valid[0L] → Failure",
                        LEDGER_MALFORMED,   // -1L — invalid
                        ENTRY_MATCHING,     // 0L
                        false
                ),

                // ── Row 5 ────────────────────────────────────────────────────────────────
                // LedgerId: valid and in storage [0L]
                // EntryId:  malformed / invalid [-1L]
                // Expected: Failure — a negative entryId is structurally invalid
                Arguments.of(
                        "LedgerId=valid[0L], EntryId=malformed[-1L] → Failure",
                        LEDGER_IN_STORAGE,  // 0L
                        ENTRY_MALFORMED,    // -1L — invalid
                        false
                )
        );
    }

    /**
     * Executes every row of the category-partition table against
     * {@link DbLedgerStorage#getEntry(long, long)}.
     *
     * @param testName      label shown in the JUnit report
     * @param ledgerId      ledger identifier under test
     * @param entryId       entry identifier under test
     * @param expectSuccess whether the call should succeed (true) or throw (false)
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("categoryPartitionCases")
    @DisplayName("getEntry — category partition")
    void testGetEntryCategories(String testName, long ledgerId, long entryId,
                                boolean expectSuccess) throws Exception {
        if (expectSuccess) {
            // Happy path: getEntry must return a non-null, readable ByteBuf
            ByteBuf result = storage.getEntry(ledgerId, entryId);
            try {
                assertNotNull(result,
                        "getEntry must return a non-null ByteBuf for: " + testName);
            } finally {
                result.release();
            }
        } else {
            // Failure path: getEntry must throw any exception
            assertThrows(Exception.class,
                    () -> storage.getEntry(ledgerId, entryId),
                    "getEntry must throw for: " + testName);
        }
    }
}