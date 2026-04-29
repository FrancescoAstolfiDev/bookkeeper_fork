package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Category-Partition tests for {@link DbLedgerStorage#getEntry(long, long)}.
 *
 * <p>The operation under test is {@code getEntry(ledgerId, entryId)}.
 * A successful retrieval requires the storage to locate an entry that
 * references BOTH identifiers simultaneously. If either identifier does not
 * match an existing entry, the operation must fail with an exception.
 *
 * <p>The storage is initialised using the BVA#1 baseline configuration
 * (write=16MB, read=16MB, new temp directory, 1 ledger dir == 1 index dir),
 * verified empirically as a stable, side-effect-free foundation.
 * Two ledgers are pre-populated in {@code setUp()}:
 * <ul>
 *   <li>Ledger {@code 0L} — entries 0 through 5 (primary ledger under test)</li>
 *   <li>Ledger {@code 1L} — entries 1 through 10 (provides the mismatched entryId scenario)</li>
 * </ul>
 *
 * <p><b>BookKeeper convention — entryId {@code -1L}:</b><br>
 * The sentinel value {@code -1L} ({@code BookieProtocol.LAST_ADD_CONFIRMED}) means
 * "retrieve the last confirmed entry of this ledger". It is NOT a malformed value —
 * the storage returns the last written entry for the given ledger.
 * Row 5 of the category partition therefore expects Success, not Failure.
 *
 * <p>All tests run against a real on-disk RocksDB + EntryLogger backend — no mocks.
 *
 * <p>Category partition table:
 * <pre>
 * +----+--------------------------------------+-----------------------------------------+---------+
 * |  # | LedgerId                             | EntryId                                 | Output  |
 * +----+--------------------------------------+-----------------------------------------+---------+
 * |  1 | Valid in storage            [0L]     | Valid with match on LedgerId     [0L]   | Success |
 * |  2 | Valid in storage            [0L]     | Valid but no match on LedgerId   [10L]  | Failure |
 * |  3 | Valid but not in storage    [3L]     | Valid in storage                 [0L]   | Failure |
 * |  4 | Malformed (negative)        [-1L]    | Valid in storage                 [0L]   | Failure |
 * |  5 | Valid in storage            [0L]     | Sentinel [-1L] - last entry             | Success |
 * |  6 | Valid in storage            [0L]     | Malformed                        [-2L]  | Failure |
 * +----+--------------------------------------+-----------------------------------------+---------+
 * </pre>
 */
@DisplayName("DbLedgerStorage — Category Partition: getEntry(ledgerId, entryId)")
public class Isw2DbLedgerStorageGetEntryBVATest {

    @TempDir
    File tempDir;

    private DbLedgerStorage storage;

    // ── Storage constants ─────────────────────────────────────────────────────

    /** Registered ledger with entries 0 through 5. */
    private static final long LEDGER_IN_STORAGE      = 0L;

    /** Registered ledger with entries 1 through 10 — provides the mismatched entryId scenario. */
    private static final long LEDGER_OTHER           = 1L;

    /** Well-formed ledger ID that was never registered — no master key, no entries. */
    private static final long LEDGER_NOT_IN_STORAGE  = 3L;

    /** Structurally invalid (negative) ledger ID — no defined semantics in BookKeeper. */
    private static final long LEDGER_MALFORMED       = -1L;

    /** EntryId that exists under {@code LEDGER_IN_STORAGE}. */
    private static final long ENTRY_MATCHING         = 0L;

    /** EntryId that exists only under {@code LEDGER_OTHER}, never written to {@code LEDGER_IN_STORAGE}. */
    private static final long ENTRY_FROM_OTHER       = 10L;

    /**
     * BookKeeper sentinel value ({@code BookieProtocol.LAST_ADD_CONFIRMED = -1L}).
     * Instructs the storage to return the last confirmed entry of the given ledger.
     * This is NOT a malformed value — it is a well-defined reserved identifier.
     */
    private static final long ENTRY_LAST_CONFIRMED   = -1L;

    /**
     * Truly malformed entryId ({@code -2L}).
     * Unlike {@code -1L}, this value has no defined semantics in BookKeeper —
     * the storage must reject it with an exception.
     */
    private static final long ENTRY_MALFORMED        = -2L;

    /** Last entryId written to {@code LEDGER_IN_STORAGE} during setUp. */
    private static final long LAST_ENTRY_IN_LEDGER_0 = 5L;

    // ── Test case enumeration ─────────────────────────────────────────────────

    /**
     * Enumerates every row of the category-partition table as a self-describing constant.
     *
     * <p>Each constant encodes:
     * <ul>
     *   <li>{@code label}           — human-readable description shown in the JUnit report</li>
     *   <li>{@code ledgerId}        — ledger identifier passed to {@code getEntry}</li>
     *   <li>{@code entryId}         — entry identifier passed to {@code getEntry}</li>
     *   <li>{@code expectSuccess}   — whether the call is expected to succeed</li>
     *   <li>{@code expectedEntryId} — on success, the entryId expected in the returned buffer
     *                                 header; {@code -99L} means skip the entryId check</li>
     * </ul>
     *
     * <p>Using an enum rather than inline {@link Arguments} keeps each case self-documenting
     * and makes the parameter source independent of construction order.
     */
    enum GetEntryCase {

        /**
         * Row 1 — LedgerId valid in storage [0L], EntryId valid with match [0L].
         * Both identifiers match an existing entry: the operation must succeed
         * and return the stored buffer.
         */
        VALID_LEDGER_MATCHING_ENTRY(
                "LedgerId=valid[0L], EntryId=valid+matching[0L] -> Success",
                LEDGER_IN_STORAGE, ENTRY_MATCHING,
                true, ENTRY_MATCHING),

        /**
         * Row 2 — LedgerId valid in storage [0L], EntryId valid in storage but
         * from a different ledger [10L].
         * EntryId 10L was written to ledger 1L, never to ledger 0L. The storage index
         * maps (ledgerId, entryId) pairs — a mismatch means no entry is found, so the
         * operation must fail.
         */
        VALID_LEDGER_MISMATCHED_ENTRY(
                "LedgerId=valid[0L], EntryId=valid+mismatched[10L] -> Failure",
                LEDGER_IN_STORAGE, ENTRY_FROM_OTHER,
                false, -99L),

        /**
         * Row 3 — LedgerId well-formed but not in storage [3L], EntryId valid [0L].
         * Ledger 3L has no master key and no entries — the storage has no record of it,
         * so the operation must fail.
         */
        VALID_LEDGER_NOT_IN_STORAGE(
                "LedgerId=valid+notInStorage[3L], EntryId=valid[0L] -> Failure",
                LEDGER_NOT_IN_STORAGE, ENTRY_MATCHING,
                false, -99L),

        /**
         * Row 4 — LedgerId malformed/negative [-1L], EntryId valid [0L].
         * A negative ledgerId is structurally invalid. Although -1L is a reserved
         * sentinel for entryId, it carries no special meaning for ledgerId —
         * the operation must fail.
         */
        MALFORMED_LEDGER_VALID_ENTRY(
                "LedgerId=malformed[-1L], EntryId=valid[0L] -> Failure",
                LEDGER_MALFORMED, ENTRY_MATCHING,
                false, -99L),

        /**
         * Row 5 — LedgerId valid in storage [0L], EntryId = LAST_ADD_CONFIRMED sentinel [-1L].
         * The sentinel value -1L instructs the storage to return the last confirmed entry
         * of the given ledger. The last entry written to ledger 0L during setUp has
         * entryId 5L, so the operation must succeed and return that entry.
         */
        VALID_LEDGER_LAST_CONFIRMED_SENTINEL(
                "LedgerId=valid[0L], EntryId=LAST_ADD_CONFIRMED[-1L] -> Success (last entry=5L)",
                LEDGER_IN_STORAGE, ENTRY_LAST_CONFIRMED,
                true, LAST_ENTRY_IN_LEDGER_0),

        /**
         * Row 6 — LedgerId valid in storage [0L], EntryId truly malformed [-2L].
         * Unlike -1L, the value -2L is not a BookKeeper sentinel and carries no defined
         * semantics. The storage must reject it with an exception.
         */
        VALID_LEDGER_MALFORMED_ENTRY(
                "LedgerId=valid[0L], EntryId=malformed[-2L] -> Failure",
                LEDGER_IN_STORAGE, ENTRY_MALFORMED,
                false, -99L);

        // ── Fields ────────────────────────────────────────────────────────────

        final String  label;
        final long    ledgerId;
        final long    entryId;
        final boolean expectSuccess;
        final long    expectedEntryId;

        GetEntryCase(String label, long ledgerId, long entryId,
                     boolean expectSuccess, long expectedEntryId) {
            this.label           = label;
            this.ledgerId        = ledgerId;
            this.entryId         = entryId;
            this.expectSuccess   = expectSuccess;
            this.expectedEntryId = expectedEntryId;
        }

        /** Converts this enum constant into a JUnit {@link Arguments} row. */
        Arguments toArguments() {
            return Arguments.of(label, ledgerId, entryId, expectSuccess, expectedEntryId);
        }
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /**
     * Boots a real {@link DbLedgerStorage} using the BVA#1 baseline configuration
     * and pre-populates two ledgers so that every parameterised case has a
     * consistent, known state to query against:
     * <ul>
     *   <li>Ledger 0L — entries 0 through 5, payload = entryId * 10</li>
     *   <li>Ledger 1L — entries 1 through 10, payload = entryId * 10</li>
     * </ul>
     */
    @BeforeEach
    void setUp() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerDirNames(new String[]{ tempDir.getAbsolutePath() });
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

        // No-op checkpoint hooks: keep tests synchronous, prevent background flush interference
        storage.setCheckpointSource(new CheckpointSource() {
            public Checkpoint newCheckpoint()                               { return Checkpoint.MAX; }
            public void checkpointComplete(Checkpoint cp, boolean compact) { /* no-op */ }
        });
        storage.setCheckpointer(new Checkpointer() {
            public void startCheckpoint(CheckpointSource.Checkpoint c) { /* no-op */ }
            public void start()                                         { /* no-op */ }
        });

        storage.start();

        // Ledger 0L: entries 0 through 5 — primary ledger under test
        storage.setMasterKey(LEDGER_IN_STORAGE, "key-0".getBytes());
        for (long i = 0; i <= LAST_ENTRY_IN_LEDGER_0; i++) {
            ByteBuf entry = buildEntry(LEDGER_IN_STORAGE, i, i * 10);
            try { storage.addEntry(entry); } finally { entry.release(); }
        }

        // Ledger 1L: entries 1 through 10 — entryId 10L is only reachable via this ledger
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

    // ── Helper ────────────────────────────────────────────────────────────────

    /**
     * Builds a well-formed entry buffer: [8B ledgerId][8B entryId][8B payload].
     * The caller is responsible for releasing the returned buffer.
     */
    private static ByteBuf buildEntry(long ledgerId, long entryId, long payload) {
        ByteBuf buf = Unpooled.buffer(Long.BYTES * 3);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeLong(payload);
        return buf;
    }

    // ── Parameter source ──────────────────────────────────────────────────────

    /**
     * Converts the {@link GetEntryCase} enum into a JUnit parameter stream.
     * One {@link Arguments} row is produced per enum constant, in declaration order,
     * which matches the category-partition table row numbering.
     */
    static Stream<Arguments> categoryPartitionCases() {
        return Stream.of(GetEntryCase.values())
                .map(GetEntryCase::toArguments);
    }

    // ── Parameterised test ────────────────────────────────────────────────────

    /**
     * Drives every row of the category-partition table against
     * {@link DbLedgerStorage#getEntry(long, long)}.
     *
     * <p><b>Success path:</b> verifies that the returned buffer is non-null,
     * that the ledgerId in the buffer header matches the requested ledgerId,
     * and that the entryId in the header matches {@code expectedEntryId}
     * (the entryId check is skipped when {@code expectedEntryId == -99L}).
     *
     * <p><b>Failure path:</b> verifies that an exception of any type is thrown.
     * Both IOException (missing entry) and BookieException subtypes are accepted
     * since the exact exception type depends on the failure reason.
     *
     * @param label           human-readable case description shown in the JUnit report
     * @param ledgerId        ledger identifier passed to {@code getEntry}
     * @param entryId         entry identifier passed to {@code getEntry}
     * @param expectSuccess   {@code true} — must succeed; {@code false} — must throw
     * @param expectedEntryId entryId expected in the returned buffer header,
     *                        or {@code -99L} to skip the entryId assertion
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("categoryPartitionCases")
    @DisplayName("getEntry — category partition")
    void testGetEntryCategories(String label, long ledgerId, long entryId,
                                boolean expectSuccess, long expectedEntryId) throws Exception {
        if (expectSuccess) {
            ByteBuf result = storage.getEntry(ledgerId, entryId);
            try {
                assertNotNull(result,
                        "getEntry must return a non-null ByteBuf for: " + label);

                long returnedLedgerId = result.getLong(result.readerIndex());
                assertEquals(ledgerId, returnedLedgerId,
                        "Returned ledgerId in buffer header must match the requested ledgerId for: "
                                + label);

                if (expectedEntryId != -99L) {
                    long returnedEntryId = result.getLong(result.readerIndex() + Long.BYTES);
                    assertEquals(expectedEntryId, returnedEntryId,
                            "Returned entryId in buffer header must be " + expectedEntryId
                                    + " for: " + label);
                }
            } finally {
                result.release();
            }
        } else {
            assertThrows(Exception.class,
                    () -> storage.getEntry(ledgerId, entryId),
                    "getEntry must throw an exception for: " + label);
        }
    }
}