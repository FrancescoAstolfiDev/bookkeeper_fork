package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.File;

import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Category Partition (CP) and Boundary Value Analysis (BVA) tests for
 * {@link DbLedgerStorage#addEntry(ByteBuf)}.
 *
 * <p>All tests run against a real on-disk RocksDB + EntryLogger backend — no mocks.
 * The storage is initialised using the BVA#1 baseline configuration
 * (write=16MB, read=16MB, new temp directory, 1 ledger dir == 1 index dir),
 * verified empirically as a stable, side-effect-free foundation.
 *
 * <p><b>Base choice strategy:</b> the baseline configuration (case #1) acts as the
 * fixed configuration for all subsequent cases — only the {@code ByteBuf} parameter
 * is varied per row while the ledger state (master key registered) is kept at the
 * baseline nominal value. If the baseline fails, all dependent cases are invalidated.
 *
 * <p><b>Entry buffer layout expected by {@code addEntry} — empirically verified:</b>
 * <pre>[ 8 bytes : ledgerId ][ 8 bytes : entryId ][ 8 bytes : payload ][ optional extra bytes ]</pre>
 *
 * <p>The implementation reads the buffer at absolute indices 0, 8, and 16 using
 * {@code getLong(int index)} (non-relative reads). Consequently:
 * <ul>
 *   <li>A buffer shorter than 24 bytes causes an {@code IndexOutOfBoundsException}
 *       at the third {@code getLong(16)} call — regardless of the writtenIndex.</li>
 *   <li>The minimum valid buffer size is therefore 24 bytes (three longs).</li>
 *   <li>A buffer of exactly 16 bytes (header only) is rejected — not accepted —
 *       because the payload long at index 16 cannot be read.</li>
 *   <li>A buffer of exactly 8 bytes (ledgerId only) is also rejected for the same reason.</li>
 * </ul>
 *
 * <p>This empirical finding updates the BVA boundary values: the lower valid bound
 * shifts from 16 bytes (assumed) to 24 bytes (verified). Cases CP#4 / BVA#4
 * and CP#3 / BVA#3 both produce exceptions, which is consistent with the revised
 * understanding of the format.
 *
 * <h3>Category Partition table</h3>
 * <pre>
 * +----+--------------------------------------------+----------------------+-------------------------------------------------------+
 * |  # | ByteBuf                                    | Ledger state         | Expected output                                       |
 * +----+--------------------------------------------+----------------------+-------------------------------------------------------+
 * |  1 | well-formed (ledgerId + entryId + payload) | master key registered| Write successful                                      |
 * |  2 | well-formed, duplicate entryId             | master key registered| Write successful (last-write, RocksDB pointer updated)|
 * |  3 | ledgerId only (8 bytes), entryId absent    | master key registered| Exception (IndexOutOfBoundsException)                 |
 * |  4 | header only, no payload (16 bytes)         | master key registered| Exception (IndexOutOfBoundsException at payload read) |
 * |  5 | empty (0 bytes)                            | master key registered| Exception (IndexOutOfBoundsException)                 |
 * +----+--------------------------------------------+----------------------+-------------------------------------------------------+
 * </pre>
 *
 * <h3>Boundary Value Analysis table — mapped 1-to-1 with CP rows</h3>
 * <pre>
 * +----+--------------------------------------------------+-------------------------------------------------------+
 * |  # | ByteBuf (size + content)                         | Expected output                                       |
 * +----+--------------------------------------------------+-------------------------------------------------------+
 * |  1 | 24 bytes: ledgerId + entryId + payload (3 longs) | Write successful (lower valid bound)                  |
 * |  2 | 24 bytes: duplicate header + different payload   | Write successful (last-write, RocksDB pointer updated)|
 * |  3 |  8 bytes: ledgerId only, entryId absent          | Exception (below lower valid bound)                   |
 * |  4 | 16 bytes: header only, payload long absent       | Exception (one long below lower valid bound)          |
 * |  5 |  0 bytes: empty                                  | Exception (absolute lower bound)                      |
 * +----+--------------------------------------------------+-------------------------------------------------------+
 * </pre>
 */
@DisplayName("DbLedgerStorage — Category Partition + BVA: addEntry(ByteBuf)")
public class Isw2DbLedgerStorageAddEntryBVATest {

    @TempDir
    File tempDir;

    private DbLedgerStorage storage;

    // ── Constants ──────────────────────────────────────────────────────────────

    private static final long LEDGER_ID      = 1L;
    private static final int  LONG_BYTES     = Long.BYTES;           //  8 bytes
    private static final int  HEADER_BYTES   = LONG_BYTES * 2;       // 16 bytes: ledgerId + entryId
    private static final int  MIN_VALID_SIZE = LONG_BYTES * 3;       // 24 bytes: ledgerId + entryId + payload

    // ── Lifecycle ──────────────────────────────────────────────────────────────

    /**
     * Boots a real {@link DbLedgerStorage} instance using the BVA#1 baseline
     * configuration (write=16MB, read=16MB, new temp directory,
     * 1 ledger dir == 1 index dir) and registers the master key for
     * {@link #LEDGER_ID} — the only ledger used across all cases.
     *
     * <p>This configuration is the <b>base choice baseline</b>. Its operational
     * correctness is verified empirically by CP#1 / BVA#1 and is therefore a
     * stable precondition for all other cases.
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

        storage.setCheckpointSource(new CheckpointSource() {
            public Checkpoint newCheckpoint()                               { return Checkpoint.MAX; }
            public void checkpointComplete(Checkpoint cp, boolean compact) { /* no-op */ }
        });
        storage.setCheckpointer(new Checkpointer() {
            public void startCheckpoint(CheckpointSource.Checkpoint c) { /* no-op */ }
            public void start()                                         { /* no-op */ }
        });
        storage.start();

        // Base choice: master key registered — nominal ledger state for all cases.
        storage.setMasterKey(LEDGER_ID, "key-1".getBytes());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (storage != null) {
            try { storage.shutdown(); } catch (Exception ignored) {}
            storage = null;
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    /**
     * Builds a well-formed entry buffer:
     * {@code [8B ledgerId][8B entryId][8B payload]}.
     *
     * <p>BVA: 24 bytes = lower valid bound (empirically verified).
     * The implementation reads three consecutive longs at absolute indices 0, 8, 16
     * via non-relative {@code getLong(int index)} calls. A buffer shorter than 24 bytes
     * throws {@code IndexOutOfBoundsException} at the third read regardless of
     * how many bytes were written.
     *
     * @param ledgerId ledger identifier written at offset 0
     * @param entryId  entry identifier written at offset 8
     * @param payload  payload value written at offset 16
     * @return a new {@link ByteBuf} of exactly 24 bytes; caller must release
     */
    private static ByteBuf buildWellFormed(long ledgerId, long entryId, long payload) {
        ByteBuf buf = Unpooled.buffer(MIN_VALID_SIZE);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeLong(payload);
        return buf;
    }

    /**
     * Builds a header-only buffer (no payload long):
     * {@code [8B ledgerId][8B entryId]}.
     *
     * <p>BVA: 16 bytes = one long below the lower valid bound.
     * The third {@code getLong(16)} call in the implementation throws
     * {@code IndexOutOfBoundsException} because there are no bytes at index 16.
     *
     * @param ledgerId ledger identifier written at offset 0
     * @param entryId  entry identifier written at offset 8
     * @return a new {@link ByteBuf} of exactly 16 bytes; caller must release
     */
    private static ByteBuf buildHeaderOnly(long ledgerId, long entryId) {
        ByteBuf buf = Unpooled.buffer(HEADER_BYTES);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        return buf;
    }

    /**
     * Builds a ledger-id-only buffer (no entryId, no payload):
     * {@code [8B ledgerId]}.
     *
     * <p>BVA: 8 bytes = exact lower bound of the ledgerId field.
     * Both the entryId and payload fields are absent — the buffer is rejected.
     *
     * @param ledgerId ledger identifier written at offset 0
     * @return a new {@link ByteBuf} of exactly 8 bytes; caller must release
     */
    private static ByteBuf buildLedgerIdOnly(long ledgerId) {
        ByteBuf buf = Unpooled.buffer(LONG_BYTES);
        buf.writeLong(ledgerId);
        return buf;
    }

    /**
     * Skips the 16-byte header and reads the payload long from a retrieved buffer.
     * The caller must release the buffer after this call.
     */
    private static long readPayload(ByteBuf buf) {
        buf.skipBytes(HEADER_BYTES);
        return buf.readLong();
    }

    // =========================================================================
    // Category Partition + BVA test cases
    // =========================================================================

    /**
     * CP #1 / BVA #1 — Well-formed buffer (24 bytes: ledgerId + entryId + payload).
     *
     * <p><b>CP:</b> well-formed entry (ledgerId + entryId + payload),
     * master key registered.
     * <b>BVA:</b> 24 bytes = lower valid bound (empirically verified).
     *
     * <p>Verifies that a complete 3-long buffer is accepted, persisted, and
     * retrievable via {@code getEntry()} after flush. Both the buffer size and
     * the payload value are asserted to confirm data integrity.
     *
     * <p>This case also validates the <b>base choice baseline</b>: its successful
     * completion is the precondition that justifies using this storage configuration
     * for all subsequent cases.
     */
    @Test
    @DisplayName("CP#1 / BVA#1 — well-formed (24 bytes: 3 longs) → Write successful (lower valid bound)")
    void cp01_bva01_wellFormedEntry_success() throws Exception {
        final long payloadValue = 0xDEADBEEFL;
        ByteBuf buf = buildWellFormed(LEDGER_ID, 0L, payloadValue);
        try {
            storage.addEntry(buf);
        } finally {
            buf.release();
        }
        storage.flush();

        ByteBuf result = storage.getEntry(LEDGER_ID, 0L);
        try {
            assertNotNull(result,
                    "Entry must be retrievable after write");
            assertEquals(MIN_VALID_SIZE, result.readableBytes(),
                    "Retrieved buffer must be exactly 24 bytes (3 longs)");
            assertEquals(payloadValue, readPayload(result),
                    "Retrieved payload must match the written value");
        } finally {
            result.release();
        }
    }

    /**
     * CP #2 / BVA #2 — Duplicate entryId: last-write semantics (24 bytes).
     *
     * <p><b>CP:</b> well-formed buffer, duplicate entryId, master key registered.
     * <b>BVA:</b> 24 bytes = lower valid bound; the second write uses the same
     * header but a different payload long to verify that RocksDB updates the pointer.
     *
     * <p>{@code DbLedgerStorage} uses RocksDB as an index: when the same
     * {@code (ledgerId, entryId)} pair is written twice, the index pointer is updated
     * to the latest physical offset in the entry log file (last-write semantics).
     * The first entry remains physically present in the log but becomes unreachable
     * via the index. The second payload must therefore be the one returned by
     * {@code getEntry()}.
     */
    @Test
    @DisplayName("CP#2 / BVA#2 — duplicate entryId (24 bytes, different payload) → last-write semantics")
    void cp02_bva02_duplicateEntryId_lastWriteSemantics() throws Exception {
        final long firstPayload  = 0x0000_0001L;
        final long secondPayload = 0x0000_0002L;

        // First write
        ByteBuf first = buildWellFormed(LEDGER_ID, 0L, firstPayload);
        try {
            storage.addEntry(first);
        } finally {
            first.release();
        }

        // Second write: same (ledgerId, entryId), different payload
        ByteBuf second = buildWellFormed(LEDGER_ID, 0L, secondPayload);
        try {
            storage.addEntry(second);
        } finally {
            second.release();
        }

        storage.flush();

        // RocksDB pointer updated to the latest offset — second payload must be returned
        ByteBuf result = storage.getEntry(LEDGER_ID, 0L);
        try {
            assertNotNull(result,
                    "Entry must be retrievable after duplicate write");
            assertEquals(secondPayload, readPayload(result),
                    "Second write must overwrite first (last-write semantics)");
        } finally {
            result.release();
        }
    }

    /**
     * CP #3 / BVA #3 — ledgerId only (8 bytes): entryId and payload absent.
     *
     * <p><b>CP:</b> buffer containing only the ledgerId (8 bytes), master key registered.
     * <b>BVA:</b> 8 bytes = lower bound of the ledgerId field; both entryId and payload
     * are absent. The implementation throws {@code IndexOutOfBoundsException} when
     * attempting to read the entryId at index 8.
     */
    @Test
    @DisplayName("CP#3 / BVA#3 — ledgerId only (8 bytes) → Exception (below lower valid bound)")
    void cp03_bva03_onlyLedgerId_exception() {
        ByteBuf buf = buildLedgerIdOnly(LEDGER_ID);
        try {
            assertThrows(Exception.class,
                    () -> storage.addEntry(buf),
                    "An 8-byte buffer must be rejected");
        } finally {
            buf.release();
        }
    }

    /**
     * CP #4 / BVA #4 — Header only, no payload long (16 bytes).
     *
     * <p><b>CP:</b> buffer containing ledgerId + entryId (16 bytes), master key registered.
     * <b>BVA:</b> 16 bytes = one long below the lower valid bound. The implementation
     * reads ledgerId at index 0 and entryId at index 8 successfully, then throws
     * {@code IndexOutOfBoundsException} at the third {@code getLong(16)} call because
     * the payload long is absent.
     *
     * <p>Note: this case was initially expected to succeed (payload being optional),
     * but empirical execution revealed that the implementation unconditionally reads
     * three longs. The expected output is therefore Exception, not Write successful.
     */
    @Test
    @DisplayName("CP#4 / BVA#4 — header only (16 bytes) → Exception (payload long absent, one long below lower valid bound)")
    void cp04_bva04_headerOnly_exception() {
        ByteBuf buf = buildHeaderOnly(LEDGER_ID, 0L);
        try {
            assertThrows(Exception.class,
                    () -> storage.addEntry(buf),
                    "A 16-byte buffer without payload long must be rejected");
        } finally {
            buf.release();
        }
    }

    /**
     * CP #5 / BVA #5 — Empty buffer (0 bytes).
     *
     * <p><b>CP:</b> empty buffer (0 bytes), master key registered.
     * <b>BVA:</b> 0 bytes = absolute lower bound; no field can be read at all.
     * The first {@code getLong(0)} call throws {@code IndexOutOfBoundsException}.
     */
    @Test
    @DisplayName("CP#5 / BVA#5 — empty buffer (0 bytes) → Exception (absolute lower bound)")
    void cp05_bva05_emptyBuffer_exception() {
        ByteBuf buf = Unpooled.buffer(0);
        try {
            assertThrows(Exception.class,
                    () -> storage.addEntry(buf),
                    "A zero-byte buffer must be rejected");
        } finally {
            buf.release();
        }
    }
}