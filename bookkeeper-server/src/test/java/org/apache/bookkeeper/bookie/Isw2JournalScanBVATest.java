package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.io.TempDir;

/**
 * Category Partition tests for {@link Journal#scanJournal}.
 *
 * <p>Method under test:
 * <pre>
 *   public long scanJournal(long journalId, long journalPos,
 *                           JournalScanner scanner, boolean skipInvalidRecord)
 * </pre>
 *
 * <p><b>Parameter categories:</b>
 * <ul>
 *   <li><b>journalId</b>
 *     <ul>
 *       <li>Valid   [1L] — .txn file with 100 entries (long file)</li>
 *       <li>Valid   [2L] — .txn file with  50 entries (short file)</li>
 *       <li>Corrupted[3L]— .txn file with  50 entries + 1 truncated record</li>
 *       <li>Invalid [4L] — ID with no corresponding .txn file on disk</li>
 *       <li>Invalid [-1] — negative long, no file can match</li>
 *       <li>Invalid [0]  — zero, no file written with id=0 in this setup</li>
 *     </ul>
 *   </li>
 *   <li><b>journalPos</b>
 *     <ul>
 *       <li>From Start [-1] — value ≤ 0 → branch (journalPos ≤ 0), scan from byte 0</li>
 *       <li>From Start  [0] — canonical zero, same ≤ 0 branch</li>
 *       <li>InFile          — positive offset aligned on the first byte of entry #10
 *                             in file 1L, computed at setup via a preliminary scan</li>
 *       <li>EOF             — positive offset equal to the return value of a complete
 *                             scan of file 2L (one past the last byte): 0 entries</li>
 *       <li>Disaligned  [1] — offset=1, lands inside the 4-byte length field of
 *                             the first record; scan produces no useful entries</li>
 *     </ul>
 *   </li>
 *   <li><b>scanner</b>
 *     <ul>
 *       <li>Valid — collects (ledgerId, entryId) pairs for assertion</li>
 *       <li>Null  — null reference; if process() would be invoked a NPE results</li>
 *     </ul>
 *   </li>
 *   <li><b>skipInvalidRecord</b>
 *     <ul>
 *       <li>true  — IOException from a bad record is swallowed; method returns normally</li>
 *       <li>false — IOException from a bad record is propagated to the caller</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p><b>Setup scenario</b> (rebuilt fresh before every parametric iteration):
 * <ul>
 *   <li>File 1L — 100 valid entries via {@code logAddEntry} + wait for ack</li>
 *   <li>File 2L —  50 valid entries via {@code logAddEntry} + wait for ack</li>
 *   <li>File 3L —  50 valid entries via {@code logAddEntry} + wait for ack,
 *       then a truncated record is appended manually after a clean shutdown.</li>
 * </ul>
 *
 * <p><b>Parametric table:</b>
 * <pre>
 * JournalId      | JournalPos        | Scanner | SkipInvalid | Output
 * ───────────────┼───────────────────┼─────────┼─────────────┼───────────────────────────
 * Valid [1L]     | From Start [-1]   | Valid   | True        | Success (100 entries)
 * Invalid [4L]   | From Start [0]    | Valid   | True        | Failure (IOException)
 * Invalid [-1]   | From Start [0]    | Valid   | True        | Failure (IOException)
 * Invalid [0]    | From Start [0]    | Valid   | True        | Failure (IOException)
 * Valid [2L]     | From Start [0]    | Valid   | False       | Success (50 entries)
 * Valid [1L]     | InFile [10 entry] | Valid   | True        | Success (90 entries)
 * Valid [2L]     | EOF [51 entry]    | Valid   | True        | Failure (0 entries)
 * Valid [2L]     | Disaligned [1]    | Valid   | True        | Failure (0 useful entries)
 * Valid [2L]     | From Start [0]    | Null    | True        | Failure (exception or 0 entries)
 * Corrupted [3L] | From Start [0]    | Valid   | True        | Success (50 entries)
 * Corrupted [3L] | From Start [0]    | Valid   | False       | Success (50 entries)
 * </pre>
 */
@DisplayName("Journal#scanJournal — Category Partition + BVA")
public class Isw2JournalScanBVATest {

    // ─────────────────────────────────────────────────────────────────────────
    // Constants
    // ─────────────────────────────────────────────────────────────────────────

    private static final int  LONG_FILE_ENTRIES          = 100;
    private static final int  SHORT_FILE_ENTRIES         = 50;
    private static final int  CORRUPTED_FILE_VALID_ENTRIES = 50;

    static final long LOGICAL_ID_LONG      = 1L;
    static final long LOGICAL_ID_SHORT     = 2L;
    static final long LOGICAL_ID_CORRUPTED = 3L;
    static final long LOGICAL_ID_MISSING   = 4L;

    private static final long LEDGER_ID            = 100L;
    private static final int  DECLARED_CORRUPT_SIZE = 128;
    private static final int  PARTIAL_CORRUPT_BYTES = 32;
    private static final long WRITE_TIMEOUT_SECS   = 10L;

    /**
     * Sentinel: resolve to the byte offset of entry #10 in file 1L at test time.
     */
    private static final long TOKEN_IN_FILE = Long.MIN_VALUE;

    /**
     * Sentinel: resolve to the EOF offset of file 2L at test time.
     */
    private static final long TOKEN_EOF = Long.MAX_VALUE;

    // ─────────────────────────────────────────────────────────────────────────
    // Parametric table
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns one {@link Arguments} row per test case.
     * Column order: testName, logicalJournalId, journalPosToken,
     * scannerIsNull, skipInvalidRecord, expectSuccess, expectedEntryCount.
     */
    static Stream<Arguments> cases() {
        return Stream.of(
                Arguments.of("valid_1L_fromStart_neg1_skip_true",
                        LOGICAL_ID_LONG, -1L, false, true, true, LONG_FILE_ENTRIES),

                Arguments.of("invalid_4L_missing_fromStart_skip_true",
                        LOGICAL_ID_MISSING, 0L, false, true, false, 0),

                Arguments.of("invalid_neg1_id_fromStart_skip_true",
                        -1L, 0L, false, true, false, 0),

                Arguments.of("invalid_0_id_fromStart_skip_true",
                        0L, 0L, false, true, false, 0),

                Arguments.of("valid_2L_fromStart_zero_skip_false",
                        LOGICAL_ID_SHORT, 0L, false, false, true, SHORT_FILE_ENTRIES),

                Arguments.of("valid_1L_inFile_entry10_skip_true",
                        LOGICAL_ID_LONG, TOKEN_IN_FILE, false, true, true, LONG_FILE_ENTRIES - 10),

                Arguments.of("valid_2L_eof_skip_true",
                        LOGICAL_ID_SHORT, TOKEN_EOF, false, true, false, 0),

                Arguments.of("valid_2L_disaligned_pos1_valid_scanner_skip_true",
                        LOGICAL_ID_SHORT, 1L, false, true, false, 0),

                Arguments.of("valid_2L_fromStart_zero_null_scanner_skip_true",
                        LOGICAL_ID_SHORT, 0L, true, true, false, 0),

                Arguments.of("corrupted_3L_fromStart_skip_true",
                        LOGICAL_ID_CORRUPTED, 0L, false, true, true, CORRUPTED_FILE_VALID_ENTRIES),

                Arguments.of("corrupted_3L_fromStart_skip_false",
                        LOGICAL_ID_CORRUPTED, 0L, false, false, true, CORRUPTED_FILE_VALID_ENTRIES)
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Infrastructure
    // ─────────────────────────────────────────────────────────────────────────

    @TempDir
    File tempDir;

    private File                journalDir;
    private ServerConfiguration conf;
    private LedgerDirsManager   dirsManager;

    /**
     * Byte offset of the first byte of entry #10 (0-based) inside file 1L.
     * Computed during {@link #setUp()} by a preliminary scan.
     */
    private long inFileOffsetEntry10;

    /**
     * Byte offset returned by a complete scan of file 2L — one past the last
     * valid record (EOF category).
     */
    private long eofOffsetFile2;

    // ─────────────────────────────────────────────────────────────────────────
    // Setup / Teardown
    // ─────────────────────────────────────────────────────────────────────────

    @BeforeEach
    void setUp() throws Exception {
        journalDir     = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        conf = new ServerConfiguration();
        conf.setJournalDirName(journalDir.getAbsolutePath());
        conf.setLedgerDirNames(new String[]{ledgerDir.getAbsolutePath()});
        conf.setMaxJournalSizeMB(1);
        conf.setJournalWriteBufferSizeKB(4);
        conf.setJournalRemovePagesFromCache(false);
        conf.setAllowLoopback(true);

        dirsManager = mock(LedgerDirsManager.class);
        when(dirsManager.getAllLedgerDirs()).thenReturn(List.of(ledgerDir));
        when(dirsManager.getWritableLedgerDirsForNewLog()).thenReturn(List.of(ledgerDir));

        // ── File 1L: 100 valid entries ────────────────────────────────────
        Journal j1 = new Journal(0, journalDir, conf, dirsManager);
        j1.start();
        writeEntries(j1, LONG_FILE_ENTRIES);
        j1.shutdown();

        // Auxiliary scan of 1L to locate byte offset of entry #10.
        final long[] capturedOffset = {-1L};
        final int[]  callCount      = {0};
        Journal auxScan1 = new Journal(0, journalDir, conf, dirsManager);
        long id1 = getNthJournalId(0);
        auxScan1.scanJournal(id1, 0L, (version, offset, entry) -> {
            callCount[0]++;
            if (callCount[0] == 11) {
                capturedOffset[0] = offset;
            }
        }, false);
        inFileOffsetEntry10 = capturedOffset[0];
        assertTrue(inFileOffsetEntry10 > 0,
                "[setUp] Could not locate entry #10 in file 1L — offset=" + inFileOffsetEntry10);

        // ── File 2L: 50 valid entries ─────────────────────────────────────
        Journal j2 = new Journal(0, journalDir, conf, dirsManager);
        j2.start();
        writeEntries(j2, SHORT_FILE_ENTRIES);
        j2.shutdown();

        Journal auxScan2 = new Journal(0, journalDir, conf, dirsManager);
        long id2 = getNthJournalId(1);
        eofOffsetFile2 = auxScan2.scanJournal(id2, 0L, (v, o, e) -> {}, false);
        assertTrue(eofOffsetFile2 > 0, "[setUp] EOF offset for file 2L must be > 0");

        // ── File 3L: 50 valid entries + truncated record ──────────────────
        Journal j3 = new Journal(0, journalDir, conf, dirsManager);
        j3.start();
        writeEntries(j3, CORRUPTED_FILE_VALID_ENTRIES);
        j3.shutdown();
        injectTruncatedRecord(2);
    }

    @AfterEach
    void tearDown() {
        // @TempDir cleans up automatically.
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parametric test body
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Executes one row from the parametric table.
     *
     * <p>Row-by-row notes:
     * <ul>
     *   <li>Row 1 — journalPos=-1 collapses to the ≤0 branch; 100 entries recovered.</li>
     *   <li>Rows 2–4 — invalid journalIds cause JournalChannel to fail on open.</li>
     *   <li>Row 5 — skipInvalidRecord=false on a clean file: 50 entries expected.</li>
     *   <li>Row 6 — inFileOffsetEntry10 skips entries 0–9; 90 entries recovered.</li>
     *   <li>Row 7 — eofOffsetFile2 positions cursor past last byte; 0 entries.</li>
     *   <li>Rows 8–9 — offset=1 misreads the length field; no useful entries.</li>
     *   <li>Rows 10–11 — truncation causes break not IOException; 50 entries regardless
     *       of skipInvalidRecord.</li>
     * </ul>
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void testScanJournal(
            String  testName,
            long    logicalJournalId,
            long    journalPosToken,
            boolean scannerIsNull,
            boolean skipInvalidRecord,
            boolean expectSuccess,
            int     expectedEntryCount) throws Exception {

        // ── 1. Resolve journalPos token ───────────────────────────────────
        long resolvedPos;
        if (journalPosToken == TOKEN_IN_FILE) {
            resolvedPos = inFileOffsetEntry10;
        } else if (journalPosToken == TOKEN_EOF) {
            resolvedPos = eofOffsetFile2;
        } else {
            resolvedPos = journalPosToken;
        }

        // ── 2. Resolve logical journalId to real on-disk id ───────────────
        long resolvedId;
        if (logicalJournalId == LOGICAL_ID_LONG) {
            resolvedId = getNthJournalId(0);
        } else if (logicalJournalId == LOGICAL_ID_SHORT) {
            resolvedId = getNthJournalId(1);
        } else if (logicalJournalId == LOGICAL_ID_CORRUPTED) {
            resolvedId = getNthJournalId(2);
        } else {
            resolvedId = logicalJournalId;
        }

        // ── 3. Build scanner ──────────────────────────────────────────────
        List<long[]> collected = new ArrayList<>();
        Journal.JournalScanner scanner = scannerIsNull ? null
                : (version, offset, entry) -> {
            if (entry.remaining() >= 16) {
                long lId = entry.getLong();
                long eId = entry.getLong();
                collected.add(new long[]{lId, eId});
            }
        };

        // ── 4. Fresh Journal instance (no start() required for scan) ──────
        Journal scanJournal = new Journal(0, journalDir, conf, dirsManager);

        // ── 5. Execute and assert ─────────────────────────────────────────
        if (expectSuccess) {
            long returnedPos = scanJournal.scanJournal(
                    resolvedId, resolvedPos, scanner, skipInvalidRecord);

            assertEquals(expectedEntryCount, collected.size(),
                    "[" + testName + "] Entry count mismatch");

            for (int i = 0; i < collected.size(); i++) {
                assertEquals(LEDGER_ID, collected.get(i)[0],
                        "[" + testName + "] Entry " + i + " — wrong ledgerId");
            }

            if (expectedEntryCount > 0) {
                assertTrue(returnedPos > resolvedPos,
                        "[" + testName + "] Return position must be > starting journalPos");
            }

        } else {
            boolean threw = false;
            try {
                scanJournal.scanJournal(
                        resolvedId, resolvedPos, scanner, skipInvalidRecord);
            } catch (Exception e) {
                threw = true;
            }

            boolean noEntries = collected.isEmpty();
            assertTrue(threw || noEntries,
                    "[" + testName + "] Expected either an exception or 0 recovered entries,"
                            + " but got " + collected.size() + " entries and no exception");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private void writeEntries(Journal j, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            ByteBuf entry = makeEntry(LEDGER_ID, i, "payload-" + i);
            CountDownLatch latch = new CountDownLatch(1);
            j.logAddEntry(LEDGER_ID, i, entry, false,
                    (rc, lId, eId, addr, ctx) -> { if (rc == 0) latch.countDown(); },
                    null);
            assertTrue(latch.await(WRITE_TIMEOUT_SECS, TimeUnit.SECONDS),
                    "Write callback not received within " + WRITE_TIMEOUT_SECS + " s"
                            + " for entry (ledger=" + LEDGER_ID + ", entry=" + i + ")");
            entry.release();
        }
    }

    private static ByteBuf makeEntry(long ledgerId, long entryId, String payload) {
        byte[] bytes = payload.getBytes();
        ByteBuf buf = Unpooled.buffer(8 + 8 + bytes.length);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeBytes(bytes);
        return buf;
    }

    private void injectTruncatedRecord(int fileIndex) throws Exception {
        long realId = getNthJournalId(fileIndex);
        File txnFile = new File(journalDir, Long.toHexString(realId) + ".txn");
        assertTrue(txnFile.exists(), "Journal file not found for injection: " + txnFile);

        try (FileChannel fc = FileChannel.open(txnFile.toPath(),
                StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {

            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            lenBuf.putInt(DECLARED_CORRUPT_SIZE);
            lenBuf.flip();
            fc.write(lenBuf);

            ByteBuffer partial = ByteBuffer.allocate(PARTIAL_CORRUPT_BYTES);
            for (int i = 0; i < PARTIAL_CORRUPT_BYTES; i++) {
                partial.put((byte) 0xAB);
            }
            partial.flip();
            fc.write(partial);
            fc.force(true);
        }
    }

    private long getNthJournalId(int n) {
        List<Long> ids = Journal.listJournalIds(journalDir, null);
        assertFalse(ids.isEmpty(), "No .txn files found in " + journalDir);
        assertTrue(n < ids.size(),
                "Requested index " + n + " but only " + ids.size() + " files exist");
        return ids.get(n);
    }
}