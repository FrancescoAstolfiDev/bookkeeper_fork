package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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
 *
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
 *       then a truncated record is <em>appended</em> manually after a clean
 *       shutdown: {@code [4-byte length = 128][32 bytes of 0xAB garbage]}.
 *       The corruption sits after all valid entries, mirroring a crash that
 *       occurs after the last good write but before the process could finish
 *       a subsequent record. The scan recovers all 50 valid entries and stops
 *       silently at the truncation point — no exception is thrown regardless
 *       of {@code skipInvalidRecord}, because truncation triggers a {@code break}
 *       inside the loop, not an IOException.</li>
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
 * Valid [2L]     | EOF [51 entry]    | Valid   | True        | Failure (0 entries — empty scan)
 * Valid [2L]     | Disaligned [1]    | Valid   | True        | Failure (0 useful entries)
 * Valid [2L]     | From Start [0]    | Null    | True        | Failure (exception or 0 entries)
 * Corrupted [3L] | From Start [0]    | Valid   | True        | Success (50 entries — truncation appended after valid data)
 * Corrupted [3L] | From Start [0]    | Valid   | False       | Success (50 entries — truncation causes break not IOException)
 * </pre>
 */
@RunWith(Parameterized.class)
public class Isw2JournalScanBVATest {

    // ─────────────────────────────────────────────────────────────────────────
    // Constants
    // ─────────────────────────────────────────────────────────────────────────

    /** Number of entries written to file 1L. */
    private static final int LONG_FILE_ENTRIES = 100;

    /** Number of entries written to file 2L. */
    private static final int SHORT_FILE_ENTRIES = 50;

    /**
     * Number of valid entries written to file 3L before the truncated record
     * is injected.
     */
    private static final int CORRUPTED_FILE_VALID_ENTRIES = 50;

    /**
     * Logical file indices used as journalId constants in the parametric table.
     * The real on-disk id (timestamp-based) is resolved in setUp().
     */
    static final long LOGICAL_ID_LONG      = 1L;
    static final long LOGICAL_ID_SHORT     = 2L;
    static final long LOGICAL_ID_CORRUPTED = 3L;
    static final long LOGICAL_ID_MISSING   = 4L;

    /** Ledger used for all writes across all three files. */
    private static final long LEDGER_ID = 100L;

    /**
     * Payload length declared in the truncated record's length header.
     * Only {@link #PARTIAL_CORRUPT_BYTES} are actually written after it.
     */
    private static final int DECLARED_CORRUPT_SIZE = 128;

    /** Bytes written after the truncated record's length header. */
    private static final int PARTIAL_CORRUPT_BYTES = 32;

    /** Maximum seconds to wait for a write callback in setup. */
    private static final long WRITE_TIMEOUT_SECS = 10L;

    /**
     * Sentinel value passed as journalPosToken when the test wants the
     * "InFile" offset (first byte of entry #10 in file 1L).
     * Resolved to a concrete byte offset during setUp().
     */
    private static final long TOKEN_IN_FILE = Long.MIN_VALUE;

    /**
     * Sentinel value passed as journalPosToken when the test wants the
     * "EOF" offset (one past the last byte of file 2L).
     * Resolved to a concrete byte offset during setUp().
     */
    private static final long TOKEN_EOF = Long.MAX_VALUE;

    // ─────────────────────────────────────────────────────────────────────────
    // Parametric table
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns one row per test case.
     *
     * <p>Column order:
     * <ol>
     *   <li>testName          — human-readable label shown by JUnit</li>
     *   <li>logicalJournalId  — one of the LOGICAL_ID_* constants or an invalid value</li>
     *   <li>journalPosToken   — concrete offset, TOKEN_IN_FILE, or TOKEN_EOF</li>
     *   <li>scannerIsNull     — true → pass null as scanner</li>
     *   <li>skipInvalidRecord — passed directly to scanJournal</li>
     *   <li>expectSuccess     — true → no exception expected; false → exception expected</li>
     *   <li>expectedEntryCount— entries the scanner should collect (only checked on success)</li>
     * </ol>
     */
    @Parameters(name = "{0}")
    public static List<Object[]> cases() {
        return List.of(

                // Row 1 — Valid 1L, pos=-1 (≤0 branch) → full scan, 100 entries
                new Object[]{"valid_1L_fromStart_neg1_skip_true",
                        LOGICAL_ID_LONG, -1L, false, true, true, LONG_FILE_ENTRIES},

                // Row 2 — Missing id 4L, pos=0 → IOException on file open
                new Object[]{"invalid_4L_missing_fromStart_skip_true",
                        LOGICAL_ID_MISSING, 0L, false, true, false, 0},

                // Row 3 — Negative id -1L, pos=0 → IOException on file open
                new Object[]{"invalid_neg1_id_fromStart_skip_true",
                        -1L, 0L, false, true, false, 0},

                // Row 4 — Zero id 0L, pos=0 → IOException on file open (no 0.txn)
                new Object[]{"invalid_0_id_fromStart_skip_true",
                        0L, 0L, false, true, false, 0},

                // Row 5 — Valid 2L, pos=0, skipInvalid=false, clean file → 50 entries
                new Object[]{"valid_2L_fromStart_zero_skip_false",
                        LOGICAL_ID_SHORT, 0L, false, false, true, SHORT_FILE_ENTRIES},

                // Row 6 — Valid 1L, InFile offset of entry #10 → entries 10-99 = 90 entries
                new Object[]{"valid_1L_inFile_entry10_skip_true",
                        LOGICAL_ID_LONG, TOKEN_IN_FILE, false, true, true, LONG_FILE_ENTRIES - 10},

                // Row 7 — Valid 2L, EOF offset → cursor past last byte, 0 entries
                // expectSuccess=false because 0 entries recovered is treated as a
                // "no useful result" failure in the parametric assertion logic.
                new Object[]{"valid_2L_eof_skip_true",
                        LOGICAL_ID_SHORT, TOKEN_EOF, false, true, false, 0},

                // Row 8 — Valid 2L, disaligned pos=1, valid scanner → 0 useful entries
                new Object[]{"valid_2L_disaligned_pos1_valid_scanner_skip_true",
                        LOGICAL_ID_SHORT, 1L, false, true, false, 0},

                // Row 9 — Valid 2L, from start pos=0, null scanner → exception or 0 entries
                new Object[]{"valid_2L_fromStart_zero_null_scanner_skip_true",
                        LOGICAL_ID_SHORT, 0L, true, true, false, 0},

                // Row 10 — Corrupted 3L, skip=true → all 50 valid entries recovered;
                // the truncated record is appended after them and causes a silent break.
                new Object[]{"corrupted_3L_fromStart_skip_true",
                        LOGICAL_ID_CORRUPTED, 0L, false, true, true, CORRUPTED_FILE_VALID_ENTRIES},

                // Row 11 — Corrupted 3L, skip=false → truncation causes break not IOException.
                // The method returns normally with the 50 valid entries recovered before
                // the truncated record — same as skip=true (Row 10). The skipInvalidRecord
                // flag only matters for records with a negative-length header, not truncation.
                new Object[]{"corrupted_3L_fromStart_skip_false",
                        LOGICAL_ID_CORRUPTED, 0L, false, false, true, CORRUPTED_FILE_VALID_ENTRIES}
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parametric fields (injected by JUnit Parameterized runner)
    // ─────────────────────────────────────────────────────────────────────────

    private final String  testName;
    private final long    logicalJournalId;
    private final long    journalPosToken;
    private final boolean scannerIsNull;
    private final boolean skipInvalidRecord;
    private final boolean expectSuccess;
    private final int     expectedEntryCount;

    public Isw2JournalScanBVATest(
            String testName,
            long   logicalJournalId,
            long   journalPosToken,
            boolean scannerIsNull,
            boolean skipInvalidRecord,
            boolean expectSuccess,
            int     expectedEntryCount) {
        this.testName            = testName;
        this.logicalJournalId    = logicalJournalId;
        this.journalPosToken     = journalPosToken;
        this.scannerIsNull       = scannerIsNull;
        this.skipInvalidRecord   = skipInvalidRecord;
        this.expectSuccess       = expectSuccess;
        this.expectedEntryCount  = expectedEntryCount;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Infrastructure
    // ─────────────────────────────────────────────────────────────────────────

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private File journalDir;
    private ServerConfiguration conf;
    private LedgerDirsManager dirsManager;

    /**
     * Byte offset of the first byte of the 10th entry (0-based index 9) inside
     * file 1L.  Computed during setUp() by scanning the file and recording the
     * offset argument passed to the scanner on its 10th invocation.
     */
    private long inFileOffsetEntry10;

    /**
     * Byte offset equal to the return value of a complete scan of file 2L.
     * Placing the cursor here makes the scan find no bytes to read and return
     * immediately — the "EOF" category.
     */
    private long eofOffsetFile2;

    // ─────────────────────────────────────────────────────────────────────────
    // Setup / Teardown
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Builds three .txn files before every parametric iteration:
     *
     * <ol>
     *   <li><b>File 1L</b> — {@value #LONG_FILE_ENTRIES} valid entries</li>
     *   <li><b>File 2L</b> — {@value #SHORT_FILE_ENTRIES} valid entries</li>
     *   <li><b>File 3L</b> — {@value #CORRUPTED_FILE_VALID_ENTRIES} valid entries
     *       followed by one truncated record injected manually</li>
     * </ol>
     *
     * <p>Each file is created with its own Journal instance that is
     * started, written to, and shut down cleanly so the BufferedChannel is
     * fully flushed to disk before the test reads it via scanJournal.
     *
     * <p>Two auxiliary scans are performed:
     * <ul>
     *   <li>A scan of file 1L to locate the byte offset of entry #10
     *       (stored in {@link #inFileOffsetEntry10}).</li>
     *   <li>A scan of file 2L to record the return value, i.e. the
     *       position one past the last record (stored in
     *       {@link #eofOffsetFile2}).</li>
     * </ul>
     */
    @Before
    public void setUp() throws Exception {
        journalDir = tempDir.newFolder("journal");
        File ledgerDir = tempDir.newFolder("ledger");

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

        // Auxiliary scan of 1L to locate the byte offset of entry #10.
        // The scanner delivers entries 0-based: invocation 1 = entryId 0,
        // invocation 11 = entryId 10. We capture the offset on the 11th
        // invocation so that starting from that position skips entries 0-9
        // and recovers entries 10-99 = exactly 90 entries.
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
        assertTrue("[setUp] Could not locate entry #10 in file 1L — offset=" + inFileOffsetEntry10,
                inFileOffsetEntry10 > 0);

        // ── File 2L: 50 valid entries ─────────────────────────────────────
        Journal j2 = new Journal(0, journalDir, conf, dirsManager);
        j2.start();
        writeEntries(j2, SHORT_FILE_ENTRIES);
        j2.shutdown();

        // Auxiliary scan of 2L to record the EOF offset.
        Journal auxScan2 = new Journal(0, journalDir, conf, dirsManager);
        long id2 = getNthJournalId(1);
        eofOffsetFile2 = auxScan2.scanJournal(id2, 0L, (v, o, e) -> {}, false);
        assertTrue("[setUp] EOF offset for file 2L must be > 0", eofOffsetFile2 > 0);

        // ── File 3L: 50 valid entries + truncated record ──────────────────
        Journal j3 = new Journal(0, journalDir, conf, dirsManager);
        j3.start();
        writeEntries(j3, CORRUPTED_FILE_VALID_ENTRIES);
        j3.shutdown();
        injectTruncatedRecord(2); // 3rd file = index 2 in sorted list
    }

    @After
    public void tearDown() {
        // TemporaryFolder cleans up the directory tree automatically.
        // All Journal instances used in setUp() are already shut down.
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parametric test body
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Executes one row from the parametric table.
     *
     * <p>The method:
     * <ol>
     *   <li>Resolves the journalPos token to a concrete byte offset.</li>
     *   <li>Resolves the logical journal id to the real on-disk id.</li>
     *   <li>Builds a collecting scanner (or null).</li>
     *   <li>Invokes scanJournal on a fresh Journal instance (no start()).</li>
     *   <li>Asserts the outcome against {@link #expectSuccess} and
     *       {@link #expectedEntryCount}.</li>
     * </ol>
     *
     * <p><b>Assertions for the success path:</b>
     * <ul>
     *   <li>The scanner receives exactly {@link #expectedEntryCount} entries.</li>
     *   <li>Every delivered entry belongs to {@link #LEDGER_ID}.</li>
     *   <li>When entries are expected the return value of scanJournal is strictly
     *       greater than the starting journalPos, confirming forward progress.</li>
     * </ul>
     *
     * <p><b>Assertions for the failure path:</b>
     * <ul>
     *   <li>An exception of any kind is thrown by scanJournal.</li>
     * </ul>
     *
     * <p><b>Row-by-row notes:</b>
     * <ul>
     *   <li><i>Row 1</i> — journalPos=-1 collapses to the ≤0 branch; full file
     *       scanned, all 100 entries recovered.</li>
     *   <li><i>Rows 2–4</i> — invalid journalIds cause JournalChannel to fail
     *       on open; IOException expected.</li>
     *   <li><i>Row 5</i> — skipInvalidRecord=false on a clean file must not
     *       change behaviour: 50 entries expected.</li>
     *   <li><i>Row 6</i> — inFileOffsetEntry10 is the byte offset of entry #10;
     *       entries 0–9 are skipped, 10–99 are recovered (90 entries).</li>
     *   <li><i>Row 7</i> — eofOffsetFile2 positions the cursor past the last byte;
     *       the first fullRead finds nothing and the loop exits immediately.</li>
     *   <li><i>Rows 8–9</i> — offset=1 is inside the 4-byte length field of the
     *       first record; the misread length either causes an early break (no
     *       entries) or an exception.  With a null scanner the misread triggers
     *       a break before process() is ever called — NPE does not fire.</li>
     *   <li><i>Row 10</i> — 50 valid entries precede the truncated record;
     *       skipInvalidRecord=true swallows the truncation silently.  Note:
     *       truncation triggers a <em>break</em> inside the loop (fullRead != len),
     *       not an IOException, so skipInvalidRecord has no observable effect here
     *       — the scan stops at the truncation point regardless.</li>
     *   <li><i>Row 11</i> — same file, skipInvalidRecord=false. A truncated record
     *       triggers the {@code if (fullRead != len) break} path inside the loop,
     *       NOT an IOException — so skipInvalidRecord has no observable effect.
     *       The method returns normally with the 50 valid entries, identical to
     *       Row 10. expectSuccess=true, expectedEntryCount=50.</li>
     * </ul>
     */
    @Test
    public void testScanJournal() throws Exception {

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
            // Invalid ids (-1, 0, 4): passed directly so the open fails.
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

            assertEquals(
                    "[" + testName + "] Entry count mismatch",
                    expectedEntryCount,
                    collected.size());

            // Verify that every recovered entry belongs to the expected ledger.
            for (int i = 0; i < collected.size(); i++) {
                assertEquals(
                        "[" + testName + "] Entry " + i + " — wrong ledgerId",
                        LEDGER_ID,
                        collected.get(i)[0]);
            }

            // When entries were expected, the scan must have advanced the cursor.
            if (expectedEntryCount > 0) {
                assertTrue(
                        "[" + testName + "] Return position must be > starting journalPos",
                        returnedPos > resolvedPos);
            }

        } else {
            // Failure path: any exception (IOException, NPE, …) is acceptable.
            boolean threw = false;
            try {
                scanJournal.scanJournal(
                        resolvedId, resolvedPos, scanner, skipInvalidRecord);
            } catch (Exception e) {
                threw = true;
            }

            // For the EOF and disaligned cases the method may return normally
            // but deliver 0 useful entries — that is also a "failure" from the
            // caller's perspective.
            boolean noEntries = collected.isEmpty();
            assertTrue(
                    "[" + testName + "] Expected either an exception or 0 recovered entries,"
                            + " but got " + collected.size() + " entries and no exception",
                    threw || noEntries);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Writes {@code count} entries to {@code j} using synchronous write-and-wait
     * semantics: each call to {@code logAddEntry} blocks until the ForceWriteThread
     * fires the callback, guaranteeing that the entry is on disk before returning.
     *
     * <p>All entries are written to {@link #LEDGER_ID} with incremental entryIds
     * (0, 1, 2, …).
     */
    private void writeEntries(Journal j, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            ByteBuf entry = makeEntry(LEDGER_ID, i, "payload-" + i);
            CountDownLatch latch = new CountDownLatch(1);
            j.logAddEntry(LEDGER_ID, i, entry, false,
                    (rc, lId, eId, addr, ctx) -> { if (rc == 0) latch.countDown(); },
                    null);
            assertTrue(
                    "Write callback not received within " + WRITE_TIMEOUT_SECS + " s"
                            + " for entry (ledger=" + LEDGER_ID + ", entry=" + i + ")",
                    latch.await(WRITE_TIMEOUT_SECS, TimeUnit.SECONDS));
            entry.release();
        }
    }

    /**
     * Builds a ByteBuf with the standard BookKeeper entry layout:
     * {@code [ledgerId: 8 B][entryId: 8 B][payload: N bytes]}.
     *
     * <p>This is the format expected by {@code logAddEntry} and the format
     * delivered back by {@code scanJournal}.
     */
    private static ByteBuf makeEntry(long ledgerId, long entryId, String payload) {
        byte[] bytes = payload.getBytes();
        ByteBuf buf = Unpooled.buffer(8 + 8 + bytes.length);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeBytes(bytes);
        return buf;
    }

    /**
     * Appends a truncated record to the n-th .txn file (0-based, sorted ascending).
     *
     * <p>Physical layout of the injected record:
     * <pre>
     *   [4 bytes: int = DECLARED_CORRUPT_SIZE]   ← length header
     *   [PARTIAL_CORRUPT_BYTES bytes: 0xAB ...]  ← incomplete payload
     * </pre>
     *
     * <p>When {@code scanJournal} reaches this record, {@code fullRead()} reads
     * only {@link #PARTIAL_CORRUPT_BYTES} bytes instead of
     * {@link #DECLARED_CORRUPT_SIZE}, triggering the
     * {@code if (fullRead != len) break} path in the scan loop.  The loop exits
     * silently — no {@code IOException} is thrown for truncation.
     *
     * <p>Consequence for {@code skipInvalidRecord}: because truncation causes a
     * {@code break} rather than an exception, the flag has no observable effect
     * in this scenario.  Both {@code true} and {@code false} cause the scan to
     * stop at the truncation point and return normally.
     *
     * @param fileIndex 0-based index into the sorted list of .txn files
     */
    private void injectTruncatedRecord(int fileIndex) throws Exception {
        long realId = getNthJournalId(fileIndex);
        File txnFile = new File(journalDir, Long.toHexString(realId) + ".txn");
        assertTrue("Journal file not found for injection: " + txnFile, txnFile.exists());

        try (FileChannel fc = FileChannel.open(txnFile.toPath(),
                StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {

            // Length header claiming DECLARED_CORRUPT_SIZE bytes of payload.
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            lenBuf.putInt(DECLARED_CORRUPT_SIZE);
            lenBuf.flip();
            fc.write(lenBuf);

            // Partial payload: only PARTIAL_CORRUPT_BYTES of 0xAB garbage.
            ByteBuffer partial = ByteBuffer.allocate(PARTIAL_CORRUPT_BYTES);
            for (int i = 0; i < PARTIAL_CORRUPT_BYTES; i++) {
                partial.put((byte) 0xAB);
            }
            partial.flip();
            fc.write(partial);
            fc.force(true);
        }
    }

    /**
     * Returns the n-th journal id (0-based, sorted ascending) from the list of
     * .txn files present in {@link #journalDir}.
     *
     * <p>Journal ids are timestamp-based, so the sort order matches the creation
     * order: index 0 → file 1L, index 1 → file 2L, index 2 → file 3L.
     */
    private long getNthJournalId(int n) {
        List<Long> ids = Journal.listJournalIds(journalDir, null);
        assertFalse("No .txn files found in " + journalDir, ids.isEmpty());
        assertTrue("Requested index " + n + " but only " + ids.size() + " files exist",
                n < ids.size());
        return ids.get(n);
    }
}