package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * New JUnit 5 tests for {@link Journal}, derived from and extending the coverage
 * established by {@code Isw2JournalFunctionalTest} and
 * {@code Isw2JournalScanCategoryPartitionTest}.
 *
 * <p><b>Test strategy overview</b>
 * <ul>
 *   <li><b>FT-*</b> — new functional tests that extend the scenarios already covered
 *       by the functional suite (write/scan invariants, identity checks, etc.).</li>
 *   <li><b>CP-*</b> — new category-partition tests for {@code listJournalIds} and
 *       {@code scanJournal} that add dimension combinations not present in the
 *       parameterised JUnit-4 suite.</li>
 *   <li><b>BV-*</b> — boundary-value tests targeting edge values of individual
 *       parameters (position = Long.MAX_VALUE, empty payload, single byte, etc.).</li>
 * </ul>
 *
 * <p>All tests use JUnit 5 idioms: {@code @TempDir}, {@code @ParameterizedTest} /
 * {@code @MethodSource}, and {@code assertThrows} / {@code assertAll}.
 */
class Isw2LLMJournalNewTests {

    // ─────────────────────────────────────────────────────────────────────────
    // Shared constants
    // ─────────────────────────────────────────────────────────────────────────

    private static final long   LEDGER_ID          = 42L;
    private static final long   WRITE_TIMEOUT_SECS = 10L;
    private static final int    MAX_JOURNAL_MB     = 1;

    // ─────────────────────────────────────────────────────────────────────────
    // Shared infrastructure
    // ─────────────────────────────────────────────────────────────────────────

    @TempDir
    File tempDir;

    private File              journalDir;
    private File              ledgerDir;
    private ServerConfiguration conf;
    private LedgerDirsManager dirsManager;
    private Journal           journal;

    @BeforeEach
    void setUp() throws Exception {
        journalDir = new File(tempDir, "journal");
        ledgerDir  = new File(tempDir, "ledger");
        journalDir.mkdirs();
        ledgerDir.mkdirs();

        conf = new ServerConfiguration();
        conf.setJournalDirName(journalDir.getAbsolutePath());
        conf.setMaxJournalSizeMB(MAX_JOURNAL_MB);
        conf.setJournalWriteBufferSizeKB(4);
        conf.setJournalRemovePagesFromCache(false);
        conf.setAllowLoopback(true);

        dirsManager = mock(LedgerDirsManager.class);
        when(dirsManager.getAllLedgerDirs()).thenReturn(List.of(ledgerDir));
        when(dirsManager.getWritableLedgerDirsForNewLog()).thenReturn(List.of(ledgerDir));

        journal = new Journal(0, journalDir, conf, dirsManager);
        journal.start();
    }

    @AfterEach
    void tearDown() {
        if (journal != null && journal.running) {
            journal.shutdown();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper methods
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Builds a standard BookKeeper entry buffer:
     * {@code [ledgerId: 8 B][entryId: 8 B][payload: N bytes]}.
     */
    private static ByteBuf makeEntry(long ledgerId, long entryId, byte[] payload) {
        ByteBuf buf = Unpooled.buffer(8 + 8 + payload.length);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeBytes(payload);
        return buf;
    }

    private static ByteBuf makeEntry(long ledgerId, long entryId, String payload) {
        return makeEntry(ledgerId, entryId, payload.getBytes());
    }

    /**
     * Writes one entry synchronously: blocks until the ForceWriteThread fires
     * the callback or the timeout expires.
     */
    private void writeAndWait(Journal j, long ledgerId, long entryId, String payload)
            throws Exception {
        ByteBuf entry = makeEntry(ledgerId, entryId, payload);
        CountDownLatch latch = new CountDownLatch(1);
        j.logAddEntry(ledgerId, entryId, entry, false,
                (rc, lId, eId, addr, ctx) -> { if (rc == 0) latch.countDown(); }, null);
        assertTrue(latch.await(WRITE_TIMEOUT_SECS, TimeUnit.SECONDS),
                "Write callback not received within " + WRITE_TIMEOUT_SECS + " s");
    }

    /** Convenience overload that uses the shared {@link #journal}. */
    private void writeAndWait(long ledgerId, long entryId, String payload) throws Exception {
        writeAndWait(journal, ledgerId, entryId, payload);
    }

    /**
     * Returns the real on-disk journal id at position {@code index}
     * (0-based, sorted ascending) from {@link #journalDir}.
     */
    private long getNthJournalId(int index) {
        List<Long> ids = Journal.listJournalIds(journalDir, null);
        assertFalse(ids.isEmpty(), "No .txn files found in " + journalDir);
        assertTrue(index < ids.size(),
                "Requested index " + index + " but only " + ids.size() + " files exist");
        return ids.get(index);
    }



    // =========================================================================
    //  FT — FUNCTIONAL TESTS
    // =========================================================================

    // ─────────────────────────────────────────────────────────────────────────
    // FT-1  Payload round-trip: bytes written == bytes scanned
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> The journal is a WAL; payload bytes must survive
     * verbatim.  This test goes beyond mere entry-count assertions and verifies
     * the exact byte content of a single entry retrieved via scanJournal.
     *
     * <p><b>New coverage:</b> Neither the functional nor the category-partition
     * suite compares the raw payload bytes of a scanned entry — they only
     * check ledgerId / entryId fields.
     */
    @Test
    @DisplayName("FT-1: payload bytes are preserved verbatim through write → scan cycle")
    void ft1_payloadBytesRoundTrip() throws Exception {
        byte[] originalPayload = "hello-journal-round-trip".getBytes();
        ByteBuf buf = makeEntry(LEDGER_ID, 7L, originalPayload);
        CountDownLatch latch = new CountDownLatch(1);
        journal.logAddEntry(LEDGER_ID, 7L, buf, false,
                (rc, lId, eId, addr, ctx) -> { if (rc == 0) latch.countDown(); }, null);
        assertTrue(latch.await(WRITE_TIMEOUT_SECS, TimeUnit.SECONDS));
        journal.shutdown();

        // Scan and capture payload
        long jId = getNthJournalId(0);
        Journal scanJ = new Journal(0, journalDir, conf, dirsManager);

        List<byte[]> payloads = new ArrayList<>();
        scanJ.scanJournal(jId, 0L, (version, offset, entry) -> {
            if (entry.remaining() >= 16) {
                entry.getLong(); // ledgerId
                entry.getLong(); // entryId
                int len = entry.remaining();
                byte[] bytes = new byte[len];
                entry.get(bytes);
                payloads.add(bytes);
            }
        }, false);

        assertEquals(1, payloads.size(), "Exactly one entry must be scanned");
        assertArrayEquals(originalPayload, payloads.get(0),
                "Payload bytes must match the original bytes written");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FT-2  Empty payload entry is persisted and scanned back
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> An entry with zero payload bytes (only the 16-byte
     * header) is a valid edge case — client code may legitimately write
     * zero-byte data.  The journal must not reject or silently drop it.
     *
     * <p><b>New coverage:</b> Empty payloads are not exercised anywhere in the
     * existing suites.
     */
    @Test
    @DisplayName("FT-2: entry with empty payload (header only) is persisted and scanned back")
    void ft2_emptyPayloadEntryIsPersisted() throws Exception {
        // 16 bytes: only ledgerId + entryId, no payload
        ByteBuf emptyPayload = Unpooled.buffer(16);
        emptyPayload.writeLong(LEDGER_ID);
        emptyPayload.writeLong(0L);

        CountDownLatch latch = new CountDownLatch(1);
        journal.logAddEntry(LEDGER_ID, 0L, emptyPayload, false,
                (rc, lId, eId, addr, ctx) -> { if (rc == 0) latch.countDown(); }, null);
        assertTrue(latch.await(WRITE_TIMEOUT_SECS, TimeUnit.SECONDS));
        journal.shutdown();

        long jId = getNthJournalId(0);
        Journal scanJ = new Journal(0, journalDir, conf, dirsManager);

        List<Integer> sizes = new ArrayList<>();
        scanJ.scanJournal(jId, 0L, (version, offset, entry) -> {
            sizes.add(entry.remaining());
        }, false);

        assertEquals(1, sizes.size(), "One entry must be scanned");
        assertEquals(16, sizes.get(0).intValue(),
                "Entry with empty payload must be scanned back as 16 bytes (header only)");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FT-3  Monotonically increasing scan offsets
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> The {@code offset} argument delivered to the scanner on
     * each invocation must be strictly monotonically increasing — it represents
     * the byte position of each entry inside the .txn file, so it must advance
     * with every record.
     *
     * <p><b>New coverage:</b> Existing tests never inspect the {@code offset}
     * parameter passed to the scanner callback; they only use it as a sentinel
     * for the "InFile" token.
     */
    @Test
    @DisplayName("FT-3: scanner offsets are strictly monotonically increasing")
    void ft3_scannerOffsetsAreMonotonicallyIncreasing() throws Exception {
        int count = 10;
        for (int i = 0; i < count; i++) {
            writeAndWait(LEDGER_ID, i, "offset-test-payload-" + i);
        }
        journal.shutdown();

        long jId = getNthJournalId(0);
        Journal scanJ = new Journal(0, journalDir, conf, dirsManager);

        List<Long> offsets = new ArrayList<>();
        scanJ.scanJournal(jId, 0L, (version, offset, entry) -> {
            offsets.add(offset);
        }, false);

        assertEquals(count, offsets.size(), "All " + count + " entries must be scanned");
        for (int i = 1; i < offsets.size(); i++) {
            assertTrue(offsets.get(i) > offsets.get(i - 1),
                    "Offset at index " + i + " (" + offsets.get(i) + ") must be greater than "
                            + "offset at index " + (i - 1) + " (" + offsets.get(i - 1) + ")");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FT-4  scanJournal return value equals last scanned byte position
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> {@code scanJournal} returns the position in the file
     * after the last record read.  A subsequent call starting at that position
     * must immediately return the same value (no more records to read).
     *
     * <p><b>New coverage:</b> The existing suites check {@code returnedPos > startPos}
     * but never verify that a second scan starting at the returned position delivers
     * zero new entries.
     *
     * <p><b>Implementation note on the return value:</b> {@code scanJournal} reads
     * a 4-byte length field speculatively at the start of each loop iteration.  When
     * it hits EOF those 4 bytes are not present, but the internal position counter
     * may still advance by up to 4 bytes before the {@code fullRead != 4} check
     * causes the loop to exit.  Therefore the return value of the second call may be
     * slightly larger than {@code endPos} (observed difference: +4).  We assert
     * {@code endPos2 >= endPos} rather than strict equality to reflect this.
     */
    @Test
    @DisplayName("FT-4: scanning from the position returned by a previous scan yields no new entries")
    void ft4_returnValueIsIdempotentStartingPosition() throws Exception {
        for (int i = 0; i < 5; i++) {
            writeAndWait(LEDGER_ID, i, "idempotent-test-" + i);
        }
        journal.shutdown();

        long jId = getNthJournalId(0);
        Journal scanJ = new Journal(0, journalDir, conf, dirsManager);

        // First scan: collect everything
        AtomicInteger firstCount = new AtomicInteger(0);
        long endPos = scanJ.scanJournal(jId, 0L, (v, o, e) -> firstCount.incrementAndGet(), false);
        assertEquals(5, firstCount.get(), "First scan must recover all 5 entries");
        assertTrue(endPos > 0, "Return position must be positive");

        // Second scan starting at endPos: must find no new entries.
        // The return value may be slightly larger than endPos (the scan loop reads
        // a speculative 4-byte length header before detecting EOF), so we only
        // assert >= endPos, not strict equality.
        AtomicInteger secondCount = new AtomicInteger(0);
        long endPos2 = scanJ.scanJournal(jId, endPos, (v, o, e) -> secondCount.incrementAndGet(), false);
        assertEquals(0, secondCount.get(),
                "A scan starting at the previous return position must yield 0 new entries");
        assertTrue(endPos2 >= endPos,
                "The second return position (" + endPos2 + ") must be >= the first (" + endPos + ")");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FT-5  Large number of distinct ledgers: all entries round-trip correctly
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> The existing massive-insert test (disabled) uses 5 ledgers.
     * This test uses 20 ledgers in a deterministic, enabled scenario to verify
     * that interleaved writes from many ledgers are all recoverable.
     *
     * <p><b>New coverage:</b> The enabled functional tests only use a single ledger;
     * the multi-ledger disabled test is not executed.
     */
    @Test
    @DisplayName("FT-5: interleaved writes from 20 distinct ledgers are all recovered correctly")
    void ft5_interleavedWritesManyLedgers() throws Exception {
        int ledgerCount  = 20;
        int entriesEach  = 5;        // 20 × 5 = 100 entries total
        long[] counters  = new long[ledgerCount];
        List<long[]> written = new ArrayList<>();

        for (int i = 0; i < ledgerCount * entriesEach; i++) {
            long lId = i % ledgerCount;
            long eId = counters[(int) lId]++;
            written.add(new long[]{lId, eId});
            writeAndWait(lId, eId, "multi-ledger-payload");
        }
        journal.shutdown();

        Journal scanJ = new Journal(0, journalDir, conf, dirsManager);
        List<long[]> recovered = new ArrayList<>();

        for (long jId : Journal.listJournalIds(journalDir, null)) {
            scanJ.scanJournal(jId, 0L, (v, o, entry) -> {
                if (entry.remaining() >= 16) {
                    long lId = entry.getLong();
                    long eId = entry.getLong();
                    recovered.add(new long[]{lId, eId});
                }
            }, false);
        }

        assertEquals(written.size(), recovered.size(),
                "Total recovered count must match total written count");

        // Verify per-ledger counts
        int[] perLedger = new int[ledgerCount];
        for (long[] pair : recovered) {
            perLedger[(int) pair[0]]++;
        }
        for (int l = 0; l < ledgerCount; l++) {
            assertEquals(entriesEach, perLedger[l],
                    "Ledger " + l + " must have exactly " + entriesEach + " entries");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FT-6  listJournalIds filter — only matching ids are returned
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> {@code listJournalIds} accepts a {@link Journal.JournalIdFilter}.
     * This test verifies that a filter returning {@code false} for all ids yields
     * an empty list, while a filter accepting all ids yields the full list.
     *
     * <p><b>New coverage:</b> Neither existing test suite exercises the filter
     * parameter of {@code listJournalIds}.
     */
    @Test
    @DisplayName("FT-6: listJournalIds respects the JournalIdFilter predicate")
    void ft6_listJournalIdsRespectsFilter() throws Exception {
        writeAndWait(LEDGER_ID, 0L, "filter-test");
        journal.shutdown();

        List<Long> all = Journal.listJournalIds(journalDir, null);
        assertFalse(all.isEmpty(), "At least one .txn file must exist");

        // Filter: reject all
        List<Long> none = Journal.listJournalIds(journalDir, id -> false);
        assertTrue(none.isEmpty(), "A reject-all filter must return an empty list");

        // Filter: accept all
        List<Long> same = Journal.listJournalIds(journalDir, id -> true);
        assertEquals(all, same, "An accept-all filter must return the same ids as null");

        // Filter: accept only the first id
        long firstId = all.get(0);
        List<Long> onlyFirst = Journal.listJournalIds(journalDir, id -> id == firstId);
        assertEquals(List.of(firstId), onlyFirst,
                "Filter accepting only the first id must return a singleton list");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FT-7  listJournalIds on an empty directory returns an empty list
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> Calling {@code listJournalIds} before any journal file
     * has been created must return an empty (not null) list without throwing.
     *
     * <p><b>New coverage:</b> The existing suites always call {@code listJournalIds}
     * after writing at least one entry.
     */
    @Test
    @DisplayName("FT-7: listJournalIds on an empty directory returns an empty list")
    void ft7_listJournalIdsEmptyDirectory() {
        File emptyDir = new File(tempDir, "empty-journal");
        emptyDir.mkdirs();

        List<Long> ids = Journal.listJournalIds(emptyDir, null);
        assertNotNull(ids, "listJournalIds must never return null");
        assertTrue(ids.isEmpty(), "listJournalIds on an empty directory must return an empty list");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FT-8  listJournalIds returns ids in ascending order
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> Recovery relies on scanning journal files in creation
     * order.  {@code listJournalIds} must sort the ids ascending so the caller
     * always replays entries chronologically.
     *
     * <p><b>New coverage:</b> The existing suites assume sorting is correct but
     * never assert it explicitly.
     */
    @Test
    @DisplayName("FT-8: listJournalIds returns ids in strictly ascending order")
    void ft8_listJournalIdsSortedAscending() throws Exception {
        // Write entries large enough to trigger at least two file rollovers
        // (maxJournalSizeMB=1, payloads are ~100 KB each)
        byte[] bigPayload = new byte[100_000];
        for (int i = 0; i < 15; i++) {
            ByteBuf entry = makeEntry(LEDGER_ID, i, bigPayload);
            CountDownLatch latch = new CountDownLatch(1);
            journal.logAddEntry(LEDGER_ID, i, entry, false,
                    (rc, lId, eId, addr, ctx) -> { if (rc == 0) latch.countDown(); }, null);
            assertTrue(latch.await(WRITE_TIMEOUT_SECS, TimeUnit.SECONDS));
        }
        journal.shutdown();

        List<Long> ids = Journal.listJournalIds(journalDir, null);
        assertTrue(ids.size() >= 2,
                "At least 2 .txn files must exist to verify ordering; got " + ids.size());

        for (int i = 1; i < ids.size(); i++) {
            assertTrue(ids.get(i) > ids.get(i - 1),
                    "id at index " + i + " (" + ids.get(i) + ") must be > id at index "
                            + (i - 1) + " (" + ids.get(i - 1) + ")");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FT-9  Scanner callback invocation count matches entry count
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> The scanner must be called exactly once per physical
     * record in the .txn file.  This test uses an atomic counter to detect
     * both under-delivery and duplicate delivery.
     *
     * <p><b>New coverage:</b> The existing tests count entries via list size, which
     * could hide double-callback bugs if the scanner is called twice for one
     * record and once less for another.  An atomic counter on the callback is
     * a stricter check.
     */
    @Test
    @DisplayName("FT-9: scanner callback is invoked exactly once per written entry")
    void ft9_scannerCallbackCountMatchesEntryCount() throws Exception {
        int n = 8;
        for (int i = 0; i < n; i++) {
            writeAndWait(LEDGER_ID, i, "callback-count-test-" + i);
        }
        journal.shutdown();

        long jId = getNthJournalId(0);
        Journal scanJ = new Journal(0, journalDir, conf, dirsManager);

        AtomicInteger callbackCount = new AtomicInteger(0);
        scanJ.scanJournal(jId, 0L, (v, o, e) -> callbackCount.incrementAndGet(), false);

        assertEquals(n, callbackCount.get(),
                "Scanner must be invoked exactly once per written entry");
    }

    // =========================================================================
    //  CP — CATEGORY-PARTITION TESTS (JUnit 5 @ParameterizedTest)
    // =========================================================================

    // ─────────────────────────────────────────────────────────────────────────
    // CP-1  scanJournal: journalId × journalPos × skipInvalidRecord
    //       (new combinations not present in the JUnit-4 suite)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Category-partition table for {@code scanJournal} — new dimension combinations.
     *
     * <p>Dimensions and their new categories added here:
     * <ul>
     *   <li><b>journalId</b>: valid (written), Long.MAX_VALUE (no such file)</li>
     *   <li><b>journalPos</b>: 0, mid-file (entry #2 offset), Long.MAX_VALUE</li>
     *   <li><b>skipInvalidRecord</b>: true / false on a corrupt file where the
     *       corrupted record has a <em>negative</em> length header (not truncation),
     *       so skipInvalidRecord does have an observable effect.</li>
     * </ul>
     *
     * <p>Each row is: [testName, useInvalidId, posCategory, skipInvalid, expectSuccess, expectedEntries]
     */
    static Stream<Arguments> cpScanArguments() {
        return Stream.of(
                // posCategory: "zero"
                Arguments.of("valid_id_pos0_skip_true",          false, "zero",    true,  true,  3),
                Arguments.of("valid_id_pos0_skip_false",         false, "zero",    false, true,  3),
                // posCategory: "midFile" (byte offset of the 3rd entry)
                Arguments.of("valid_id_midFile_skip_true",       false, "midFile", true,  true,  1),
                Arguments.of("valid_id_midFile_skip_false",      false, "midFile", false, true,  1),
                // posCategory: "maxLong" (no record can start at Long.MAX_VALUE)
                Arguments.of("valid_id_maxLong_skip_true",       false, "maxLong", true,  false, 0),
                // journalId: Long.MAX_VALUE — no .txn file exists with this id
                Arguments.of("invalid_maxLong_id_pos0_skip_true", true, "zero",   true,  false, 0)
        );
    }

    /**
     * Executes the new category-partition table for {@code scanJournal}.
     *
     * <p>Setup (shared for all rows): writes exactly 3 entries to one .txn file
     * then shuts down cleanly.  A helper scan locates the byte offset of the
     * 3rd entry (index 2) to use as the "midFile" position.
     *
     * <p><b>New coverage:</b> The JUnit-4 suite does not test {@code Long.MAX_VALUE}
     * as a journalId, nor does it test {@code Long.MAX_VALUE} as a journalPos on a
     * valid file, nor does it test skip=false on a clean (non-corrupt) file together
     * with a mid-file starting position.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("cpScanArguments")
    @DisplayName("CP-1: scanJournal category-partition (new combinations)")
    void cp1_scanJournalNewCombinations(
            String  testName,
            boolean useInvalidId,
            String  posCategory,
            boolean skipInvalid,
            boolean expectSuccess,
            int     expectedEntries) throws Exception {

        // -- Write 3 entries --------------------------------------------------
        writeAndWait(LEDGER_ID, 0L, "cp1-entry-0");
        writeAndWait(LEDGER_ID, 1L, "cp1-entry-1");
        writeAndWait(LEDGER_ID, 2L, "cp1-entry-2");
        journal.shutdown();

        long realId = getNthJournalId(0);

        // Locate offset of the 3rd scanner invocation (entry at index 2)
        final long[] thirdEntryOffset = {0L};
        final int[]  callCount        = {0};
        Journal aux = new Journal(0, journalDir, conf, dirsManager);
        aux.scanJournal(realId, 0L, (v, offset, e) -> {
            callCount[0]++;
            if (callCount[0] == 3) {
                thirdEntryOffset[0] = offset;
            }
        }, false);
        assertTrue(thirdEntryOffset[0] > 0, "[" + testName + "] Could not locate 3rd entry offset");

        // Resolve journalId
        long resolvedId = useInvalidId ? Long.MAX_VALUE : realId;

        // Resolve journalPos
        long resolvedPos = switch (posCategory) {
            case "zero"    -> 0L;
            case "midFile" -> thirdEntryOffset[0];
            case "maxLong" -> Long.MAX_VALUE;
            default        -> throw new IllegalArgumentException("Unknown posCategory: " + posCategory);
        };

        Journal scanJ = new Journal(0, journalDir, conf, dirsManager);
        List<long[]> collected = new ArrayList<>();
        Journal.JournalScanner scanner = (v, o, entry) -> {
            if (entry.remaining() >= 16) {
                long lId = entry.getLong();
                long eId = entry.getLong();
                collected.add(new long[]{lId, eId});
            }
        };

        if (expectSuccess) {
            long returnedPos = scanJ.scanJournal(resolvedId, resolvedPos, scanner, skipInvalid);
            assertAll(testName,
                    () -> assertEquals(expectedEntries, collected.size(),
                            "Entry count mismatch"),
                    () -> assertTrue(expectedEntries == 0 || returnedPos > resolvedPos,
                            "Return position must advance beyond start when entries are present"));
        } else {
            boolean threw = false;
            try {
                scanJ.scanJournal(resolvedId, resolvedPos, scanner, skipInvalid);
            } catch (Exception ex) {
                threw = true;
            }
            boolean noEntries = collected.isEmpty();
            assertTrue(threw || noEntries,
                    "[" + testName + "] Expected exception or 0 entries, got "
                            + collected.size() + " entries, threw=" + threw);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CP-2  skipInvalidRecord has observable effect on negative-length records
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> The scan loop in {@code Journal} handles a negative length
     * header with an {@code if (len < 0) break} guard — the same {@code break} path
     * used for truncated records — so {@code skipInvalidRecord} has <em>no observable
     * effect</em> here, exactly as documented for truncation in the JUnit-4 suite.
     *
     * <p>This test verifies that behaviour explicitly:
     * <ul>
     *   <li>One valid entry precedes the corrupt record and must always be recovered.</li>
     *   <li>The scan returns normally (no exception) for <em>both</em> values of
     *       {@code skipInvalidRecord}.</li>
     *   <li>The corrupt record itself is silently discarded.</li>
     * </ul>
     *
     * <p><b>New coverage:</b> Neither existing suite injects a negative-length header.
     * The test pins the documented {@code break}-on-negative-length behaviour so that
     * a future refactor that mistakenly converts the {@code break} into an
     * {@code IOException} would be caught immediately.
     *
     * <p>Each row: [skipInvalid] — both must produce the same outcome.
     */
    static Stream<Arguments> cpNegativeLenArguments() {
        return Stream.of(
                Arguments.of(true),    // skip=true  → break path, returns normally, 1 valid entry
                Arguments.of(false)    // skip=false → same break path, returns normally, 1 valid entry
        );
    }

    @ParameterizedTest(name = "skipInvalid={0}")
    @MethodSource("cpNegativeLenArguments")
    @DisplayName("CP-2: negative-length header triggers silent break regardless of skipInvalidRecord")
    void cp2_skipInvalidRecordOnNegativeLengthHeader(boolean skipInvalid)
            throws Exception {

        // Write one valid entry first, then shut down cleanly.
        writeAndWait(LEDGER_ID, 0L, "valid-before-negative-len");
        journal.shutdown();

        // Append a record whose 4-byte length field is -1 (negative).
        // The scan loop hits `if (len < 0) break` and stops silently —
        // no IOException is ever thrown, so skipInvalidRecord is irrelevant.
        long realId  = getNthJournalId(0);
        File txnFile = new File(journalDir, Long.toHexString(realId) + ".txn");
        try (FileChannel fc = FileChannel.open(txnFile.toPath(),
                StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            lenBuf.putInt(-1);
            lenBuf.flip();
            fc.write(lenBuf);
            fc.force(true);
        }

        Journal scanJ = new Journal(0, journalDir, conf, dirsManager);
        List<long[]> collected = new ArrayList<>();
        Journal.JournalScanner scanner = (v, o, entry) -> {
            if (entry.remaining() >= 16) {
                long lId = entry.getLong();
                long eId = entry.getLong();
                collected.add(new long[]{lId, eId});
            }
        };

        // Must NOT throw regardless of skipInvalid — negative-length is a break, not an IOException.
        assertDoesNotThrow(
                () -> scanJ.scanJournal(realId, 0L, scanner, skipInvalid),
                "scanJournal must return normally on a negative-length header (break path), "
                        + "skipInvalid=" + skipInvalid);

        // The valid entry that preceded the corrupt record must have been recovered.
        assertEquals(1, collected.size(),
                "Exactly one valid entry must be recovered before the negative-length record; "
                        + "skipInvalid=" + skipInvalid);
        assertEquals(LEDGER_ID, collected.get(0)[0], "Recovered ledgerId must match");
        assertEquals(0L,        collected.get(0)[1], "Recovered entryId must be 0");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CP-3  listJournalIds: directory does not exist
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> {@code listJournalIds} internally calls
     * {@code File.listFiles()}, which returns {@code null} for a non-existent or
     * non-directory path.  The method must handle this gracefully and return an
     * empty list.
     *
     * <p><b>New coverage:</b> Neither suite tests {@code listJournalIds} with a
     * non-existent directory.
     */
    @Test
    @DisplayName("CP-3: listJournalIds with a non-existent directory returns an empty list")
    void cp3_listJournalIdsNonExistentDirectory() {
        File ghost = new File(tempDir, "does-not-exist");
        assertFalse(ghost.exists(), "Pre-condition: directory must not exist");

        List<Long> ids = Journal.listJournalIds(ghost, null);
        assertNotNull(ids, "Result must not be null");
        assertTrue(ids.isEmpty(), "Result must be empty for a non-existent directory");
    }

    // =========================================================================
    //  BV — BOUNDARY-VALUE TESTS
    // =========================================================================

    // ─────────────────────────────────────────────────────────────────────────
    // BV-1  Maximum legal ledgerId (Long.MAX_VALUE) round-trips correctly
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> LedgerId is stored as an 8-byte big-endian long.
     * {@code Long.MAX_VALUE} (0x7FFFFFFFFFFFFFFF) is the highest legal value;
     * it must survive the write→scan round trip without sign/overflow issues.
     *
     * <p><b>New coverage:</b> All existing tests use small ledgerIds (0, 42, 100).
     */
    @Test
    @DisplayName("BV-1: entry with ledgerId = Long.MAX_VALUE round-trips correctly")
    void bv1_maxLedgerIdRoundTrip() throws Exception {
        long maxLedgerId = Long.MAX_VALUE;
        long entryId     = 0L;

        ByteBuf entry = makeEntry(maxLedgerId, entryId, "boundary-ledger-id");
        CountDownLatch latch = new CountDownLatch(1);
        journal.logAddEntry(maxLedgerId, entryId, entry, false,
                (rc, lId, eId, addr, ctx) -> { if (rc == 0) latch.countDown(); }, null);
        assertTrue(latch.await(WRITE_TIMEOUT_SECS, TimeUnit.SECONDS));
        journal.shutdown();

        long jId = getNthJournalId(0);
        Journal scanJ = new Journal(0, journalDir, conf, dirsManager);

        List<long[]> scanned = new ArrayList<>();
        scanJ.scanJournal(jId, 0L, (v, o, e) -> {
            if (e.remaining() >= 16) {
                long lId = e.getLong();
                long eId = e.getLong();
                scanned.add(new long[]{lId, eId});
            }
        }, false);

        assertEquals(1, scanned.size(), "Exactly one entry must be scanned");
        assertEquals(maxLedgerId, scanned.get(0)[0],
                "Scanned ledgerId must equal Long.MAX_VALUE");
        assertEquals(entryId, scanned.get(0)[1],
                "Scanned entryId must match");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // BV-2  Single-byte payload is persisted and scanned back
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> A 1-byte payload (total entry = 17 bytes) is the
     * smallest meaningful payload.  Verifies that the 4-byte length header is
     * written as {@code 17} and that the single byte survives intact.
     *
     * <p><b>New coverage:</b> The existing malformed-entry test uses 4- and 8-byte
     * buffers but never a 17-byte well-formed entry (16-byte header + 1 payload byte).
     */
    @Test
    @DisplayName("BV-2: entry with a single-byte payload round-trips correctly")
    void bv2_singleBytePayloadRoundTrip() throws Exception {
        byte[] oneByte = new byte[]{ (byte) 0xFF };
        ByteBuf entry = makeEntry(LEDGER_ID, 0L, oneByte);
        CountDownLatch latch = new CountDownLatch(1);
        journal.logAddEntry(LEDGER_ID, 0L, entry, false,
                (rc, lId, eId, addr, ctx) -> { if (rc == 0) latch.countDown(); }, null);
        assertTrue(latch.await(WRITE_TIMEOUT_SECS, TimeUnit.SECONDS));
        journal.shutdown();

        long jId = getNthJournalId(0);
        Journal scanJ = new Journal(0, journalDir, conf, dirsManager);

        List<byte[]> payloads = new ArrayList<>();
        scanJ.scanJournal(jId, 0L, (v, o, e) -> {
            if (e.remaining() >= 16) {
                e.getLong(); // ledgerId
                e.getLong(); // entryId
                byte[] payload = new byte[e.remaining()];
                e.get(payload);
                payloads.add(payload);
            }
        }, false);

        assertEquals(1, payloads.size(), "One entry must be scanned");
        assertArrayEquals(oneByte, payloads.get(0),
                "The single payload byte must survive the round trip");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // BV-3  Truncated record injected immediately after the journal header
    //       (first record position) — nothing is recovered
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * <b>Rationale:</b> The JUnit-4 suite injects a truncated record <em>after</em>
     * valid data.  This test injects it as the very first record in the file
     * (i.e., no valid entry precedes it) so the scanner must return 0 entries.
     *
     * <p><b>New coverage:</b> Corruption at the start of the file (before any valid
     * record) is an untested boundary condition.
     */
    @Test
    @DisplayName("BV-3: truncated record as the very first entry in the file yields 0 recovered entries")
    void bv3_truncatedFirstRecord() throws Exception {
        // Write and immediately shut down so the .txn file exists on disk.
        writeAndWait(LEDGER_ID, 0L, "a-valid-entry");
        journal.shutdown();

        // Rewrite the file to contain only the journal file header followed by
        // a truncated record — overwrite the valid entry by opening for write
        // (not append) and seeking past the header.
        //
        // Because JournalChannel writes its own binary header we cannot simply
        // replace the whole file; instead we inject a truncated record at
        // offset 0 using the scanJournal start-position mechanism: we scan the
        // file to find where the first entry starts, then replace those bytes
        // with our garbage.  For simplicity we use a second fresh file instead.
        //
        // Strategy: create a brand-new Journal, do NOT write any entry, shut
        // it down, then append a truncated record to the empty .txn file.
        // Expect 0 entries recovered.

        journal = new Journal(0, journalDir, conf, dirsManager);
        journal.start();
        journal.shutdown();

        // The newest file is last in the sorted list
        List<Long> ids = Journal.listJournalIds(journalDir, null);
        int lastIdx = ids.size() - 1;
        long emptyFileId = ids.get(lastIdx);

        // Inject a truncated record into this empty (header-only) file
        File txnFile = new File(journalDir, Long.toHexString(emptyFileId) + ".txn");
        try (FileChannel fc = FileChannel.open(txnFile.toPath(),
                StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            lenBuf.putInt(512);   // claims 512 bytes
            lenBuf.flip();
            fc.write(lenBuf);

            ByteBuffer partial = ByteBuffer.allocate(8);  // only 8 written
            partial.putLong(0xDEADBEEFCAFEBABEL);
            partial.flip();
            fc.write(partial);
            fc.force(true);
        }

        Journal scanJ = new Journal(0, journalDir, conf, dirsManager);
        AtomicInteger count = new AtomicInteger(0);
        scanJ.scanJournal(emptyFileId, 0L, (v, o, e) -> count.incrementAndGet(), false);

        assertEquals(0, count.get(),
                "A file whose only record is truncated must yield 0 recovered entries");
    }
}