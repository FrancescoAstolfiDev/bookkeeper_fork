package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Functional tests for the BookKeeper Journal.
 * Validates the physical persistence of entries and the ability to scan them back.
 */
public class Isw2JournalFunctionalTests {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private Journal journal;
    private ServerConfiguration conf;
    private LedgerDirsManager dirsManager;
    private File journalDir;

    private static final int  VALID_ENTRY_COUNT = 5;
    private static final long LEDGER_ID         = 100L;
    private static final int  MAX_JOURNAL_SIZE_MB   = 1;
    private static final int  ENTRY_PAYLOAD_SIZE    = 20_000;

    /** Total number of entries written across all ledgers. */
    private static final int  TOTAL_ENTRIES         = 500;

    /** Number of distinct ledgers to distribute writes across. */
    private static final int  LEDGER_COUNT          = 5;

    /**
     * Minimum number of .txn files we assert must have been created.
     * Given the sizes above the actual number will be ~10; we assert ≥ 3
     * to keep the check meaningful but robust to minor timing variations.
     */
    private static final int  MIN_EXPECTED_FILES    = 3;

    /** Per-write callback timeout. */
    private static final long WRITE_TIMEOUT_SECS    = 10L;



    // -----------------------------------------------------------------------
    // Setup / teardown
    // -----------------------------------------------------------------------

    @Before
    public void setup() throws Exception {
        journalDir = tempDir.newFolder("journal");
        File ledgerDir = tempDir.newFolder("ledger");

        conf = new ServerConfiguration();
        conf.setJournalDirName(journalDir.getAbsolutePath());
        conf.setJournalRemovePagesFromCache(false);

        dirsManager = mock(LedgerDirsManager.class);
        when(dirsManager.getAllLedgerDirs()).thenReturn(List.of(ledgerDir));
        when(dirsManager.getWritableLedgerDirsForNewLog()).thenReturn(List.of(ledgerDir));

        journal = new Journal(0, journalDir, conf, dirsManager);
        journal.start();
    }

    @After
    public void tearDown() {
        if (journal != null && journal.running) {
            journal.shutdown();
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Build a correctly-formatted entry buffer: ledgerId (8 B) | entryId (8 B) | payload.
     * This is the layout that logAddEntry expects and that scanJournal delivers back.
     */
    private static ByteBuf makeEntry(long ledgerId, long entryId, String payload) {
        byte[] payloadBytes = payload.getBytes();
        ByteBuf buf = Unpooled.buffer(8 + 8 + payloadBytes.length);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeBytes(payloadBytes);
        return buf;
    }
    private static ByteBuf makeEntry(long ledgerId, long entryId, String payload,int payload_size) {
        byte[] payloadBytes = payload.getBytes();
        ByteBuf buf = Unpooled.buffer(8 + 8 + payload_size);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeBytes(payloadBytes);
        return buf;
    }

    /**
     * Write one entry to the journal and block until the write callback fires (max 5 s).
     * Each call to writeAndWait guarantees that the entry has been acknowledged by the
     * ForceWriteThread before returning — no separate latch needed at call sites.
     */
    private void writeAndWait(long ledgerId, long entryId, String payload) throws Exception {
        ByteBuf entry = makeEntry(ledgerId, entryId, payload);
        CountDownLatch latch = new CountDownLatch(1);

        journal.logAddEntry(ledgerId, entryId, entry, false, (rc, lId, eId, addr, ctx) -> {
            if (rc == 0) latch.countDown();
        }, null);

        assertTrue(
                "Write callback for entry (" + ledgerId + "," + entryId + ") not received within 5 s",
                latch.await(WRITE_TIMEOUT_SECS, TimeUnit.SECONDS)
        );
    }
    private void writeAndWait(long ledgerId, long entryId, String payload, int payload_size) throws Exception {
        ByteBuf entry = makeEntry(ledgerId, entryId, payload,payload_size);
        CountDownLatch latch = new CountDownLatch(1);

        journal.logAddEntry(ledgerId, entryId, entry, false, (rc, lId, eId, addr, ctx) -> {
            if (rc == 0) latch.countDown();
        }, null);

        assertTrue(
                "Write callback for entry (" + ledgerId + "," + entryId + ") not received within 5 s",
                latch.await(WRITE_TIMEOUT_SECS, TimeUnit.SECONDS)
        );
    }

    // -----------------------------------------------------------------------
    // Test 1 – multiple entries are persisted in insertion order
    // -----------------------------------------------------------------------

    @Test
    public void testJournalWritesMultipleEntriesInOrder() throws Exception {

        // 1. Write VALID_ENTRY_COUNT entries, each confirmed before the next is sent.
        //    writeAndWait() already blocks until the callback fires, so by the time
        //    the loop ends every entry is guaranteed to be on disk.
        //    BUG FIXED: the original code created a second CountDownLatch(VALID_ENTRY_COUNT)
        //    that was never counted down, causing a guaranteed 10-second timeout.
        for (int i = 1; i <= VALID_ENTRY_COUNT; i++) {
            writeAndWait(LEDGER_ID, i, "entry-payload-" + i);
        }

        // 2. Shut down to seal and fully flush the .txn file.
        journal.shutdown();

        // 3. Build the list of entry ids we expect to read back (1 … VALID_ENTRY_COUNT).
        //    BUG FIXED: the original code left expectedEntries empty, so assertEquals
        //    always compared 0 (expected) against the real count (actual) and always failed.
        List<Long> expectedEntryIds = new ArrayList<>();
        for (long i = 1; i <= VALID_ENTRY_COUNT; i++) {
            expectedEntryIds.add(i);
        }

        // 4. Scan and collect the entry ids in the order the scanner delivers them.
        List<Long> actualEntryIds = new ArrayList<>();

        List<Long> journalIds = Journal.listJournalIds(journalDir, null);
        assertFalse("No .txn file found after shutdown", journalIds.isEmpty());
        long journalId = journalIds.get(0);

        journal.scanJournal(journalId, 0, (version, offset, entry) -> {
            // The ByteBuffer starts with ledgerId (8 B) then entryId (8 B).
            if (entry.remaining() >= 16) {
                long lId = entry.getLong();
                long eId = entry.getLong();
                if (lId == LEDGER_ID) {
                    actualEntryIds.add(eId);
                }
            }
        }, false);

        // 5. Assertions: count and order must both match.
        assertEquals("Number of recovered entries mismatch",
                expectedEntryIds.size(), actualEntryIds.size());
        assertEquals("Entries were not recovered in the written order",
                expectedEntryIds, actualEntryIds);
    }

    // -----------------------------------------------------------------------
    // Test 2 – only valid entries survive a mid-write crash (truncated record)
    // -----------------------------------------------------------------------

    /**
     * Scenario:
     *  1. Write VALID_ENTRY_COUNT entries and wait for each ack.
     *  2. Shut down cleanly so BufferedChannel is fully flushed to disk.
     *  3. Manually append a truncated record to the .txn file:
     *       [4-byte length = DECLARED_ENTRY_SIZE][PARTIAL_BYTES of garbage]
     *     This is byte-for-byte identical to a process being killed after the
     *     OS flushed the length word but before it flushed the entry body.
     *  4. Re-scan from offset 0 with a fresh Journal (no start()).
     *  5. Assert that exactly VALID_ENTRY_COUNT entries are recovered and that
     *     the truncated record is silently discarded.
     *
     * Why manual corruption instead of Thread.interrupt()?
     *   Interrupting the journal thread races with the ForceWriteThread; the
     *   number of entries committed before the interrupt is non-deterministic.
     *   Corrupting the file after a clean shutdown gives a 100 % reproducible
     *   truncated record at a known byte offset.
     */
    @Test
    public void testScanRecoversOnlyValidEntriesAfterInterruptedWrite() throws Exception {

        // Phase 1: write VALID_ENTRY_COUNT entries, each confirmed before the next.
        for (int i = 0; i < VALID_ENTRY_COUNT; i++) {
            writeAndWait(LEDGER_ID, i, "entry-payload-" + i);
        }

        // Phase 2: clean shutdown — guarantees BufferedChannel is fully flushed.
        journal.shutdown();

        // Phase 3: append a truncated record to simulate a mid-write crash.
        //
        // On-disk format per record:
        //   [int length  – 4 bytes]   ← payload byte count that follows
        //   [byte[] data – length bytes]
        //
        // We write a length header that promises DECLARED_ENTRY_SIZE bytes,
        // then write only PARTIAL_BYTES — scanJournal's fullRead() will get
        // fewer bytes than declared and break out of its loop silently
        // (Journal.java line 864-868: if fullRead != len → break).
        final int DECLARED_ENTRY_SIZE = 128;
        final int PARTIAL_BYTES       = 32;

        List<Long> journalIds = Journal.listJournalIds(journalDir, null);
        assertFalse("No .txn file found after shutdown", journalIds.isEmpty());
        long journalId = journalIds.get(0);

        File txnFile = new File(journalDir, Long.toHexString(journalId) + ".txn");
        assertTrue("Journal file does not exist: " + txnFile, txnFile.exists());

        try (FileChannel fc = FileChannel.open(txnFile.toPath(),
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {
            // Length header: promises DECLARED_ENTRY_SIZE bytes of payload.
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            lenBuf.putInt(DECLARED_ENTRY_SIZE);
            lenBuf.flip();
            fc.write(lenBuf);

            // Partial payload: only PARTIAL_BYTES written (crash simulation).
            ByteBuffer partialPayload = ByteBuffer.allocate(PARTIAL_BYTES);
            for (int i = 0; i < PARTIAL_BYTES; i++) {
                partialPayload.put((byte) 0xAB);   // recognisable garbage pattern
            }
            partialPayload.flip();
            fc.write(partialPayload);

            fc.force(true);
        }

        // Phase 4: open a fresh Journal instance for scanning only (no start()).
        // scanJournal() opens the .txn file via JournalChannel directly and
        // does not require the write thread to be running.
        Journal scanJournal = new Journal(0, journalDir, conf, dirsManager);

        List<Long[]> recovered = new ArrayList<>();  // [ledgerId, entryId] per entry

        scanJournal.scanJournal(journalId, 0, (version, offset, entry) -> {
            if (entry.remaining() >= 16) {
                entry.mark();
                long lId = entry.getLong();
                long eId = entry.getLong();
                entry.reset();
                recovered.add(new Long[]{lId, eId});
            }
        }, false);  // skipInvalidRecord=false: a truncated record just stops the loop,
        // it never throws, so this flag does not affect this scenario.

        // Phase 5: only the VALID_ENTRY_COUNT good entries must have been recovered.
        assertEquals(
                "Scanner should recover exactly " + VALID_ENTRY_COUNT + " valid entries",
                VALID_ENTRY_COUNT,
                recovered.size()
        );

        for (int i = 0; i < VALID_ENTRY_COUNT; i++) {
            assertEquals("Recovered ledger id mismatch at index " + i,
                    LEDGER_ID, (long) recovered.get(i)[0]);
            assertEquals("Recovered entry id mismatch at index " + i,
                    (long) i,  (long) recovered.get(i)[1]);
        }
    }
    @Test
    public void testMassiveInsertAcrossMultipleJournalFiles() throws Exception {

        // ── Phase 1: write TOTAL_ENTRIES across LEDGER_COUNT ledgers ─────
        //
        // Entries are distributed round-robin across ledgers so that each
        // ledger gets TOTAL_ENTRIES / LEDGER_COUNT entries.  We track the
        // exact (ledgerId, entryId) pairs written so the scan assertion is
        // precise rather than just a count check.
        //
        // entryId is per-ledger and starts at 0.
        long[] entryCounters = new long[LEDGER_COUNT];  // next entryId per ledger
        // writtenPairs[i] = { ledgerId, entryId } for entry i in insertion order.
        List<long[]> writtenPairs = new ArrayList<>(TOTAL_ENTRIES);

        for (int i = 0; i < TOTAL_ENTRIES; i++) {
            long ledgerId = i % LEDGER_COUNT;           // ledger ids: 0 … LEDGER_COUNT-1
            long entryId  = entryCounters[(int) ledgerId]++;
            writtenPairs.add(new long[]{ledgerId, entryId});
            writeAndWait(ledgerId, entryId,"entry-payload-" + i,ENTRY_PAYLOAD_SIZE);
        }

        // ── Phase 2: shutdown to seal all open .txn files ────────────────
        journal.shutdown();

        // ── Phase 3: assert that at least MIN_EXPECTED_FILES files exist ─
        List<Long> journalIds = Journal.listJournalIds(journalDir, null);
        assertTrue(
                "Expected at least " + MIN_EXPECTED_FILES + " journal files due to rollover, "
                        + "but found only " + journalIds.size()
                        + ". Check MAX_JOURNAL_SIZE_MB and ENTRY_PAYLOAD_SIZE.",
                journalIds.size() >= MIN_EXPECTED_FILES
        );
        System.out.println("[INFO] Journal files created: " + journalIds.size());

        // ── Phase 4: scan every file and collect recovered entries ────────
        //
        // We scan in ascending journalId order (the same order files were
        // created) and accumulate all recovered (ledgerId, entryId) pairs into
        // a flat list.  Within a single file the scanner delivers entries in
        // write order; across files the concatenated list must preserve the
        // global insertion order.
        Journal scanJournal = new Journal(0, journalDir, conf, dirsManager);
        // No start() — scanJournal() opens each .txn file directly.

        List<long[]> recoveredPairs = new ArrayList<>(TOTAL_ENTRIES);

        for (long journalId : journalIds) {
            scanJournal.scanJournal(journalId, 0, (version, offset, entry) -> {
                // Each ByteBuffer starts with ledgerId (8 B) then entryId (8 B).
                if (entry.remaining() >= 16) {
                    entry.mark();
                    long lId = entry.getLong();
                    long eId = entry.getLong();
                    entry.reset();
                    recoveredPairs.add(new long[]{lId, eId});
                }
            }, false);
        }

        System.out.println("[INFO] Total entries recovered: " + recoveredPairs.size());

        // ── Phase 5: assertions ───────────────────────────────────────────

        // 5a. Total count must match exactly.
        assertEquals(
                "Total recovered entry count does not match total written count",
                TOTAL_ENTRIES,
                recoveredPairs.size()
        );

        // 5b. Global insertion order must be preserved across all files.
        //     The journal processes entries sequentially, so the flat scan
        //     must reproduce the exact (ledgerId, entryId) sequence we wrote.
        for (int i = 0; i < TOTAL_ENTRIES; i++) {
            long[] written   = writtenPairs.get(i);
            long[] recovered = recoveredPairs.get(i);
            assertEquals(
                    "LedgerId mismatch at position " + i
                            + " (expected ledger=" + written[0] + " entry=" + written[1] + ")",
                    written[0], recovered[0]
            );
            assertEquals(
                    "EntryId mismatch at position " + i
                            + " (expected ledger=" + written[0] + " entry=" + written[1] + ")",
                    written[1], recovered[1]
            );
        }

        // 5c. No duplicates: each (ledgerId, entryId) must appear exactly once.
        //     We reconstruct the per-ledger entry id sets and check for gaps /
        //     duplicates independently of insertion order.
        long[][] perLedgerMax = new long[LEDGER_COUNT][1];
        int[]    perLedgerSeen = new int[LEDGER_COUNT];
        for (long[] pair : recoveredPairs) {
            int ledgerIdx = (int) pair[0];
            perLedgerSeen[ledgerIdx]++;
        }
        long expectedPerLedger = TOTAL_ENTRIES / LEDGER_COUNT;
        for (int l = 0; l < LEDGER_COUNT; l++) {
            assertEquals(
                    "Ledger " + l + " should have exactly " + expectedPerLedger + " entries",
                    expectedPerLedger,
                    perLedgerSeen[l]
            );
        }
    }
}