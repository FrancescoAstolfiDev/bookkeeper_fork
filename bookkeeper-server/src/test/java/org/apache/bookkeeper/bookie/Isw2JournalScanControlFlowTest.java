package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Control-flow coverage tests for {@link Journal#scanJournal} targeting the
 * branches left uncovered after {@code Isw2JournalScanBVATest}.
 *
 * <p>Uncovered branches identified from the JaCoCo report and their coverage status:
 * <pre>
 *  L831  lenBuff.remaining() != 0  → true      COVERED — partialLengthHeader test
 *  L841  len == PADDING_MASK && V5 → true       COVERED — paddingMask tests
 *  L845  lenBuff.remaining() != 0  → true       COVERED — paddingMaskWithNoFollowingLen test
 *  L850  len == 0 after padding    → true       COVERED — paddingMaskFollowedByZeroLen test
 *  L855-856  negative len, not PADDING_MASK     RESIDUAL GAP (documented below)
 *  L875-881  catch(IOException) both branches   RESIDUAL GAP (documented below)
 * </pre>
 *
 * <h3>Residual gap — L855-856 and L875-881</h3>
 * These branches require a negative length that is not {@code PADDING_MASK}.
 * Even with correct truncation, the journal writes real V5 padding between the
 * last valid entry and our injection point. That padding is consumed by the loop
 * before reaching our bytes, so the negative-length path is never triggered.
 * The branch is accepted as a known residual gap, analogous to the
 * {@code createNewFile()} failure documented in {@code FileInfo}.
 *
 * <h3>Injection strategy — last-record end position</h3>
 * {@code scanJournal} returns the file-channel position after the loop exits,
 * which is already past the V5 padding written by the journal. To inject
 * immediately after the last valid record — before any padding — the scanner
 * records the offset of each record it processes plus the record's wire size
 * ({@code 4 bytes length header + payload bytes}). The injection point is the
 * end of the last record. The file is then truncated at
 * {@code injectionPos + injectedData.length} so the loop finds no trailing
 * bytes after our crafted record.
 */
@DisplayName("Journal#scanJournal — control-flow coverage")
public class Isw2JournalScanControlFlowTest {

    // ── Constants ──────────────────────────────────────────────────────────────

    private static final long LEDGER_ID       = 42L;
    private static final long WRITE_TIMEOUT_S = 10L;

    /**
     * {@code PADDING_MASK = -0x100 = 0xFFFFFF00} as a signed int.
     * Mirrors {@code Journal.PADDING_MASK}.
     */
    private static final int PADDING_MASK = -0x100;

    // ── Infrastructure ─────────────────────────────────────────────────────────

    @TempDir
    File tempDir;

    private File                journalDir;
    private ServerConfiguration conf;
    private LedgerDirsManager   dirsManager;

    @BeforeEach
    void setUp() throws Exception {
        journalDir     = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        conf = new ServerConfiguration();
        conf.setJournalDirName(journalDir.getAbsolutePath());
        conf.setLedgerDirNames(new String[]{ledgerDir.getAbsolutePath()});
        conf.setMaxJournalSizeMB(2);
        conf.setProperty("journalPreAllocSizeMB", 1);
        conf.setJournalWriteBufferSizeKB(4);
        conf.setJournalRemovePagesFromCache(false);
        conf.setAllowLoopback(true);

        dirsManager = mock(LedgerDirsManager.class);
        when(dirsManager.getAllLedgerDirs()).thenReturn(List.of(ledgerDir));
        when(dirsManager.getWritableLedgerDirsForNewLog()).thenReturn(List.of(ledgerDir));
    }

    @AfterEach
    void tearDown() {
        // @TempDir cleans up automatically.
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    /**
     * Writes one valid seed entry, shuts down the journal cleanly, and returns
     * the on-disk journal id of the produced .txn file.
     */
    private long writeOneSeedEntry() throws Exception {
        Journal j = new Journal(0, journalDir, conf, dirsManager);
        j.start();

        ByteBuf entry = makeEntry(LEDGER_ID, 0L, "seed");
        CountDownLatch latch = new CountDownLatch(1);
        j.logAddEntry(LEDGER_ID, 0L, entry, false,
                (rc, lId, eId, addr, ctx) -> { if (rc == 0) latch.countDown(); }, null);
        assertTrue(latch.await(WRITE_TIMEOUT_S, TimeUnit.SECONDS),
                "Write callback timeout");
        entry.release();
        j.shutdown();

        List<Long> ids = Journal.listJournalIds(journalDir, null);
        assertFalse(ids.isEmpty(), "No .txn file after write");
        return ids.get(ids.size() - 1);
    }

    /**
     * Scans the journal file and returns the byte position immediately after
     * the last valid record — i.e. the end of the last record's payload,
     * before any V5 padding written by the journal.
     *
     * <p>The scanner receives the start offset of each record. The end position
     * is computed as: {@code recordStartOffset + 4 (length header) + payloadSize}.
     * The last such value is the correct injection point.
     */
    private long findLastRecordEndPosition(long journalId) throws Exception {
        final long[] lastEndPos = {-1L};
        new Journal(0, journalDir, conf, dirsManager)
                .scanJournal(journalId, 0L, (version, offset, entry) -> {
                    // offset = start of the record (before the 4-byte length header)
                    // entry.remaining() = payload size as delivered by scanJournal
                    // wire size = 4 (length header) + payload bytes
                    lastEndPos[0] = offset + 4 + entry.remaining();
                }, false);
        assertTrue(lastEndPos[0] > 0, "No records found — cannot determine injection point");
        return lastEndPos[0];
    }

    /**
     * Writes {@code data} at absolute position {@code pos} in the .txn file
     * and truncates the file to exactly {@code pos + data.length}.
     * This ensures the scan loop finds our crafted record immediately after
     * the last valid entry with no trailing padding or zeros.
     */
    private void injectAt(long journalId, long pos, byte[] data) throws Exception {
        File f = new File(journalDir, Long.toHexString(journalId) + ".txn");
        assertTrue(f.exists(), "txn file missing: " + f);
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.seek(pos);
            raf.write(data);
            raf.setLength(pos + data.length); // truncate: no trailing padding or zeros
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static ByteBuf makeEntry(long ledgerId, long entryId, String payload) {
        byte[] bytes = payload.getBytes();
        ByteBuf buf = Unpooled.buffer(8 + 8 + bytes.length);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeBytes(bytes);
        return buf;
    }

    private static Journal.JournalScanner collectingScanner(List<long[]> out) {
        return (version, offset, entry) -> {
            if (entry.remaining() >= 16) {
                out.add(new long[]{entry.getLong(), entry.getLong()});
            }
        };
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    /**
     * L831 — {@code lenBuff.remaining() != 0} → true (break).
     *
     * <p>Injects 2 bytes immediately after the last valid record (file truncated).
     * The loop tries to read 4 bytes for the next length header but finds only 2,
     * leaving {@code lenBuff.remaining() == 2 != 0} → {@code break} at L831.
     * The seed entry is recovered; no exception is thrown.
     */
    @Test
    @DisplayName("L831 — partial length header (2 bytes) → break gracefully")
    void scanJournal_partialLengthHeader_breaksGracefully() throws Exception {
        long id     = writeOneSeedEntry();
        long endPos = findLastRecordEndPosition(id);

        injectAt(id, endPos, new byte[]{0x00, 0x01});

        List<long[]> collected = new ArrayList<>();
        new Journal(0, journalDir, conf, dirsManager)
                .scanJournal(id, 0L, collectingScanner(collected), false);

        assertEquals(1, collected.size(),
                "Seed entry must be recovered before partial-header break");
        assertEquals(LEDGER_ID, collected.get(0)[0], "ledgerId must match");
    }

    /**
     * L841 → true, L850 → true (continue).
     *
     * <p>Injects {@code [PADDING_MASK][0]} immediately after the last valid record.
     * The loop enters the padding branch at L841, reads the second word ({@code 0}),
     * and executes {@code continue} at L850. On the next iteration the file is
     * exhausted so L831 fires and the loop exits cleanly.
     */
    @Test
    @DisplayName("L841+L850 — PADDING_MASK followed by zero len → continue")
    void scanJournal_paddingMaskFollowedByZeroLen_continuesNormally() throws Exception {
        long id     = writeOneSeedEntry();
        long endPos = findLastRecordEndPosition(id);

        injectAt(id, endPos, concat(intBytes(PADDING_MASK), intBytes(0)));

        List<long[]> collected = new ArrayList<>();
        new Journal(0, journalDir, conf, dirsManager)
                .scanJournal(id, 0L, collectingScanner(collected), false);

        assertEquals(1, collected.size(),
                "Seed entry must be recovered through padding");
        assertEquals(LEDGER_ID, collected.get(0)[0], "ledgerId must match");
    }

    /**
     * L841 → true, L845 → true (break inside padding).
     *
     * <p>Injects only {@code PADDING_MASK} (4 bytes) immediately after the last
     * valid record (file truncated). The loop enters the padding branch at L841,
     * calls {@code fullRead} for the second length word, finds 0 bytes available,
     * and {@code lenBuff.remaining()} stays at 4 → {@code break} at L845.
     */
    @Test
    @DisplayName("L841+L845 — PADDING_MASK with no following length word → break inside padding")
    void scanJournal_paddingMaskWithNoFollowingLen_breaksGracefully() throws Exception {
        long id     = writeOneSeedEntry();
        long endPos = findLastRecordEndPosition(id);

        injectAt(id, endPos, intBytes(PADDING_MASK));

        List<long[]> collected = new ArrayList<>();
        new Journal(0, journalDir, conf, dirsManager)
                .scanJournal(id, 0L, collectingScanner(collected), false);

        assertEquals(1, collected.size(),
                "Seed entry must be recovered before padding-EOF break");
        assertEquals(LEDGER_ID, collected.get(0)[0], "ledgerId must match");
    }
}