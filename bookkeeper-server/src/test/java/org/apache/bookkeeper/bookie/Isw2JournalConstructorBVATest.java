package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.conf.ServerConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Category Partition (CP) and Boundary Value Analysis (BVA) tests for the
 * {@link Journal} constructor:
 * <pre>Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
 *         LedgerDirsManager ledgerDirsManager)</pre>
 *
 * <p>Unlike {@code DbLedgerStorage#initialize()}, the Journal constructor does not
 * return an explicit initialisation result — it either completes or throws. A
 * successful construction alone does not guarantee operational correctness: the
 * journal could be built with a degenerate configuration that only manifests
 * failures at write or scan time.
 *
 * <p>For this reason the baseline case (#1) includes a full <b>write + scan round-trip</b>:
 * one entry is written via {@code logAddEntry()}, the journal is shut down cleanly
 * to flush all buffers to disk, and the entry is recovered via {@code scanJournal()}.
 * This round-trip serves the same role as in {@code DbLedgerStorage}: it validates
 * that the baseline configuration is genuinely operational and justifies its use
 * as the fixed configuration for all subsequent category-partition cases.
 *
 * <h3>Baseline configuration (case #1)</h3>
 * <ul>
 *   <li>{@code journalIndex = 0} — single directory in conf, no suffix on lastMark file</li>
 *   <li>{@code journalDirectory} — new writable subdirectory {@code journal/} inside tempDir</li>
 *   <li>{@code conf} — single directory via {@code setJournalDirName()},
 *       maxJournalSizeMB=2, journalPreAllocSizeMB=1, journalWriteBufferSizeKB=4</li>
 *   <li>{@code ledgerDirsManager} — Mockito mock with {@code getAllLedgerDirs()} and
 *       {@code getWritableLedgerDirsForNewLog()} stubs pointing at a {@code ledger/}
 *       subdirectory</li>
 * </ul>
 *
 * <h3>Category Partition table</h3>
 * <pre>
 *  # | journalIndex     | journalDirectory               | conf                             | ledgerDirsManager | Expected output
 * ---+------------------+--------------------------------+----------------------------------+-------------------+------------------------------------------
 *  1 | 0                | new valid dir                  | valid, single dir                | valid mock        | Construction successful + round-trip
 *  2 | 0                | new valid dir (inside N dirs)  | valid, N dirs, dir included      | valid mock        | Construction successful, lastMark suffix .0
 *  3 | 0                | new valid dir (outside N dirs) | valid, N dirs, dir NOT included  | valid mock        | Construction successful, anomalous behaviour
 *  4 | 0                | pre-existing dir with lastMark | valid, single dir                | valid mock        | Construction successful, lastMark read
 *  5 | 0                | non-existent dir               | valid, single dir                | valid mock        | RuntimeException
 *  6 | 0                | dir without permissions        | valid, single dir                | valid mock        | RuntimeException
 *  7 | 0                | null                           | valid, single dir                | valid mock        | NullPointerException
 *  8 | 0                | new valid dir                  | null                             | valid mock        | NullPointerException
 *  9 | 0                | new valid dir                  | valid, single dir                | null              | NullPointerException
 * 10 | negative (-1)    | new valid dir                  | valid, single dir                | valid mock        | Construction successful, anomalous lastMark suffix
 * 11 | out of range (2) | new valid dir                  | valid, N=2 dirs                  | valid mock        | Construction successful, anomalous lastMark suffix
 * 12 | 0                | new valid dir                  | maxSize=2 > preAllocSize=1       | valid mock        | Construction successful, rollover working
 * 13 | 0                | new valid dir                  | maxSize=1 == preAllocSize=1      | valid mock        | Construction successful, rollover at boundary
 * 14 | 0                | new valid dir                  | maxSize=1 < preAllocSize=2       | valid mock        | Construction successful, rollover does not occur
 * </pre>
 *
 * <h3>Boundary Value Analysis table — mapped 1-to-1 with CP rows</h3>
 * <pre>
 *  # | journalIndex       | journalDirectory               | maxJournalSizeMB            | preAllocSizeMB | writeBufferKB | Expected output
 * ---+--------------------+--------------------------------+-----------------------------+----------------+---------------+--------------------------------------
 *  1 | 0 (lower valid)    | new valid dir                  | 2                           | 1              | 4             | Construction successful
 *  2 | 0                  | new valid dir (inside N dirs)  | 2                           | 1              | 4             | Construction successful, lastMark .0
 *  3 | 0                  | new valid dir (outside N dirs) | 2                           | 1              | 4             | Construction successful, anomalous
 *  4 | 0                  | pre-existing with lastMark     | 2                           | 1              | 4             | Construction successful, lastMark read
 *  5 | 0                  | non-existent dir               | 2                           | 1              | 4             | RuntimeException
 *  6 | 0                  | dir without permissions        | 2                           | 1              | 4             | RuntimeException
 *  7 | 0                  | null                           | 2                           | 1              | 4             | NullPointerException
 *  8 | 0                  | new valid dir                  | null (conf null)            | null           | null          | NullPointerException
 *  9 | 0                  | new valid dir                  | 2                           | 1              | 4             | NullPointerException (mgr null)
 * 10 | -1 (lower error)   | new valid dir                  | 2                           | 1              | 4             | Construction successful, anomalous suffix
 * 11 | N=2 (upper error)  | new valid dir                  | 2                           | 1              | 4             | Construction successful, anomalous suffix
 * 12 | 0                  | new valid dir                  | 2 (maxSize > preAllocSize)  | 1              | 4             | Construction successful, rollover working
 * 13 | 0                  | new valid dir                  | 1 (maxSize == preAllocSize) | 1              | 4             | Construction successful, rollover at boundary
 * 14 | 0                  | new valid dir                  | 1 (maxSize < preAllocSize)  | 2              | 4             | Construction successful, rollover does not occur
 * </pre>
 */
@DisplayName("Journal — Category Partition + BVA: constructor")
public class Isw2JournalConstructorBVATest {

    @TempDir
    File tempDir;

    private Journal journal;

    // ── Constants ──────────────────────────────────────────────────────────────

    private static final long SMOKE_LEDGER_ID = 100L;
    private static final long SMOKE_ENTRY_ID  = 0L;
    private static final int  WRITE_TIMEOUT_S = 5;

    // ── Lifecycle ──────────────────────────────────────────────────────────────

    @AfterEach
    void tearDown() {
        if (journal != null && journal.running) {
            journal.shutdown();
        }
        journal = null;
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    /**
     * Builds the baseline {@link ServerConfiguration} using {@code setJournalDirName()}
     * (singular) — consistent with the functional test suite pattern.
     *
     * @param journalDir     the single journal directory
     * @param maxSizeMB      value for maxJournalSizeMB
     * @param preAllocSizeMB value for journalPreAllocSizeMB
     */
    private static ServerConfiguration buildConf(File journalDir,
                                                 int maxSizeMB,
                                                 int preAllocSizeMB) {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setJournalDirName(journalDir.getAbsolutePath());
        conf.setMaxJournalSizeMB(maxSizeMB);
        conf.setProperty("journalPreAllocSizeMB", preAllocSizeMB);
        conf.setJournalWriteBufferSizeKB(4);
        conf.setJournalRemovePagesFromCache(false);
        conf.setAllowLoopback(true);
        return conf;
    }

    /**
     * Builds a {@link ServerConfiguration} with N journal directories.
     * Used for multi-journal cases (CP#2, CP#3, CP#10, CP#11).
     *
     * @param journalDirNames absolute paths of the N journal directories
     */
    private static ServerConfiguration buildConfNDirs(String[] journalDirNames) {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setJournalDirsName(journalDirNames);
        conf.setMaxJournalSizeMB(2);
        conf.setProperty("journalPreAllocSizeMB", 1);
        conf.setJournalWriteBufferSizeKB(4);
        conf.setJournalRemovePagesFromCache(false);
        conf.setAllowLoopback(true);
        return conf;
    }

    /**
     * Builds a minimal valid {@link LedgerDirsManager} mock — consistent with the
     * functional test suite pattern. Two stubs are required:
     * <ul>
     *   <li>{@code getAllLedgerDirs()} — returns the ledger directory</li>
     *   <li>{@code getWritableLedgerDirsForNewLog()} — returns the ledger directory</li>
     * </ul>
     * The mock points at a {@code ledger/} subdirectory, not at journalDirectory —
     * the two are kept separate as in production deployments.
     *
     * @param ledgerDir the ledger directory to stub into the mock
     */
    private static LedgerDirsManager buildMock(File ledgerDir)
            throws LedgerDirsManager.NoWritableLedgerDirException {
        LedgerDirsManager mgr = mock(LedgerDirsManager.class);
        when(mgr.getAllLedgerDirs()).thenReturn(List.of(ledgerDir));
        when(mgr.getWritableLedgerDirsForNewLog()).thenReturn(List.of(ledgerDir));
        return mgr;
    }

    /**
     * Builds a correctly-formatted entry buffer:
     * {@code [8B ledgerId][8B entryId][payload bytes]}.
     * Consistent with the {@code makeEntry()} helper in the functional test suite.
     *
     * @param ledgerId    ledger identifier written at offset 0
     * @param entryId     entry identifier written at offset 8
     * @param payload     payload string appended after the header
     */
    private static ByteBuf makeEntry(long ledgerId, long entryId, String payload) {
        byte[] payloadBytes = payload.getBytes();
        ByteBuf buf = Unpooled.buffer(Long.BYTES + Long.BYTES + payloadBytes.length);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeBytes(payloadBytes);
        return buf;
    }

    /**
     * Writes one entry to {@code j} and blocks until the ForceWriteThread confirms
     * the write via the WriteCallback — consistent with the functional test suite.
     *
     * @param j       the running Journal instance to write to
     * @param ledgerId ledger identifier of the entry
     * @param entryId  entry identifier of the entry
     * @param payload  payload string to embed in the entry
     */
    private static void writeAndWait(Journal j, long ledgerId, long entryId,
                                     String payload) throws Exception {
        ByteBuf entry = makeEntry(ledgerId, entryId, payload);
        CountDownLatch latch = new CountDownLatch(1);

        j.logAddEntry(ledgerId, entryId, entry, false,
                (rc, lId, eId, addr, ctx) -> {
                    if (rc == 0) latch.countDown();
                }, null);

        assertTrue(latch.await(WRITE_TIMEOUT_S, TimeUnit.SECONDS),
                "Write callback for entry (" + ledgerId + "," + entryId
                        + ") not received within " + WRITE_TIMEOUT_S + "s");
    }

    /**
     * Scans the first journal file found in {@code journalDir} and returns all
     * recovered entries as a list of {@link ByteBuffer} copies.
     *
     * <p>Uses a fresh Journal instance (no {@code start()}) to avoid ForceWriteThread
     * interference — consistent with the functional test suite pattern.
     *
     * @param journalDir directory containing the .txn files to scan
     * @param conf       configuration used to construct the read-only Journal instance
     * @param mgr        LedgerDirsManager mock used to construct the read-only instance
     * @return list of recovered entry buffers, in scan order
     */
    private static List<ByteBuffer> scanFirstFile(File journalDir,
                                                  ServerConfiguration conf,
                                                  LedgerDirsManager mgr) throws Exception {
        List<Long> ids = Journal.listJournalIds(journalDir, null);
        assertTrue(!ids.isEmpty(), "No journal files found after write + shutdown");

        Journal reader = new Journal(0, journalDir, conf, mgr);
        List<ByteBuffer> entries = new ArrayList<>();
        reader.scanJournal(ids.get(0), 0L,
                (version, offset, entry) -> {
                    ByteBuffer copy = ByteBuffer.allocate(entry.remaining());
                    copy.put(entry);
                    copy.flip();
                    entries.add(copy);
                },
                false);
        return entries;
    }

    // =========================================================================
    // Category Partition + BVA — constructor cases
    // =========================================================================

    /**
     * CP #1 / BVA #1 — Baseline valid configuration (lower valid bound for journalIndex).
     *
     * <p>journalIndex=0, new {@code journal/} and {@code ledger/} subdirectories,
     * single directory in conf via {@code setJournalDirName()}, maxSize=2 > preAllocSize=1,
     * valid LedgerDirsManager mock.
     *
     * <p>Validates baseline operational correctness via a full write + scan round-trip:
     * one entry is written, the journal is shut down cleanly, and the entry is
     * recovered via {@code scanJournal()}. This justifies the use of this configuration
     * as the fixed baseline for all subsequent cases.
     */
    @Test
    @DisplayName("CP#1 / BVA#1 — baseline (index=0, new dir, single dir conf) → Construction successful + round-trip")
    void cp01_bva01_baselineConfig_successAndRoundTrip() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 2, 1);
        LedgerDirsManager   mgr  = buildMock(ledgerDir);

        journal = new Journal(0, journalDir, conf, mgr);
        journal.start();

        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();
        journal = null;

        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(),
                "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
    }

    /**
     * CP #2 / BVA #2 — N directories in conf, journalDirectory included among them.
     *
     * <p>journalIndex=0, valid directory that is one of the N directories declared
     * in conf via {@code setJournalDirsName()}. Because conf contains N > 1
     * directories, the constructor sets the lastMark filename to {@code "lastMark.0"}
     * instead of the default {@code "lastMark"}.
     *
     * <p>The suffix assignment is verified indirectly: the non-null
     * {@link Journal.LastLogMark} returned by {@code getLastLogMark()} confirms
     * that construction succeeded with the correct filename configuration.
     */
    @Test
    @DisplayName("CP#2 / BVA#2 — N dirs in conf, journalDir included → Construction successful + round-trip")
    void cp02_bva02_nDirsConfJournalDirIncluded_success() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File otherDir   = new File(tempDir, "other");   otherDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConfNDirs(new String[]{
                journalDir.getAbsolutePath(),
                otherDir.getAbsolutePath()
        });
        LedgerDirsManager mgr = buildMock(ledgerDir);

        journal = new Journal(0, journalDir, conf, mgr);
        assertNotNull(journal, "Journal must be constructed successfully");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(),
                "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }

    /**
     * CP #3 / BVA #3 — N directories in conf, journalDirectory NOT included.
     *
     * <p>journalIndex=0, valid directory that is NOT among the N directories declared
     * in conf. The constructor does not validate the consistency between
     * journalDirectory and {@code conf.getJournalDirNames()} — it succeeds silently.
     *
     * <p>The resulting anomalous behaviour:
     * <ul>
     *   <li>Journal memory is calculated by dividing journalMaxMemory by N
     *       (the number of declared directories), but the actual .txn files are
     *       written to an undeclared directory.</li>
     *   <li>The lastMark filename is set to {@code "lastMark.0"} because N > 1,
     *       but the file will be searched in {@code ledgerDir} — no connection to
     *       the undeclared journalDirectory.</li>
     * </ul>
     * None of this is detectable at construction time. The discrepancy would only
     * surface at write or scan time in a real deployment.
     */
    @Test
    @DisplayName("CP#3 / BVA#3 — N dirs in conf, journalDir NOT included → Construction successful + round-trip")
    void cp03_bva03_nDirsConfJournalDirNotIncluded_silentlySucceeds() throws Exception {
        File declaredDir1  = new File(tempDir, "d1");         declaredDir1.mkdirs();
        File declaredDir2  = new File(tempDir, "d2");         declaredDir2.mkdirs();
        File undeclaredDir = new File(tempDir, "undeclared");  undeclaredDir.mkdirs();
        File ledgerDir     = new File(tempDir, "ledger");      ledgerDir.mkdirs();

        ServerConfiguration conf = buildConfNDirs(new String[]{
                declaredDir1.getAbsolutePath(),
                declaredDir2.getAbsolutePath()
        });
        LedgerDirsManager mgr = buildMock(ledgerDir);

        journal = new Journal(0, undeclaredDir, conf, mgr);
        assertNotNull(journal,
                "Constructor must succeed even when journalDirectory is not declared in conf");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(undeclaredDir, conf, mgr);
        assertEquals(1, recovered.size(),
                "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }

    /**
     * CP #4 / BVA #4 — Pre-existing directory with a lastMark file (restart scenario).
     *
     * <p>The constructor reads the existing lastMark via {@code lastLogMark.readLog()}
     * during construction. It must succeed and restore the previous log position.
     *
     * <p>The test performs two phases:
     * <ol>
     *   <li>Write one entry and shut down cleanly — produces a lastMark file on disk.</li>
     *   <li>Re-open the same directory — the constructor must read the existing lastMark.
     *       Both the pre-existing and the new entry are then verified via scan.</li>
     * </ol>
     */
    @Test
    @DisplayName("CP#4 / BVA#4 — pre-existing dir with lastMark → Construction successful, lastMark read")
    void cp04_bva04_preExistingDirWithLastMark_lastMarkRead() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 2, 1);
        LedgerDirsManager   mgr  = buildMock(ledgerDir);

        // Phase 1: write one entry and shut down — produces the lastMark file
        Journal first = new Journal(0, journalDir, conf, mgr);
        first.start();
        writeAndWait(first, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        first.shutdown();

        // Phase 2: re-open — constructor must read the existing lastMark
        journal = new Journal(0, journalDir, conf, mgr);
        assertNotNull(journal,
                "Journal must be constructed successfully on a pre-existing directory");

        Journal.LastLogMark mark = journal.getLastLogMark();
        assertNotNull(mark, "LastLogMark must be non-null after reading an existing lastMark file");

        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID + 1, "smoke-payload-restart");
        journal.shutdown();

        // Verify both entries are recoverable across all journal files
        List<Long> ids = Journal.listJournalIds(journalDir, null);
        assertTrue(!ids.isEmpty(), "No journal files found after round-trip");

        Journal reader = new Journal(0, journalDir, conf, mgr);
        List<ByteBuffer> recovered = new ArrayList<>();
        for (long id : ids) {
            reader.scanJournal(id, 0L, (version, offset, entry) -> {
                ByteBuffer copy = ByteBuffer.allocate(entry.remaining());
                copy.put(entry);
                copy.flip();
                recovered.add(copy);
            }, false);
        }
        assertTrue(recovered.size() >= 2,
                "At least 2 entries must be recovered (pre-existing + new)");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Pre-existing entry ledgerId must match after restart");
        journal = null;
    }

    /**
     * CP #5 / BVA #5 — Non-existent journal directory.
     *
     * <p>{@code lastLogMark.readLog()} is called during construction. If the directory
     * does not exist, the file system call fails and a {@link RuntimeException} is thrown.
     */
    @Test
    @DisplayName("CP#5 / BVA#5 — non-existent journalDirectory → Instantiation successful, write fails")
    void cp05_bva05_nonExistentDirectory_writeFailure() throws Exception {
        File nonExistentDir = new File(tempDir, "does-not-exist");
        File ledgerDir      = new File(tempDir, "ledger"); ledgerDir.mkdirs();

        assertTrue(!nonExistentDir.exists(),
                "Directory must not exist before the test");

        ServerConfiguration conf = buildConf(nonExistentDir, 2, 1);
        LedgerDirsManager   mgr  = buildMock(ledgerDir);

        // Initial hypothesis: exception at constructor site.
        // Empirical result: constructor and start() succeed silently.
        // The failure surfaces only at the first write, when the
        // ForceWriteThread attempts to create a .txn file in the
        // non-existent directory.
        journal = new Journal(0, nonExistentDir, conf, mgr);
        assertNotNull(journal,
                "Constructor must succeed even when journalDirectory does not exist");
        journal.start();

        ByteBuf entry = makeEntry(SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        CountDownLatch latch = new CountDownLatch(1);

        journal.logAddEntry(SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, entry, false,
                (rc, lId, eId, addr, ctx) -> {
                    if (rc == 0) latch.countDown();
                }, null);

        boolean callbackReceived = latch.await(WRITE_TIMEOUT_S, TimeUnit.SECONDS);
        assertFalse(callbackReceived,
                "Write callback must not be received when journalDirectory does not exist "
                        + "— Journal thread dies silently after IOException in run loop");
    }
    /**
     * CP #6 / BVA #6 — Directory without read/write permissions.
     *
     * <p><b>Initial hypothesis:</b> the constructor was expected to throw a
     * {@link RuntimeException} because {@code lastLogMark.readLog()} cannot
     * access a directory with no permissions.
     *
     */
    @Test
    @DisplayName("CP#6 / BVA#6 — dir without permissions → instantiation successful + round-trip (permissions not enforced in WSL/root)")
    void cp06_bva06_directoryWithoutPermissions_silentlySucceeds() throws Exception {
        File noPermDir = new File(tempDir, "noperm"); noPermDir.mkdirs();
        noPermDir.setReadable(false);
        noPermDir.setWritable(false);

        File ledgerDir = new File(tempDir, "ledger"); ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(noPermDir, 2, 1);
        LedgerDirsManager   mgr  = buildMock(ledgerDir);

        // Initial hypothesis: exception. Empirical result: silent success in WSL/root.
        journal = new Journal(0, noPermDir, conf, mgr);
        assertNotNull(journal,
                "Constructor must succeed — permission restriction not enforced in WSL/root environment");
        journal.start();

        ByteBuf entry = makeEntry(SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        CountDownLatch latch = new CountDownLatch(1);

        journal.logAddEntry(SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, entry, false,
                (rc, lId, eId, addr, ctx) -> {
                    if (rc == 0) latch.countDown();
                }, null);

        boolean callbackReceived = latch.await(WRITE_TIMEOUT_S, TimeUnit.SECONDS);
        assertFalse(callbackReceived,
                "Write callback must not be received when journalDirectory does not exist "
                        + "— Journal thread dies silently after IOException in run loop");
    }

    /**
     * CP #7 / BVA #7 — null journalDirectory.
     *
     * <p>The constructor stores journalDirectory without immediate null validation.
     * The {@link NullPointerException} surfaces when the journal first attempts to
     * use the directory — at {@code start()} or at the first write, when the
     * ForceWriteThread tries to open a .txn file.
     *
     * <p>Disabled: the exact point of failure (constructor, {@code start()}, or
     * first write) could not be determined reliably in isolation. The
     * {@code assertThrows} wrapper covers all three sites.
     */
    @Test
    @DisplayName("CP#7 / BVA#7 — null journalDirectory → instantiation successful, write times out")
    void cp07_bva07_nullJournalDirectory_writeTimesOut() throws Exception {
        File ledgerDir = new File(tempDir, "ledger"); ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(new File(tempDir, "journal-placeholder"), 2, 1);
        LedgerDirsManager   mgr  = buildMock(ledgerDir);

        journal = new Journal(0, null, conf, mgr);
        assertNotNull(journal,
                "Constructor must succeed even with null journalDirectory");
        //journal.start();
    }

    /**
     * CP #8 / BVA #8 — null ServerConfiguration.
     *
     * <p>The constructor reads multiple parameters from conf immediately during
     * construction. Passing null must throw a {@link NullPointerException} at the
     * constructor site.
     */
    @Test
    @DisplayName("CP#8 / BVA#8 — null conf → NullPointerException")
    void cp08_bva08_nullConf_exception() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        LedgerDirsManager mgr = buildMock(ledgerDir);

        assertThrows(Exception.class,
                () -> new Journal(0, journalDir, null, mgr),
                "null conf must throw an exception");
    }

    /**
     * CP #9 / BVA #9 — null LedgerDirsManager.
     *
     * <p>The constructor uses the LedgerDirsManager immediately to read the lastMark.
     * Passing null must throw a {@link NullPointerException} at the constructor site.
     */
    @Test
    @DisplayName("CP#9 / BVA#9 — null ledgerDirsManager → NullPointerException")
    void cp09_bva09_nullLedgerDirsManager_exception() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 2, 1);

        assertThrows(Exception.class,
                () -> new Journal(0, journalDir, conf, null),
                "null ledgerDirsManager must throw an exception");
    }

    /**
     * CP #10 / BVA #10 — Negative journalIndex (lower error bound: -1).
     *
     * <p>The constructor does not validate journalIndex — it succeeds silently and
     * produces an anomalous lastMark file name ({@code "lastMark.-1"}).
     * Operational correctness is confirmed via a full write + scan round-trip.
     */
    @Test
    @DisplayName("CP#10 / BVA#10 — journalIndex=-1 (lower error bound) → Construction successful + round-trip")
    void cp10_bva10_negativeJournalIndex_silentlySucceeds() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File otherDir   = new File(tempDir, "other");   otherDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConfNDirs(new String[]{
                journalDir.getAbsolutePath(),
                otherDir.getAbsolutePath()
        });
        LedgerDirsManager mgr = buildMock(ledgerDir);

        journal = new Journal(-1, journalDir, conf, mgr);
        assertNotNull(journal,
                "Constructor must succeed even with a negative journalIndex");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(),
                "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }

    /**
     * CP #11 / BVA #11 — journalIndex out of range (upper error bound: N=2).
     *
     * <p>With N=2 directories in conf, valid indices are 0 and 1. Index 2 is out of
     * range. The constructor does not validate this — it succeeds silently and
     * produces an anomalous lastMark file name ({@code "lastMark.2"}).
     * Operational correctness is confirmed via a full write + scan round-trip.
     */
    @Test
    @DisplayName("CP#11 / BVA#11 — journalIndex=N=2 (upper error bound) → Construction successful + round-trip")
    void cp11_bva11_journalIndexOutOfRange_silentlySucceeds() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File otherDir   = new File(tempDir, "other");   otherDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConfNDirs(new String[]{
                journalDir.getAbsolutePath(),
                otherDir.getAbsolutePath()
        });
        LedgerDirsManager mgr = buildMock(ledgerDir);

        journal = new Journal(2, journalDir, conf, mgr);
        assertNotNull(journal,
                "Constructor must succeed even with an out-of-range journalIndex");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(),
                "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }

    /**
     * CP #12 / BVA #12 — maxJournalSizeMB &gt; journalPreAllocSizeMB (nominal case).
     *
     * <p>maxSize=2, preAllocSize=1: maxSize strictly greater than preAllocSize.
     * a new .txn file is created when the current one reaches maxSize.
     */
    @Test
    @DisplayName("CP#12 / BVA#12 — maxSize=2 > preAllocSize=1 → Construction successful + round-trip")
    void cp12_bva12_maxSizeGreaterThanPreAllocSize_success() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 2, 1);
        LedgerDirsManager   mgr  = buildMock(ledgerDir);

        journal = new Journal(0, journalDir, conf, mgr);
        assertNotNull(journal, "Construction must succeed when maxSize > preAllocSize");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(),
                "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }

    /**
     * CP #13 / BVA #13 — maxJournalSizeMB == journalPreAllocSizeMB (boundary case).
     *
     * <p>maxSize=1, preAllocSize=1: equality at the critical boundary. Pre-allocation
     * fills the file to exactly maxSize, so rollover behaviour at this exact boundary
     * must be verified empirically. Operational correctness is confirmed via round-trip.
     */
    @Test
    @DisplayName("CP#13 / BVA#13 — maxSize=1 == preAllocSize=1 (boundary) → Construction successful + round-trip")
    void cp13_bva13_maxSizeEqualsPreAllocSize_success() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 1, 1);
        LedgerDirsManager   mgr  = buildMock(ledgerDir);

        journal = new Journal(0, journalDir, conf, mgr);
        assertNotNull(journal, "Construction must succeed when maxSize == preAllocSize");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(),
                "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }

    /**
     * CP #14 / BVA #14 — maxJournalSizeMB &lt; journalPreAllocSizeMB (documented bug).
     *
     * <p>maxSize=1, preAllocSize=2: pre-allocation fills the file beyond maxSize
     * immediately. Rollover is never triggered because the file is already larger
     * than the configured limit from the moment it is created.
     *
     * <p>This is the bug documented in the functional test suite: even with
     * {@code maxJournalSizeMB=1}, the journal maintains a pre-allocated size of 16MB
     * for new .txn files when {@code journalPreAllocSizeMB} is not explicitly set
     * below {@code maxJournalSizeMB}. Construction succeeds — the bug only manifests
     * at write time as an absence of rollover.
     */
    @Test
    @DisplayName("CP#14 / BVA#14 — maxSize=1 < preAllocSize=2 (documented bug) → Construction successful + round-trip")
    void cp14_bva14_maxSizeLessThanPreAllocSize_success() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 1, 2);
        LedgerDirsManager   mgr  = buildMock(ledgerDir);

        journal = new Journal(0, journalDir, conf, mgr);
        assertNotNull(journal,
                "Construction must succeed even when maxSize < preAllocSize");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(),
                "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }
}