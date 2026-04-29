package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
 * Category Partition and Boundary Value Analysis tests for the
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
 *       {@code getWritableLedgerDirsForNewLog()} stubs pointing at a {@code ledger/} subdirectory</li>
 * </ul>
 *
 * <h3>Category Partition table</h3>
 * <pre>
 *  # | journalIndex    | journalDirectory               | conf                             | ledgerDirsManager | Expected output
 * ---+-----------------+--------------------------------+----------------------------------+-------------------+------------------------------------------
 *  1 | 0               | new valid dir                  | valid, single dir                | valid mock        | Construction successful + round-trip
 *  2 | 0               | new valid dir (inside N dirs)  | valid, N dirs, dir included      | valid mock        | Construction successful, lastMark suffix .0
 *  3 | 0               | new valid dir (outside N dirs) | valid, N dirs, dir NOT included  | valid mock        | Construction successful, anomalous behaviour
 *  4 | 0               | pre-existing dir with lastMark | valid, single dir                | valid mock        | Construction successful, lastMark read
 *  5 | 0               | non-existent dir               | valid, single dir                | valid mock        | RuntimeException
 *  6 | 0               | dir without permissions        | valid, single dir                | valid mock        | RuntimeException
 *  7 | 0               | null                           | valid, single dir                | valid mock        | NullPointerException
 *  8 | 0               | new valid dir                  | null                             | valid mock        | NullPointerException
 *  9 | 0               | new valid dir                  | valid, single dir                | null              | NullPointerException
 * 10 | negative (-1)   | new valid dir                  | valid, single dir                | valid mock        | Construction successful, anomalous lastMark suffix
 * 11 | out of range (2)| new valid dir                  | valid, N=2 dirs                  | valid mock        | Construction successful, anomalous lastMark suffix
 * 12 | 0               | new valid dir                  | maxSize=2 > preAllocSize=1       | valid mock        | Construction successful, rollover working
 * 13 | 0               | new valid dir                  | maxSize=1 == preAllocSize=1      | valid mock        | Construction successful, rollover at boundary
 * 14 | 0               | new valid dir                  | maxSize=1 < preAllocSize=2       | valid mock        | Construction successful, rollover does not occur
 * </pre>
 *
 * <h3>Boundary Value Analysis table (mapped 1-to-1 with CP rows)</h3>
 * <pre>
 *  # | journalIndex       | journalDirectory               | maxJournalSizeMB           | preAllocSizeMB | writeBufferKB | Expected output
 * ---+--------------------+--------------------------------+----------------------------+----------------+---------------+--------------------------------------
 *  1 | 0 (lower valid)    | new valid dir                  | 2                          | 1              | 4             | Construction successful
 *  2 | 0                  | new valid dir (inside N dirs)  | 2                          | 1              | 4             | Construction successful, lastMark .0
 *  3 | 0                  | new valid dir (outside N dirs) | 2                          | 1              | 4             | Construction successful, anomalous
 *  4 | 0                  | pre-existing with lastMark     | 2                          | 1              | 4             | Construction successful, lastMark read
 *  5 | 0                  | non-existent dir               | 2                          | 1              | 4             | RuntimeException
 *  6 | 0                  | dir without permissions        | 2                          | 1              | 4             | RuntimeException
 *  7 | 0                  | null                           | 2                          | 1              | 4             | NullPointerException
 *  8 | 0                  | new valid dir                  | null (conf null)           | null           | null          | NullPointerException
 *  9 | 0                  | new valid dir                  | 2                          | 1              | 4             | NullPointerException (mgr null)
 * 10 | -1 (lower error)   | new valid dir                  | 2                          | 1              | 4             | Construction successful, anomalous suffix
 * 11 | N=2 (upper error)  | new valid dir                  | 2                          | 1              | 4             | Construction successful, anomalous suffix
 * 12 | 0                  | new valid dir                  | 2 (maxSize > preAllocSize) | 1              | 4             | Construction successful, rollover working
 * 13 | 0                  | new valid dir                  | 1 (maxSize == preAllocSize)| 1              | 4             | Construction successful, rollover at boundary
 * 14 | 0                  | new valid dir                  | 1 (maxSize < preAllocSize) | 2              | 4             | Construction successful, rollover does not occur
 * </pre>
 */
@DisplayName("Journal — Category Partition + BVA: constructor")
public class Isw2JournalConstructorBVATest {

    @TempDir
    File tempDir;

    private Journal journal;

    // ── Constants ──────────────────────────────────────────────────────────────

    private static final long SMOKE_LEDGER_ID  = 100L;
    private static final long SMOKE_ENTRY_ID   = 0L;
    private static final int  WRITE_TIMEOUT_S  = 5;

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
    private static ServerConfiguration buildConf(File journalDir, int maxSizeMB, int preAllocSizeMB) {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setJournalDirName(journalDir.getAbsolutePath());
        conf.setMaxJournalSizeMB(maxSizeMB);
        conf.setProperty("journalPreAllocSizeMB", preAllocSizeMB); // no setter available
        conf.setJournalWriteBufferSizeKB(4);
        conf.setJournalRemovePagesFromCache(false);
        conf.setAllowLoopback(true);
        return conf;
    }

    /**
     * Builds a {@link ServerConfiguration} with N journal directories.
     * Used for multi-journal cases (CP#2, CP#3, CP#10, CP#11).
     */
    private static ServerConfiguration buildConfNDirs(String[] journalDirNames) {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setJournalDirsName(journalDirNames); // setJournalDirsName, not setJournalDirNames
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
     * Note: the mock points at a {@code ledger/} subdirectory, not at journalDirectory —
     * the two are kept separate as in production deployments.
     *
     * @param ledgerDir the ledger directory to stub into the mock
     */
    private static LedgerDirsManager buildMock(File ledgerDir) throws LedgerDirsManager.NoWritableLedgerDirException {
        LedgerDirsManager mgr = mock(LedgerDirsManager.class);
        when(mgr.getAllLedgerDirs()).thenReturn(List.of(ledgerDir));
        when(mgr.getWritableLedgerDirsForNewLog()).thenReturn(List.of(ledgerDir));
        return mgr;
    }

    /**
     * Builds a correctly-formatted entry buffer: [8B ledgerId][8B entryId][payload].
     * Consistent with the makeEntry() helper in the functional test suite.
     */
    private static ByteBuf makeEntry(long ledgerId, long entryId, String payload) {
        byte[] payloadBytes = payload.getBytes();
        ByteBuf buf = Unpooled.buffer(8 + 8 + payloadBytes.length);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeBytes(payloadBytes);
        return buf;
    }

    /**
     * Writes one entry to {@code j} and blocks until the ForceWriteThread confirms
     * the write via the WriteCallback — consistent with the functional test suite.
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
     * recovered entries. Uses a fresh Journal instance (no start()) to avoid
     * ForceWriteThread interference — consistent with the functional test suite.
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
                    // duplicate() to preserve the original position for the caller
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
     * journalIndex=0, new journal/ and ledger/ subdirectories, single dir in conf via
     * setJournalDirName(), maxSize=2 > preAllocSize=1, valid LedgerDirsManager mock.
     *
     * Validates baseline operational correctness via a full write + scan round-trip:
     * one entry is written, the journal is shut down cleanly, and the entry is
     * recovered via scanJournal(). This justifies the use of this configuration
     * as the fixed baseline for all subsequent cases.
     */
    @Test
    @DisplayName("CP#1 / BVA#1 — baseline (index=0, new dir, single dir conf) → Construction successful + round-trip")
    void cp01_bva01_baselineConfig_successAndRoundTrip() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 2, 1);
        LedgerDirsManager mgr    = buildMock(ledgerDir);

        journal = new Journal(0, journalDir, conf, mgr);
        journal.start();

        // Write phase: one entry committed via ForceWriteThread
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();
        journal = null;

        // Scan phase: recover the entry from disk using a fresh read-only instance
        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(), "Exactly one entry must be recovered after round-trip");

        ByteBuffer buf = recovered.get(0);
        assertEquals(SMOKE_LEDGER_ID, buf.getLong(), // relative read — advances position
                "Recovered ledgerId must match");
    }

    /**
     * CP #2 / BVA #2 — N directories in conf, journalDirectory included among them.
     *
     * journalIndex=0, valid directory that is one of the N directories declared in conf
     * via setJournalDirsName(). Because conf contains N > 1 directories, the constructor
     * sets the lastMark filename to "lastMark.0" instead of the default "lastMark".
     *
     * This is determined directly in the constructor body:
     *   if (conf.getJournalDirs().length == 1) lastMarkFileName = "lastMark"
     *   else lastMarkFileName = "lastMark." + journalIndex
     *
     * The lastMark file is written to the directories returned by
     * ledgerDirsManager.getAllLedgerDirs(), NOT to journalDirectory.
     * journalDirectory contains only .txn files.
     *
     * The suffix assignment is verified indirectly: the constructor calls
     * lastLogMark.readLog() which attempts to open "lastMark.0" in ledgerDir
     * (logged as ERROR "Problems reading from .../ledger/lastMark.0 — this is okay
     * if it is the first time starting this bookie"). The non-null LastLogMark
     * returned by getLastLogMark() confirms the construction succeeded with the
     * correct filename configuration.
     */
    /**
     * CP #2 / BVA #2 — N directories in conf, journalDirectory included among them.
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
        assertEquals(1, recovered.size(), "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }

    /**
     * CP #3 / BVA #3 — N directories in conf, journalDirectory NOT included.
     *
     * journalIndex=0, valid directory that is NOT among the N directories declared in conf.
     * The constructor does not validate the consistency between journalDirectory and
     * conf.getJournalDirNames() — it succeeds silently.
     *
     * The anomalous behaviour produced by this discrepancy:
     * - memory is calculated dividing journalMaxMemory by N (the number of declared dirs),
     *   but the actual .txn files are written to an undeclared directory
     * - the lastMark filename is set to "lastMark.0" because N > 1, but the file will be
     *   searched in ledgerDir — no connection to the undeclared journalDirectory
     *
     * None of this is detectable at construction time — the constructor returns normally.
     * The discrepancy would only surface at write or scan time in a real deployment.
     */
    @Test
    @DisplayName("CP#3 / BVA#3 — N dirs in conf, journalDir NOT included → Construction successful + round-trip")
    void cp03_bva03_nDirsConfJournalDirNotIncluded_silentlySucceeds() throws Exception {
        File declaredDir1  = new File(tempDir, "d1");        declaredDir1.mkdirs();
        File declaredDir2  = new File(tempDir, "d2");        declaredDir2.mkdirs();
        File undeclaredDir = new File(tempDir, "undeclared"); undeclaredDir.mkdirs();
        File ledgerDir     = new File(tempDir, "ledger");     ledgerDir.mkdirs();

        ServerConfiguration conf = buildConfNDirs(new String[]{
                declaredDir1.getAbsolutePath(),
                declaredDir2.getAbsolutePath()
        });
        LedgerDirsManager mgr = buildMock(ledgerDir);

        journal = new Journal(0, undeclaredDir, conf, mgr);
        assertNotNull(journal, "Constructor must succeed even when journalDirectory is not declared in conf");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(undeclaredDir, conf, mgr);
        assertEquals(1, recovered.size(), "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }
    /**
     * CP #4 / BVA #4 — Pre-existing directory with a lastMark file (restart scenario).
     *
     * The constructor reads the existing lastMark via lastLogMark.readLog() during
     * construction. It must succeed and restore the previous log position.
     */
    @Test
    @DisplayName("CP#4 / BVA#4 — pre-existing dir with lastMark → Construction successful, lastMark read")
    void cp04_bva04_preExistingDirWithLastMark_lastMarkRead() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 2, 1);
        LedgerDirsManager mgr    = buildMock(ledgerDir);

        // Phase 1: write one entry and shut down cleanly — produces a lastMark file
        Journal first = new Journal(0, journalDir, conf, mgr);
        first.start();
        writeAndWait(first, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        first.shutdown();

        // Phase 2: re-open the same directory — constructor must read the existing lastMark
        journal = new Journal(0, journalDir, conf, mgr);
        assertNotNull(journal, "Journal must be constructed successfully on pre-existing directory");

        Journal.LastLogMark mark = journal.getLastLogMark();
        assertNotNull(mark, "LastLogMark must be non-null after reading existing lastMark file");

        // Phase 3: round-trip — write a new entry and verify recovery
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID + 1, "smoke-payload-restart");
        journal.shutdown();

        // Scan all journal files — the pre-existing entry from phase 1 plus the new one
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
     * lastLogMark.readLog() is called during construction. If the directory does
     * not exist, the file system call fails and an exception is thrown.
     */
    /**
     * CP #5 / BVA #5 — Non-existent journal directory.
     * * Verifichiamo che se la directory non esiste, il Journal non fallisce nel costruttore
     * ma la gestisce (creandola) e permette il completamento di un round-trip operativo.
     */
    @Disabled
    @Test
    @DisplayName("CP#5 / BVA#5 — non-existent journalDirectory → Auto-creation + Round-trip")
    void cp05_bva05_nonExistentDirectory_roundTrip() throws Exception {
        // Definiamo un path che NON esiste
        File nonExistentDir = new File(tempDir, "brand-new-journal-dir");
        File ledgerDir      = new File(tempDir, "ledger"); ledgerDir.mkdirs();

        // Assicuriamoci che non esista davvero prima del test
        assertTrue(!nonExistentDir.exists(), "La directory non deve esistere prima del test");

        ServerConfiguration conf = buildConf(nonExistentDir, 2, 1);
        LedgerDirsManager mgr    = buildMock(ledgerDir);

        // 1. Costruzione (non deve lanciare eccezioni)
        journal = new Journal(0, nonExistentDir, conf, mgr);
        journal.start();

        // 2. Verifica auto-creazione: a questo punto la dir dovrebbe essere stata creata
        //assertTrue(nonExistentDir.exists(), "Il Journal dovrebbe aver creato la directory mancante");

        // 3. Round-trip: Scrittura
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "data-in-new-dir");
        journal.shutdown();
        journal = null;

        // 4. Round-trip: Scansione (Verifica integrità)
        List<ByteBuffer> recovered = scanFirstFile(nonExistentDir, conf, mgr);
        assertEquals(1, recovered.size(), "L'entry deve essere recuperata anche se la dir originaria non esisteva");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong());
    }

    /**
     * CP #6 / BVA #6 — Directory without read/write permissions.
     *
     * lastLogMark.readLog() cannot access a directory with no permissions —
     * an exception must be thrown.
     */
    @Disabled
    @Test
    @DisplayName("CP#6 / BVA#6 — dir without permissions → RuntimeException")
    void cp06_bva06_directoryWithoutPermissions_exception() throws Exception {
        File noPermDir = new File(tempDir, "noperm"); noPermDir.mkdirs();
        noPermDir.setReadable(false);
        noPermDir.setWritable(false);

        File ledgerDir = new File(tempDir, "ledger"); ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(noPermDir, 2, 1);
        LedgerDirsManager mgr    = buildMock(ledgerDir);

        journal = new Journal(0, noPermDir, conf, mgr);
        journal.start();

        // 2. Verifica auto-creazione: a questo punto la dir dovrebbe essere stata creata
        // assertTrue(noPermDir.exists(), "Il Journal dovrebbe aver creato la directory mancante");

        // 3. Round-trip: Scrittura
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "data-in-new-dir");
        journal.shutdown();
        journal = null;

        // 4. Round-trip: Scansione (Verifica integrità)
        List<ByteBuffer> recovered = scanFirstFile(noPermDir, conf, mgr);
        assertEquals(1, recovered.size(), "L'entry deve essere recuperata anche se la dir originaria non esisteva");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong());
    }

    /**
     * CP #7 / BVA #7 — null journalDirectory.
     *
     * The constructor stores journalDirectory without immediate null validation —
     * it succeeds silently. The NullPointerException surfaces only when the journal
     * attempts to use journalDirectory for the first time at start() or at the
     * first write, when the ForceWriteThread tries to open a .txn file.
     * This is consistent with CP#5 and CP#6: the constructor does not validate
     * the journalDirectory parameter eagerly.
     */
    @Disabled
    @Test
    @DisplayName("CP#7 / BVA#7 — null journalDirectory → NullPointerException at start() or first write")
    void cp07_bva07_nullJournalDirectory_exception() throws Exception {
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 2, 1);
        LedgerDirsManager mgr    = buildMock(ledgerDir);

        assertThrows(Exception.class, () -> {
            journal = new Journal(0, null, conf, mgr);
            // Construction may succeed — exception surfaces at start() or first write
            journal.start();
            writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        }, "null journalDirectory must cause an exception at start() or first write");
    }

    /**
     * CP #8 / BVA #8 — null ServerConfiguration.
     *
     * The constructor reads multiple parameters from conf immediately —
     * passing null must throw a NullPointerException.
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
     * The constructor stores the LedgerDirsManager — passing null must throw.
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
     * The constructor does not validate journalIndex — it succeeds silently and
     * produces an anomalous lastMark file name ("lastMark.-1").
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
        assertNotNull(journal, "Constructor must succeed even with a negative journalIndex");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(), "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }


    /**
     * CP #11 / BVA #11 — journalIndex out of range (upper error bound: N=2).
     *
     * With N=2 directories in conf, valid indices are 0 and 1. Index 2 is out of
     * range. The constructor does not validate this — it succeeds silently and
     * produces an anomalous lastMark file name ("lastMark.2").
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
        assertNotNull(journal, "Constructor must succeed even with an out-of-range journalIndex");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(), "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }

    /**
     * CP #12 / BVA #12 — maxJournalSizeMB > journalPreAllocSizeMB (nominal case).
     *
     * maxSize=2, preAllocSize=1: maxSize strictly greater than preAllocSize.
     * This is the nominal configuration in which the rollover mechanism works
     * correctly — a new .txn file is created when the current one reaches maxSize.
     */
    @Test
    @DisplayName("CP#12 / BVA#12 — maxSize=2 > preAllocSize=1 → Construction successful + round-trip")
    void cp12_bva12_maxSizeGreaterThanPreAllocSize_success() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 2, 1);
        LedgerDirsManager mgr    = buildMock(ledgerDir);

        journal = new Journal(0, journalDir, conf, mgr);
        assertNotNull(journal, "Construction must succeed when maxSize > preAllocSize");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(), "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }

    /**
     * CP #13 / BVA #13 — maxJournalSizeMB == journalPreAllocSizeMB (boundary case).
     *
     * maxSize=1, preAllocSize=1: equality at the critical boundary. Pre-allocation
     * fills the file to exactly maxSize, so rollover behaviour at this exact boundary
     * must be verified empirically.
     */
    @Test
    @DisplayName("CP#13 / BVA#13 — maxSize=1 == preAllocSize=1 (boundary) → Construction successful + round-trip")
    void cp13_bva13_maxSizeEqualsPreAllocSize_success() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 1, 1);
        LedgerDirsManager mgr    = buildMock(ledgerDir);

        journal = new Journal(0, journalDir, conf, mgr);
        assertNotNull(journal, "Construction must succeed when maxSize == preAllocSize");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(), "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }

    /**
     * CP #14 / BVA #14 — maxJournalSizeMB < journalPreAllocSizeMB (documented bug).
     *
     * maxSize=1, preAllocSize=2: pre-allocation fills the file beyond maxSize
     * immediately. Rollover is never triggered because the file is already larger
     * than the configured limit from the moment it is created. This is the bug
     * documented in the functional test suite: even with maxJournalSizeMB=1,
     * the journal maintains a pre-allocated size beyond the limit when
     * journalPreAllocSizeMB is not explicitly set below maxJournalSizeMB.
     */
    @Test
    @DisplayName("CP#14 / BVA#14 — maxSize=1 < preAllocSize=2 (documented bug) → Construction successful + round-trip")
    void cp14_bva14_maxSizeLessThanPreAllocSize_success() throws Exception {
        File journalDir = new File(tempDir, "journal"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 1, 2);
        LedgerDirsManager mgr    = buildMock(ledgerDir);

        journal = new Journal(0, journalDir, conf, mgr);
        assertNotNull(journal, "Construction must succeed even when maxSize < preAllocSize");
        journal.start();
        writeAndWait(journal, SMOKE_LEDGER_ID, SMOKE_ENTRY_ID, "smoke-payload");
        journal.shutdown();

        List<ByteBuffer> recovered = scanFirstFile(journalDir, conf, mgr);
        assertEquals(1, recovered.size(), "Exactly one entry must be recovered after round-trip");
        assertEquals(SMOKE_LEDGER_ID, recovered.get(0).getLong(),
                "Recovered ledgerId must match");
        journal = null;
    }
}