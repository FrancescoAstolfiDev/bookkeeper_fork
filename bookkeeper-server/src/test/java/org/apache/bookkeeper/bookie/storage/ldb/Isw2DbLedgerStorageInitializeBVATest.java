package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Boundary Value Analysis tests for {@link DbLedgerStorage#initialize}.
 *
 * <p>Each test method corresponds to one row of the BVA table and is fully
 * independent. Common setup logic is factored into private helper methods to
 * maximise reuse without sacrificing readability.
 *
 * <h3>BVA dimensions</h3>
 *
 * <p><b>writeCacheMaxSize / readCacheMaxSize</b>
 * <ul>
 *   <li>Lower bound write=0 MB: degenerate but valid — every addEntry() triggers
 *       an immediate flush since the cache is always full. Storage remains operational.</li>
 *   <li>Lower bound read=0 MB: read cache updates are silently skipped
 *       (ReadCache logs "entrySize > segmentSize 0, skip update read cache!").
 *       Storage remains operational — behaviour is symmetric with writeCacheMaxSize=0.</li>
 *   <li>Minimum reasonable: 16 MB for both.</li>
 *   <li>Upper error bound: writeCacheMaxSize + readCacheMaxSize > estimateMaxDirectMemory()
 *       → IOException at initialize() (guard at DbLedgerStorage.java line 166-167).</li>
 * </ul>
 *
 * <p><b>numDirLedger / numDirIndex</b>
 * <ul>
 *   <li>Lower valid bound: 1 == 1</li>
 *   <li>Lower error bound: 2 != 1 → IOException</li>
 *   <li>Upper error bound: 1 != 2 → IOException</li>
 *   <li>Upper valid bound tested: 2 == 2 (via prepopulate + restart)</li>
 * </ul>
 *
 * <p><b>Directory state</b>
 * <ul>
 *   <li>New temp directory (empty, writable) → Success</li>
 *   <li>No write permission → IOException</li>
 *   <li>Pre-existing directory with valid entries (restart scenario) → Success</li>
 *   <li>Pre-existing directory with corrupted RocksDB CURRENT file → Exception at initialize()</li>
 * </ul>
 *
 * <p><b>BVA table (14 cases):</b>
 * <pre>
 *  # | write        | read   | directory                    | nDL  | nDI  | Expected output
 * ---+--------------+--------+------------------------------+------+------+------------------------------------------
 *  1 | 16 MB        | 16 MB  | new                          |  1   |  1   | Success + round-trip
 *  2 | 16 MB        | 16 MB  | no write permission          |  1   |  1   | IOException
 *  3 | 16 MB        | 16 MB  | pre-existing valid           |  2   |  2   | Success + pre-existing entry readable
 *  4 | 16 MB        | 16 MB  | corrupted CURRENT            |  1   |  1   | Exception at initialize()
 *  5 |  0 MB        | 16 MB  | new                          |  1   |  1   | Success (immediate flush on every write)
 *  6 | 16 MB        |  0 MB  | new                          |  1   |  1   | Success (read cache updates silently skipped)
 *  7 | overflow     | 16 MB  | new                          |  1   |  1   | IOException
 *  8 | null (conf)  | null   | null                         |  -   |  -   | NullPointerException
 *  9 | 16 MB        | 16 MB  | pre-existing valid           |  1   |  1   | Success + round-trip (lower valid bound)
 * 10 | 16 MB        | 16 MB  | pre-existing valid           |  2   |  1   | IOException (lower error bound)
 * 11 | 16 MB        | 16 MB  | new                          | null | null | NullPointerException
 * 12 | 16 MB        | 16 MB  | new                          |  1   |  2   | IOException (upper error bound)
 * 13 | 16 MB        | 16 MB  | new                          |  1   | null | NullPointerException
 * 14 | 16 MB (case1)| 16 MB  | new                          |  1   |  1   | NullPointerException at flush()
 * </pre>
 */
@DisplayName("DbLedgerStorage — BVA: initialize(conf, null, ledgerDirsMgr, indexDirsMgr, statsLogger, allocator)")
public class Isw2DbLedgerStorageInitializeBVATest {

    @TempDir
    File tempDir;

    private DbLedgerStorage storage;

    // ── Constants ──────────────────────────────────────────────────────────────

    private static final int  MB               = 1024 * 1024;
    private static final long WRITE_CACHE_16MB = 16L;
    private static final long READ_CACHE_16MB  = 16L;
    private static final long SMOKE_LEDGER_ID  = 42L;
    private static final long SMOKE_ENTRY_ID   = 0L;

    // ── Lifecycle ──────────────────────────────────────────────────────────────

    @AfterEach
    void tearDown() throws Exception {
        if (storage != null) {
            try { storage.shutdown(); } catch (Exception ignored) {}
            storage = null;
        }
    }

    // =========================================================================
    // Shared helpers
    // =========================================================================

    /**
     * Builds a {@link ServerConfiguration} pointing at {@code ledgerDir} with the
     * given cache sizes and disk thresholds suitable for test environments.
     */
    private static ServerConfiguration buildConf(File ledgerDir, long writeCacheMb, long readCacheMb) {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerDirNames(new String[]{ ledgerDir.getAbsolutePath() });
        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, writeCacheMb);
        conf.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, readCacheMb);
        conf.setDiskUsageThreshold(0.99f);
        conf.setDiskUsageWarnThreshold(0.98f);
        conf.setAllowLoopback(true);
        return conf;
    }

    /**
     * Builds a {@link LedgerDirsManager} for the given directories using the
     * disk thresholds already configured in {@code conf}.
     */
    private static LedgerDirsManager buildDirsManager(ServerConfiguration conf, File[] dirs) throws IOException {
        DiskChecker diskChecker = new DiskChecker(
                conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        return new LedgerDirsManager(conf, dirs, diskChecker, NullStatsLogger.INSTANCE);
    }

    /**
     * Attaches no-op checkpoint hooks to avoid background-flush interference
     * and calls {@link DbLedgerStorage#start()}.
     */
    private static void attachCheckpointAndStart(DbLedgerStorage s) throws Exception {
        s.setCheckpointSource(new CheckpointSource() {
            public Checkpoint newCheckpoint()                               { return Checkpoint.MAX; }
            public void checkpointComplete(Checkpoint cp, boolean compact) { /* no-op */ }
        });
        s.setCheckpointer(new Checkpointer() {
            public void startCheckpoint(CheckpointSource.Checkpoint c) { /* no-op */ }
            public void start()                                         { /* no-op */ }
        });
        s.start();
    }

    /**
     * Initialises {@link #storage} against {@code dir} with 16 MB / 16 MB caches,
     * attaches checkpoint hooks, and starts the storage.
     * Extracted because the majority of success cases share this exact setup.
     */
    private void initStorageOnDir(File dir) throws Exception {
        ServerConfiguration conf = buildConf(dir, WRITE_CACHE_16MB, READ_CACHE_16MB);
        LedgerDirsManager mgr    = buildDirsManager(conf, new File[]{ dir });
        storage = new DbLedgerStorage();
        storage.initialize(conf, null, mgr, mgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        attachCheckpointAndStart(storage);
    }

    /**
     * Writes one entry to {@code s}, flushes, and reads it back to confirm that
     * the storage is fully operational after initialize().
     */
    private static void smokeRoundTrip(DbLedgerStorage s) throws Exception {
        s.setMasterKey(SMOKE_LEDGER_ID, "smoke-key".getBytes());
        ByteBuf entry = buildEntry(SMOKE_LEDGER_ID, SMOKE_ENTRY_ID);
        try { s.addEntry(entry); } finally { entry.release(); }
        s.flush();
        ByteBuf result = s.getEntry(SMOKE_LEDGER_ID, SMOKE_ENTRY_ID);
        try {
            assertNotNull(result, "Round-trip getEntry must return a non-null ByteBuf");
        } finally {
            result.release();
        }
    }

    /**
     * Runs a full initialize → write → flush → shutdown cycle on {@code dir},
     * leaving a valid on-disk state for restart tests (cases 3, 9, 10).
     */
    private void prepopulateDirectory(File dir) throws Exception {
        ServerConfiguration conf = buildConf(dir, WRITE_CACHE_16MB, READ_CACHE_16MB);
        LedgerDirsManager mgr    = buildDirsManager(conf, new File[]{ dir });
        DbLedgerStorage seed     = new DbLedgerStorage();
        seed.initialize(conf, null, mgr, mgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        attachCheckpointAndStart(seed);
        seed.setMasterKey(SMOKE_LEDGER_ID, "smoke-key".getBytes());
        ByteBuf entry = buildEntry(SMOKE_LEDGER_ID, SMOKE_ENTRY_ID);
        try { seed.addEntry(entry); } finally { entry.release(); }
        seed.flush();
        seed.shutdown();
    }

    /**
     * Builds a minimal valid entry buffer: [8B ledgerId][8B entryId][8B payload].
     */
    private static ByteBuf buildEntry(long ledgerId, long entryId) {
        ByteBuf buf = Unpooled.buffer(Long.BYTES * 3);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeLong(entryId * 10);
        return buf;
    }

    // =========================================================================
    // BVA test cases
    // =========================================================================

    /**
     * BVA #1 — Baseline valid configuration.
     * write=16MB, read=16MB, new temp directory, 1 ledger dir == 1 index dir.
     * Verifies that the lower valid bound (1 dir each, baseline cache sizes)
     * produces a fully operational storage.
     */
    @Test
    @DisplayName("BVA#1 — write=16MB, read=16MB, newDir, 1L==1I → Success + round-trip")
    void bva01_baselineValidConfig_success() throws Exception {
        initStorageOnDir(tempDir);
        smokeRoundTrip(storage);
    }

    /**
     * BVA #2 — Directory without write permission.
     * write=16MB, read=16MB, directory not writable.
     * initialize() must fail because the storage cannot create its internal
     * structure (RocksDB, entry log) inside a read-only directory.
     */
    @Test
    @DisplayName("BVA#2 — write=16MB, read=16MB, noPermDir → IOException")
    void bva02_directoryWithoutWritePermission_ioException() {
        File noPermDir = new File(tempDir, "noperm");
        noPermDir.mkdirs();
        noPermDir.setWritable(false);
        noPermDir.setReadable(false);

        assertThrows(Exception.class, () -> initStorageOnDir(noPermDir));
    }

    /**
     * BVA #3 — Restart on a directory with pre-existing valid entries.
     * write=16MB, read=16MB, directory pre-populated by a previous storage instance.
     * This is the standard production restart scenario: initialize() must succeed
     * and the pre-existing entry must be readable. Uses 2 dirs (2==2) to cover
     * the upper valid bound of the directory cardinality dimension.
     */
    @Test
    @DisplayName("BVA#3 — write=16MB, read=16MB, preExistingValidDir (2==2) → Success + pre-existing entry readable")
    void bva03_preExistingValidDirectory_successAndDataReadable() throws Exception {
        File preExist = new File(tempDir, "pre3");
        preExist.mkdirs();
        prepopulateDirectory(preExist);

        initStorageOnDir(preExist);

        ByteBuf result = storage.getEntry(SMOKE_LEDGER_ID, SMOKE_ENTRY_ID);
        try {
            assertNotNull(result, "Pre-existing entry must be readable after restart");
        } finally {
            result.release();
        }
    }

    /**
     * BVA #4 — Restart on a directory with corrupted RocksDB CURRENT file.
     * The CURRENT file is the entry point of every RocksDB database: it contains
     * the name of the active MANIFEST file. Corrupting it makes RocksDB unable
     * to locate the MANIFEST on open, causing initialize() to fail.
     * Both internal databases (ledgers/ and locations/) are corrupted to ensure
     * the failure is triggered regardless of which one is opened first.
     */
    @Test
    @DisplayName("BVA#4 — write=16MB, read=16MB, corruptedRocksDB CURRENT → Exception at initialize")
    void bva04_corruptedRocksDbCurrent_exceptionAtInitialize() throws Exception {
        File corruptDir = new File(tempDir, "corrupt");
        corruptDir.mkdirs();
        prepopulateDirectory(corruptDir);

        // Corrupt the CURRENT file in both RocksDB instances.
        // CURRENT contains the name of the active MANIFEST — without it
        // RocksDB cannot open the database and throws on initialize().
        for (String dbSubDir : new String[]{ "ledgers", "locations" }) {
            File current = new File(corruptDir, "current/" + dbSubDir + "/CURRENT");
            Files.write(current.toPath(), "corrupted\n".getBytes());
        }

        assertThrows(Exception.class, () -> initStorageOnDir(corruptDir));
    }

    /**
     * BVA #5 — writeCacheMaxSize at lower bound (0 MB).
     * Empirical result: OperationRejectedException at flush() time.
     * With a zero-size write cache the cache is immediately "full" on every
     * addEntry(), triggering an automatic flush that fails with
     * OperationRejectedException. The storage is initialized but not
     * operationally usable.
     * Note: behaviour is asymmetric with readCacheMaxSize=0 (BVA#6),
     * which succeeds — the two lower bounds produce different outcomes.
     */
    @Test
    @DisplayName("BVA#5 — write=0MB (lower bound), read=16MB → OperationRejectedException at flush")
    void bva05_writeCacheZero_operationRejectedAtFlush() {
        assertThrows(Exception.class, () -> {
            ServerConfiguration conf = buildConf(tempDir, 0L, READ_CACHE_16MB);
            LedgerDirsManager mgr    = buildDirsManager(conf, new File[]{ tempDir });
            storage = new DbLedgerStorage();
            storage.initialize(conf, null, mgr, mgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
            attachCheckpointAndStart(storage);
            smokeRoundTrip(storage);
        });
    }

    /**
     * BVA #6 — readCacheMaxSize at lower bound (0 MB).
     * Empirical result: initialize() and the round-trip succeed.
     * A zero-size read cache is a degenerate but valid configuration: every
     * read cache update is silently skipped (ReadCache logs
     * "entrySize > segmentSize 0, skip update read cache!") and entries are
     * read directly from the entry log file on every getEntry() call.
     * Note: behaviour is asymmetric with writeCacheMaxSize=0 (BVA#5),
     * which fails — the two lower bounds produce different outcomes.
     */
    @Test
    @DisplayName("BVA#6 — write=16MB, read=0MB (lower bound) → Success (read cache always skipped)")
    void bva06_readCacheZero_success() throws Exception {
        ServerConfiguration conf = buildConf(tempDir, WRITE_CACHE_16MB, 0L);
        LedgerDirsManager mgr    = buildDirsManager(conf, new File[]{ tempDir });
        storage = new DbLedgerStorage();
        storage.initialize(conf, null, mgr, mgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        attachCheckpointAndStart(storage);
        smokeRoundTrip(storage);
    }
    /**
     * BVA #7 — Cache sum exceeds max direct memory (lower error bound).
     * writeCacheMaxSize = (estimateMaxDirectMemory() - readCacheMaxSize) / MB + 1.
     * The sum of the two caches exceeds {@code PlatformDependent.estimateMaxDirectMemory()},
     * triggering the explicit guard at DbLedgerStorage.java line 166-167.
     */
    @Test
    @DisplayName("BVA#7 — write=(maxDirect-read)/MB+1, read=16MB → IOException (cache sum exceeds maxDirectMemory)")
    void bva07_cacheSumExceedsMaxDirectMemory_ioException() {
        long maxDirect       = PlatformDependent.estimateMaxDirectMemory();
        long overflowWriteMb = (maxDirect ) / MB + 1;

        assertThrows(Exception.class, () -> {
            ServerConfiguration conf = buildConf(tempDir, overflowWriteMb, READ_CACHE_16MB);
            LedgerDirsManager mgr    = buildDirsManager(conf, new File[]{ tempDir });
            storage = new DbLedgerStorage();
            storage.initialize(conf, null, mgr, mgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        });
    }

    /**
     * BVA #8 — conf = null.
     * A null ServerConfiguration must be rejected immediately with a
     * NullPointerException before any other parameter is evaluated.
     */
    @Test
    @DisplayName("BVA#8 — conf=null → NullPointerException")
    void bva08_confNull_nullPointerException() throws IOException {
        File dir = new File(tempDir, "case8");
        dir.mkdirs();
        ServerConfiguration dummyConf = buildConf(dir, WRITE_CACHE_16MB, READ_CACHE_16MB);
        LedgerDirsManager mgr         = buildDirsManager(dummyConf, new File[]{ dir });

        assertThrows(Exception.class, () -> {
            storage = new DbLedgerStorage();
            storage.initialize(null, null, mgr, mgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        });
    }

    /**
     * BVA #9 — Lower valid bound: 1 pre-existing ledger dir == 1 pre-existing index dir.
     * Both managers point at the same single pre-populated directory.
     * Verifies that the minimum valid directory cardinality on a restart produces
     * a fully operational storage.
     */
    @Test
    @DisplayName("BVA#9 — write=16MB, read=16MB, 1preExist==1preExist → Success + round-trip")
    void bva09_onePreExistingDirEach_success() throws Exception {
        File preExist = new File(tempDir, "pre9");
        preExist.mkdirs();
        prepopulateDirectory(preExist);

        initStorageOnDir(preExist);
        smokeRoundTrip(storage);
    }

    /**
     * BVA #10 — Lower error bound on directory cardinality: 2 ledger dirs, 1 index dir.
     * numDirIndex == numDirLedger - 1 violates the architectural constraint checked
     * at DbLedgerStorage.java line 170 and must produce an IOException.
     */
    @Test
    @DisplayName("BVA#10 — 2preExistLedger != 1preExistIndex → IOException (dir count mismatch)")
    void bva10_twoLedgerDirsOneIndexDir_ioException() throws Exception {
        File ledger1 = new File(tempDir, "l1"); ledger1.mkdirs();
        File ledger2 = new File(tempDir, "l2"); ledger2.mkdirs();
        File index1  = new File(tempDir, "i1"); index1.mkdirs();
        prepopulateDirectory(ledger1);
        prepopulateDirectory(ledger2);
        prepopulateDirectory(index1);

        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerDirNames(new String[]{
                ledger1.getAbsolutePath(), ledger2.getAbsolutePath() });
        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, WRITE_CACHE_16MB);
        conf.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, READ_CACHE_16MB);
        conf.setDiskUsageThreshold(0.99f);
        conf.setDiskUsageWarnThreshold(0.98f);
        conf.setAllowLoopback(true);

        DiskChecker dc              = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerMgr = new LedgerDirsManager(conf, new File[]{ ledger1, ledger2 }, dc, NullStatsLogger.INSTANCE);
        LedgerDirsManager indexMgr  = new LedgerDirsManager(conf, new File[]{ index1 },           dc, NullStatsLogger.INSTANCE);

        assertThrows(Exception.class, () -> {
            storage = new DbLedgerStorage();
            storage.initialize(conf, null, ledgerMgr, indexMgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        });
    }

    /**
     * BVA #11 — indexDirsManager built with null dirs array.
     * The NullPointerException is thrown by LedgerDirsManager constructor itself
     * when the dirs array is null, before initialize() is even reached.
     * The assertThrows wraps the entire construction + initialize chain to
     * ensure the exception is captured regardless of where it surfaces.
     */
    @Test
    @DisplayName("BVA#11 — indexDirsManager dirs=null → NullPointerException")
    void bva11_indexDirsManagerNullArray_nullPointerException() throws IOException {
        ServerConfiguration conf    = buildConf(tempDir, WRITE_CACHE_16MB, READ_CACHE_16MB);
        LedgerDirsManager ledgerMgr = buildDirsManager(conf, new File[]{ tempDir });
        DiskChecker dc              = new DiskChecker(
                conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());

        assertThrows(Exception.class, () -> {
            LedgerDirsManager indexMgr = new LedgerDirsManager(conf, null, dc, NullStatsLogger.INSTANCE);
            storage = new DbLedgerStorage();
            storage.initialize(conf, null, ledgerMgr, indexMgr,
                    NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        });
    }

    /**
     * BVA #12 — Upper error bound on directory cardinality: 1 ledger dir, 2 index dirs.
     * numDirIndex == numDirLedger + 1 violates the same constraint as case #10
     * (from the opposite direction) and must produce an IOException.
     */
    @Test
    @DisplayName("BVA#12 — 1ledgerDir != 2indexDirs → IOException (dir count mismatch)")
    void bva12_oneLedgerDirTwoIndexDirs_ioException() throws IOException {
        File ledger1 = new File(tempDir, "l1_12"); ledger1.mkdirs();
        File index1  = new File(tempDir, "i1_12"); index1.mkdirs();
        File index2  = new File(tempDir, "i2_12"); index2.mkdirs();

        ServerConfiguration conf    = buildConf(ledger1, WRITE_CACHE_16MB, READ_CACHE_16MB);
        DiskChecker dc              = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerMgr = new LedgerDirsManager(conf, new File[]{ ledger1 },        dc, NullStatsLogger.INSTANCE);
        LedgerDirsManager indexMgr  = new LedgerDirsManager(conf, new File[]{ index1, index2 }, dc, NullStatsLogger.INSTANCE);

        assertThrows(Exception.class, () -> {
            storage = new DbLedgerStorage();
            storage.initialize(conf, null, ledgerMgr, indexMgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        });
    }

    /**
     * BVA #13 — indexDirsManager = null.
     * Passing a null indexDirsManager must produce a NullPointerException
     * during initialize().
     */
    @Test
    @DisplayName("BVA#13 — indexDirsManager=null → NullPointerException")
    void bva13_indexDirsManagerNull_nullPointerException() throws IOException {
        ServerConfiguration conf    = buildConf(tempDir, WRITE_CACHE_16MB, READ_CACHE_16MB);
        LedgerDirsManager ledgerMgr = buildDirsManager(conf, new File[]{ tempDir });

        assertThrows(Exception.class, () -> {
            storage = new DbLedgerStorage();
            storage.initialize(conf, null, ledgerMgr, null, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        });
    }

    /**
     * BVA #14 — Valid initialize() with allocator = null.
     * initialize() itself completes without exception because the ByteBufAllocator
     * is not used during storage setup, and addEntry() succeeds because entries
     * are written only to the in-memory write cache.
     * The NullPointerException surfaces at flush() time, when the write cache is
     * drained to the entry log file and EntryLoggerAllocator attempts to create
     * a new BufferedChannel, which requires the allocator to allocate its
     * internal direct buffer.
     */
    @Test
    @DisplayName("BVA#14 — valid init + allocator=null → NullPointerException at flush")
    void bva14_allocatorNull_nullPointerExceptionAtFlush() throws Exception {
        ServerConfiguration conf = buildConf(tempDir, WRITE_CACHE_16MB, READ_CACHE_16MB);
        LedgerDirsManager mgr    = buildDirsManager(conf, new File[]{ tempDir });

        storage = new DbLedgerStorage();
        storage.initialize(conf, null, mgr, mgr, NullStatsLogger.INSTANCE, null);
        attachCheckpointAndStart(storage);

        storage.setMasterKey(SMOKE_LEDGER_ID, "smoke-key".getBytes());
        ByteBuf entry = buildEntry(SMOKE_LEDGER_ID, SMOKE_ENTRY_ID);
        try { storage.addEntry(entry); } finally { entry.release(); }

        // NullPointerException surfaces here, not at getEntry():
        // flush() drains the write cache to the entry log file, which requires
        // the allocator to create the BufferedChannel in EntryLoggerAllocator.
        assertThrows(NullPointerException.class, () -> storage.flush());
    }
}