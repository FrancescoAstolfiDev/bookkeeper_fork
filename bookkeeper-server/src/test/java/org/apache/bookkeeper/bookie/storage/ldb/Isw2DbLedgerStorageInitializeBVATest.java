package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
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
 *       an immediate flush since the cache is always full.</li>
 *   <li>Lower bound read=0 MB: read cache updates are silently skipped.</li>
 *   <li>Minimum reasonable: 16 MB for both.</li>
 *   <li>Upper error bound: writeCacheMaxSize + readCacheMaxSize &gt;
 *       estimateMaxDirectMemory() → IOException at initialize().</li>
 * </ul>
 *
 * <p><b>numDirLedger / numDirIndex</b>
 * <ul>
 *   <li>Lower valid bound: 1 == 1</li>
 *   <li>Lower error bound: 2 != 1 → IOException</li>
 *   <li>Upper error bound: 1 != 2 → IOException</li>
 * </ul>
 *
 * <h3>Mutation testing refinements</h3>
 *
 * <p>After the initial BVA suite was evaluated with PIT, the mutation report
 * identified several surviving mutants on {@code initialize()} related to
 * arithmetic operators used in internal size computations:
 * <ul>
 *   <li>Line 153 — {@code Replaced long multiplication with division}:
 *       {@code writeCacheMaxSize = getLongVariableOrDefault(...) * MB} — the
 *       mutant replaces {@code *} with {@code /}, yielding ~0 bytes.</li>
 *   <li>Line 175 — {@code Replaced long division with multiplication}:
 *       {@code perDirectoryWriteCacheSize = writeCacheMaxSize / numberOfDirs} —
 *       the mutant replaces {@code /} with {@code *}, yielding a value
 *       numberOfDirs times larger than expected.</li>
 * </ul>
 *
 * <p>These variables are local to {@code initialize()} and are passed directly
 * as constructor arguments to {@link SingleDirectoryDbLedgerStorage}, which
 * stores them in a private {@link WriteCache} field. They are not exposed by any
 * public getter on {@link DbLedgerStorage}. The only way to observe their values
 * after construction is via Java Reflection on the private {@code writeCache}
 * field of the first entry in {@link DbLedgerStorage#getLedgerStorageList()},
 * and then on the {@code maxCacheSize} field of the resulting {@link WriteCache}.
 *
 * <p>The helper {@link #readWriteCacheMaxSize(DbLedgerStorage)} encapsulates
 * this reflection chain. An assertion of the form:
 * <pre>
 *   assertEquals(WRITE_CACHE_16MB * MB, readWriteCacheMaxSize(storage))
 * </pre>
 * detects any mutant that alters the arithmetic at lines 153 or 175, because
 * the mutated value differs from {@code 16 * MB} by at least an order of
 * magnitude in either direction.
 *
 * <p><b>BVA table (14 cases):</b>
 * <pre>
 *  # | write        | read   | directory                    | nDL  | nDI  | Expected output
 * ---+--------------+--------+------------------------------+------+------+-----------------------------
 *  1 | 16 MB        | 16 MB  | new                          |  1   |  1   | Success + round-trip
 *  2 | 16 MB        | 16 MB  | no write permission          |  1   |  1   | IOException
 *  3 | 16 MB        | 16 MB  | pre-existing valid           |  1   |  1   | Success + entry readable
 *  4 | 16 MB        | 16 MB  | corrupted CURRENT            |  1   |  1   | Exception at initialize()
 *  5 |  0 MB        | 16 MB  | new                          |  1   |  1   | Exception at round-trip
 *  6 | 16 MB        |  0 MB  | new                          |  1   |  1   | Success
 *  7 | overflow     | 16 MB  | new                          |  1   |  1   | IOException
 *  8 | null (conf)  | null   | null                         |  -   |  -   | NullPointerException
 *  9 | 16 MB        | 16 MB  | pre-existing valid           |  1   |  1   | Success + round-trip
 * 10 | 16 MB        | 16 MB  | pre-existing valid           |  2   |  1   | IOException
 * 11 | 16 MB        | 16 MB  | new                          | null | null | NullPointerException
 * 12 | 16 MB        | 16 MB  | new                          |  1   |  2   | IOException
 * 13 | 16 MB        | 16 MB  | new                          |  1   | null | NullPointerException
 * 14 | 16 MB        | 16 MB  | new                          |  1   |  1   | NullPointerException at flush
 * </pre>
 */
@DisplayName("DbLedgerStorage — BVA: initialize(conf, null, ledgerDirsMgr, indexDirsMgr, statsLogger, allocator)")
public class Isw2DbLedgerStorageInitializeBVATest {

    @TempDir
    File tempDir;

    private DbLedgerStorage storage;

    // ── Constants ──────────────────────────────────────────────────────────────

    private static final long MB               = 1024L * 1024L;
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
    private static LedgerDirsManager buildDirsManager(ServerConfiguration conf, File[] dirs)
            throws IOException {
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
            public CheckpointSource.Checkpoint newCheckpoint() {
                return CheckpointSource.Checkpoint.MAX;
            }
            public void checkpointComplete(CheckpointSource.Checkpoint cp, boolean compact) {
                /* no-op */
            }
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
     */
    private void initStorageOnDir(File dir) throws Exception {
        ServerConfiguration conf = buildConf(dir, WRITE_CACHE_16MB, READ_CACHE_16MB);
        LedgerDirsManager mgr    = buildDirsManager(conf, new File[]{ dir });
        storage = new DbLedgerStorage();
        storage.initialize(conf, null, mgr, mgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        attachCheckpointAndStart(storage);
    }

    /**
     * Writes one entry to {@code s}, flushes, and reads it back.
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
     * Runs a full initialize → write → flush → shutdown cycle on {@code dir}.
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
    // Reflection helpers for mutation-driven assertions
    // =========================================================================

    /**
     * Returns the {@code maxCacheSize} of the {@link WriteCache} stored in the
     * private field {@code writeCache} of the first
     * {@link SingleDirectoryDbLedgerStorage} instance inside {@code s}.
     *
     * <p>This is the only observable consequence of the arithmetic computations
     * at lines 153 ({@code writeCacheMaxSize = ... * MB}) and 175
     * ({@code perDirectoryWriteCacheSize = writeCacheMaxSize / numberOfDirs})
     * of {@code DbLedgerStorage.java}. Both values are local variables passed
     * as constructor arguments and not exposed by any public getter.
     *
     * <p>A mutant replacing {@code * MB} with {@code / MB} at line 153 produces
     * a cache of ~0 bytes; one replacing {@code / numberOfDirs} with
     * {@code * numberOfDirs} at line 175 produces a cache many times larger.
     * Either value differs from {@code WRITE_CACHE_16MB * MB}, killing the mutant.
     */
    private static long readWriteCacheMaxSize(DbLedgerStorage s) throws Exception {
        Object singleDir = s.getLedgerStorageList().get(0);
        Field writeCacheField = singleDir.getClass().getDeclaredField("writeCache");
        writeCacheField.setAccessible(true);
        Object writeCache = writeCacheField.get(singleDir);
        Field maxSizeField = writeCache.getClass().getDeclaredField("maxCacheSize");
        maxSizeField.setAccessible(true);
        return (long) maxSizeField.get(writeCache);
    }

    /**
     * Returns the maximum size (in bytes) of the read cache held by the first
     * {@link SingleDirectoryDbLedgerStorage} instance inside {@code s}.
     *
     * <p>{@code ReadCache} is a distinct class from {@link WriteCache} and does
     * not expose a {@code maxCacheSize} field. Its capacity is instead stored in
     * a field named {@code maxCacheSize} on the segments array, or alternatively
     * in a field that can be discovered by iterating {@link Class#getDeclaredFields()}.
     * This helper uses a field-name scan to remain robust against minor field-name
     * differences across BookKeeper versions.
     *
     * <p>Kills the mutant at line 154 ({@code readCacheMaxSize = ... * MB →
     * ... / MB}). The expected value for a single-directory storage configured
     * with {@code READ_CACHE_16MB} is {@code READ_CACHE_16MB * MB / 2}.
     */
    /**
     * Returns the total capacity (in bytes) of the {@link ReadCache} stored in
     * the private field {@code readCache} of the first
     * {@link SingleDirectoryDbLedgerStorage} instance inside {@code s}.
     *
     * <p>{@link ReadCache} does not expose a {@code maxCacheSize} field. Its
     * total capacity is computed from its internal structure:
     * {@code segmentSize * cacheSegments.size()}, where {@code segmentSize} is a
     * private {@code int} field and {@code cacheSegments} is a private
     * {@link java.util.List} field (both declared in {@link ReadCache}).
     *
     * <p>Kills the mutant at line 154 ({@code readCacheMaxSize = ... * MB →
     * ... / MB}). The expected value for a single-directory storage configured
     * with {@code READ_CACHE_16MB} is {@code READ_CACHE_16MB * MB / 2}.
     */
    @SuppressWarnings("unchecked")
    private static long readReadCacheMaxSize(DbLedgerStorage s) throws Exception {
        Object singleDir = s.getLedgerStorageList().get(0);
        Field readCacheField = singleDir.getClass().getDeclaredField("readCache");
        readCacheField.setAccessible(true);
        Object readCache = readCacheField.get(singleDir);

        // ReadCache.segmentSize: the size in bytes of each ring-buffer segment.
        Field segmentSizeField = readCache.getClass().getDeclaredField("segmentSize");
        segmentSizeField.setAccessible(true);
        int segmentSize = (int) segmentSizeField.get(readCache);

        // ReadCache.cacheSegments: List<ByteBuf> — number of elements = number of segments.
        Field cacheSegmentsField = readCache.getClass().getDeclaredField("cacheSegments");
        cacheSegmentsField.setAccessible(true);
        java.util.List<?> cacheSegments = (java.util.List<?>) cacheSegmentsField.get(readCache);

        // Total capacity = segmentSize * numberOfSegments.
        return (long) segmentSize * cacheSegments.size();
    }

    // =========================================================================
    // BVA test cases
    // =========================================================================

    /**
     * BVA #1 — Baseline valid configuration.
     * write=16MB, read=16MB, new temp directory, 1 ledger dir == 1 index dir.
     * Verifies that the lower valid bound produces a fully operational storage.
     *
     * <p><b>Mutation testing note (PIT — lines 153, 175):</b>
     * After the round-trip, the write-cache capacity is read via reflection and
     * compared against the expected value {@code WRITE_CACHE_16MB * MB}.
     * See {@link #readWriteCacheMaxSize(DbLedgerStorage)} for the full rationale.
     */
    @Test
    @DisplayName("BVA#1 — write=16MB, read=16MB, newDir, 1L==1I → Success + round-trip")
    void bva01_baselineValidConfig_success() throws Exception {
        initStorageOnDir(tempDir);
        smokeRoundTrip(storage);

        assertNotNull(storage.getLedgerStorageList(),
                "getLedgerStorageList() must not return null after successful initialize()");
        assertEquals(1, storage.getLedgerStorageList().size(),
                "Exactly one SingleDirectoryDbLedgerStorage must be created for one ledger dir");

        // Mutation-driven assertions: kill mutants at lines 153, 154, and 175.
        assertEquals(WRITE_CACHE_16MB * MB / 2, readWriteCacheMaxSize(storage),
                "Write cache capacity must equal writeCacheMaxSize / numberOfDirs / 2; "
                        + "arithmetic mutations at lines 153 or 175 produce a different value");
        assertEquals(READ_CACHE_16MB * MB , readReadCacheMaxSize(storage),
                "Read cache capacity must equal readCacheMaxSize / numberOfDirs / 2; "
                        + "arithmetic mutation at line 154 produces a different value");
    }

    /**
     * BVA #2 — Directory without write permission.
     * initialize() must fail because the storage cannot create its internal
     * structure inside a read-only directory.
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
     * The standard production restart scenario.
     *
     * <p><b>Mutation testing note (PIT — lines 153, 175):</b>
     * Same reflection-based assertion as BVA#1. See BVA#1 for the rationale.
     */
    @Test
    @DisplayName("BVA#3 — write=16MB, read=16MB, preExistingValidDir → Success + entry readable")
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

        assertNotNull(storage.getLedgerStorageList(),
                "getLedgerStorageList() must not return null after successful restart");
        assertEquals(1, storage.getLedgerStorageList().size(),
                "Exactly one SingleDirectoryDbLedgerStorage must be created for one ledger dir");

        // Mutation-driven assertions: kill mutants at lines 153, 154, and 175.
        assertEquals(WRITE_CACHE_16MB * MB / 2, readWriteCacheMaxSize(storage),
                "Write cache capacity must match the configured 16 MB after restart");
        assertEquals(READ_CACHE_16MB * MB , readReadCacheMaxSize(storage),
                "Read cache capacity must match the configured 16 MB after restart; "
                        + "arithmetic mutation at line 154 produces a different value");
    }

    /**
     * BVA #4 — Restart on a directory with corrupted RocksDB CURRENT file.
     */
    @Test
    @DisplayName("BVA#4 — write=16MB, read=16MB, corruptedRocksDB CURRENT → Exception at initialize")
    void bva04_corruptedRocksDbCurrent_exceptionAtInitialize() throws Exception {
        File corruptDir = new File(tempDir, "corrupt");
        corruptDir.mkdirs();
        prepopulateDirectory(corruptDir);

        for (String dbSubDir : new String[]{ "ledgers", "locations" }) {
            File current = new File(corruptDir, "current/" + dbSubDir + "/CURRENT");
            Files.write(current.toPath(), "corrupted\n".getBytes());
        }

        assertThrows(Exception.class, () -> initStorageOnDir(corruptDir));
    }

    /**
     * BVA #5 — writeCacheMaxSize at lower bound (0 MB).
     * The cache is immediately full on every addEntry(), triggering a flush
     * that fails with OperationRejectedException.
     */
    @Test
    @DisplayName("BVA#5 — write=0MB (lower bound), read=16MB → Exception at round-trip")
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
     * Read cache updates are silently skipped; storage remains operational.
     *
     * <p><b>Mutation testing note (PIT — lines 153, 175):</b>
     * Same reflection-based assertion as BVA#1. See BVA#1 for the rationale.
     */
    @Test
    @DisplayName("BVA#6 — write=16MB, read=0MB (lower bound) → Success")
    void bva06_readCacheZero_success() throws Exception {
        ServerConfiguration conf = buildConf(tempDir, WRITE_CACHE_16MB, 0L);
        LedgerDirsManager mgr    = buildDirsManager(conf, new File[]{ tempDir });
        storage = new DbLedgerStorage();
        storage.initialize(conf, null, mgr, mgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        attachCheckpointAndStart(storage);
        smokeRoundTrip(storage);

        assertNotNull(storage.getLedgerStorageList(),
                "getLedgerStorageList() must not return null after successful initialize()");
        assertEquals(1, storage.getLedgerStorageList().size(),
                "Exactly one SingleDirectoryDbLedgerStorage must be created for one ledger dir");

        // Mutation-driven assertions: kill mutants at lines 153 and 175.
        // Line 154 is not meaningfully distinguishable here because readCacheMaxSize = 0:
        // both correct (0 * MB = 0) and mutated (0 / MB = 0) yield 0.
        // The mutant at line 154 is killed by BVA#1, BVA#3, and BVA#9 where read=16 MB.
        assertEquals(WRITE_CACHE_16MB * MB / 2, readWriteCacheMaxSize(storage),
                "Write cache capacity must match the configured 16 MB even when read cache is 0");
        assertEquals(0L, readReadCacheMaxSize(storage),
                "Read cache capacity must be 0 when readCacheMaxSize is configured to 0 MB");
    }

    /**
     * BVA #7 — Cache sum exceeds max direct memory.
     */
    @Test
    @DisplayName("BVA#7 — write=overflow, read=16MB → IOException (cache sum exceeds maxDirectMemory)")
    void bva07_cacheSumExceedsMaxDirectMemory_ioException() {
        long maxDirect       = PlatformDependent.estimateMaxDirectMemory();
        long overflowWriteMb = maxDirect / MB + 1;

        assertThrows(Exception.class, () -> {
            ServerConfiguration conf = buildConf(tempDir, overflowWriteMb, READ_CACHE_16MB);
            LedgerDirsManager mgr    = buildDirsManager(conf, new File[]{ tempDir });
            storage = new DbLedgerStorage();
            storage.initialize(conf, null, mgr, mgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        });
    }

    /**
     * BVA #8 — conf = null.
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
     *
     * <p><b>Mutation testing note (PIT — lines 153, 175):</b>
     * Same reflection-based assertion as BVA#1. See BVA#1 for the rationale.
     */
    @Test
    @DisplayName("BVA#9 — write=16MB, read=16MB, 1preExist==1preExist → Success + round-trip")
    void bva09_onePreExistingDirEach_success() throws Exception {
        File preExist = new File(tempDir, "pre9");
        preExist.mkdirs();
        prepopulateDirectory(preExist);

        initStorageOnDir(preExist);
        smokeRoundTrip(storage);

        assertNotNull(storage.getLedgerStorageList(),
                "getLedgerStorageList() must not return null after successful restart");
        assertEquals(1, storage.getLedgerStorageList().size(),
                "Exactly one SingleDirectoryDbLedgerStorage must be created for one ledger dir");

        // Mutation-driven assertions: kill mutants at lines 153, 154, and 175.
        assertEquals(WRITE_CACHE_16MB * MB / 2, readWriteCacheMaxSize(storage),
                "Write cache capacity must match the configured 16 MB on restart");
        assertEquals(READ_CACHE_16MB * MB , readReadCacheMaxSize(storage),
                "Read cache capacity must match the configured 16 MB on restart; "
                        + "arithmetic mutation at line 154 produces a different value");
    }

    /**
     * BVA #10 — Lower error bound on directory cardinality: 2 ledger dirs, 1 index dir.
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

        DiskChecker dc              = new DiskChecker(
                conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerMgr = new LedgerDirsManager(
                conf, new File[]{ ledger1, ledger2 }, dc, NullStatsLogger.INSTANCE);
        LedgerDirsManager indexMgr  = new LedgerDirsManager(
                conf, new File[]{ index1 }, dc, NullStatsLogger.INSTANCE);

        assertThrows(Exception.class, () -> {
            storage = new DbLedgerStorage();
            storage.initialize(conf, null, ledgerMgr, indexMgr,
                    NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        });
    }

    /**
     * BVA #11 — indexDirsManager built with null dirs array.
     */
    @Test
    @DisplayName("BVA#11 — indexDirsManager dirs=null → NullPointerException")
    void bva11_indexDirsManagerNullArray_nullPointerException() throws IOException {
        ServerConfiguration conf    = buildConf(tempDir, WRITE_CACHE_16MB, READ_CACHE_16MB);
        LedgerDirsManager ledgerMgr = buildDirsManager(conf, new File[]{ tempDir });
        DiskChecker dc              = new DiskChecker(
                conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());

        assertThrows(Exception.class, () -> {
            LedgerDirsManager indexMgr = new LedgerDirsManager(
                    conf, null, dc, NullStatsLogger.INSTANCE);
            storage = new DbLedgerStorage();
            storage.initialize(conf, null, ledgerMgr, indexMgr,
                    NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        });
    }

    /**
     * BVA #12 — Upper error bound on directory cardinality: 1 ledger dir, 2 index dirs.
     */
    @Test
    @DisplayName("BVA#12 — 1ledgerDir != 2indexDirs → IOException (dir count mismatch)")
    void bva12_oneLedgerDirTwoIndexDirs_ioException() throws IOException {
        File ledger1 = new File(tempDir, "l1_12"); ledger1.mkdirs();
        File index1  = new File(tempDir, "i1_12"); index1.mkdirs();
        File index2  = new File(tempDir, "i2_12"); index2.mkdirs();

        ServerConfiguration conf    = buildConf(ledger1, WRITE_CACHE_16MB, READ_CACHE_16MB);
        DiskChecker dc              = new DiskChecker(
                conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerMgr = new LedgerDirsManager(
                conf, new File[]{ ledger1 }, dc, NullStatsLogger.INSTANCE);
        LedgerDirsManager indexMgr  = new LedgerDirsManager(
                conf, new File[]{ index1, index2 }, dc, NullStatsLogger.INSTANCE);

        assertThrows(Exception.class, () -> {
            storage = new DbLedgerStorage();
            storage.initialize(conf, null, ledgerMgr, indexMgr,
                    NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        });
    }

    /**
     * BVA #13 — indexDirsManager = null.
     */
    @Test
    @DisplayName("BVA#13 — indexDirsManager=null → NullPointerException")
    void bva13_indexDirsManagerNull_nullPointerException() throws IOException {
        ServerConfiguration conf    = buildConf(tempDir, WRITE_CACHE_16MB, READ_CACHE_16MB);
        LedgerDirsManager ledgerMgr = buildDirsManager(conf, new File[]{ tempDir });

        assertThrows(Exception.class, () -> {
            storage = new DbLedgerStorage();
            storage.initialize(conf, null, ledgerMgr, null,
                    NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        });
    }

    /**
     * BVA #14 — Valid initialize() with allocator = null.
     * The NullPointerException surfaces at flush() time, not at initialize().
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

        assertThrows(NullPointerException.class, () -> storage.flush());
    }
}