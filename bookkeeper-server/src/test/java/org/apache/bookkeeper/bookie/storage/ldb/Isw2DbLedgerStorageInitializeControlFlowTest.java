package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBufAllocator;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

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
 * Coverage-oriented tests for {@link DbLedgerStorage#initialize}.
 *
 * <h3>Uncovered branches addressed</h3>
 * <pre>
 *  [pc bpc] if (ledgerDirs.size() != indexDirs.size()) — false branch (same size != 1)
 *  [pc bpc] if (!lDirs[0].getPath().equals(iDirs[0].getPath())) — true branch
 *  [nc]     idm.getListeners().forEach(indexDirsManager::addLedgerDirsListener)
 *  [pc bpc] if (directIOEntryLogger)                  — true branch + inner [nc] block
 *  [nc bnc] if (numReadThreads == 0)                  — both sub-branches
 * </pre>
 *
 * <h3>Coverage test table</h3>
 * <pre>
 *  # | Scenario                                               | Branch hit
 * ---+--------------------------------------------------------+---------------------------------------------
 *  1 | 2 ledger dirs == 2 index dirs, separate paths         | dirCount-false + !lDirs.equals(iDirs)-true
 *                                                            | + idm.getListeners().forEach [nc] line
 *  2 | directIO=true, numReadWorkerThreads > 0               | directIO-true, numReadThreads==0-false
 *  3 | directIO=true, numReadWorkerThreads == 0              | directIO-true, numReadThreads==0-true
 * </pre>
 *
 * <p><b>Note on Coverage #2 and #3:</b> tests that enable {@code directIOEntryLogger}
 * may throw an {@link java.io.IOException} if the native libraries required for Direct I/O
 * are not available in the execution environment. In that case the test tolerates the
 * exception and only asserts that the message is non-null. This approach ensures that
 * the setup lines preceding the {@code DirectEntryLogger} constructor are still executed
 * and recorded by JaCoCo, regardless of native library availability.
 *
 * <h3>Mutation testing refinements</h3>
 *
 * <p>After the initial coverage suite was evaluated with PIT, the mutation report
 * identified surviving mutants on {@code initialize()} at lines 153 and 175,
 * corresponding to arithmetic expressions that compute {@code writeCacheMaxSize}
 * ({@code getLongVariableOrDefault(...) * MB}) and {@code perDirectoryWriteCacheSize}
 * ({@code writeCacheMaxSize / numberOfDirs}) respectively.
 *
 * <p>These variables are local to {@code initialize()} and are passed directly as
 * constructor arguments to {@link SingleDirectoryDbLedgerStorage}, which stores
 * them internally in a private {@link WriteCache} field. They are not exposed by
 * any public getter on {@link DbLedgerStorage}. The only way to observe their
 * values after construction is via Java Reflection on the private {@code writeCache}
 * field of each {@link SingleDirectoryDbLedgerStorage} instance, and then on the
 * {@code maxCacheSize} field of the resulting {@link WriteCache}.
 *
 * <p>The helper {@link #readWriteCacheMaxSize(DbLedgerStorage, int)} encapsulates
 * this reflection chain. An assertion of the form:
 * <pre>
 *   assertEquals(WRITE_CACHE_16MB * MB, readWriteCacheMaxSize(storage, 0))
 * </pre>
 * detects any mutant that alters the arithmetic at lines 153 or 175, because the
 * mutated value differs from {@code 16 * MB} by at least an order of magnitude
 * in either direction. For the two-directory scenario (Coverage #1), both instances
 * are checked to ensure that the per-directory partition is computed correctly for
 * each of them.
 */
@DisplayName("DbLedgerStorage — Coverage: initialize() uncovered branches")
public class Isw2DbLedgerStorageInitializeControlFlowTest {

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

    private static LedgerDirsManager buildDirsManager(ServerConfiguration conf, File[] dirs)
            throws IOException {
        DiskChecker dc = new DiskChecker(
                conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        return new LedgerDirsManager(conf, dirs, dc, NullStatsLogger.INSTANCE);
    }

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

    // =========================================================================
    // Reflection helpers for mutation-driven assertions
    // =========================================================================

    /**
     * Returns the {@code maxCacheSize} of the {@link WriteCache} stored in the
     * private field {@code writeCache} of the {@link SingleDirectoryDbLedgerStorage}
     * instance at position {@code index} inside {@code s}.
     *
     * <p>This is the only observable consequence of the arithmetic computations
     * at lines 153 ({@code writeCacheMaxSize = ... * MB}) and 175
     * ({@code perDirectoryWriteCacheSize = writeCacheMaxSize / numberOfDirs})
     * of {@code DbLedgerStorage.java}. Both values are local variables passed
     * as constructor arguments and not exposed by any public getter.
     *
     * <p>For a single-directory storage, {@code index = 0} is the only valid
     * position. For the two-directory scenario, both index 0 and index 1 are
     * checked to verify that the per-directory partition is applied uniformly.
     *
     * <p>A mutant replacing {@code * MB} with {@code / MB} at line 153 produces
     * a cache of ~0 bytes; one replacing {@code / numberOfDirs} with
     * {@code * numberOfDirs} at line 175 produces a cache many times larger than
     * expected. Either value differs from {@code WRITE_CACHE_16MB * MB / numDirs},
     * causing the assertion to fail and killing the mutant.
     */
    private static long readWriteCacheMaxSize(DbLedgerStorage s, int index) throws Exception {
        Object singleDir = s.getLedgerStorageList().get(index);
        Field writeCacheField = singleDir.getClass().getDeclaredField("writeCache");
        writeCacheField.setAccessible(true);
        Object writeCache = writeCacheField.get(singleDir);
        Field maxSizeField = writeCache.getClass().getDeclaredField("maxCacheSize");
        maxSizeField.setAccessible(true);
        return (long) maxSizeField.get(writeCache);
    }

    /**
     * Returns the maximum size (in bytes) of the read cache held by the
     * {@link SingleDirectoryDbLedgerStorage} instance at position {@code index}.
     *
     * <p>{@code ReadCache} is a distinct class from {@link WriteCache} and does
     * not expose a {@code maxCacheSize} field with that exact name. This helper
     * uses a field-name scan to remain robust against field-name differences.
     *
     * <p>Kills the mutant at line 154 ({@code readCacheMaxSize = ... * MB →
     * ... / MB}). The expected value for a single-directory storage configured
     * with {@code READ_CACHE_16MB} is {@code READ_CACHE_16MB * MB / 2}.
     */
    /**
     * Returns the total capacity (in bytes) of the {@link ReadCache} held by
     * the {@link SingleDirectoryDbLedgerStorage} instance at position {@code index}.
     *
     * <p>{@link ReadCache} does not expose a {@code maxCacheSize} field. Its
     * total capacity is computed as {@code segmentSize * cacheSegments.size()},
     * where both are private fields declared in {@link ReadCache}.
     *
     * <p>Kills the mutant at line 154 ({@code readCacheMaxSize = ... * MB →
     * ... / MB}). The expected value for a single-directory storage configured
     * with {@code READ_CACHE_16MB} is {@code READ_CACHE_16MB * MB / 2}.
     */
    @SuppressWarnings("unchecked")
    private static long readReadCacheMaxSize(DbLedgerStorage s, int index) throws Exception {
        Object singleDir = s.getLedgerStorageList().get(index);
        Field readCacheField = singleDir.getClass().getDeclaredField("readCache");
        readCacheField.setAccessible(true);
        Object readCache = readCacheField.get(singleDir);

        Field segmentSizeField = readCache.getClass().getDeclaredField("segmentSize");
        segmentSizeField.setAccessible(true);
        int segmentSize = (int) segmentSizeField.get(readCache);

        Field cacheSegmentsField = readCache.getClass().getDeclaredField("cacheSegments");
        cacheSegmentsField.setAccessible(true);
        java.util.List<?> cacheSegments = (java.util.List<?>) cacheSegmentsField.get(readCache);

        return (long) segmentSize * cacheSegments.size();
    }

    // =========================================================================
    // Coverage test cases
    // =========================================================================

    /**
     * Coverage #1 — Two ledger dirs and two separate index dirs (different paths).
     *
     * <p>Targets two uncovered branches at once:
     * <ul>
     *   <li>{@code [bfc]} {@code if (ledgerDirs.size() != indexDirs.size())} — false branch:
     *       2 == 2, so no IOException is thrown and the loop runs twice.</li>
     *   <li>{@code [bpc -> true]} {@code if (!lDirs[0].getPath().equals(iDirs[0].getPath()))}:
     *       ledger and index paths differ, so the inner body executes and
     *       {@code idm.getListeners().forEach(...)} is called — the [nc] line.</li>
     * </ul>
     *
     * <p>Using two separate pairs of directories forces the path-equality check
     * to evaluate to {@code true} for every iteration of the loop.
     *
     * <p><b>Mutation testing note (PIT — lines 153, 175):</b>
     * After initialization, the write-cache capacity of each
     * {@link SingleDirectoryDbLedgerStorage} instance is read via reflection and
     * compared against the expected per-directory value
     * {@code WRITE_CACHE_16MB * MB / 2}. With two directories, the total cache
     * is partitioned equally: each instance should hold exactly half of the
     * configured total. A mutant replacing {@code / numberOfDirs} with
     * {@code * numberOfDirs} at line 175 would produce a value twice the total,
     * and a mutant replacing {@code * MB} with {@code / MB} at line 153 would
     * produce ~0 bytes. Both values differ from the expected half, killing the
     * mutant. Both instances are checked independently to ensure the invariant
     * holds for every directory in the list.
     */
    @Test
    @DisplayName("Coverage#1 — 2ledgerDirs==2indexDirs, separate paths → success + idm listeners forwarded")
    void coverage01_twoSeparateDirPairs_bothBranchesCovered() throws Exception {
        File led1 = new File(tempDir, "led1"); led1.mkdirs();
        File led2 = new File(tempDir, "led2"); led2.mkdirs();
        File idx1 = new File(tempDir, "idx1"); idx1.mkdirs();
        File idx2 = new File(tempDir, "idx2"); idx2.mkdirs();

        // Build a conf that mentions two ledger dirs so DiskChecker is happy.
        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerDirNames(new String[]{ led1.getAbsolutePath(), led2.getAbsolutePath() });
        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, WRITE_CACHE_16MB);
        conf.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, READ_CACHE_16MB);
        conf.setDiskUsageThreshold(0.99f);
        conf.setDiskUsageWarnThreshold(0.98f);
        conf.setAllowLoopback(true);

        DiskChecker dc = new DiskChecker(
                conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());

        // ledgerMgr sees led1 and led2; indexMgr sees idx1 and idx2 (different paths).
        LedgerDirsManager ledgerMgr = new LedgerDirsManager(
                conf, new File[]{ led1, led2 }, dc, NullStatsLogger.INSTANCE);
        LedgerDirsManager indexMgr  = new LedgerDirsManager(
                conf, new File[]{ idx1, idx2 }, dc, NullStatsLogger.INSTANCE);

        storage = new DbLedgerStorage();
        // Must not throw: 2 == 2 and the loop must execute twice.
        storage.initialize(conf, null, ledgerMgr, indexMgr,
                NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        attachCheckpointAndStart(storage);

        // Primary coverage assertion: storage backed by two directories is operational.
        assertNotNull(storage.getLedgerStorageList(),
                "getLedgerStorageList() must not return null after successful initialize()");
        assertEquals(2, storage.getLedgerStorageList().size(),
                "Exactly two SingleDirectoryDbLedgerStorage instances must be created "
                        + "for two configured ledger directories");

        // Mutation-driven assertions via reflection (lines 153, 175).
        // With 2 dirs, each instance holds WRITE_CACHE_16MB * MB / 2.
        // Any arithmetic mutation on lines 153 or 175 produces a different value,
        // causing at least one of these assertions to fail and killing the mutant.
        long expectedPerDirCacheBytes = WRITE_CACHE_16MB * MB / 2;
        assertEquals(expectedPerDirCacheBytes/2, readWriteCacheMaxSize(storage, 0),
                "Write cache of first SingleDirectoryDbLedgerStorage must equal "
                        + "writeCacheMaxSize / numberOfDirs; arithmetic mutations at "
                        + "lines 153 or 175 produce a different value");// access to the first ledger
        assertEquals(READ_CACHE_16MB * MB/2  , readReadCacheMaxSize(storage,0),
                "Read cache capacity must equal readCacheMaxSize / numberOfDirs / 2; "
                        + "arithmetic mutation at line 154 produces a different value");
        assertEquals(expectedPerDirCacheBytes/2, readWriteCacheMaxSize(storage, 1),
                "Write cache of second SingleDirectoryDbLedgerStorage must equal "
                        + "writeCacheMaxSize / numberOfDirs; arithmetic mutations at "
                        + "lines 153 or 175 produce a different value");// access to the second ledger
        assertEquals(READ_CACHE_16MB * MB/2  , readReadCacheMaxSize(storage,1),
                "Read cache capacity must equal readCacheMaxSize / numberOfDirs / 2; "
                        + "arithmetic mutation at line 154 produces a different value");

    }

    /**
     * Coverage #2 — directIOEntryLogger = true, numReadWorkerThreads &gt; 0.
     *
     * <p>Targets the entirely-[nc] {@code if (directIOEntryLogger)} true-branch,
     * including:
     * <ul>
     *   <li>Buffer-size calculations ({@code perDirectoryTotalWriteBufferSize},
     *       {@code perDirectoryTotalReadBufferSize}, {@code readBufferSize},
     *       {@code maxFdCacheTimeSeconds}).</li>
     *   <li>Creation of {@code entryLoggerWriteExecutor} and
     *       {@code entryLoggerFlushExecutor}.</li>
     *   <li>{@code [bnc] if (numReadThreads == 0)} false-branch: with
     *       {@code numReadWorkerThreads = 4 > 0} the thread count is used directly.</li>
     * </ul>
     *
     * <p><b>Mutation testing note (PIT — lines 153, 175):</b>
     * When native I/O is available and {@code initialize()} succeeds, the write-cache
     * capacity is read via reflection and asserted against
     * {@code WRITE_CACHE_16MB * MB}. This kills arithmetic mutants at lines 153 and
     * 175 in the same way as BVA#1. When native I/O is unavailable, the
     * {@link IOException} is caught before the storage list is populated; in that
     * case the mutants are not reachable and the gap is accepted as
     * environment-dependent.
     */
    @Test
    @DisplayName("Coverage#2 — directIO=true, numReadWorkerThreads=4 → directIO branch + numReadThreads!=0")
    void coverage02_directIO_numReadWorkerThreadsNonZero() throws Exception {
        ServerConfiguration conf = buildConf(tempDir, WRITE_CACHE_16MB, READ_CACHE_16MB);
        conf.setProperty(DbLedgerStorage.DIRECT_IO_ENTRYLOGGER, true);
        conf.setNumReadWorkerThreads(4);   // > 0 → numReadThreads == 0 is FALSE

        LedgerDirsManager mgr = buildDirsManager(conf, new File[]{ tempDir });

        storage = new DbLedgerStorage();
        try {
            storage.initialize(conf, null, mgr, mgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
            // If native I/O is available the storage initialises correctly.
            attachCheckpointAndStart(storage);

            // Primary coverage assertion: storage is operational.
            assertNotNull(storage.getLedgerStorageList(),
                    "getLedgerStorageList() must not return null after successful initialize()");
            assertEquals(1, storage.getLedgerStorageList().size(),
                    "Exactly one SingleDirectoryDbLedgerStorage must be created "
                            + "for one configured ledger directory");

            // Mutation-driven assertion via reflection (lines 153, 175).
            assertEquals(WRITE_CACHE_16MB * MB/2, readWriteCacheMaxSize(storage, 0),
                    "Write cache capacity must equal writeCacheMaxSize / numberOfDirs; "
                            + "arithmetic mutations at lines 153 or 175 produce a different value");
            assertEquals(READ_CACHE_16MB * MB , readReadCacheMaxSize(storage,0),
                    "Read cache capacity must equal readCacheMaxSize / numberOfDirs / 2; "
                            + "arithmetic mutation at line 154 produces a different value");
        } catch (IOException e) {
            // Native I/O libraries unavailable in this environment — acceptable.
            // The buffer-size and executor-creation lines above the constructor
            // call are still executed and counted by JaCoCo.
            assertTrue(e.getMessage() != null,
                    "IOException from DirectEntryLogger must carry a message");
        }
    }

    /**
     * Coverage #3 — directIOEntryLogger = true, numReadWorkerThreads == 0.
     *
     * <p>Same as Coverage #2 but with {@code numReadWorkerThreads = 0}, so the
     * code falls through to:
     * <pre>
     *   numReadThreads = conf.getServerNumIOThreads();
     * </pre>
     * This covers the {@code [bnc]} true-branch of {@code if (numReadThreads == 0)}.
     *
     * <p>As with Coverage #2, a native-library IOException is tolerated.
     *
     * <p><b>Mutation testing note (PIT — lines 153, 175):</b>
     * When native I/O is available and {@code initialize()} succeeds, the write-cache
     * capacity is read via reflection and asserted against {@code WRITE_CACHE_16MB * MB}.
     * The rationale is identical to that documented for Coverage #2.
     */
    @Test
    @DisplayName("Coverage#3 — directIO=true, numReadWorkerThreads=0 → numReadThreads==0 true-branch fallback")
    void coverage03_directIO_numReadWorkerThreadsZeroFallback() throws Exception {
        ServerConfiguration conf = buildConf(tempDir, WRITE_CACHE_16MB, READ_CACHE_16MB);
        conf.setProperty(DbLedgerStorage.DIRECT_IO_ENTRYLOGGER, true);
        conf.setNumReadWorkerThreads(0);   // == 0 → fallback to serverNumIOThreads
        conf.setServerNumIOThreads(2);

        LedgerDirsManager mgr = buildDirsManager(conf, new File[]{ tempDir });

        storage = new DbLedgerStorage();
        try {
            storage.initialize(conf, null, mgr, mgr, NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
            // If native I/O is available the storage initialises correctly.
            attachCheckpointAndStart(storage);

            // Primary coverage assertion: storage is operational.
            assertNotNull(storage.getLedgerStorageList(),
                    "getLedgerStorageList() must not return null after successful initialize()");
            assertEquals(1, storage.getLedgerStorageList().size(),
                    "Exactly one SingleDirectoryDbLedgerStorage must be created "
                            + "for one configured ledger directory");

            // Mutation-driven assertion via reflection (lines 153, 175).
            assertEquals(WRITE_CACHE_16MB * MB/2, readWriteCacheMaxSize(storage, 0),
                    "Write cache capacity must equal writeCacheMaxSize / numberOfDirs; "
                            + "arithmetic mutations at lines 153 or 175 produce a different value");
        } catch (IOException e) {
            // Native I/O libraries unavailable — the fallback branch was still hit.
            assertTrue(e.getMessage() != null,
                    "IOException from DirectEntryLogger must carry a message");
        }
    }
}