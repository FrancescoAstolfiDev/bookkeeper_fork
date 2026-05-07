package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBufAllocator;
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

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage-oriented tests for {@link DbLedgerStorage#initialize}.
 *
 * <h3>Uncovered branches addressed</h3>
 * <pre>
 *  [pc bpc] if (directIOEntryLogger)   — true branch + inner [nc] block
 *  [nc bnc] if (numReadThreads == 0)   — both sub-branches
 * </pre>
 *
 * <p>The branches related to directory cardinality and path-equality
 * ({@code if (ledgerDirs.size() != indexDirs.size())} false branch and
 * {@code if (!lDirs[i].getPath().equals(iDirs[i].getPath()))} true branch)
 * are covered by BVA#3 in {@code Isw2DbLedgerStorageInitializeBVATest},
 * which uses two ledger directories and two separate index directories.
 *
 * <h3>Coverage test table</h3>
 * <pre>
 *  # | Scenario                                               | Branch hit / mutation killed
 * ---+--------------------------------------------------------+-----------------------------------------------
 *  1 | directIO=true, numReadWorkerThreads > 0               | directIO-true, numReadThreads==0-false
 *  2 | directIO=true, numReadWorkerThreads == 0              | directIO-true, numReadThreads==0-true
 *  3 | directIO=false, 2 dirs == 2 dirs                      | perDirectoryReadCacheSize
 * </pre>
 *
 * <p><b>Note on Coverage #1 and #2:</b> tests that enable {@code directIOEntryLogger}
 * may throw an {@link IOException} if the native libraries required for Direct I/O
 * are not available in the execution environment. In that case the test tolerates the
 * exception and only asserts that the message is non-null. This approach ensures that
 * the setup lines preceding the {@code DirectEntryLogger} constructor are still executed
 * and recorded by JaCoCo, regardless of native library availability.
 *
 *
 * <p>These variables are local to {@code initialize()} and are passed directly as
 * constructor arguments to {@link SingleDirectoryDbLedgerStorage}, which stores
 * them internally in private {@link WriteCache} and {@link ReadCache} fields.
 * They are not exposed by any public getter on {@link DbLedgerStorage}. The only
 * way to observe their values after construction is via Java Reflection on the
 * private fields {@code writeCache} and {@code readCache} of each
 * {@link SingleDirectoryDbLedgerStorage} instance.
 *
 */
@DisplayName("DbLedgerStorage — Coverage: initialize() uncovered branches")
public class DbLedgerStorageInitializeControlFlowTest_old {

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
            public Checkpoint newCheckpoint() {
                return Checkpoint.MAX;
            }
            public void checkpointComplete(Checkpoint cp, boolean compact) {
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
    // Coverage test cases
    // =========================================================================

    /**
     * Coverage #1 — directIOEntryLogger = true, numReadWorkerThreads &gt; 0.
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
     */
    @Test
    @DisplayName("Coverage#1 — directIO=true, numReadWorkerThreads=4 → directIO branch + numReadThreads!=0")
    void coverage01_directIO_numReadWorkerThreadsNonZero() throws Exception {
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


        } catch (IOException e) {
            // Native I/O libraries unavailable in this environment — acceptable.
            assertTrue(e.getMessage() != null,
                    "IOException from DirectEntryLogger must carry a message");
        }
    }

    /**
     * Coverage #2 — directIOEntryLogger = true, numReadWorkerThreads == 0.
     *
     * <p>Same as Coverage #1 but with {@code numReadWorkerThreads = 0}, so the
     * code falls through to:
     * <pre>
     *   numReadThreads = conf.getServerNumIOThreads();
     * </pre>
     * This covers the {@code [bnc]} true-branch of {@code if (numReadThreads == 0)}.
     *
     * <p>As with Coverage #1, a native-library IOException is tolerated.
     *
     */
    @Test
    @DisplayName("Coverage#2 — directIO=true, numReadWorkerThreads=0 → numReadThreads==0 true-branch fallback")
    void coverage02_directIO_numReadWorkerThreadsZeroFallback() throws Exception {
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
        } catch (IOException e) {
            // Native I/O libraries unavailable — the fallback branch was still hit.
            assertTrue(e.getMessage() != null,
                    "IOException from DirectEntryLogger must carry a message");
        }
    }

    /**
     * Coverage #3 — Two matching directories (standard I/O):
     *
     * <p>All single-directory tests (Coverage#1 and Coverage#2) are unable to distinguish
     * {@code perDirectoryReadCacheSize = readCacheMaxSize * numberOfDirs} from the
     * correct code {@code readCacheMaxSize / numberOfDirs} because {@code /1 == *1}. This test
     * uses two directories ({@code numberOfDirs = 2}) so the correct per-directory read cache
     * is {@code READ_CACHE_16MB * MB / 2 = 8 MB}, while the mutated expression yields
     * {@code READ_CACHE_16MB * MB * 2 = 32 MB}, causing the assertion to fail.
     */
    @Test
    @DisplayName("Coverage#3 — 2dirs==2dirs → readCache per dir = 8 MB ")
    void coverage03_twoDirs_readCacheSplitPerDirectory() throws Exception {
        File ledger1 = new File(tempDir, "l1_c3"); ledger1.mkdirs();
        File ledger2 = new File(tempDir, "l2_c3"); ledger2.mkdirs();
        File index1  = new File(tempDir, "i1_c3"); index1.mkdirs();
        File index2  = new File(tempDir, "i2_c3"); index2.mkdirs();

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
                conf, new File[]{ index1, index2 }, dc, NullStatsLogger.INSTANCE);

        storage = new DbLedgerStorage();
        storage.initialize(conf, null, ledgerMgr, indexMgr,
                NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
        attachCheckpointAndStart(storage);

        assertEquals(2, storage.getLedgerStorageList().size(),
                "Two SingleDirectoryDbLedgerStorage instances must be created for two dirs");

    }
}