package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Incremental coverage tests for:
 * <ol>
 *   <li>The six-argument constructor
 *       {@code Journal(int, File, ServerConfiguration, LedgerDirsManager, StatsLogger, ByteBufAllocator)}</li>
 *   <li>{@code Journal#scanJournal(long, long, JournalScanner, boolean)}</li>
 * </ol>
 *
 * <h2>Constructor — new branches covered</h2>
 *
 * <h3>C-1  BusyWait queue path (line 663-664)</h3>
 * <pre>
 *   if (conf.isBusyWaitEnabled()) {
 *       queue             = new BlockingMpscQueue<>(...);   // ← line 663
 *       forceWriteRequests = new BlockingMpscQueue<>(...);  // ← line 664
 *   }
 * </pre>
 * All pre-existing tests use the 4-arg convenience constructor which eventually
 * calls the 6-arg variant with {@code busyWait=false}.  The {@code true} branch
 * is therefore never entered.  Enabling {@code busyWait} via
 * {@code conf.setEnableBusyWait(true)} covers lines 663-664.
 *
 * <h3>C-2  Multiple journal directories → suffixed lastMark file name (line 700)</h3>
 * <pre>
 *   if (conf.getJournalDirs().length == 1) {
 *       lastMarkFileName = LAST_MARK_DEFAULT_NAME;           // ← covered by BVA suite
 *   } else {
 *       lastMarkFileName = LAST_MARK_DEFAULT_NAME + "." + journalIndex;  // ← line 700
 *   }
 * </pre>
 * The 4-arg tests that pass multiple dirs already cover a variant of this branch
 * indirectly, but the 6-arg constructor is exercised here explicitly so that the
 * branch is instrumented under the full-signature call path.
 *
 * <h3>C-3  Custom StatsLogger scoping (line 659)</h3>
 * {@code statsLogger.scopeLabel(...)} is invoked on every construction.
 * Passing a real (non-null, non-Null) {@code StatsLogger} that records the
 * scope call exercises the real delegation path.
 *
 * <h3>C-4  Custom ByteBufAllocator stored and accessible (line 657)</h3>
 * The allocator is stored in {@code this.allocator} and later returned by
 * {@code getBufferedChannelBuilder()}.  Passing a {@link PooledByteBufAllocator}
 * confirms the assignment and the builder delegation.
 *
 * <h3>C-5  Invalid JournalChannelProvider via 6-arg constructor (lines 709-711)</h3>
 * Same IOException-wrapping branch already tested in the 4-arg variant; here
 * exercised through the full 6-arg signature to ensure JaCoCo counts it for
 * that constructor's branch table.
 *
 * <h2>scanJournal — new branches covered</h2>
 *
 * <h3>S-1  journalPos &gt; 0 branch (line 818)</h3>
 * <pre>
 *   } else {
 *       recLog = new JournalChannel(..., journalPos, ...);  // ← line 818-820
 *   }
 * </pre>
 * BVA row "inFile_entry10" uses a positive offset but goes through the 4-arg
 * constructor.  Here we exercise the same branch via the 6-arg constructor so
 * the instrument maps correctly.
 *
 * <h3>S-2  {@code len == 0} immediate break (line 836-837)</h3>
 * <pre>
 *   if (len == 0) {
 *       break;   // ← line 837
 *   }
 * </pre>
 * Injecting a 4-byte zero word immediately after the last valid record causes
 * {@code len} to be 0 on the next iteration, which triggers the break.
 * None of the existing tests inject a bare zero-length record (as opposed to
 * the padding-mask path); the branch therefore remains uncovered.
 *
 * <h3>S-3  Padding record with a non-zero following length (line 853 + 870 guard)</h3>
 * <pre>
 *   if (len == PADDING_MASK && journalVersion >= V5) {
 *       ...
 *       len = lenBuff.getInt();
 *       if (len == 0) { continue; }         // NOT taken for non-zero payload
 *       isPaddingRecord = true;             // ← line 853, taken
 *   }
 *   ...
 *   if (!isPaddingRecord) {                 // ← line 870, false → scanner NOT called
 *       scanner.process(...);
 *   }
 * </pre>
 * Existing control-flow tests only cover the {@code len == 0 → continue} arm.
 * This test injects {@code [PADDING_MASK][nonZeroLen][nonZeroLen bytes]} so the
 * payload is fully readable; {@code isPaddingRecord = true} and the scanner is
 * skipped.  The entry count stays at 1 (the seed), confirming that the padding
 * record was consumed but not forwarded to the scanner.
 *
 * <h3>S-4  {@code recBuff} reallocation (line 860-862)</h3>
 * <pre>
 *   recBuff.clear();
 *   if (recBuff.remaining() < len) {
 *       recBuff = ByteBuffer.allocate(len);   // ← line 862
 *   }
 * </pre>
 * The initial {@code recBuff} capacity is 64 KiB.  Injecting a record whose
 * declared length is 65 KiB + 1 byte (> 64 KiB) forces the reallocation branch.
 * Because the payload is fully present on disk the record is also passed to the
 * scanner, which counts it alongside the seed entry.
 *
 * <h3>S-5  IOException from scanner, {@code skipInvalidRecord = true} (lines 875-878)</h3>
 * <pre>
 *   } catch (IOException e) {
 *       if (skipInvalidRecord) {
 *           LOG.warn("...");           // ← line 877 — covered here
 *       } else {
 *           throw e;                  // ← line 879 — covered in BVA suite via invalid id
 *       }
 *       return recLog.fc.position();  // ← line 881 — covered here
 *   }
 * </pre>
 * The scanner's {@code process()} method is declared to throw {@code IOException}.
 * Throwing from within the scanner callback enters the catch block.  With
 * {@code skipInvalidRecord = true} the exception is swallowed and the method
 * returns the current file position rather than propagating.
 *
 * <h3>S-6  IOException from scanner, {@code skipInvalidRecord = false} (line 879)</h3>
 * Same scanner-throw setup as S-5 but with {@code skipInvalidRecord = false};
 * the caught IOException is re-thrown to the caller.
 */
@DisplayName("Journal — 6-arg constructor + scanJournal: incremental coverage")
class Isw2LLMJournalTest {

    // ── Constants ──────────────────────────────────────────────────────────────

    private static final long LEDGER_ID       = 42L;
    private static final long WRITE_TIMEOUT_S = 10L;
    private static final int  PADDING_MASK    = -0x100; // Journal.PADDING_MASK

    // ── Infrastructure ─────────────────────────────────────────────────────────

    @TempDir
    File tempDir;

    private File                journalDir;
    private File                ledgerDir;
    private ServerConfiguration conf;
    private LedgerDirsManager   dirsManager;

    /** Kept so that tearDown can shut it down safely on test failure. */
    private Journal journal;

    @BeforeEach
    void setUp() throws Exception {
        journalDir = new File(tempDir, "journal");
        journalDir.mkdirs();
        ledgerDir = new File(tempDir, "ledger");
        ledgerDir.mkdirs();

        conf = new ServerConfiguration();
        conf.setJournalDirName(journalDir.getAbsolutePath());
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
        if (journal != null && journal.running) {
            journal.shutdown();
        }
        journal = null;
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    private Journal build6Arg(ServerConfiguration c, StatsLogger sl, ByteBufAllocator alloc) {
        return new Journal(0, journalDir, c, dirsManager, sl, alloc);
    }

    /** Writes one entry and blocks until the write callback fires. */
    private void writeOneEntry(Journal j) throws Exception {
        ByteBuf entry = makeEntry(LEDGER_ID, 0L, "seed");
        CountDownLatch latch = new CountDownLatch(1);
        j.logAddEntry(LEDGER_ID, 0L, entry, false,
                (rc, lId, eId, addr, ctx) -> { if (rc == 0) latch.countDown(); }, null);
        assertTrue(latch.await(WRITE_TIMEOUT_S, TimeUnit.SECONDS), "Write callback timeout");
        entry.release();
    }

    /** Writes a seed entry, shuts the journal down, and returns the on-disk journal id. */
    private long writeSeedAndShutdown() throws Exception {
        journal = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        journal.start();
        writeOneEntry(journal);
        journal.shutdown();
        journal = null;

        List<Long> ids = Journal.listJournalIds(journalDir, null);
        assertFalse(ids.isEmpty(), "No .txn file produced after write");
        return ids.get(ids.size() - 1);
    }

    /**
     * Scans the journal file once and returns the byte position immediately
     * after the last valid record (before any V5 padding the journal may have
     * appended).
     */
    private long lastRecordEndPosition(long journalId) throws Exception {
        final long[] endPos = {-1L};
        Journal aux = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        aux.scanJournal(journalId, 0L, (version, offset, entry) ->
                endPos[0] = offset + 4 + entry.remaining(), false);
        assertTrue(endPos[0] > 0, "No records found — cannot determine injection position");
        return endPos[0];
    }

    /**
     * Writes {@code data} starting at absolute position {@code pos} in the .txn file
     * and truncates the file to exactly {@code pos + data.length} so there are no
     * trailing padding bytes after our crafted data.
     */
    private void injectAt(long journalId, long pos, byte[] data) throws Exception {
        File f = txnFile(journalId);
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.seek(pos);
            raf.write(data);
            raf.setLength(pos + data.length);
        }
    }

    private File txnFile(long journalId) {
        File f = new File(journalDir, Long.toHexString(journalId) + ".txn");
        assertTrue(f.exists(), "Missing .txn file: " + f);
        return f;
    }

    private static byte[] intToBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] p : parts) total += p.length;
        byte[] result = new byte[total];
        int offset = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, result, offset, p.length);
            offset += p.length;
        }
        return result;
    }

    private static ByteBuf makeEntry(long ledgerId, long entryId, String payload) {
        byte[] bytes = payload.getBytes();
        ByteBuf buf = Unpooled.buffer(8 + 8 + bytes.length);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeBytes(bytes);
        return buf;
    }

    /** Reads private field {@code flushWhenQueueEmpty} via reflection. */
    private static boolean readFlushWhenQueueEmpty(Journal j) throws Exception {
        Field f = Journal.class.getDeclaredField("flushWhenQueueEmpty");
        f.setAccessible(true);
        return (boolean) f.get(j);
    }

    /** Reads private field {@code allocator} via reflection. */
    private static ByteBufAllocator readAllocator(Journal j) throws Exception {
        Field f = Journal.class.getDeclaredField("allocator");
        f.setAccessible(true);
        return (ByteBufAllocator) f.get(j);
    }

    // ==========================================================================
    // ── CONSTRUCTOR TESTS ──────────────────────────────────────────────────────
    // ==========================================================================

    // ── C-1: BusyWait path — lines 663-664 ─────────────────────────────────────

    /**
     * C-1 — {@code conf.isBusyWaitEnabled() = true} selects BlockingMpscQueue
     * for both {@code queue} and {@code forceWriteRequests} (lines 663-664).
     *
     * <p>This branch requires {@code busyWait} to be enabled on the configuration.
     * The test confirms that construction succeeds and that the journal is
     * operationally correct by performing a write + scan round-trip.
     *
     * <p>Note: {@code CpuAffinity.acquireCore()} inside {@code ForceWriteThread.run()}
     * may log a warning on CI (no dedicated core available) but never throws, so the
     * test remains stable.
     */
    @Test
    @DisplayName("C-1 — busyWaitEnabled=true → BlockingMpscQueue path (lines 663-664)")
    void constructor_busyWaitEnabled_usesBlockingMpscQueue() throws Exception {
        conf.setBusyWaitEnabled(true);

        journal = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        assertNotNull(journal, "Construction must succeed with busyWait=true");

        // Verify the queue fields are the MPSC variant via their class name.
        Field queueField = Journal.class.getDeclaredField("queue");
        queueField.setAccessible(true);
        Object queue = queueField.get(journal);
        assertTrue(queue.getClass().getSimpleName().contains("BlockingMpscQueue"),
                "queue must be a BlockingMpscQueue when busyWait=true, was: "
                        + queue.getClass().getName());

        Field fwField = Journal.class.getDeclaredField("forceWriteRequests");
        fwField.setAccessible(true);
        Object fwq = fwField.get(journal);
        assertTrue(fwq.getClass().getSimpleName().contains("BlockingMpscQueue"),
                "forceWriteRequests must be a BlockingMpscQueue when busyWait=true, was: "
                        + fwq.getClass().getName());

        // Verify operational correctness via round-trip.
        journal.start();
        writeOneEntry(journal);
        journal.shutdown();

        List<long[]> recovered = new ArrayList<>();
        Journal scan = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        long id = Journal.listJournalIds(journalDir, null).get(0);
        scan.scanJournal(id, 0L, (version, offset, entry) -> {
            if (entry.remaining() >= 16) {
                recovered.add(new long[]{entry.getLong(), entry.getLong()});
            }
        }, false);

        assertEquals(1, recovered.size(), "One entry must survive the round-trip");
        assertEquals(LEDGER_ID, recovered.get(0)[0], "ledgerId must match");
        journal = null;
    }

    // ── C-2: Multiple dirs → suffixed lastMark file name via 6-arg constructor ──

    /**
     * C-2 — With N=2 journal directories in the configuration, the 6-arg constructor
     * must produce {@code lastMarkFileName = "lastMark.0"} (line 700), not the plain
     * {@code "lastMark"} used for single-directory configurations (line 698).
     *
     * <p>The field is {@code private final} and is read via reflection.
     */
    @Test
    @DisplayName("C-2 — multiDir conf → lastMarkFileName suffixed (line 700) via 6-arg constructor")
    void constructor_multipleJournalDirs_suffixedLastMarkFileName() throws Exception {
        File dir2 = new File(tempDir, "journal2");
        dir2.mkdirs();

        ServerConfiguration multiConf = new ServerConfiguration();
        multiConf.setJournalDirsName(new String[]{
                journalDir.getAbsolutePath(),
                dir2.getAbsolutePath()
        });
        multiConf.setMaxJournalSizeMB(2);
        multiConf.setProperty("journalPreAllocSizeMB", 1);
        multiConf.setJournalWriteBufferSizeKB(4);
        multiConf.setJournalRemovePagesFromCache(false);
        multiConf.setAllowLoopback(true);

        journal = new Journal(0, journalDir, multiConf, dirsManager,
                NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

        assertNotNull(journal, "Construction must succeed with multiple journal dirs");

        Field f = Journal.class.getDeclaredField("lastMarkFileName");
        f.setAccessible(true);
        String lastMarkFileName = (String) f.get(journal);

        assertEquals("lastMark.0", lastMarkFileName,
                "lastMarkFileName must be 'lastMark.0' when conf has multiple journal dirs "
                        + "and journalIndex=0 (line 700)");
    }

    // ── C-3: Custom StatsLogger scoping via 6-arg constructor ──────────────────

    /**
     * C-3 — The constructor calls {@code statsLogger.scopeLabel("journalIndex", "0")}
     * at line 659.  Passing a delegating {@link StatsLogger} that counts
     * {@code scopeLabel} invocations verifies the real delegation path rather than
     * the no-op {@link NullStatsLogger}.
     *
     * <p>A {@link NullStatsLogger} is used as the inner delegate so the construction
     * remains light-weight and side-effect free.
     */
    @Test
    @DisplayName("C-3 — custom StatsLogger.scopeLabel() is called during 6-arg construction (line 659)")
    void constructor_customStatsLogger_scopeLabelInvoked() throws Exception {
        AtomicInteger scopeCallCount = new AtomicInteger(0);

        // Minimal StatsLogger that counts scopeLabel() calls and delegates to NullStatsLogger.
        StatsLogger countingLogger = new StatsLogger() {
            private final StatsLogger delegate = NullStatsLogger.INSTANCE;

            @Override
            public OpStatsLogger getOpStatsLogger(String name) {
                return delegate.getOpStatsLogger(name);
            }

            @Override
            public OpStatsLogger getThreadScopedOpStatsLogger(String name) {
                return delegate.getThreadScopedOpStatsLogger(name);
            }

            @Override
            public Counter getCounter(String name) {
                return delegate.getCounter(name);
            }

            @Override
            public Counter getThreadScopedCounter(String name) {
                return delegate.getThreadScopedCounter(name);
            }

            @Override
            public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
                delegate.registerGauge(name, gauge);
            }

            @Override
            public <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {
                delegate.unregisterGauge(name, gauge);
            }

            @Override
            public StatsLogger scope(String name) {
                return delegate.scope(name);
            }

            @Override
            public StatsLogger scopeLabel(String labelName, String labelValue) {
                scopeCallCount.incrementAndGet();
                return delegate.scopeLabel(labelName, labelValue);
            }

            @Override
            public void removeScope(String name, StatsLogger scopedLogger) {
                delegate.removeScope(name, scopedLogger);
            }
        };

        journal = build6Arg(conf, countingLogger, UnpooledByteBufAllocator.DEFAULT);

        assertNotNull(journal, "Construction must succeed with a custom StatsLogger");
        assertTrue(scopeCallCount.get() >= 1,
                "statsLogger.scopeLabel() must have been called at least once during construction; "
                        + "actual call count: " + scopeCallCount.get());
    }

    // ── C-4: Custom ByteBufAllocator stored and returned by builder ────────────

    /**
     * C-4 — The allocator passed to the 6-arg constructor is stored in
     * {@code this.allocator} (line 657) and later returned by
     * {@code getBufferedChannelBuilder()}.
     *
     * <p>This test passes a {@link PooledByteBufAllocator} and then reads the stored
     * field via reflection to assert identity.  It also invokes
     * {@code getBufferedChannelBuilder()} to confirm the builder is non-null
     * (exercising the lambda at line 1235 that captures {@code allocator}).
     */
    @Test
    @DisplayName("C-4 — custom ByteBufAllocator stored and returned by getBufferedChannelBuilder() (line 657)")
    void constructor_customAllocator_storedAndReturnedByBuilder() throws Exception {
        ByteBufAllocator customAlloc = PooledByteBufAllocator.DEFAULT;

        journal = build6Arg(conf, NullStatsLogger.INSTANCE, customAlloc);

        assertNotNull(journal, "Construction must succeed with a custom ByteBufAllocator");

        ByteBufAllocator stored = readAllocator(journal);
        assertNotNull(stored, "Stored allocator must not be null");
        assertEquals(customAlloc, stored,
                "The stored allocator must be identical to the one passed to the constructor");

        Journal.BufferedChannelBuilder builder = journal.getBufferedChannelBuilder();
        assertNotNull(builder,
                "getBufferedChannelBuilder() must return a non-null builder that captures the allocator");
    }

    // ── C-5: Invalid JournalChannelProvider via 6-arg constructor ──────────────

    /**
     * C-5 — The IOException-wrapping branch at lines 709-711 is exercised through
     * the full 6-arg constructor signature.
     *
     * <p>{@link FileChannelProvider#newProvider(String)} resolves the provider by
     * class name.  A non-existent class name causes an {@link IOException} that the
     * constructor catches and re-throws as a {@link RuntimeException}.
     *
     * <p>Assertions mirror those in
     * {@code Isw2JournalConstructorControlFlowTest#coverage_invalidFileChannelProvider_throwsRuntimeException}
     * but target the 6-arg call site so that JaCoCo maps the branch to the correct
     * constructor's coverage table.
     */
    @Test
    @DisplayName("C-5 — invalid JournalChannelProvider via 6-arg constructor → RuntimeException wrapping IOException (lines 709-711)")
    void constructor_6arg_invalidChannelProvider_throwsRuntimeExceptionWrappingIOException() {
        conf.setJournalChannelProvider("com.nonexistent.InvalidProvider");

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new Journal(0, journalDir, conf, dirsManager,
                        NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT),
                "6-arg constructor must throw RuntimeException for an invalid channel provider class");

        assertNotNull(ex.getCause(),
                "The RuntimeException must wrap the original IOException as its cause");
        assertInstanceOf(IOException.class, ex.getCause(),
                "The wrapped cause must be an IOException from FileChannelProvider.newProvider()");
    }

    // ── C-6: flushWhenQueueEmpty via 6-arg constructor — all three branches ─────

    /**
     * C-6a — BRANCH A via 6-arg constructor: {@code maxGroupWaitInNanos <= 0}
     * (short-circuit, result {@code true}).
     */
    @Test
    @DisplayName("C-6a — 6-arg: flushWhenQueueEmpty=true when maxGroupWaitMSec=0 (branch A)")
    void constructor_6arg_flushWhenQueueEmpty_branchA() throws Exception {
        conf.setJournalMaxGroupWaitMSec(0);
        conf.setJournalFlushWhenQueueEmpty(false); // irrelevant: short-circuit prevents evaluation

        journal = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

        assertTrue(readFlushWhenQueueEmpty(journal),
                "flushWhenQueueEmpty must be true when maxGroupWaitMSec=0 (branch A)");
    }

    /**
     * C-6b — BRANCH B via 6-arg constructor: first operand false, second operand
     * {@code getJournalFlushWhenQueueEmpty() = true} → result {@code true}.
     */
    @Test
    @DisplayName("C-6b — 6-arg: flushWhenQueueEmpty=true when maxGroupWait>0 and explicit=true (branch B)")
    void constructor_6arg_flushWhenQueueEmpty_branchB() throws Exception {
        conf.setJournalMaxGroupWaitMSec(1);
        conf.setJournalFlushWhenQueueEmpty(true);

        journal = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

        assertTrue(readFlushWhenQueueEmpty(journal),
                "flushWhenQueueEmpty must be true when maxGroupWait>0 but explicit=true (branch B)");
    }

    /**
     * C-6c — BRANCH C via 6-arg constructor: both operands false → result {@code false}.
     */
    @Test
    @DisplayName("C-6c — 6-arg: flushWhenQueueEmpty=false when maxGroupWait>0 and explicit=false (branch C)")
    void constructor_6arg_flushWhenQueueEmpty_branchC() throws Exception {
        conf.setJournalMaxGroupWaitMSec(1);
        conf.setJournalFlushWhenQueueEmpty(false);

        journal = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

        assertFalse(readFlushWhenQueueEmpty(journal),
                "flushWhenQueueEmpty must be false when maxGroupWait>0 and explicit=false (branch C)");
    }

    // ==========================================================================
    // ── SCAN JOURNAL TESTS ─────────────────────────────────────────────────────
    // ==========================================================================

    // ── S-1: journalPos > 0 branch (line 818) ──────────────────────────────────

    /**
     * S-1 — When {@code journalPos > 0}, the scan opens the journal channel at the
     * given offset (line 818-820) instead of from the start.
     *
     * <p>A seed entry is written at position 0.  The injection point (the byte
     * position immediately after the seed entry) is used as {@code journalPos};
     * the scanner should then find zero entries because the cursor starts after
     * the only valid record in the file.
     *
     * <p>The method must return without throwing, and the returned position must
     * be ≥ the supplied {@code journalPos}.
     */
    @Test
    @DisplayName("S-1 — journalPos > 0 → JournalChannel opened at offset (line 818)")
    void scanJournal_positiveJournalPos_opensAtOffset() throws Exception {
        long id     = writeSeedAndShutdown();
        long endPos = lastRecordEndPosition(id);

        List<long[]> collected = new ArrayList<>();
        Journal scanJ = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

        // Start scanning from the position AFTER the seed entry → 0 entries expected.
        long returnedPos = scanJ.scanJournal(id, endPos, (version, offset, entry) -> {
            if (entry.remaining() >= 16) {
                collected.add(new long[]{entry.getLong(), entry.getLong()});
            }
        }, false);

        assertEquals(0, collected.size(),
                "No entries should be found when journalPos starts after the last valid record");
        assertTrue(returnedPos >= endPos,
                "Returned position must be >= the supplied journalPos");
    }

    // ── S-2: len == 0 immediate break (line 836-837) ───────────────────────────

    /**
     * S-2 — Injecting a 4-byte zero word immediately after the last valid record
     * causes the length read to yield {@code len = 0}, triggering the
     * {@code break} at line 837.
     *
     * <p>The scanner must have received exactly the seed entry before the break,
     * and no exception must be thrown.
     */
    @Test
    @DisplayName("S-2 — len==0 after valid records → immediate break (line 837)")
    void scanJournal_zeroLength_immediateBreak() throws Exception {
        long id     = writeSeedAndShutdown();
        long endPos = lastRecordEndPosition(id);

        // Inject a bare 4-byte zero word (len=0) and truncate.
        injectAt(id, endPos, intToBytes(0));

        List<long[]> collected = new ArrayList<>();
        Journal scanJ = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        scanJ.scanJournal(id, 0L, (version, offset, entry) -> {
            if (entry.remaining() >= 16) {
                collected.add(new long[]{entry.getLong(), entry.getLong()});
            }
        }, false);

        assertEquals(1, collected.size(),
                "Seed entry must be recovered before the zero-length break");
        assertEquals(LEDGER_ID, collected.get(0)[0], "ledgerId must match");
    }

    // ── S-3: Padding record with non-zero payload (line 853 + 870 guard) ────────

    /**
     * S-3 — A padding record ({@code PADDING_MASK} followed by a non-zero length word
     * and the corresponding payload bytes) sets {@code isPaddingRecord = true} at
     * line 853.  The {@code if (!isPaddingRecord)} guard at line 870 evaluates to
     * {@code false}, so the scanner is NOT called for the padding record.
     *
     * <p>Injected layout (immediately after seed entry, file truncated afterwards):
     * <pre>
     *   [PADDING_MASK: 4 bytes] [paddingLen=8: 4 bytes] [8 zero bytes payload]
     * </pre>
     * The seed entry count must remain 1 (the padding record is silently consumed).
     */
    @Test
    @DisplayName("S-3 — PADDING_MASK + non-zero len → isPaddingRecord=true, scanner skipped (lines 853, 870)")
    void scanJournal_paddingMaskWithNonZeroLen_scannerNotCalled() throws Exception {
        long id     = writeSeedAndShutdown();
        long endPos = lastRecordEndPosition(id);

        int paddingPayloadLen = 8;
        byte[] paddingPayload = new byte[paddingPayloadLen]; // all zeros
        byte[] injection = concat(
                intToBytes(PADDING_MASK),
                intToBytes(paddingPayloadLen),
                paddingPayload
        );
        injectAt(id, endPos, injection);

        List<long[]> collected = new ArrayList<>();
        Journal scanJ = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        scanJ.scanJournal(id, 0L, (version, offset, entry) -> {
            if (entry.remaining() >= 16) {
                collected.add(new long[]{entry.getLong(), entry.getLong()});
            }
        }, false);

        assertEquals(1, collected.size(),
                "Only the seed entry must be reported; the padding record must not be forwarded to the scanner");
        assertEquals(LEDGER_ID, collected.get(0)[0], "ledgerId must match");
    }

    // ── S-4: recBuff reallocation (line 860-862) ────────────────────────────────

    /**
     * S-4 — When a record's declared length exceeds the current capacity of
     * {@code recBuff} (initial capacity: 64 KiB), the buffer is reallocated
     * (line 862).
     *
     * <p>Injected layout:
     * <pre>
     *   [len = 65 KiB + 1 = 66561 bytes] [66561 bytes payload starting with ledgerId + entryId]
     * </pre>
     * The payload is crafted so that its first 16 bytes are a valid
     * {@code (ledgerId, entryId)} pair.  After the scan the collector should
     * have two entries: the original seed plus the large injected record.
     */
    @Test
    @DisplayName("S-4 — record length > 64 KiB → recBuff reallocated (line 862)")
    void scanJournal_largeRecord_recBuffReallocated() throws Exception {
        long id     = writeSeedAndShutdown();
        long endPos = lastRecordEndPosition(id);

        // Build a payload larger than the initial 64 KiB recBuff.
        int payloadLen = 64 * 1024 + 1; // 65 537 bytes
        byte[] payload = new byte[payloadLen];

        // Embed a recognisable (ledgerId, entryId) in the first 16 bytes.
        long injectedLedgerId = 99L;
        long injectedEntryId  = 1L;
        ByteBuffer header = ByteBuffer.wrap(payload);
        header.putLong(injectedLedgerId);
        header.putLong(injectedEntryId);

        byte[] injection = concat(intToBytes(payloadLen), payload);
        injectAt(id, endPos, injection);

        List<long[]> collected = new ArrayList<>();
        Journal scanJ = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        scanJ.scanJournal(id, 0L, (version, offset, entry) -> {
            if (entry.remaining() >= 16) {
                collected.add(new long[]{entry.getLong(), entry.getLong()});
            }
        }, false);

        // We expect the seed entry plus the oversized injected record.
        assertEquals(2, collected.size(),
                "Seed entry and the large injected record must both be recovered "
                        + "after recBuff reallocation");
        assertEquals(injectedLedgerId, collected.get(1)[0],
                "Second entry ledgerId must match the injected value");
        assertEquals(injectedEntryId, collected.get(1)[1],
                "Second entry entryId must match the injected value");
    }

    // ── S-5: IOException from scanner, skipInvalidRecord = true (lines 877-881) ─

    /**
     * S-5 — When the scanner's {@code process()} method throws an
     * {@link IOException} and {@code skipInvalidRecord = true}, the catch block
     * at lines 875-878 logs a warning and the method returns normally (line 881)
     * instead of propagating the exception.
     *
     * <p>The scanner always throws so the catch block is entered on the very first
     * (seed) entry.  The method must return without throwing.
     */
    @Test
    @DisplayName("S-5 — scanner throws IOException, skipInvalidRecord=true → swallowed, returns position (lines 877, 881)")
    void scanJournal_scannerThrowsIOException_skipTrue_swallowedReturnsPosition() throws Exception {
        long id = writeSeedAndShutdown();

        Journal scanJ = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

        Journal.JournalScanner throwingScanner = (version, offset, entry) -> {
            throw new IOException("Simulated scanner failure for S-5");
        };

        // Must NOT throw: the exception is swallowed when skipInvalidRecord=true.
        long returnedPos = scanJ.scanJournal(id, 0L, throwingScanner, true);

        assertTrue(returnedPos > 0,
                "scanJournal must return a positive file position even after swallowing the scanner's IOException");
    }

    // ── S-6: IOException from scanner, skipInvalidRecord = false (line 879) ─────

    /**
     * S-6 — When the scanner's {@code process()} method throws an
     * {@link IOException} and {@code skipInvalidRecord = false}, the catch block
     * at line 879 re-throws the exception to the caller.
     *
     * <p>The test asserts that {@link IOException} propagates out of
     * {@code scanJournal} with the original message preserved.
     */
    @Test
    @DisplayName("S-6 — scanner throws IOException, skipInvalidRecord=false → propagated to caller (line 879)")
    void scanJournal_scannerThrowsIOException_skipFalse_propagated() throws Exception {
        long id = writeSeedAndShutdown();

        Journal scanJ = build6Arg(conf, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

        String expectedMessage = "Simulated scanner failure for S-6";
        Journal.JournalScanner throwingScanner = (version, offset, entry) -> {
            throw new IOException(expectedMessage);
        };

        IOException thrown = assertThrows(IOException.class,
                () -> scanJ.scanJournal(id, 0L, throwingScanner, false),
                "scanJournal must propagate the scanner's IOException when skipInvalidRecord=false");

        assertEquals(expectedMessage, thrown.getMessage(),
                "The propagated IOException must carry the original message from the scanner");
    }
}