package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Control-flow coverage tests for the {@link Journal} constructor.
 *
 * <h3>Branch 1 — {@code flushWhenQueueEmpty} (line 693)</h3>
 * <pre>
 *   this.flushWhenQueueEmpty = maxGroupWaitInNanos &lt;= 0
 *                              || conf.getJournalFlushWhenQueueEmpty();
 * </pre>
 * JaCoCo treats the short-circuit OR as three distinct branches:
 * <ul>
 *   <li>BRANCH A — {@code maxGroupWaitInNanos <= 0} is true → short-circuit, result true</li>
 *   <li>BRANCH B — {@code maxGroupWaitInNanos <= 0} is false, {@code getJournalFlushWhenQueueEmpty()} is true → result true</li>
 *   <li>BRANCH C — {@code maxGroupWaitInNanos <= 0} is false, {@code getJournalFlushWhenQueueEmpty()} is false → result false</li>
 * </ul>
 * The field is {@code private final} with no getter: it is read via reflection.
 *
 * <h3>Branch 2 — {@code FileChannelProvider} IOException (lines 709-711)</h3>
 * <pre>
 *   try {
 *       this.fileChannelProvider = FileChannelProvider.newProvider(conf.getJournalChannelProvider());
 *   } catch (IOException e) {                                          // line 709 — nc
 *       LOG.error("Failed to initiate file channel provider: ...");   // line 710 — nc
 *       throw new RuntimeException(e);                                // line 711 — nc
 *   }
 * </pre>
 * {@link org.apache.bookkeeper.bookie.FileChannelProvider#newProvider(String)} instantiates
 * the provider class by name via reflection and throws {@link java.io.IOException} if the
 * class cannot be found or instantiated. Passing an invalid class name triggers this branch.
 */
@DisplayName("Journal — Control-flow coverage: constructor branches (lines 693, 709-711)")
public class Isw2JournalConstructorControlFlowTest {

    @TempDir
    File tempDir;

    private Journal journal;

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
     * Builds a minimal {@link ServerConfiguration} with a single journal directory.
     * Mirrors the {@code buildConf()} helper used in {@code Isw2JournalConstructorBVATest}.
     */
    private static ServerConfiguration buildConf(File journalDir, int maxSizeMB, int preAllocSizeMB) {
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
     * Builds a minimal valid {@link LedgerDirsManager} mock.
     * Mirrors the {@code buildMock()} helper used in {@code Isw2JournalConstructorBVATest}.
     */
    private static LedgerDirsManager buildMock(File ledgerDir)
            throws LedgerDirsManager.NoWritableLedgerDirException {
        LedgerDirsManager mgr = mock(LedgerDirsManager.class);
        when(mgr.getAllLedgerDirs()).thenReturn(List.of(ledgerDir));
        when(mgr.getWritableLedgerDirsForNewLog()).thenReturn(List.of(ledgerDir));
        return mgr;
    }

    /**
     * Reads the private field {@code flushWhenQueueEmpty} from a Journal instance
     * via reflection. Required because the field is {@code private final} with no
     * getter or {@code @VisibleForTesting} annotation.
     *
     * @param j the Journal instance to inspect
     * @return the current value of {@code flushWhenQueueEmpty}
     */
    private static boolean readFlushWhenQueueEmpty(Journal j) throws Exception {
        java.lang.reflect.Field f = Journal.class.getDeclaredField("flushWhenQueueEmpty");
        f.setAccessible(true);
        return (boolean) f.get(j);
    }

    // =========================================================================
    // Branch 1 — flushWhenQueueEmpty (line 693)
    // =========================================================================

    /**
     * Coverage — BRANCH A: {@code maxGroupWaitInNanos <= 0} is true.
     *
     * <p>With {@code setJournalMaxGroupWaitMSec(0)}, {@code maxGroupWaitInNanos} is
     * computed as {@code TimeUnit.MILLISECONDS.toNanos(0) = 0}, satisfying {@code <= 0}.
     * The OR short-circuits: {@code getJournalFlushWhenQueueEmpty()} is never evaluated
     * and {@code flushWhenQueueEmpty} is set to {@code true}.
     *
     * <p>{@code setJournalFlushWhenQueueEmpty(false)} is set explicitly to confirm
     * that the result depends solely on the first operand.
     */
    @Test
    @DisplayName("Coverage — flushWhenQueueEmpty=true quando maxGroupWaitMSec=0 (short-circuit, BRANCH A)")
    void coverage_flushWhenQueueEmpty_trueWhenNoGroupWait() throws Exception {
        File journalDir = new File(tempDir, "journal-cov-a"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger-cov-a");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 2, 1);
        conf.setJournalMaxGroupWaitMSec(0);
        conf.setJournalFlushWhenQueueEmpty(false); // irrilevante: non viene valutato

        LedgerDirsManager mgr = buildMock(ledgerDir);

        journal = new Journal(0, journalDir, conf, mgr);
        assertNotNull(journal, "Constructor must succeed");

        assertTrue(readFlushWhenQueueEmpty(journal),
                "flushWhenQueueEmpty deve essere true quando maxGroupWaitMSec=0 "
                        + "(maxGroupWaitInNanos <= 0 → short-circuit)");
    }

    /**
     * Coverage — BRANCH B: {@code maxGroupWaitInNanos <= 0} is false,
     * {@code getJournalFlushWhenQueueEmpty()} is true.
     *
     * <p>With {@code setJournalMaxGroupWaitMSec(1)},
     * {@code maxGroupWaitInNanos = 1_000_000 > 0}, so the first operand is false.
     * The second operand {@code getJournalFlushWhenQueueEmpty() = true} is evaluated
     * and determines the final result {@code true}.
     */
    @Test
    @DisplayName("Coverage — flushWhenQueueEmpty=true quando maxGroupWait>0 e flushWhenQueueEmpty=true (BRANCH B)")
    void coverage_flushWhenQueueEmpty_trueWhenExplicitlyEnabled() throws Exception {
        File journalDir = new File(tempDir, "journal-cov-b"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger-cov-b");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 2, 1);
        conf.setJournalMaxGroupWaitMSec(1);        // maxGroupWaitInNanos > 0 → primo operando false
        conf.setJournalFlushWhenQueueEmpty(true);  // secondo operando true → risultato true

        LedgerDirsManager mgr = buildMock(ledgerDir);

        journal = new Journal(0, journalDir, conf, mgr);
        assertNotNull(journal, "Constructor must succeed");

        assertTrue(readFlushWhenQueueEmpty(journal),
                "flushWhenQueueEmpty deve essere true quando maxGroupWait>0 "
                        + "ma getJournalFlushWhenQueueEmpty()=true (BRANCH B)");
    }

    /**
     * Coverage — BRANCH C: both operands are false → result false.
     *
     * <p>With {@code setJournalMaxGroupWaitMSec(1)}, the first operand is false.
     * With {@code setJournalFlushWhenQueueEmpty(false)}, the second operand is also
     * false. The final result is {@code false}: the Journal will not flush automatically
     * on an empty queue but will instead wait for the group-wait timeout.
     */
    @Test
    @DisplayName("Coverage — flushWhenQueueEmpty=false quando maxGroupWait>0 e flushWhenQueueEmpty=false (BRANCH C)")
    void coverage_flushWhenQueueEmpty_falseWhenGroupWaitAndNotExplicit() throws Exception {
        File journalDir = new File(tempDir, "journal-cov-c"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger-cov-c");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 2, 1);
        conf.setJournalMaxGroupWaitMSec(1);         // maxGroupWaitInNanos > 0 → primo operando false
        conf.setJournalFlushWhenQueueEmpty(false);  // secondo operando false → risultato false

        LedgerDirsManager mgr = buildMock(ledgerDir);

        journal = new Journal(0, journalDir, conf, mgr);
        assertNotNull(journal, "Constructor must succeed");

        assertFalse(readFlushWhenQueueEmpty(journal),
                "flushWhenQueueEmpty deve essere false quando maxGroupWait>0 "
                        + "e getJournalFlushWhenQueueEmpty()=false (BRANCH C)");
    }

    // =========================================================================
    // Branch 2 — FileChannelProvider IOException (lines 709-711)
    // =========================================================================

    /**
     * Coverage — lines 709-711: {@code catch (IOException e)} block.
     *
     * <p>{@link org.apache.bookkeeper.bookie.FileChannelProvider#newProvider(String)}
     * resolves the provider class by name via reflection. Passing a non-existent class
     * name causes instantiation to fail with an {@link java.io.IOException}, which the
     * constructor catches and re-throws as a {@link RuntimeException}.
     *
     * <p>The test verifies that:
     * <ol>
     *   <li>A {@link RuntimeException} is thrown at constructor site (line 711).</li>
     *   <li>The cause is an {@link java.io.IOException} (line 709 catch entered).</li>
     * </ol>
     *
     * <p>No {@code journal.start()} is called because construction fails before
     * the Journal instance is returned — {@code @AfterEach} handles the null case safely.
     */
    @Test
    @DisplayName("Coverage — lines 709-711: invalid JournalChannelProvider class → RuntimeException wrapping IOException")
    void coverage_invalidFileChannelProvider_throwsRuntimeException() throws Exception {
        File journalDir = new File(tempDir, "journal-cov-d"); journalDir.mkdirs();
        File ledgerDir  = new File(tempDir, "ledger-cov-d");  ledgerDir.mkdirs();

        ServerConfiguration conf = buildConf(journalDir, 2, 1);
        // Passing a non-existent class name causes FileChannelProvider.newProvider()
        // to fail with IOException → constructor catches it → throws RuntimeException
        conf.setJournalChannelProvider("com.nonexistent.InvalidFileChannelProvider");

        LedgerDirsManager mgr = buildMock(ledgerDir);

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new Journal(0, journalDir, conf, mgr),
                "Constructor must throw RuntimeException when JournalChannelProvider class is invalid");

        assertNotNull(ex.getCause(),
                "RuntimeException must wrap the original IOException as its cause");

        assertInstanceOf(java.io.IOException.class, ex.getCause(),
                "The wrapped cause must be an IOException from FileChannelProvider.newProvider()");
    }

}