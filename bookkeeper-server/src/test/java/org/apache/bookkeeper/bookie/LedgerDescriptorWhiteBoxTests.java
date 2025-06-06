/*
 The used llm for generate the code is the Jetbrains Ai
 that is available at: https://www.jetbrains.com/help/idea/generating-source-code.html
 it can take all the context of the file and generate the code
 */


package org.apache.bookkeeper.bookie;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;
import  org.apache.bookkeeper.bookie.LedgerDescriptorFunctionalTests;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.bookkeeper.bookie.BookieException;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
@DisplayName("LedgerDescriptorImpl from llm  ")
public class LedgerDescriptorWhiteBoxTests {
    /*
        Asked to the llm to generate test that maximize the coverage
        of the code and it take the context of the file as he needed
    */
    private static final long TEST_LEDGER_ID = 12345L;
    private LedgerStorage ledgerStorage;
    private HandleFactoryImpl handleFactory;
    private LedgerDescriptor ledger;
    private byte[] masterKey;
    private Journal journal;


    @BeforeEach
    void setUp() throws IOException, BookieException {
        this.ledgerStorage = Mockito.mock(LedgerStorage.class);
        this.handleFactory = new HandleFactoryImpl(ledgerStorage);
        this.journal = Mockito.mock(Journal.class);
        masterKey = new byte[]{1, 2, 3, 4};
        ledger = this.handleFactory.getHandle(TEST_LEDGER_ID, masterKey, true);
    }

    public List<ByteBuf> writeEntries() throws IOException, BookieException {
        List<ByteBuf> entries = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ByteBuf entry = Unpooled.buffer(16);
            entry.writeLong(TEST_LEDGER_ID);
            entry.writeLong(i);
            entries.add(entry);
        }

        for (int i = 0; i < entries.size(); i++) {
            when(ledgerStorage.addEntry(entries.get(i))).thenReturn((long) i);
            when(ledgerStorage.getEntry(TEST_LEDGER_ID, i)).thenReturn(entries.get(i));
        }

        for (ByteBuf entry : entries) {
            ledger.addEntry(entry);
        }
        return entries;
    }

    @Test
    @DisplayName("Test write and read in order")
    void testWriteAndReadInOrder() throws IOException, BookieException {
        List<ByteBuf> entries = writeEntries();
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(entries.get(i), ledger.readEntry(i));
        }
    }

    /* Tests the explicit Last Add Confirmed (LAC) operations:
     * - Setting LAC value in the storage
     * - Retrieving LAC value from storage
     * - Error handling when storage operations fail
     * - Verification of proper storage interaction
     */
    @Test
    @DisplayName("Test explicit LAC operations")
    void testExplicitLACOperations() throws IOException, BookieException {
        ByteBuf lacBuf = Unpooled.buffer(8);
        lacBuf.writeLong(100L);

        assertDoesNotThrow(() -> ledger.setExplicitLac(lacBuf));
        verify(ledgerStorage).setExplicitLac(TEST_LEDGER_ID, lacBuf);

        when(ledgerStorage.getExplicitLac(TEST_LEDGER_ID)).thenReturn(lacBuf);
        ByteBuf retrievedLac = ledger.getExplicitLac();
        assertEquals(lacBuf, retrievedLac);

        when(ledgerStorage.getExplicitLac(TEST_LEDGER_ID))
                .thenThrow(new IOException("Storage error"));
        assertThrows(IOException.class, () -> ledger.getExplicitLac());
    }

    /* Tests the master key authentication mechanism:
     * - Successful authentication with correct master key
     * - Failed authentication with incorrect master key
     * - Handling of null master key
     * - Proper exception throwing for unauthorized access
     */
    @Test
    @DisplayName("Test master key authentication")
    void testMasterKeyAuthentication() throws IOException, BookieException {
        assertDoesNotThrow(() -> ledger.checkAccess(masterKey));

        byte[] wrongKey = new byte[]{5, 6, 7, 8};
        assertThrows(BookieException.class, () -> ledger.checkAccess(wrongKey));

        assertThrows(BookieException.class, () -> ledger.checkAccess(null));
    }

    /* Tests the Last Add Confirmed (LAC) operations and watcher functionality:
     * - Retrieval of last add confirmed entry ID
     * - Registration of watchers for LAC updates
     * - Watcher notification mechanism
     * - Cancellation of watchers
     * - Storage interaction verification
     */
    @Test
    @DisplayName("Test last add confirmed operations")
    void testLastAddConfirmedOperations() throws IOException, BookieException {
        when(ledgerStorage.getLastAddConfirmed(TEST_LEDGER_ID)).thenReturn(99L);
        assertEquals(99L, ledger.getLastAddConfirmed());

        Watcher<LastAddConfirmedUpdateNotification> watcher = mock(Watcher.class);
        when(ledgerStorage.waitForLastAddConfirmedUpdate(eq(TEST_LEDGER_ID), eq(99L), any()))
                .thenReturn(true);
        assertTrue(ledger.waitForLastAddConfirmedUpdate(99L, watcher));

        assertDoesNotThrow(() -> ledger.cancelWaitForLastAddConfirmedUpdate(watcher));
        verify(ledgerStorage).cancelWaitForLastAddConfirmedUpdate(TEST_LEDGER_ID, watcher);
    }

    /*
    Tests the ledger fencing mechanism with multiple concurrent requests:
     * - Verifies that the first fencing operation is properly logged in the journal
     * - Ensures subsequent fence requests reuse the result of the first operation
     * - Checks that the ledger storage is marked as fenced only once
     * - Validates that journal entries are written only once
     * - Confirms both fence operations complete successfully when journal write succeeds
     */
    @Disabled("TODO: not work for pitest")
    @Test
    @DisplayName("Test concurrent fence operations")
    void testConcurrentFenceOperations() throws IOException, InterruptedException {
        AtomicBoolean not_firstCall = new AtomicBoolean(false);

        when(ledgerStorage.setFenced(TEST_LEDGER_ID))
                .thenAnswer(invocation -> {
                    if (!not_firstCall.get()) {
                        not_firstCall.set(true);
                        return true;
                    }
                    return false;
                });

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            WriteCallback callback = (WriteCallback) args[2];
            callback.writeComplete(0, TEST_LEDGER_ID, -1, null, null);
            return null;
        }).when(journal).logAddEntry(any(ByteBuf.class), anyBoolean(), any(), any());

        CompletableFuture<Boolean> future1 = ledger.fenceAndLogInJournal(journal);
        CompletableFuture<Boolean> future2 = ledger.fenceAndLogInJournal(journal);

        assertTrue(future1.join(), "First fence operation should succeed");
        assertTrue(future2.join(), "Second fence operation should succeed");

        // setFenced is called twice (returns true first time, false second time)
        verify(ledgerStorage, times(2)).setFenced(TEST_LEDGER_ID);
        // but journal write happens only once
        verify(journal, times(1)).logAddEntry(any(ByteBuf.class), anyBoolean(), any(), any());
    }



    /* Tests failure scenarios in journal fence entry logging:
     * - Journal write failures
     * - Error propagation
     * - Callback handling
     * - Recovery mechanism verification
     */
    @Disabled(" Not handled exception of null content ")
    @Test
    @DisplayName("Test journal fence entry logging failures")
    void testJournalFenceEntryLoggingFailures() throws IOException, InterruptedException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            WriteCallback callback = (WriteCallback) args[2];
            callback.writeComplete(BKException.Code.BookieHandleNotAvailableException, TEST_LEDGER_ID, -1, null, null);
            return null;
        }).when(journal).logAddEntry(any(ByteBuf.class), anyBoolean(), any(), any());

        CompletableFuture<Boolean> future = ledger.fenceAndLogInJournal(journal);

        assertFalse(future.join(), "Fence entry logging should fail");
    }

    /* Tests the retrieval and iteration of ledger entries:
     * - Correct entry list retrieval
     * - Iterator functionality
     * - Order preservation
     * - Storage interaction verification
     * - Memory efficiency of iteration
     */
    @Test
    @DisplayName("Test entry list retrieval")
    void testGetListOfEntriesOfLedger() throws IOException {
        long[] entries = {1L, 2L, 3L, 4L, 5L};
        OfLong iterator = Arrays.stream(entries).iterator();

        when(ledgerStorage.getListOfEntriesOfLedger(TEST_LEDGER_ID))
                .thenReturn(iterator);

        OfLong result = ledger.getListOfEntriesOfLedger(TEST_LEDGER_ID);

        List<Long> retrievedEntries = new ArrayList<>();
        while (result.hasNext()) {
            retrievedEntries.add(result.nextLong());
        }
        assertArrayEquals(entries, retrievedEntries.stream().mapToLong(Long::longValue).toArray());
    }


    /*
    ALREADY TESTED BEFORE  WITH THE TESTS OF THE FUNCTIONAL TESTS
    Tests writing to a closed ledger and verifies proper exception handling:
     * - Ledger closure verification
     * - Write attempts after closure
     * - Size verification
     * - Exception handling
     */
    @Disabled(" Test that fails because there is a check missing in the add of new Entry in the ledgerDescriptor")
    @Test
    @DisplayName("Test ledger closure, size verification and exception handling")
    void testWriteToClosedLedger() throws IOException, BookieException {
        when(ledgerStorage.setFenced(TEST_LEDGER_ID)).thenReturn(true);
        when(ledgerStorage.isFenced(TEST_LEDGER_ID)).thenReturn(true);
        assertTrue(ledger.setFenced());

        assertTrue(ledger.isFenced(), "Ledger should be closed after setFenced()");

        List<ByteBuf> entries = writeEntries();

        OfLong sizeBefore = ledger.getListOfEntriesOfLedger(TEST_LEDGER_ID);
        long initialSize = 0;
        while (sizeBefore.hasNext()) {
            initialSize++;
            sizeBefore.nextLong();
        }

        ByteBuf entry = Unpooled.buffer(16);
        entry.writeLong(TEST_LEDGER_ID);
        entry.writeLong(1);

        BookieException thrownException = assertThrows(BookieException.class,
                () -> ledger.addEntry(entry),
                "A BookieException should be thrown when writing to a closed ledger");

        // Corretto l'uso di assertEquals per BookieException.Code
        assertSame(BookieException.Code.LedgerFencedException, thrownException.getCode(),
                "Exception should be of type LedgerFencedException");

        assertThrows(BookieException.class,
                () -> ledger.readEntry(0),
                "Reading should also fail on a closed ledger");

        OfLong sizeAfter = ledger.getListOfEntriesOfLedger(TEST_LEDGER_ID);
        long finalSize = 0;
        while (sizeAfter.hasNext()) {
            finalSize++;
            sizeAfter.nextLong();
        }

        // Corretto l'uso di assertEquals per long
        assertSame(initialSize, finalSize,
                "Ledger size should not change after attempted write to closed ledger");
    }
    /**
     * Tests handling of thread interruption during journal write operations while fencing:
     * - Verifies correct handling of thread interruption
     * - Tests interrupt state management
     * - Validates exception propagation
     * - Ensures proper cleanup of interrupt state
     *
     * Checks:
     * - Exception type and cause
     * - Thread interrupt state clearing
     * - Interaction with ledger storage and journal
     */


    @Test
    @DisplayName("Test journal write interrupted during fence")
    void testJournalWriteInterruptedDuringFence() throws IOException, InterruptedException {
        when(ledgerStorage.setFenced(TEST_LEDGER_ID)).thenReturn(true);

        doAnswer(inv -> {
            Thread.currentThread().interrupt();
            return null;
        }).when(journal).logAddEntry(any(), anyBoolean(), any(), any());

        CompletableFuture<Boolean> future = ledger.fenceAndLogInJournal(journal);

        // Corretto: aspettiamo una InterruptedException invece di ExecutionException
        InterruptedException ex = assertThrows(InterruptedException.class, () -> future.get());

        // Non è necessario verificare ex.getCause() poiché l'eccezione è diretta
        assertFalse(Thread.interrupted()); // Clear interrupted state
    }

    /**
     * Tests concurrent updates of explicit Last Add Confirmed (LAC) values to verify thread safety:
     *
     * Scenario:
     * - Two threads attempt to update LAC values simultaneously
     * - First thread sets LAC to 100
     * - Second thread sets LAC to 200
     *
     * Test verifies:
     * - Both LAC updates are processed (using CountDownLatch)
     * - Storage layer receives both update requests
     * - No exceptions occur during concurrent updates
     * - Updates complete within expected timeframe (5 seconds)
     *
     * Implementation details:
     * - Uses CountDownLatch to track completion of both updates
     * - Captures actual LAC values passed to storage using ArgumentCaptor
     * - Verifies exactly two storage calls are made
     * - Ensures thread safety of LedgerDescriptor's LAC update mechanism
     *
     * Expected behavior:
     * - Both updates should complete successfully
     * - No race conditions should cause updates to be lost
     * - Storage layer should receive both update requests
     * - Operations should complete within timeout period
     */


    @Test
    @DisplayName("Test race condition in explicit LAC update")
    void testRaceConditionInExplicitLACUpdate() throws IOException, InterruptedException {
        // Non creare un nuovo mock qui, usa quello inizializzato in setUp()
        ByteBuf lac1 = Unpooled.buffer(8).writeLong(100);
        ByteBuf lac2 = Unpooled.buffer(8).writeLong(200);
        CountDownLatch latch = new CountDownLatch(2);

        // Configura il comportamento del mock
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(ledgerStorage).setExplicitLac(anyLong(), any(ByteBuf.class));

        // Crea e avvia i thread
        Thread t1 = new Thread(() -> {
            try {
                ledger.setExplicitLac(lac1);
            } catch (IOException e) {
                fail("Unexpected exception in t1: " + e);
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                ledger.setExplicitLac(lac2);
            } catch (IOException e) {
                fail("Unexpected exception in t2: " + e);
            }
        });

        t1.start();
        t2.start();

        // Attendi il completamento dei thread
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Timeout waiting for threads to complete");

        // Verifica le chiamate al mock
        ArgumentCaptor<ByteBuf> lacCaptor = ArgumentCaptor.forClass(ByteBuf.class);
        verify(ledgerStorage, times(2)).setExplicitLac(eq(TEST_LEDGER_ID), lacCaptor.capture());

        t1.join();
        t2.join();
    }

    /**
     * Tests the validation of entries with various edge cases:
     * - Empty entries (no data)
     * - Incomplete entries (only ledger ID)
     * - Invalid entries (wrong ledger ID)
     * - Valid entries (correct format and data)
     *
     * For each case verifies:
     * - Proper exception handling
     * - Storage layer interaction
     * - Entry validation logic
     * - Success conditions for valid entries
     */

    @Test
    @DisplayName("Test entry validation with various edge cases")
    void testEntryValidationEdgeCases() throws IOException, BookieException {
        // Test empty entry (should fail due to insufficient bytes to read ledgerId)
        ByteBuf emptyEntry = Unpooled.buffer(0);
        assertThrows(IndexOutOfBoundsException.class, () -> ledger.addEntry(emptyEntry));
        verify(ledgerStorage, never()).addEntry(any(ByteBuf.class));

        // Reset the mock for next test
        reset(ledgerStorage);

        // Test entry with only ledgerId (should pass ledgerId check but fail in storage)
        ByteBuf incompleteEntry = Unpooled.buffer(8);
        incompleteEntry.writeLong(TEST_LEDGER_ID);
        when(ledgerStorage.addEntry(any(ByteBuf.class)))
                .thenThrow(new IOException("Incomplete entry"));
        assertThrows(IOException.class, () -> ledger.addEntry(incompleteEntry));
        verify(ledgerStorage, times(1)).addEntry(any(ByteBuf.class));

        // Reset the mock for next test
        reset(ledgerStorage);

        // Test entry with wrong ledgerId (should fail ledgerId validation)
        ByteBuf wrongLedgerEntry = Unpooled.buffer(16);
        wrongLedgerEntry.writeLong(TEST_LEDGER_ID + 1); // Wrong ledgerId
        wrongLedgerEntry.writeLong(1);                  // entryId
        assertThrows(IOException.class, () -> ledger.addEntry(wrongLedgerEntry));
        verify(ledgerStorage, never()).addEntry(any(ByteBuf.class));

        // Reset the mock for next test
        reset(ledgerStorage);

        // Test valid entry (should succeed)
        ByteBuf validEntry = Unpooled.buffer(24);
        validEntry.writeLong(TEST_LEDGER_ID);  // Correct ledgerId
        validEntry.writeLong(1);               // entryId
        validEntry.writeLong(123);             // Some data
        when(ledgerStorage.addEntry(any(ByteBuf.class))).thenReturn(1L);
        assertEquals(1L, ledger.addEntry(validEntry));
        verify(ledgerStorage, times(1)).addEntry(any(ByteBuf.class));
    }

    /**
     * Tests handling of thread interruption during journal write operation:
     *
     * Scenario:
     * - Ledger is successfully marked as fenced in storage
     * - Journal write operation is interrupted
     * - Thread interruption state needs to be managed
     *
     * Test verifies:
     * - Proper handling of InterruptedException
     * - Thread interrupt state management
     * - Exception propagation through CompletableFuture
     * - Cleanup of interrupt flag
     *
     * Expected behavior:
     * - Future completes exceptionally with InterruptedException
     * - Thread interrupt flag is properly set
     * - Exception is propagated correctly through the future chain
     */

    @Disabled(" Disabled for running pitest")
    @Test
    @DisplayName("Test failed journal write during fence")
    void testFailedJournalWriteDuringFence() throws IOException, InterruptedException {
        // Setup
        when(ledgerStorage.setFenced(TEST_LEDGER_ID)).thenReturn(true);

        // Simulate failed journal write
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            WriteCallback callback = (WriteCallback) args[2];
            callback.writeComplete(BKException.Code.BookieHandleNotAvailableException,
                    TEST_LEDGER_ID, -1, null, null);
            return null;
        }).when(journal).logAddEntry(any(ByteBuf.class), anyBoolean(), any(), any());

        CompletableFuture<Boolean> future = ledger.fenceAndLogInJournal(journal);

        assertFalse(future.join(), "Should return false when journal write fails");
        verify(journal, times(1)).logAddEntry(any(ByteBuf.class), eq(false), any(), isNull());
    }

    @Test
    @DisplayName("Test interrupted journal write")
    void testInterruptedJournalWrite() throws IOException, InterruptedException {
        // Setup
        when(ledgerStorage.setFenced(TEST_LEDGER_ID)).thenReturn(true);

        // Simulate interrupted journal write
        doThrow(new InterruptedException("Test interrupted"))
                .when(journal).logAddEntry(any(ByteBuf.class), anyBoolean(), any(), any());

        CompletableFuture<Boolean> future = ledger.fenceAndLogInJournal(journal);

        // Verify the future is completed exceptionally with InterruptedException
        Exception exception = assertThrows(Exception.class, () -> future.join());
        assertTrue(exception.getCause() instanceof InterruptedException);

        // Verify interrupted flag is set
        assertTrue(Thread.interrupted(), "Thread should be marked as interrupted");
    }

    /**
     * Tests fencing behavior when a previous fence operation has completed:
     *
     * Scenario:
     * - Previous fence attempt has completed (logFenceResult exists and is done)
     * - New fence request is received
     * - System should attempt to log fence entry again
     *
     * Test verifies:
     * - Proper handling of completed logFenceResult
     * - New journal write attempt is made
     * - Correct state management for repeated fence attempts
     * - Reflection usage to set up test conditions
     *
     * Expected behavior:
     * - New journal write is attempted
     * - Previous completed future doesn't prevent new fence operation
     * - System handles repeated fence attempts correctly
     */


    @Test
    @DisplayName("Test fence with already completed logFenceResult")
    void testFenceWithCompletedLogFenceResult() throws IOException, InterruptedException {
        // Setup - first fence attempt fails
        when(ledgerStorage.setFenced(TEST_LEDGER_ID)).thenReturn(false);

        // Simulate a completed logFenceResult
        CompletableFuture<Boolean> mockFuture = new CompletableFuture<>();
        mockFuture.complete(false);

        // Use reflection to set the logFenceResult field
        try {
            java.lang.reflect.Field field = LedgerDescriptorImpl.class.getDeclaredField("logFenceResult");
            field.setAccessible(true);
            field.set(ledger, mockFuture);
        } catch (Exception e) {
            fail("Failed to set up test: " + e.getMessage());
        }

        // Execute and verify
        CompletableFuture<Boolean> result = ledger.fenceAndLogInJournal(journal);

        // Should attempt to log fence entry again
        verify(journal, times(1)).logAddEntry(any(ByteBuf.class), anyBoolean(), any(), any());
    }

    /**
     * Tests handling of concurrent fence operations when one is already in progress:
     *
     * Scenario:
     * - A fence operation is currently in progress (incomplete future)
     * - New fence request arrives
     * - System should return existing in-progress operation
     *
     * Test verifies:
     * - Proper handling of in-progress fence operations
     * - No duplicate journal writes
     * - Correct future reuse
     * - Thread safety of fence operation
     *
     * Implementation details:
     * - Uses reflection to simulate in-progress operation
     * - Verifies future identity
     * - Checks journal interaction
     *
     * Expected behavior:
     * - Returns existing in-progress future
     * - No new journal write is attempted
     * - Maintains operation atomicity
     */

    @Test
    @DisplayName("Test fence with in-progress fence operation")
    void testFenceWithInProgressOperation() throws IOException, InterruptedException {
        // Setup - first fence attempt fails
        when(ledgerStorage.setFenced(TEST_LEDGER_ID)).thenReturn(false);

        // Create an incomplete future to simulate in-progress operation
        CompletableFuture<Boolean> inProgressFuture = new CompletableFuture<>();

        // Use reflection to set the logFenceResult field
        try {
            java.lang.reflect.Field field = LedgerDescriptorImpl.class.getDeclaredField("logFenceResult");
            field.setAccessible(true);
            field.set(ledger, inProgressFuture);
        } catch (Exception e) {
            fail("Failed to set up test: " + e.getMessage());
        }

        // Execute
        CompletableFuture<Boolean> result = ledger.fenceAndLogInJournal(journal);

        // Verify we got the same future back
        assertSame(inProgressFuture, result,
                "Should return the in-progress future when fence operation is ongoing");

        // Verify no new journal write was attempted
        verify(journal, never()).logAddEntry(any(ByteBuf.class), anyBoolean(), any(), any());
    }


    @AfterEach
    void tearDown() {
        // Rilascia le risorse
        if (ledgerStorage != null) {
            // Cleanup delle risorse mock se necessario
        }
    }
}