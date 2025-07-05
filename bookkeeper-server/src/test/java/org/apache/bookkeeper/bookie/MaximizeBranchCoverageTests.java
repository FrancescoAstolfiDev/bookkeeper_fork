package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests to maximize MC/DC coverage for the fenceAndLogInJournal method.
 */
public class MaximizeBranchCoverageTests {

    @Mock
    private LedgerStorage mockLedgerStorage;

    @Mock
    private Journal mockJournal;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        // Enable debug logging for test coverage
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.bookkeeper.bookie.LedgerDescriptorImpl", "debug");
    }

    /**
     * Test case 1: First time fencing (setFenced returns true)
     * This tests the path where setFenced() returns true and logFenceEntryInJournal is called.
     */
    @Test
    public void testFenceAndLogInJournal_FirstTimeFencing() throws Exception {
        // Setup
        byte[] masterKey = new byte[] { 1, 2, 3, 4 };
        long ledgerId = 123L;

        // Mock setFenced to return true (first time fencing)
        when(mockLedgerStorage.setFenced(ledgerId)).thenReturn(true);

        // Mock journal to complete the future successfully
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ByteBuf entry = invocation.getArgument(0);
                WriteCallback callback = invocation.getArgument(2);
                // Simulate a successful write
                callback.writeComplete(0, ledgerId, 0, null, null);
                return null;
            }
        }).when(mockJournal).logAddEntry(any(ByteBuf.class), anyBoolean(), any(WriteCallback.class), any());

        // Create the descriptor
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        // Execute
        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);

        // Verify
        assertTrue(future.isDone());
        assertTrue(future.get());

        // Verify fenceEntryPersisted is set to true
        AtomicBoolean fenceEntryPersisted = (AtomicBoolean) getPrivateField(descriptor, "fenceEntryPersisted");
        assertTrue(fenceEntryPersisted.get());
    }

    /**
     * Test case 1b: First time fencing with journal write failure
     * This tests the path where setFenced() returns true and logFenceEntryInJournal is called,
     * but the journal write fails.
     */
    @Test
    public void testFenceAndLogInJournal_FirstTimeFencing_JournalWriteFailure() throws Exception {
        // Setup
        byte[] masterKey = new byte[] { 1, 2, 3, 4 };
        long ledgerId = 123L;

        // Mock setFenced to return true (first time fencing)
        when(mockLedgerStorage.setFenced(ledgerId)).thenReturn(true);

        // Mock journal to complete the future with failure
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ByteBuf entry = invocation.getArgument(0);
                WriteCallback callback = invocation.getArgument(2);
                // Simulate a failed write (non-zero rc)
                callback.writeComplete(1, ledgerId, 0, null, null);
                return null;
            }
        }).when(mockJournal).logAddEntry(any(ByteBuf.class), anyBoolean(), any(WriteCallback.class), any());

        // Create the descriptor
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        // Execute
        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);

        // Verify
        assertTrue(future.isDone());
        assertFalse(future.get());

        // Verify fenceEntryPersisted is still false
        AtomicBoolean fenceEntryPersisted = (AtomicBoolean) getPrivateField(descriptor, "fenceEntryPersisted");
        assertFalse(fenceEntryPersisted.get());
    }

    /**
     * Test case 2: Already fenced, logFenceResult is null, and fenceEntryPersisted is false
     * This tests the path where setFenced() returns false, logFenceResult is null, and fenceEntryPersisted is false.
     */
    @Test
    public void testFenceAndLogInJournal_AlreadyFenced_LogFenceResultNull_FenceEntryNotPersisted() throws Exception {
        // Setup
        byte[] masterKey = new byte[] { 1, 2, 3, 4 };
        long ledgerId = 123L;

        // Mock setFenced to return false (already fenced)
        when(mockLedgerStorage.setFenced(ledgerId)).thenReturn(false);

        // Create the descriptor
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        // Set fenceEntryPersisted to false using reflection (default is false, but setting explicitly for clarity)
        AtomicBoolean fenceEntryPersisted = new AtomicBoolean(false);
        setPrivateField(descriptor, "fenceEntryPersisted", fenceEntryPersisted);

        // Execute
        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);

        // Verify
        assertTrue(future.isDone());
        assertTrue(future.get());
    }

    /**
     * Test case 3: Already fenced, logFenceResult is null, and fenceEntryPersisted is true
     * This tests the path where setFenced() returns false, logFenceResult is null, and fenceEntryPersisted is true.
     */
    @Test
    public void testFenceAndLogInJournal_AlreadyFenced_LogFenceResultNull_FenceEntryPersisted() throws Exception {
        // Setup
        byte[] masterKey = new byte[] { 1, 2, 3, 4 };
        long ledgerId = 123L;

        // Mock setFenced to return false (already fenced)
        when(mockLedgerStorage.setFenced(ledgerId)).thenReturn(false);

        // Create the descriptor
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        // Set fenceEntryPersisted to true using reflection
        AtomicBoolean fenceEntryPersisted = new AtomicBoolean(true);
        setPrivateField(descriptor, "fenceEntryPersisted", fenceEntryPersisted);

        // Execute
        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);

        // Verify
        assertTrue(future.isDone());
        assertTrue(future.get());
    }

    /**
     * Test case 3: Already fenced, logFenceResult not null, and fenceEntryPersisted is true
     * This tests the path where setFenced() returns false, logFenceResult is not null, 
     * and fenceEntryPersisted is true.
     */
    @Test
    public void testFenceAndLogInJournal_AlreadyFenced_LogFenceResultNotNull_FenceEntryPersisted() throws Exception {
        // Setup
        byte[] masterKey = new byte[] { 1, 2, 3, 4 };
        long ledgerId = 123L;

        // Mock setFenced to return false (already fenced)
        when(mockLedgerStorage.setFenced(ledgerId)).thenReturn(false);

        // Create the descriptor
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        // Set logFenceResult to a non-null value using reflection
        CompletableFuture<Boolean> mockFuture = new CompletableFuture<>();
        setPrivateField(descriptor, "logFenceResult", mockFuture);

        // Set fenceEntryPersisted to true using reflection
        AtomicBoolean fenceEntryPersisted = new AtomicBoolean(true);
        setPrivateField(descriptor, "fenceEntryPersisted", fenceEntryPersisted);

        // Execute
        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);

        // Verify
        assertTrue(future.isDone());
        assertTrue(future.get());
    }

    /**
     * Test case 4: Already fenced, logFenceResult not null, fenceEntryPersisted is false, 
     * and logFenceResult is done
     * This tests the path where setFenced() returns false, logFenceResult is not null, 
     * fenceEntryPersisted is false, and logFenceResult.isDone() is true.
     */
    @Test
    public void testFenceAndLogInJournal_AlreadyFenced_LogFenceResultNotNull_FenceEntryNotPersisted_LogFenceResultDone() 
            throws Exception {
        // Setup
        byte[] masterKey = new byte[] { 1, 2, 3, 4 };
        long ledgerId = 123L;

        // Mock setFenced to return false (already fenced)
        when(mockLedgerStorage.setFenced(ledgerId)).thenReturn(false);

        // Mock journal to complete the future successfully
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ByteBuf entry = invocation.getArgument(0);
                WriteCallback callback = invocation.getArgument(2);
                callback.writeComplete(0, ledgerId, 0, null, null);
                return null;
            }
        }).when(mockJournal).logAddEntry(any(ByteBuf.class), anyBoolean(), any(WriteCallback.class), any());

        // Create the descriptor
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        // Set logFenceResult to a completed future using reflection
        CompletableFuture<Boolean> completedFuture = CompletableFuture.completedFuture(false);
        setPrivateField(descriptor, "logFenceResult", completedFuture);

        // Set fenceEntryPersisted to false using reflection
        AtomicBoolean fenceEntryPersisted = new AtomicBoolean(false);
        setPrivateField(descriptor, "fenceEntryPersisted", fenceEntryPersisted);

        // Execute
        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);

        // Verify
        assertTrue(future.isDone());
        assertTrue(future.get());
    }

    @Test
    public void testLogFenceEntryCallback() throws Exception {
        // Setup
        byte[] masterKey = new byte[]{1, 2, 3, 4};
        long ledgerId = 123L;
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        // Test caso di successo (rc = 0)
        doAnswer(inv -> {
            WriteCallback callback = inv.getArgument(2);
            callback.writeComplete(0, ledgerId, 0, null, null);
            return null;
        }).when(mockJournal).logAddEntry(any(), anyBoolean(), any(), any());

        when(mockLedgerStorage.setFenced(anyLong())).thenReturn(true);

        CompletableFuture<Boolean> future1 = descriptor.fenceAndLogInJournal(mockJournal);
        assertTrue(future1.get());

        // Test caso di fallimento (rc != 0)
        doAnswer(inv -> {
            WriteCallback callback = inv.getArgument(2);
            callback.writeComplete(1, ledgerId, 0, null, null);
            return null;
        }).when(mockJournal).logAddEntry(any(), anyBoolean(), any(), any());

        CompletableFuture<Boolean> future2 = descriptor.fenceAndLogInJournal(mockJournal);
        assertFalse(future2.get());
    }

    @Test
    public void testInterruptedJournalWrite() throws Exception {
        // Setup
        byte[] masterKey = new byte[]{1, 2, 3, 4};
        long ledgerId = 123L;
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        when(mockLedgerStorage.setFenced(anyLong())).thenReturn(true);

        doThrow(new InterruptedException())
                .when(mockJournal).logAddEntry(any(), anyBoolean(), any(), any());

        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);
        assertTrue(future.isCompletedExceptionally());
        assertTrue(Thread.interrupted()); // verifica che il flag interrupted sia settato

        // Pulisci lo stato interrupted per gli altri test
        Thread.interrupted();
    }

    @Test
    public void testAsyncLogFenceEntryCallback() throws Exception {
        // Setup
        byte[] masterKey = new byte[]{1, 2, 3, 4};
        long ledgerId = 123L;
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        // Abilita debug logging
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.bookkeeper.bookie.LedgerDescriptorImpl", "debug");

        when(mockLedgerStorage.setFenced(anyLong())).thenReturn(true);

        // Simula una chiamata asincrona ritardata
        doAnswer(inv -> {
            WriteCallback callback = inv.getArgument(2);
            // Esegui il callback in un thread separato dopo un breve ritardo
            new Thread(() -> {
                try {
                    Thread.sleep(100); // Simula un ritardo di rete/IO
                    callback.writeComplete(0, ledgerId, 0, null, null);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
            return null;
        }).when(mockJournal).logAddEntry(any(), anyBoolean(), any(), any());

        // Esegui la chiamata
        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);

        // Verifica che il future non sia completato immediatamente
        assertFalse("Future should not complete immediately", future.isDone());

        // Attendi il completamento
        Boolean result = future.get(1000, TimeUnit.MILLISECONDS);
        assertTrue("Future should complete successfully", result);
        assertTrue("Future should be done after callback", future.isDone());

        // Verifica che fenceEntryPersisted sia stato impostato
        AtomicBoolean fenceEntryPersisted = (AtomicBoolean) getPrivateField(descriptor, "fenceEntryPersisted");
        assertTrue("fenceEntryPersisted should be true after successful callback", fenceEntryPersisted.get());

        // Test con fallimento ritardato
        doAnswer(inv -> {
            WriteCallback callback = inv.getArgument(2);
            new Thread(() -> {
                try {
                    Thread.sleep(100);
                    callback.writeComplete(1, ledgerId, 0, null, null); // rc != 0
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
            return null;
        }).when(mockJournal).logAddEntry(any(), anyBoolean(), any(), any());

        CompletableFuture<Boolean> failedFuture = descriptor.fenceAndLogInJournal(mockJournal);
        assertFalse("Failed future should not complete immediately", failedFuture.isDone());

        Boolean failedResult = failedFuture.get(1000, TimeUnit.MILLISECONDS);
        assertFalse("Future should complete with false for failed write", failedResult);

        // Ripulisci
        System.clearProperty("org.slf4j.simpleLogger.log.org.apache.bookkeeper.bookie.LedgerDescriptorImpl");
    }

    /**
     * Test case 5: Already fenced, logFenceResult not null, fenceEntryPersisted is false, 
     * and logFenceResult is not done
     * This tests the path where setFenced() returns false, logFenceResult is not null, 
     * fenceEntryPersisted is false, and logFenceResult.isDone() is false.
     */
    @Test
    public void testFenceAndLogInJournal_AlreadyFenced_LogFenceResultNotNull_FenceEntryNotPersisted_LogFenceResultNotDone() 
            throws Exception {
        // Setup
        byte[] masterKey = new byte[] { 1, 2, 3, 4 };
        long ledgerId = 123L;

        // Mock setFenced to return false (already fenced)
        when(mockLedgerStorage.setFenced(ledgerId)).thenReturn(false);

        // Create the descriptor
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        // Set logFenceResult to a non-completed future using reflection
        CompletableFuture<Boolean> nonCompletedFuture = new CompletableFuture<>();
        setPrivateField(descriptor, "logFenceResult", nonCompletedFuture);

        // Set fenceEntryPersisted to false using reflection
        AtomicBoolean fenceEntryPersisted = new AtomicBoolean(false);
        setPrivateField(descriptor, "fenceEntryPersisted", fenceEntryPersisted);

        // Execute
        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);

        // Verify
        assertFalse(future.isDone());
        assertEquals(nonCompletedFuture, future);

        // Complete the future to avoid hanging test
        nonCompletedFuture.complete(true);
    }

    /**
     * Test case 6: Journal throws InterruptedException during logFenceEntryInJournal
     * This tests the path where journal.logAddEntry throws InterruptedException.
     */
    @Test
    public void testFenceAndLogInJournal_JournalThrowsInterruptedException() throws Exception {
        // Setup
        byte[] masterKey = new byte[] { 1, 2, 3, 4 };
        long ledgerId = 123L;

        // Mock setFenced to return true (first time fencing)
        when(mockLedgerStorage.setFenced(ledgerId)).thenReturn(true);

        // Mock journal to throw InterruptedException
        doThrow(new InterruptedException("Test exception"))
            .when(mockJournal).logAddEntry(any(ByteBuf.class), anyBoolean(), any(WriteCallback.class), any());

        // Create the descriptor
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        // Execute
        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);

        // Verify
        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());

        // Verify thread interrupted status
        assertTrue(Thread.currentThread().isInterrupted());

        // Clear interrupted status for other tests
        Thread.interrupted();
    }

    /**
     * Helper method to set private fields using reflection.
     */
    private void setPrivateField(Object object, String fieldName, Object value) throws Exception {
        Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(object, value);
    }

    /**
     * Helper method to get private fields using reflection.
     */
    private Object getPrivateField(Object object, String fieldName) throws Exception {
        Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(object);
    }

    @Test
    public void testFenceEntryPersistedStateTransitions() throws Exception {
        byte[] masterKey = new byte[]{1, 2, 3, 4};
        long ledgerId = 123L;
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        when(mockLedgerStorage.setFenced(anyLong())).thenReturn(true);

        // Test transizione da false a true con compareAndSet
        AtomicBoolean fenceEntryPersisted = new AtomicBoolean(false);
        setPrivateField(descriptor, "fenceEntryPersisted", fenceEntryPersisted);

        doAnswer(inv -> {
            WriteCallback callback = inv.getArgument(2);
            callback.writeComplete(0, ledgerId, 0, null, null);
            return null;
        }).when(mockJournal).logAddEntry(any(), anyBoolean(), any(), any());

        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);
        assertTrue(future.get());
        assertTrue(fenceEntryPersisted.get());
    }


    @Test
    public void testInterruptedJournalWriteWithDebugLogging() throws Exception {
        // Setup
        byte[] masterKey = new byte[]{1, 2, 3, 4};
        long ledgerId = 123L;
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        // Abilita il debug logging
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.bookkeeper.bookie.LedgerDescriptorImpl", "debug");

        when(mockLedgerStorage.setFenced(anyLong())).thenReturn(true);

        // Simula InterruptedException
        doThrow(new InterruptedException("Test interruption"))
                .when(mockJournal).logAddEntry(any(), anyBoolean(), any(), any());

        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);

        assertTrue(future.isCompletedExceptionally());
        assertTrue(Thread.interrupted());

        // Ripristina logging level e stato interrupted
        System.clearProperty("org.slf4j.simpleLogger.log.org.apache.bookkeeper.bookie.LedgerDescriptorImpl");
        Thread.interrupted();
    }

    @Test
    public void testLogFenceEntryCallbackWithDebugLogging() throws Exception {
        // Setup
        byte[] masterKey = new byte[]{1, 2, 3, 4};
        long ledgerId = 123L;
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(masterKey, ledgerId, mockLedgerStorage);

        // Abilita il debug logging
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.bookkeeper.bookie.LedgerDescriptorImpl", "debug");

        when(mockLedgerStorage.setFenced(anyLong())).thenReturn(true);

        // Test 1: Successo con debug logging (rc = 0)
        doAnswer(inv -> {
            WriteCallback callback = inv.getArgument(2);
            callback.writeComplete(0, ledgerId, 0, null, null);
            return null;
        }).when(mockJournal).logAddEntry(any(), anyBoolean(), any(), any());

        CompletableFuture<Boolean> future1 = descriptor.fenceAndLogInJournal(mockJournal);
        assertTrue(future1.get());

        // Test 2: Fallimento con debug logging (rc != 0)
        doAnswer(inv -> {
            WriteCallback callback = inv.getArgument(2);
            callback.writeComplete(1, ledgerId, 0, null, null);
            return null;
        }).when(mockJournal).logAddEntry(any(), anyBoolean(), any(), any());

        CompletableFuture<Boolean> future2 = descriptor.fenceAndLogInJournal(mockJournal);
        assertFalse(future2.get());

        // Test 3: Fallimento con codice di errore specifico
        doAnswer(inv -> {
            WriteCallback callback = inv.getArgument(2);
            callback.writeComplete(BKException.Code.ReadException, ledgerId, 0, null, null);
            return null;
        }).when(mockJournal).logAddEntry(any(), anyBoolean(), any(), any());

        CompletableFuture<Boolean> future3 = descriptor.fenceAndLogInJournal(mockJournal);
        assertFalse(future3.get());

        // Test 4: Ripristina logging level
        System.clearProperty("org.slf4j.simpleLogger.log.org.apache.bookkeeper.bookie.LedgerDescriptorImpl");
    }


}
