package org.apache.bookkeeper.bookie;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Category Partition Tests for LedgerDescriptorImpl methods.
 */
public class Isw2LedgerDescriptorCategoryPartitionTests {

    @Mock
    private LedgerStorage mockLedgerStorage;

    @Mock
    private Journal mockJournal;

    private LedgerDescriptorImpl descriptor;
    long validLedgerId = 123L;
    private AutoCloseable mocks;
    @Before
    public void setUp() {
        // Initialize mocks and keep track of the session for cleanup
        mocks = MockitoAnnotations.openMocks(this);
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);
    }
    @After
    public void tearDown() throws Exception {
        // Release resources after each test
        if (mocks != null) {
            mocks.close();
        }
    }

    /**
     * Test cases for fenceAndLogInJournal method with different journal values.
     */
    @Test
    public void testFenceAndLogInJournalWithValidJournal() throws IOException {

        // Mock the ledgerStorage to return false for setFenced (already fenced)
        when(mockLedgerStorage.setFenced(validLedgerId)).thenReturn(false);
        // This should not throw an exception
        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(mockJournal);
        // The future should be completed
        assertTrue(future.isDone());
        // The future should be completed with true
        try {
            assertTrue(future.get());
        } catch (Exception e) {
            fail("Future should be completed with true, but got exception: " + e.getMessage());
        }
    }

    @Test
    public void testFenceAndLogInJournalWithLowSpaceLimit() throws Exception {
        // 1. Setup: Mock the LedgerStorage
        when(mockLedgerStorage.setFenced(validLedgerId)).thenReturn(true);

        // 2. Mock: Create the journal
        Journal lowSpaceJournal = mock(Journal.class);

        doAnswer(invocation -> {
            // The callback is the 3rd argument (index 2)
            Object callback = invocation.getArgument(2);

            // FIND THE METHOD MANUALLY:
            // We look for a method with 5 parameters: (int, long, long, LogMark, Object)
            // This bypasses the "NoSuchMethodException" because we don't rely on the name "writeComplete"
            java.lang.reflect.Method writeMethod = java.util.Arrays.stream(callback.getClass().getMethods())
                    .filter(m -> m.getParameterCount() == 5)
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Could not find callback method on " + callback.getClass()));

            // Ensure the method is accessible (important for Lambdas)
            writeMethod.setAccessible(true);

            // Simulate rc = WriteException (-1)
            writeMethod.invoke(callback,
                    org.apache.bookkeeper.client.BKException.Code.WriteException, // arg 0: rc
                    validLedgerId,                                                // arg 1: ledgerId
                    -1L,                                                          // arg 2: entryId
                    null,                                                         // arg 3: LogMark
                    invocation.getArgument(3)                                     // arg 4: ctx
            );

            return null;
        }).when(lowSpaceJournal).logAddEntry(any(ByteBuf.class), anyBoolean(), any(), any());

        // 3. Execution
        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(lowSpaceJournal);

        // 4. Verification
        assertTrue("The future should be completed", future.isDone());
        assertFalse(future.get(), "The result must be false when Journal persistence fails");
    }

    @Test
    public void testFenceAndLogInJournalWithNullJournal() {
        // Null journal
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        // Mock the ledgerStorage to return true for setFenced
        try {
            when(mockLedgerStorage.setFenced(validLedgerId)).thenReturn(true);
        } catch (IOException e) {
            fail("Unexpected IOException during mock setup: " + e.getMessage());
        }

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        try {
            descriptor.fenceAndLogInJournal(null);
            fail("Should throw an exception when journal is null");
        } catch (NullPointerException e) {
            // Test passes if NullPointerException is thrown
        } catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }


}