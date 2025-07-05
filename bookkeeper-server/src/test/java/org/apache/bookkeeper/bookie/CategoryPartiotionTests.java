package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledUnsafeHeapByteBuf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.common.util.Watcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Category Partition Tests for LedgerDescriptorImpl methods.
 */
public class CategoryPartiotionTests {

    @Mock
    private LedgerStorage mockLedgerStorage;

    @Mock
    private Journal mockJournal;

    private Map<Long, LedgerDescriptorImpl> existingLedgers;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        existingLedgers = new HashMap<>();
    }

    /**
     * Test cases for LedgerDescriptorImpl constructor with different masterKey values.
     */
    @Test
    public void testConstructorWithValidMasterKey() {
        // Valid master key (non-empty)
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        assertNotNull(descriptor);
        assertEquals(validLedgerId, descriptor.getLedgerId());
    }

    @Test
    public void testConstructorWithEmptyMasterKey() {
        // Empty master key
        byte[] emptyMasterKey = new byte[0];
        long validLedgerId = 123L;

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(emptyMasterKey, validLedgerId, mockLedgerStorage);

        assertNotNull(descriptor);
        assertEquals(validLedgerId, descriptor.getLedgerId());
    }


    @Test
    public void testConstructorWithInvalidLedgerId() {
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long invalidLedgerId = -1L; // Negative ledger IDs are considered invalid

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, invalidLedgerId, mockLedgerStorage);

        // The constructor doesn't validate ledgerId, so it should still create the object
        assertNotNull(descriptor);
        assertEquals(invalidLedgerId, descriptor.getLedgerId());
    }

    @Test
    public void testConstructorWithAlreadyUsedLedgerId() {
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long existingLedgerId = 456L;

        // Create a ledger with the ID that will be "already used"
        LedgerDescriptorImpl existingDescriptor = new LedgerDescriptorImpl(validMasterKey, existingLedgerId, mockLedgerStorage);
        existingLedgers.put(existingLedgerId, existingDescriptor);

        // Create another ledger with the same ID
        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, existingLedgerId, mockLedgerStorage);

        // The constructor doesn't check for duplicate ledgerIds, so it should still create the object
        assertNotNull(descriptor);
        assertEquals(existingLedgerId, descriptor.getLedgerId());
    }

    /**
     * Test cases for LedgerDescriptorImpl constructor with different ledgerStorage values.
     */
    @Test
    public void testConstructorWithValidLedgerStorage() {
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        assertNotNull(descriptor);
        assertEquals(validLedgerId, descriptor.getLedgerId());
    }

    @Test
    public void testConstructorWithInvalidLedgerStorage() throws IOException, BookieException {
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        // Create a mock that throws exceptions for all methods to simulate an invalid storage
        LedgerStorage invalidStorage = Mockito.mock(LedgerStorage.class);
        Mockito.when(invalidStorage.addEntry(Mockito.any())).thenThrow(new IOException("Invalid storage"));

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, invalidStorage);

        // The constructor doesn't validate the ledgerStorage, so it should still create the object
        assertNotNull(descriptor);
        assertEquals(validLedgerId, descriptor.getLedgerId());
    }

    @Test
    public void testConstructorWithNullLedgerStorage() {
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        try {
            new LedgerDescriptorImpl(validMasterKey, validLedgerId, null);
            // If we reach here, the constructor didn't throw an exception for null ledgerStorage
            // This is expected since the constructor doesn't validate its parameters
            // But we should document this behavior
        } catch (NullPointerException e) {
            // If an exception is thrown, it means the constructor does some validation
            fail("Constructor should not throw exception for null ledgerStorage");
        }
    }

    /**
     * Test cases for checkAccess method with different masterKey values.
     */
    @Test
    public void testCheckAccessWithValidMasterKey() throws IOException, BookieException {
        // Valid master key (same as the one used to create the descriptor)
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // This should not throw an exception
        descriptor.checkAccess(validMasterKey);
    }

    @Test
    public void testCheckAccessWithInvalidMasterKey() {
        // Invalid master key (different from the one used to create the descriptor)
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        byte[] invalidMasterKey = new byte[] { 5, 6, 7, 8 };
        long validLedgerId = 123L;

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        try {
            descriptor.checkAccess(invalidMasterKey);
            fail("Should throw an exception when master keys don't match");
        } catch (BookieException e) {
            // Test passes if BookieException is thrown
            assertEquals(BookieException.Code.UnauthorizedAccessException, e.getCode());
        } catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    /**
     * Test cases for setExplicitLac method with different lac values.
     */
    @Test
    public void testSetExplicitLacWithValidRecord() throws IOException {
        // Valid record
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        ByteBuf validLac = Unpooled.buffer(8);
        validLac.writeLong(validLedgerId);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // This should not throw an exception
        descriptor.setExplicitLac(validLac);
    }

    @Test
    public void testSetExplicitLacWithDuplicateRecord() throws IOException {
        // Duplicate record (same ledgerId)
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        ByteBuf duplicateLac = Unpooled.buffer(8);
        duplicateLac.writeLong(validLedgerId);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // Mock the ledgerStorage to throw an exception when a duplicate record is written
        doThrow(new IOException("Duplicate record")).when(mockLedgerStorage).setExplicitLac(eq(validLedgerId), any(ByteBuf.class));

        try {
            descriptor.setExplicitLac(duplicateLac);
            fail("Should throw an exception when writing a duplicate record");
        } catch (IOException e) {
            // Test passes if IOException is thrown
            assertEquals("Duplicate record", e.getMessage());
        }
    }

    @Test
    public void testSetExplicitLacWithUnconfirmedRecord() throws IOException {
        // Unconfirmed record
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        ByteBuf unconfirmedLac = Unpooled.buffer(8);
        unconfirmedLac.writeLong(validLedgerId);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // Mock the ledgerStorage to throw an exception when an unconfirmed record is written
        doThrow(new IOException("Unconfirmed record")).when(mockLedgerStorage).setExplicitLac(eq(validLedgerId), any(ByteBuf.class));

        try {
            descriptor.setExplicitLac(unconfirmedLac);
            fail("Should throw an exception when writing an unconfirmed record");
        } catch (IOException e) {
            // Test passes if IOException is thrown
            assertEquals("Unconfirmed record", e.getMessage());
        }
    }

    @Test
    public void testSetExplicitLacWithInvalidRecord() throws IOException {
        // Invalid record (null)
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // Mock the ledgerStorage to throw an exception when an invalid record is written
        doThrow(new IOException("Invalid record")).when(mockLedgerStorage).setExplicitLac(eq(validLedgerId), eq(null));

        try {
            descriptor.setExplicitLac(null);
            fail("Should throw an exception when writing an invalid record");
        } catch (IOException e) {
            // Test passes if IOException is thrown
            assertEquals("Invalid record", e.getMessage());
        }
    }

    /**
     * Test cases for fenceAndLogInJournal method with different journal values.
     */
    @Test
    public void testFenceAndLogInJournalWithValidJournal() throws IOException {
        // Valid journal
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        // Mock the ledgerStorage to return false for setFenced (already fenced)
        when(mockLedgerStorage.setFenced(validLedgerId)).thenReturn(false);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

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
    public void testFenceAndLogInJournalWithInvalidJournal() throws IOException {
        // Invalid journal (throws exception)
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        // Mock the ledgerStorage to return true for setFenced
        when(mockLedgerStorage.setFenced(validLedgerId)).thenReturn(true);

        // Mock the journal to throw an exception
        Journal invalidJournal = mock(Journal.class);
        try {
            doThrow(new InterruptedException("Invalid journal"))
                .when(invalidJournal).logAddEntry(any(ByteBuf.class), eq(false), any(), any());
        } catch (InterruptedException e) {
            // This won't happen in the mock setup
            fail("Unexpected exception during mock setup: " + e.getMessage());
        }

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // This should not throw an exception, but the future should be completed exceptionally
        CompletableFuture<Boolean> future = descriptor.fenceAndLogInJournal(invalidJournal);

        // The future should be completed
        assertTrue(future.isDone());

        // The future should be completed exceptionally
        assertTrue(future.isCompletedExceptionally());
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

    /**
     * Test cases for addEntry method with different entry values.
     */
    @Test
    public void testAddEntryWithValidRecord() throws IOException, BookieException {
        // Valid record
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        // Create a valid entry with the correct ledgerId
        ByteBuf validEntry = Unpooled.buffer(8);
        validEntry.writeLong(validLedgerId);

        // Mock the ledgerStorage to return a valid entry ID
        when(mockLedgerStorage.addEntry(any(ByteBuf.class))).thenReturn(1L);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // This should not throw an exception
        long entryId = descriptor.addEntry(validEntry);

        // The entry ID should be the one returned by the ledgerStorage
        assertEquals(1L, entryId);
    }

    @Test
    public void testAddEntryWithDuplicateRecord() throws IOException, BookieException {
        // Duplicate record
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        // Create a valid entry with the correct ledgerId
        ByteBuf duplicateEntry = Unpooled.buffer(8);
        duplicateEntry.writeLong(validLedgerId);

        // Mock the ledgerStorage to throw an exception when a duplicate record is written
        when(mockLedgerStorage.addEntry(any(ByteBuf.class))).thenThrow(new IOException("Duplicate record"));

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        try {
            descriptor.addEntry(duplicateEntry);
            fail("Should throw an exception when writing a duplicate record");
        } catch (IOException e) {
            // Test passes if IOException is thrown
            assertEquals("Duplicate record", e.getMessage());
        }
    }

    @Test
    public void testAddEntryWithInvalidRecord() throws IOException, BookieException {
        // Invalid record (null)
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        try {
            descriptor.addEntry(null);
            fail("Should throw an exception when writing an invalid record");
        } catch (NullPointerException e) {
            // Test passes if NullPointerException is thrown
        }
    }

    @Test
    public void testAddEntryWithWrongLedgerId() throws IOException, BookieException {
        // Entry with wrong ledgerId
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        long wrongLedgerId = 456L;

        // Create an entry with a different ledgerId
        ByteBuf wrongEntry = Unpooled.buffer(8);
        wrongEntry.writeLong(wrongLedgerId);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        try {
            descriptor.addEntry(wrongEntry);
            fail("Should throw an exception when writing an entry with wrong ledgerId");
        } catch (IOException e) {
            // Test passes if IOException is thrown
            assertEquals("Entry for ledger " + wrongLedgerId + " was sent to " + validLedgerId, e.getMessage());
        }
    }

    /**
     * Test cases for readEntry method with different entryId values.
     */
    @Test
    public void testReadEntryWithValidEntryId() throws IOException, BookieException {
        // Valid entry ID
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        long validEntryId = 1L;

        // Mock the ledgerStorage to return a valid entry
        ByteBuf validEntry = Unpooled.buffer(8);
        validEntry.writeLong(validLedgerId);
        when(mockLedgerStorage.getEntry(validLedgerId, validEntryId)).thenReturn(validEntry);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // This should not throw an exception
        ByteBuf entry = descriptor.readEntry(validEntryId);

        // The entry should be the one returned by the ledgerStorage
        assertEquals(validEntry, entry);
    }

    @Test
    public void testReadEntryWithNonExistentEntryId() throws IOException, BookieException {
        // Entry ID that doesn't exist
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        long nonExistentEntryId = 999L;

        // Mock the ledgerStorage to throw an exception when a non-existent entry is requested
        when(mockLedgerStorage.getEntry(validLedgerId, nonExistentEntryId))
            .thenThrow(new BookieException.OperationRejectedException());

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        try {
            descriptor.readEntry(nonExistentEntryId);
            fail("Should throw an exception when reading a non-existent entry");
        } catch (BookieException e) {
            // Test passes if BookieException is thrown
            assertEquals(BookieException.Code.OperationRejectedException, e.getCode());
        }
    }

    @Test
    public void testReadEntryWithInvalidEntryId() throws IOException, BookieException {
        // Invalid entry ID (negative)
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        long invalidEntryId = -1L;

        // Mock the ledgerStorage to throw an exception when an invalid entry ID is requested
        when(mockLedgerStorage.getEntry(validLedgerId, invalidEntryId))
            .thenThrow(new BookieException.OperationRejectedException());

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        try {
            descriptor.readEntry(invalidEntryId);
            fail("Should throw an exception when reading an entry with invalid ID");
        } catch (BookieException e) {
            // Test passes if BookieException is thrown
            assertEquals(BookieException.Code.OperationRejectedException, e.getCode());
        }
    }

    /**
     * Test cases for waitForLastAddConfirmedUpdate method with different previousLAC and watcher values.
     */
    @Test
    public void testWaitForLastAddConfirmedUpdateWithUnconfirmedRecord() throws IOException {
        // Last unconfirmed record
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        long unconfirmedLAC = 10L;

        // Mock the watcher
        @SuppressWarnings("unchecked")
        Watcher<LastAddConfirmedUpdateNotification> validWatcher = mock(Watcher.class);

        // Mock the ledgerStorage to return true (update is pending)
        when(mockLedgerStorage.waitForLastAddConfirmedUpdate(eq(validLedgerId), eq(unconfirmedLAC), eq(validWatcher)))
            .thenReturn(true);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // This should not throw an exception
        boolean result = descriptor.waitForLastAddConfirmedUpdate(unconfirmedLAC, validWatcher);

        // The result should be true (update is pending)
        assertTrue(result);
    }

    @Test
    public void testWaitForLastAddConfirmedUpdateWithNotLastRecord() throws IOException {
        // Not the last record
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        long notLastLAC = 5L;

        // Mock the watcher
        @SuppressWarnings("unchecked")
        Watcher<LastAddConfirmedUpdateNotification> validWatcher = mock(Watcher.class);

        // Mock the ledgerStorage to return false (no update is pending)
        when(mockLedgerStorage.waitForLastAddConfirmedUpdate(eq(validLedgerId), eq(notLastLAC), eq(validWatcher)))
            .thenReturn(false);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // This should not throw an exception
        boolean result = descriptor.waitForLastAddConfirmedUpdate(notLastLAC, validWatcher);

        // The result should be false (no update is pending)
        assertEquals(false, result);
    }

    @Test
    public void testWaitForLastAddConfirmedUpdateWithNonExistentRecord() throws IOException {
        // Non-existent record
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        long nonExistentLAC = 999L;

        // Mock the watcher
        @SuppressWarnings("unchecked")
        Watcher<LastAddConfirmedUpdateNotification> validWatcher = mock(Watcher.class);

        // Mock the ledgerStorage to throw an exception when a non-existent record is requested
        doThrow(new IOException("Non-existent record"))
            .when(mockLedgerStorage).waitForLastAddConfirmedUpdate(eq(validLedgerId), eq(nonExistentLAC), eq(validWatcher));

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        try {
            descriptor.waitForLastAddConfirmedUpdate(nonExistentLAC, validWatcher);
            fail("Should throw an exception when waiting for a non-existent record");
        } catch (IOException e) {
            // Test passes if IOException is thrown
            assertEquals("Non-existent record", e.getMessage());
        }
    }

    @Test
    public void testWaitForLastAddConfirmedUpdateWithInvalidWatcher() throws IOException {
        // Valid record but invalid watcher (null)
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        long validLAC = 10L;

        // Mock the ledgerStorage to return false for null watcher
        when(mockLedgerStorage.waitForLastAddConfirmedUpdate(eq(validLedgerId), eq(validLAC), eq(null)))
            .thenReturn(false);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // This should not throw an exception
        boolean result = descriptor.waitForLastAddConfirmedUpdate(validLAC, null);

        // The result should be false
        assertEquals(false, result);

        // Verify that the ledgerStorage.waitForLastAddConfirmedUpdate method was called with the correct parameters
        Mockito.verify(mockLedgerStorage).waitForLastAddConfirmedUpdate(validLedgerId, validLAC, null);
    }

    /**
     * Test cases for cancelWaitForLastAddConfirmedUpdate method with different watcher values.
     */
    @Test
    public void testCancelWaitForLastAddConfirmedUpdateWithValidWatcher() throws IOException {
        // Valid watcher
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        // Mock the watcher
        @SuppressWarnings("unchecked")
        Watcher<LastAddConfirmedUpdateNotification> validWatcher = mock(Watcher.class);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // This should not throw an exception
        descriptor.cancelWaitForLastAddConfirmedUpdate(validWatcher);

        // Verify that the ledgerStorage.cancelWaitForLastAddConfirmedUpdate method was called with the correct parameters
        Mockito.verify(mockLedgerStorage).cancelWaitForLastAddConfirmedUpdate(validLedgerId, validWatcher);
    }

    @Test
    public void testCancelWaitForLastAddConfirmedUpdateWithInvalidWatcher() throws IOException {
        // Invalid watcher (null)
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // This should not throw an exception
        descriptor.cancelWaitForLastAddConfirmedUpdate(null);

        // Verify that the ledgerStorage.cancelWaitForLastAddConfirmedUpdate method was called with the correct parameters
        Mockito.verify(mockLedgerStorage).cancelWaitForLastAddConfirmedUpdate(validLedgerId, null);
    }

    /**
     * Test cases for getListOfEntriesOfLedger method with different ledgerId values.
     */
    @Test
    public void testGetListOfEntriesOfLedgerWithValidLedgerId() throws IOException {
        // Valid ledger ID
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;

        // Mock the ledgerStorage to return a valid list of entries
        OfLong validEntries = mock(OfLong.class);
        when(mockLedgerStorage.getListOfEntriesOfLedger(validLedgerId)).thenReturn(validEntries);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // This should not throw an exception
        OfLong entries = descriptor.getListOfEntriesOfLedger(validLedgerId);

        // The entries should be the ones returned by the ledgerStorage
        assertEquals(validEntries, entries);
    }

    @Test
    public void testGetListOfEntriesOfLedgerWithInvalidLedgerId() throws IOException {
        // Invalid ledger ID (negative)
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        long invalidLedgerId = -1L;

        // Mock the ledgerStorage to throw an exception when an invalid ledger ID is requested
        doThrow(new IOException("Invalid ledger ID"))
            .when(mockLedgerStorage).getListOfEntriesOfLedger(invalidLedgerId);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        try {
            descriptor.getListOfEntriesOfLedger(invalidLedgerId);
            fail("Should throw an exception when getting entries for an invalid ledger ID");
        } catch (IOException e) {
            // Test passes if IOException is thrown
            assertEquals("Invalid ledger ID", e.getMessage());
        }
    }

    @Test
    public void testGetListOfEntriesOfLedgerWithAlreadyUsedLedgerId() throws IOException {
        // Already used ledger ID
        byte[] validMasterKey = new byte[] { 1, 2, 3, 4 };
        long validLedgerId = 123L;
        long alreadyUsedLedgerId = 456L;

        // Create a ledger with the ID that will be "already used"
        LedgerDescriptorImpl existingDescriptor = new LedgerDescriptorImpl(validMasterKey, alreadyUsedLedgerId, mockLedgerStorage);
        existingLedgers.put(alreadyUsedLedgerId, existingDescriptor);

        // Mock the ledgerStorage to return a valid list of entries for the already used ledger ID
        OfLong validEntries = mock(OfLong.class);
        when(mockLedgerStorage.getListOfEntriesOfLedger(alreadyUsedLedgerId)).thenReturn(validEntries);

        LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(validMasterKey, validLedgerId, mockLedgerStorage);

        // This should not throw an exception
        OfLong entries = descriptor.getListOfEntriesOfLedger(alreadyUsedLedgerId);

        // The entries
}
}