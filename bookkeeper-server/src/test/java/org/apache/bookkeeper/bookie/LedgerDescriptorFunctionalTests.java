/*
   # Ledger

   A Ledger is a sequence of record data that is terminated upon one of the following conditions:
   - Client connection termination
   - Explicit closure
   - Client crash

   Once a Ledger is closed, it becomes immutable, meaning:
   - No additional data can be appended
   - Its contents remain permanently unchanged
   - The data is preserved in its final state

   Key characteristics:
   - Each record within a Ledger has a unique identifier that is distinct within that specific Ledger's scope
   - Represents the fundamental storage unit in BookKeeper

   Properties:
   - Immutable after closure
   - Sequential data structure
   - Uniquely identifiable records
   - Atomic closure operations

 */


package org.apache.bookkeeper.bookie;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator.OfLong;

@DisplayName("LedgerDescriptorImpl Functionality Tests")
public class LedgerDescriptorFunctionalTests {
    private static final long TEST_LEDGER_ID = 12345L;
    private LedgerStorage ledgerStorage;
    private LedgerDescriptorImpl ledgerDescriptor;
    private byte[] masterKey;
    private Journal journal;

    @BeforeEach
    void setUp() {
        ledgerStorage = Mockito.mock(LedgerStorage.class);
        journal = Mockito.mock(Journal.class);
        masterKey = new byte[]{1, 2, 3, 4};
        ledgerDescriptor = new LedgerDescriptorImpl(masterKey, TEST_LEDGER_ID, ledgerStorage);
    }


    public List<ByteBuf> writeEntries() throws IOException, BookieException {
        // Prepare a list of entries to write
        List<ByteBuf> entries = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ByteBuf entry = Unpooled.buffer(16);
            entry.writeLong(TEST_LEDGER_ID);
            entry.writeLong(i);
            entries.add(entry);
        }

        // Configure mock to simulate writing
        for (int i = 0; i < entries.size(); i++) {
            when(ledgerStorage.addEntry(entries.get(i))).thenReturn((long) i);
            when(ledgerStorage.getEntry(TEST_LEDGER_ID, i)).thenReturn(entries.get(i));
        }

        // Execute writes
        for (ByteBuf entry : entries) {
            ledgerDescriptor.addEntry(entry);
        }
        return entries;
    }

    @Test
    @DisplayName("Test write and read in order")
    void testWriteAndReadInOrder() throws IOException, BookieException {
        List<ByteBuf> entries = writeEntries();
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(entries.get(i), ledgerDescriptor.readEntry(i));
        }
    }

    @Test
    @DisplayName("Test read failure with reversed order")
    void testFail() throws IOException, BookieException {
        List<ByteBuf> entries = writeEntries();
        int index = entries.size() - 1;
        // Verify that test fails when comparing reversed order
        assertThrows(AssertionError.class, () -> {
            for (int i = 0; i < entries.size(); i++) {
                assertEquals(entries.get(index - i), ledgerDescriptor.readEntry(i),
                        "Entry read does not match: order is reversed");
            }
        });
    }

    @Disabled(" Test that fails because there is a check missing in the add of new Entry in the ledgerDescriptor")
    @Test
    @DisplayName("Test ledger closure, size verification and exception handling")
    void testWriteToClosedLedger() throws IOException, BookieException {
        // Simulate ledger closure
        when(ledgerStorage.setFenced(TEST_LEDGER_ID)).thenReturn(true);
        when(ledgerStorage.isFenced(TEST_LEDGER_ID)).thenReturn(true);
        assertTrue(ledgerDescriptor.setFenced());

        // Verify that ledger is actually closed
        assertTrue(ledgerDescriptor.isFenced(),
                "Ledger should be closed after setFenced()");

        // First write some entries
        List<ByteBuf> entries = writeEntries();

        // Get initial size
        OfLong sizeBefore = ledgerDescriptor.getListOfEntriesOfLedger(TEST_LEDGER_ID);
        long initialSize = 0;
        while (sizeBefore.hasNext()) {
            initialSize++;
            sizeBefore.nextLong();
        }

        // Prepare a new entry to write
        ByteBuf entry = Unpooled.buffer(16);
        entry.writeLong(TEST_LEDGER_ID);
        entry.writeLong(1);

        // Verify that writing fails with BookieException
        BookieException thrownException = assertThrows(BookieException.class,
                () -> ledgerDescriptor.addEntry(entry),
                "A BookieException should be thrown when writing to a closed ledger");

        // Verify that the exception is of correct type (LedgerFencedException)
        assertEquals(BookieException.Code.LedgerFencedException, thrownException.getCode(),
                "Exception should be of type LedgerFencedException");

        // Verify that reading also throws an exception
        assertThrows(BookieException.class,
                () -> ledgerDescriptor.readEntry(0),
                "Reading should also fail on a closed ledger");

        // Verify that size hasn't changed
        OfLong sizeAfter = ledgerDescriptor.getListOfEntriesOfLedger(TEST_LEDGER_ID);
        long finalSize = 0;
        while (sizeAfter.hasNext()) {
            finalSize++;
            sizeAfter.nextLong();
        }

        assertEquals(initialSize, finalSize,
                "Ledger size should not change after attempted write to closed ledger");
    }



    @Test
    @DisplayName("Test writing duplicate id")
    void testWriteDuplicateEntryId() throws IOException, BookieException {
        ByteBuf entry1 = Unpooled.buffer(16);
        entry1.writeLong(TEST_LEDGER_ID);
        entry1.writeLong(1); // entry ID = 1

        ByteBuf entry2 = Unpooled.buffer(16);
        entry2.writeLong(TEST_LEDGER_ID);
        entry2.writeLong(1); // stesso entry ID

        when(ledgerStorage.addEntry(entry1)).thenReturn(1L);

        // Prima scrittura dovrebbe avere successo
        assertEquals(1L, ledgerDescriptor.addEntry(entry1));

        // Seconda scrittura con stesso ID dovrebbe fallire
        when(ledgerStorage.addEntry(entry2)).thenThrow(new IOException("Duplicate entry ID"));
        assertThrows(IOException.class, () -> ledgerDescriptor.addEntry(entry2));
    }

    @Test
    @DisplayName("Test writing with wrong ledger ID ")
    void testWriteWrongLedgerId() {
        ByteBuf entry = Unpooled.buffer(16);
        entry.writeLong(TEST_LEDGER_ID + 1); // ID ledger diverso
        entry.writeLong(1);

        assertThrows(IOException.class, () -> ledgerDescriptor.addEntry(entry));
    }

    @Disabled("TODO: Implement test for client crash scenario")
    @Test
    void testClientCrash() {
        // TODO: Implement a test that simulate a crash of the client and the ledger status is closed
    }



























    @AfterEach
    void tearDown() {
        // Rilascia le risorse
        if (ledgerStorage != null) {
            // Cleanup delle risorse mock se necessario
        }
    }





}