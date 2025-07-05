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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.atomic.AtomicBoolean;

@DisplayName("LedgerDescriptorImpl Functionality Tests")
public class LedgerDescriptorFunctionalTests {
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
        ledger = this.handleFactory.getHandle(TEST_LEDGER_ID, masterKey,true);
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

    @Test
    @DisplayName("Test read failure with reversed order")
    void testFail() throws IOException, BookieException {
        List<ByteBuf> entries = writeEntries();
        int index = entries.size() - 1;
        // Verify that test fails when comparing reversed order
        assertThrows(AssertionError.class, () -> {
            for (int i = 0; i < entries.size(); i++) {
                assertEquals(entries.get(index - i), ledger.readEntry(i),
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
        assertTrue(ledger.setFenced());

        // Verify that ledger is actually closed
        Assertions.assertTrue(ledger.isFenced(), "Ledger should be closed after setFenced()");

        // First write some entries
        List<ByteBuf> entries = writeEntries();

        // Get initial siz
        OfLong sizeBefore = ledger.getListOfEntriesOfLedger(TEST_LEDGER_ID);
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
                () -> ledger.addEntry(entry),
                "A BookieException should be thrown when writing to a closed ledger");

        // Verify that the exception is of correct type (LedgerFencedException)
        assertEquals(BookieException.Code.LedgerFencedException, thrownException.getCode(),
                "Exception should be of type LedgerFencedException");

        // Verify that reading also throws an exception
        assertThrows(BookieException.class,
                () -> ledger.readEntry(0),
                "Reading should also fail on a closed ledger");

        // Verify that size hasn't changed
        OfLong sizeAfter = ledger.getListOfEntriesOfLedger(TEST_LEDGER_ID);
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
        assertEquals(1L, ledger.addEntry(entry1));

        // Seconda scrittura con stesso ID dovrebbe fallire
        when(ledgerStorage.addEntry(entry2)).thenThrow(new IOException("Duplicate entry ID"));
        assertThrows(IOException.class, () -> ledger.addEntry(entry2));
    }

    @Test
    @DisplayName("Test writing with wrong ledger ID ")
    void testWriteWrongLedgerId() {
        ByteBuf entry = Unpooled.buffer(16);
        entry.writeLong(TEST_LEDGER_ID + 1); // ID ledger diverso
        entry.writeLong(1);

        assertThrows(IOException.class, () -> ledger.addEntry(entry));
    }

    @Disabled("TODO: Implement test for client crash scenario")
    @Test
    @DisplayName("Test client crash scenario with storage fencing")
    void testClientCrash() throws IOException, BookieException {
        // Setup iniziale
        ByteBuf entry = Unpooled.buffer(16);
        entry.writeLong(TEST_LEDGER_ID);
        entry.writeLong(1);

        // Verifica stato iniziale - lo storage non dovrebbe essere fenced
        when(ledgerStorage.isFenced(TEST_LEDGER_ID)).thenReturn(false);
        assertFalse(ledger.isFenced(), "Lo storage non dovrebbe essere fenced inizialmente");

        // Simuliamo scrittura pre-crash
        when(ledgerStorage.addEntry(any(ByteBuf.class))).thenReturn(1L);
        ledger.addEntry(entry);

        // Simuliamo il crash del client
        // 1. Prima setFenced() viene chiamato sullo storage
        when(ledgerStorage.setFenced(TEST_LEDGER_ID)).thenReturn(true);
        // 2. Poi isFenced() restituirà true per tutte le chiamate successive
        when(ledgerStorage.isFenced(TEST_LEDGER_ID)).thenReturn(true);

        // Verifichiamo che:
        // 1. setFenced() ritorna true (primo fencing)
        assertTrue("Il primo setFenced dovrebbe ritornare true", ledger.setFenced());
        // 2. Una seconda chiamata ritorna false (già fenced)
        assertFalse(ledger.setFenced(), "Il secondo setFenced dovrebbe ritornare false");

        // Verifichiamo che isFenced() rifletta lo stato dello storage
        assertTrue("Il ledger dovrebbe riportare lo stato fenced dello storage", ledger.isFenced());

        // Verifichiamo che lo storage sia stato chiamato correttamente
        verify(ledgerStorage).setFenced(TEST_LEDGER_ID);
        verify(ledgerStorage, atLeast(1)).isFenced(TEST_LEDGER_ID);

        // Tentiamo una nuova scrittura - dovrebbe essere bloccata dallo storage
        ByteBuf newEntry = Unpooled.buffer(16);
        newEntry.writeLong(TEST_LEDGER_ID);
        newEntry.writeLong(2);

        // La scrittura dovrebbe essere rifiutata con LedgerFencedException
        assertThrows(BookieException.class,
                () -> ledger.addEntry(newEntry),
                "Le scritture dovrebbero essere rifiutate quando lo storage è fenced"
        );

        // Verifichiamo che lo storage non sia stato chiamato per la nuova scrittura
        verify(ledgerStorage, never()).addEntry(newEntry);
    }

    /*

     * Test that every ledger has a unique client that write and the 
     others can only read if they have the abcess. 
     All of the access is with the same masterkey

     */
    @Test
    @DisplayName("Test single writer multiple readers pattern")
    void testSingleWriterMultipleReaders() throws IOException, BookieException {
        // Writer writes some entries
        assertDoesNotThrow(() -> {
            // Writer can write
            ByteBuf entry = Unpooled.buffer(16);
            entry.writeLong(TEST_LEDGER_ID);
            entry.writeLong(1);
            ledger.addEntry(entry);

            // Writer can read
            ledger.readEntry(0);
        }, "Writer should have full access");

        // Simulate writer crash
        when(ledgerStorage.setFenced(TEST_LEDGER_ID)).thenReturn(true);
        ledger.setFenced();

        // After fencing, no one can write
        ByteBuf newEntry = Unpooled.buffer(16);
        newEntry.writeLong(TEST_LEDGER_ID);
        newEntry.writeLong(2);

    /*
                Failure to be fixed
    assertThrows(BookieException.class,
            () -> ledger.addEntry(newEntry),
           "After fencing, no client should be able to write");
     */
        // readers can still read
        assertDoesNotThrow(() -> {
            // Reader 1
            ledger.checkAccess(masterKey);
            ledger.readEntry(0);

            // Reader 2
            ledger.checkAccess(masterKey);
            ledger.readEntry(0);
        }, "Multiple readers should be able to read after fencing");
    }

    /* 
     There is a write on the journal just when the ledger is fenced. 
     Or the first write has a fault.
      */
    @Test
    @DisplayName("Test ledger fence and log")
    void testGetFenceAndLog() throws IOException, BookieException {
        AtomicBoolean not_firstCall = new AtomicBoolean(false);
        when(ledgerStorage.isFenced(TEST_LEDGER_ID))
                .thenAnswer(invocation -> not_firstCall.get());

        when(ledgerStorage.setFenced(TEST_LEDGER_ID))
                .thenAnswer(invocation -> {
                    if (!not_firstCall.get()) {
                        not_firstCall.set(true);
                        return true;
                        // just the first call returns true because there is effectively a change in the status
                    }
                    return false;
                    // next call returns false as there is no change in the status
                });

        // Write some entries and verify
        writeEntries();

        // Verify initial state
        boolean isFenced = ledger.isFenced();
        assertFalse(isFenced, "The ledger should be in write-enabled state");

        // First call to fenceAndLogInJournal
        ledger.fenceAndLogInJournal(journal);
        //TODO: read the journal to see effectively the content that is consistent on the write

        isFenced = ledger.isFenced();
        assertTrue("The ledger should be in write-disabled state", isFenced);

        // Second call to fenceAndLogInJournal
        ledger.fenceAndLogInJournal(journal);
        //TODO: read the journal to see effectively no change from last time 
        isFenced = ledger.isFenced();
        assertTrue("The ledger should remain in write-disabled state", isFenced);
    }




    @AfterEach
    void tearDown() {
        // Rilascia le risorse
        if (ledgerStorage != null) {
            // Cleanup delle risorse mock se necessario
        }
    }






}