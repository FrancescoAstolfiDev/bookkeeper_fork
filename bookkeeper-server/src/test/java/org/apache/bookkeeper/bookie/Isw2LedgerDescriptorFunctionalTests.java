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
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@DisplayName("LedgerDescriptorImpl Functionality Tests")
public class Isw2LedgerDescriptorFunctionalTests {
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
        long lastEntryId = (long) entries.size() - 1;
        when(ledgerStorage.getLastAddConfirmed(TEST_LEDGER_ID))
                .thenReturn(lastEntryId);

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
        long expectedLac = (long) entries.size() - 1;
        long actualLac = ledger.getLastAddConfirmed();
        assertEquals(expectedLac, actualLac, "The last add confirmed must be the last entry");
    }


    @Test
    @DisplayName("Test ledger closure, size verification and exception handling")
    void testWriteClosedLedger() throws IOException, BookieException {
        // Simulate ledger closure
        when(ledgerStorage.setFenced(TEST_LEDGER_ID)).thenReturn(true);
        when(ledgerStorage.isFenced(TEST_LEDGER_ID)).thenReturn(true);
        when(ledgerStorage.addEntry(any(ByteBuf.class)))
                .thenThrow(BookieException.create(BookieException.Code.LedgerFencedException));
        // this is an exception that it must be return from the ledgerStorage because there is no check in the ledgerDescriptior for performance reasons


        // Verify that ledger is actually closed
        assertTrue(ledger.setFenced());
        Assertions.assertTrue(ledger.isFenced(), "Ledger should be closed after setFenced()");


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


    }

    @Test
    @DisplayName("Lettura di entry esistenti dopo che il ledger è stato chiuso")
    void testReadEntryAfterFencing() throws IOException, BookieException {
        // 1. PREPARAZIONE: Scrittura di un'entry prima della chiusura
        long entryId = 0L;
        ByteBuf data = Unpooled.buffer(16);
        data.writeLong(TEST_LEDGER_ID);
        data.writeLong(entryId);

        // Configuriamo il mock per accettare la scrittura e restituire i dati
        when(ledgerStorage.addEntry(any(ByteBuf.class))).thenReturn(entryId);
        when(ledgerStorage.getEntry(TEST_LEDGER_ID, entryId)).thenReturn(data.duplicate());

        // Eseguiamo la scrittura tramite il descrittore
        ledger.addEntry(data);

        // 2. CHIUSURA: Attiviamo il fencing
        // In LedgerDescriptorImpl, setFenced() chiama ledgerStorage.setFenced()
        when(ledgerStorage.setFenced(TEST_LEDGER_ID)).thenReturn(true);
        when(ledgerStorage.isFenced(TEST_LEDGER_ID)).thenReturn(true);


        assertTrue(ledger.setFenced());
        Assertions.assertTrue(ledger.isFenced(), "Ledger should be closed after setFenced()");

        // 3. LETTURA: Verifica che la lettura funzioni ancora
        // readEntry delega semplicemente a ledgerStorage.getEntry
        ByteBuf readData = ledger.readEntry(entryId);

        // VERIFICA
        assertNotNull(readData, "La lettura non dovrebbe restituire null");
        assertEquals(TEST_LEDGER_ID, readData.getLong(0), "Il Ledger ID deve corrispondere");
        assertEquals(entryId, readData.getLong(8), "L'Entry ID deve corrispondere");

        // Verifichiamo che il descrittore abbia effettivamente consultato lo storage
        verify(ledgerStorage, times(1)).getEntry(TEST_LEDGER_ID, entryId);
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


    @AfterEach
    void tearDown() {
        // Rilascia le risorse
        if (ledgerStorage != null) {
            // Cleanup delle risorse mock se necessario
        }
    }
}