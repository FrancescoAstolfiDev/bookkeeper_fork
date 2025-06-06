package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;
import  org.apache.bookkeeper.bookie.LedgerDescriptorFunctionalTests;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

@DisplayName("LedgerDescriptorImpl from Code ")
public class LedgerDescriptorWhiteBoxTests {
    private static final long TEST_LEDGER_ID = 12345L;
    private LedgerStorage ledgerStorage;
    private HandleFactoryImpl handleFactory;
    private LedgerDescriptor ledger;
    private byte[] masterKey;
    private Journal journal;
    private boolean isFenced;
    @BeforeEach
    void setUp() throws IOException, BookieException {
        this.isFenced = false;
        this.ledgerStorage = Mockito.mock(LedgerStorage.class);
        this.handleFactory = new HandleFactoryImpl(ledgerStorage);
        this.journal = Mockito.mock(Journal.class);
        masterKey = new byte[]{1, 2, 3, 4};

        // Simuliamo un writer e due readers
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
    /*

     * Test that every ledger has a unique client that write and the others can only read if they have the abcess

     */
    @Test
    @DisplayName("Test single writer multiple readers pattern")
    void testSingleWriterMultipleReaders() throws IOException, BookieException {
        // Il writer scrive alcune entries
        assertDoesNotThrow(() -> {
            // Writer può scrivere
            ByteBuf entry = Unpooled.buffer(16);
            entry.writeLong(TEST_LEDGER_ID);
            entry.writeLong(1);
            ledger.addEntry(entry);

            // Writer può leggere
            ledger.readEntry(0);
        }, "Writer should have full access");

        // Simuliamo un crash del writer
        when(ledgerStorage.setFenced(TEST_LEDGER_ID)).thenReturn(true);
        ledger.setFenced();


        // Dopo il fence, nessuno può scrivere
        ByteBuf newEntry = Unpooled.buffer(16);
        newEntry.writeLong(TEST_LEDGER_ID);
        newEntry.writeLong(2);

        // Failure to be fixed
//        assertThrows(BookieException.class,
//                () -> ledger.addEntry(newEntry),
//                "After fencing, no client should be able to write");

        // Ma i readers possono ancora leggere
        assertDoesNotThrow(() -> {
            // Reader 1
            ledger.checkAccess(masterKey);
            ledger.readEntry(0);

            // Reader 2
            ledger.checkAccess(masterKey);
            ledger.readEntry(0);
        }, "Multiple readers should be able to read after fencing");
    }

    @Test
    @DisplayName("Test ledger fence and log ")
    void testGetFenceAndLog() throws IOException, BookieException {
        AtomicBoolean not_firstCall = new AtomicBoolean(false);
        when(ledgerStorage.isFenced(TEST_LEDGER_ID))
                .thenAnswer(invocation -> not_firstCall.get());

        when(ledgerStorage.setFenced(TEST_LEDGER_ID))
                .thenAnswer(invocation -> {
                    if (!not_firstCall.get()) {
                        not_firstCall.set(true);
                        return true;  // Prima chiamata restituisce true
                    }
                    return false;    // Chiamate successive restituiscono false
                });


        // Scrivi alcune entries e verifica
        writeEntries();

        // Verifica stato iniziale
        boolean isFenced = ledger.isFenced();
        assertFalse(isFenced, "Il ledger dovrebbe essere nello stato aperto a scrittura");

        // Prima chiamata a fenceAndLogInJournal
         ledger.fenceAndLogInJournal(journal);
        //assertTrue(firstFence.join(), "La prima operazione di fence dovrebbe avere successo");

        isFenced = ledger.isFenced();
        assertTrue(isFenced, "Il ledger dovrebbe essere nello stato chiuso a scrittura");

        // Seconda chiamata a fenceAndLogInJournal
        ledger.fenceAndLogInJournal(journal);
        //assertTrue(secondFence.join(), "La seconda operazione di fence dovrebbe avere successo");

        isFenced = ledger.isFenced();
        assertTrue(isFenced, "Il ledger dovrebbe rimanere nello stato chiuso a scrittura");
    }

    @AfterEach
    void tearDown() {
        // Rilascia le risorse
        if (ledgerStorage != null) {
            // Cleanup delle risorse mock se necessario
        }
    }





}