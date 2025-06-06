package org.apache.bookkeeper.bookie;



import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledUnsafeHeapByteBuf;
import org.apache.bookkeeper.common.util.Watcher;

import org.junit.jupiter.api.*;

import static org.evosuite.runtime.EvoAssertions.verifyException;
import static org.evosuite.shaded.org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.evosuite.runtime.ViolatedAssumptionAnswer;

import java.io.IOException;

@DisplayName("LedgerDescriptor White Box Tests for Coverage")
class EvosuiteTests {



    @Test
    public void test00()  throws Throwable  {
        byte[] byteArray0 = new byte[6];
        SortedLedgerStorage sortedLedgerStorage0 = new SortedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 0L, sortedLedgerStorage0);
        UnpooledByteBufAllocator unpooledByteBufAllocator0 = new UnpooledByteBufAllocator(true, true);
        UnpooledUnsafeHeapByteBuf unpooledUnsafeHeapByteBuf0 = new UnpooledUnsafeHeapByteBuf(unpooledByteBufAllocator0, 0, 14);
        ByteBuf byteBuf0 = unpooledUnsafeHeapByteBuf0.writeLongLE((-560L));
        try {
            ledgerDescriptorImpl0.addEntry(byteBuf0);
            fail("Expecting exception: IOException");

        } catch(IOException e) {
            //
            // Entry for ledger -3387269869736034305 was sent to 0
            //
            verifyException("org.apache.bookkeeper.bookie.LedgerDescriptorImpl", e);
        }
    }

    @Test
    public void test01()  throws Throwable  {
        byte[] byteArray0 = new byte[8];
        InterleavedLedgerStorage interleavedLedgerStorage0 = new InterleavedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 0L, interleavedLedgerStorage0);
        long long0 = ledgerDescriptorImpl0.getLedgerId();
        assertEquals(0L, long0);
    }

    @Test
    public void test02()  throws Throwable  {
        byte[] byteArray0 = new byte[0];
        InterleavedLedgerStorage interleavedLedgerStorage0 = new InterleavedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, (-175L), interleavedLedgerStorage0);
        long long0 = ledgerDescriptorImpl0.getLedgerId();
        assertEquals((-175L), long0);
    }

    @Test
    public void test03()  throws Throwable  {
        byte[] byteArray0 = new byte[1];
        SortedLedgerStorage sortedLedgerStorage0 = new SortedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 1L, sortedLedgerStorage0);
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.setFenced();
            fail("Expecting exception: NullPointerException");

        } catch(NullPointerException e) {
            //
            // no message in exception (getMessage() returned null)
            //
            verifyException("org.apache.bookkeeper.bookie.InterleavedLedgerStorage", e);
        }
    }

    @Test
    public void test04()  throws Throwable  {
        byte[] byteArray0 = new byte[0];
        InterleavedLedgerStorage interleavedLedgerStorage0 = new InterleavedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 0L, interleavedLedgerStorage0);
        Watcher<LastAddConfirmedUpdateNotification> watcher0 = (Watcher<LastAddConfirmedUpdateNotification>) mock(Watcher.class, new ViolatedAssumptionAnswer());
        InterleavedStorageRegenerateIndexOp.DryRunLedgerCache interleavedStorageRegenerateIndexOp_DryRunLedgerCache0 = new InterleavedStorageRegenerateIndexOp.DryRunLedgerCache();
        interleavedLedgerStorage0.ledgerCache = (LedgerCache) interleavedStorageRegenerateIndexOp_DryRunLedgerCache0;
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.cancelWaitForLastAddConfirmedUpdate(watcher0);
            fail("Expecting exception: UnsupportedOperationException");

        } catch(UnsupportedOperationException e) {
            //
            // no message in exception (getMessage() returned null)
            //
            verifyException("org.apache.bookkeeper.bookie.InterleavedStorageRegenerateIndexOp$DryRunLedgerCache", e);
        }
    }

    @Test
    public void test05()  throws Throwable  {
        byte[] byteArray0 = new byte[2];
        SortedLedgerStorage sortedLedgerStorage0 = new SortedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 1L, sortedLedgerStorage0);
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.addEntry((ByteBuf) null);
            fail("Expecting exception: NullPointerException");

        } catch(NullPointerException e) {
            //
            // no message in exception (getMessage() returned null)
            //
            verifyException("org.apache.bookkeeper.bookie.LedgerDescriptorImpl", e);
        }
    }

    @Test
    public void test06()  throws Throwable  {
        byte[] byteArray0 = new byte[7];
        SortedLedgerStorage sortedLedgerStorage0 = new SortedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, (-4358L), sortedLedgerStorage0);
        UnpooledByteBufAllocator unpooledByteBufAllocator0 = new UnpooledByteBufAllocator(false, false);
        ByteBuf byteBuf0 = unpooledByteBufAllocator0.heapBuffer();
        try {
            ledgerDescriptorImpl0.addEntry(byteBuf0);
            fail("Expecting exception: IOException");

        } catch(IOException e) {
            //
            // Entry for ledger 0 was sent to -4358
            //
            verifyException("org.apache.bookkeeper.bookie.LedgerDescriptorImpl", e);
        }
    }
    @Test
    public void test07()  throws Throwable  {
        byte[] byteArray0 = new byte[0];
        SortedLedgerStorage sortedLedgerStorage0 = new SortedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 0L, sortedLedgerStorage0);
        UnpooledByteBufAllocator unpooledByteBufAllocator0 = new UnpooledByteBufAllocator(true, true);
        UnpooledUnsafeHeapByteBuf unpooledUnsafeHeapByteBuf0 = new UnpooledUnsafeHeapByteBuf(unpooledByteBufAllocator0, 14, 2004318071);
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.addEntry(unpooledUnsafeHeapByteBuf0);
            fail("Expecting exception: IndexOutOfBoundsException");

        } catch(IndexOutOfBoundsException e) {
            //
            // index: 8, length: 8 (expected: range(0, 14))
            //
            verifyException("io.netty.buffer.AbstractByteBuf", e);
        }
    }

    /**
     * Tests the exception thrown when accessing a ledger descriptor with a different master key
     */
    @Test
    public void test08() throws Throwable {
        // Create master key with known value
        byte[] masterKey1 = new byte[]{1};  // First master key
        byte[] masterKey2 = new byte[]{2};  // Different master key

        // Create ledger descriptor with first master key
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(
                masterKey1,
                1L,
                new SortedLedgerStorage()
        );

        try {
            // Try to access with different master key
            ledgerDescriptorImpl0.checkAccess(masterKey2);
            fail("Should throw an exception when master keys don't match");
        } catch (BookieException e) {

            // Test passes if BookieException is thrown
            assertEquals(BookieException.Code.UnauthorizedAccessException, e.getCode());
        }
    }



    @Test
    public void test09()  throws Throwable  {
        byte[] byteArray0 = new byte[1];
        SortedLedgerStorage sortedLedgerStorage0 = new SortedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 257L, sortedLedgerStorage0);
        ledgerDescriptorImpl0.checkAccess(byteArray0);
        assertEquals(257L, ledgerDescriptorImpl0.getLedgerId());
    }

    @Test
    public void test10()  throws Throwable  {
        byte[] byteArray0 = new byte[5];
        InterleavedLedgerStorage interleavedLedgerStorage0 = new InterleavedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, (-2030L), interleavedLedgerStorage0);
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.readEntry((-2030L));
            fail("Expecting exception: NullPointerException");

        } catch(NullPointerException e) {
            //
            // no message in exception (getMessage() returned null)
            //
            verifyException("org.apache.bookkeeper.bookie.InterleavedLedgerStorage", e);
        }
    }

    @Test
    public void test11()  throws Throwable  {
        byte[] byteArray0 = new byte[3];
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 72057594037927808L, (LedgerStorage) null);
        long long0 = ledgerDescriptorImpl0.getLedgerId();
        assertEquals(72057594037927808L, long0);
    }

    @Test
    public void test12()  throws Throwable  {
        byte[] byteArray0 = new byte[1];
        SortedLedgerStorage sortedLedgerStorage0 = new SortedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 4294967296L, sortedLedgerStorage0);
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.getLastAddConfirmed();
            fail("Expecting exception: NullPointerException");

        } catch(NullPointerException e) {
            //
            // no message in exception (getMessage() returned null)
            //
            verifyException("org.apache.bookkeeper.bookie.InterleavedLedgerStorage", e);
        }
    }

    @Test
    public void test13()  throws Throwable  {
        byte[] byteArray0 = new byte[7];
        SortedLedgerStorage sortedLedgerStorage0 = new SortedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 2847L, sortedLedgerStorage0);
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.setExplicitLac((ByteBuf) null);
            fail("Expecting exception: NullPointerException");

        } catch(NullPointerException e) {
            //
            // no message in exception (getMessage() returned null)
            //
            verifyException("org.apache.bookkeeper.bookie.InterleavedLedgerStorage", e);
        }
    }

    @Test
    public void test14()  throws Throwable  {
        byte[] byteArray0 = new byte[1];
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, (byte)48, (LedgerStorage) null);
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.waitForLastAddConfirmedUpdate((byte)48, (Watcher<LastAddConfirmedUpdateNotification>) null);
            fail("Expecting exception: NullPointerException");

        } catch(NullPointerException e) {
            //
            // no message in exception (getMessage() returned null)
            //
            verifyException("org.apache.bookkeeper.bookie.LedgerDescriptorImpl", e);
        }
    }

    @Test
    public void test15()  throws Throwable  {
        byte[] byteArray0 = new byte[1];
        SortedLedgerStorage sortedLedgerStorage0 = new SortedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, (-154L), sortedLedgerStorage0);
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.getExplicitLac();
            fail("Expecting exception: NullPointerException");

        } catch(NullPointerException e) {
            //
            // no message in exception (getMessage() returned null)
            //
            verifyException("org.apache.bookkeeper.bookie.InterleavedLedgerStorage", e);
        }
    }

    @Test
    public void test16()  throws Throwable  {
        byte[] byteArray0 = new byte[1];
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, (byte)48, (LedgerStorage) null);
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.cancelWaitForLastAddConfirmedUpdate((Watcher<LastAddConfirmedUpdateNotification>) null);
            fail("Expecting exception: NullPointerException");

        } catch(NullPointerException e) {
            //
            // no message in exception (getMessage() returned null)
            //
            verifyException("org.apache.bookkeeper.bookie.LedgerDescriptorImpl", e);
        }
    }
    @Test
    public void test17()  throws Throwable  {
        byte[] byteArray0 = new byte[1];
        SortedLedgerStorage sortedLedgerStorage0 = new SortedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 1L, sortedLedgerStorage0);
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.getListOfEntriesOfLedger(1L);
            fail("Expecting exception: NullPointerException");

        } catch(NullPointerException e) {
            //
            // no message in exception (getMessage() returned null)
            //
            verifyException("org.apache.bookkeeper.bookie.SortedLedgerStorage", e);
        }
    }
    @Test
    public void test18()  throws Throwable  {
        byte[] byteArray0 = new byte[1];
        SortedLedgerStorage sortedLedgerStorage0 = new SortedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 257L, sortedLedgerStorage0);
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.isFenced();
            fail("Expecting exception: NullPointerException");

        } catch(NullPointerException e) {
            //
            // no message in exception (getMessage() returned null)
            //
            verifyException("org.apache.bookkeeper.bookie.InterleavedLedgerStorage", e);
        }
    }


    @Test
    public void test19()  throws Throwable  {
        byte[] byteArray0 = new byte[7];
        SortedLedgerStorage sortedLedgerStorage0 = new SortedLedgerStorage();
        LedgerDescriptorImpl ledgerDescriptorImpl0 = new LedgerDescriptorImpl(byteArray0, 4503599627370496L, sortedLedgerStorage0);
        // Undeclared exception!
        try {
            ledgerDescriptorImpl0.fenceAndLogInJournal((Journal) null);
            fail("Expecting exception: NullPointerException");

        } catch(NullPointerException e) {
            //
            // no message in exception (getMessage() returned null)
            //
            verifyException("org.apache.bookkeeper.bookie.InterleavedLedgerStorage", e);
        }
    }


}
