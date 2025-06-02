package org.apache.bookkeeper.bookie;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@DisplayName("Manual Testing for LedgerDescriptorImpl")
public class ManualTestingTest {  // Reso public
    private static final long TEST_LEDGER_ID = 12345L;
    private LedgerStorage ledgerStorage;
    private byte[] masterKey;
    private LedgerDescriptorImpl ledgerDescriptor;
    private Method checkAccessMethod;

    @BeforeEach
    public void setUp() throws Exception {  // Reso public
        ledgerStorage = Mockito.mock(LedgerStorage.class);
        masterKey = new byte[]{1, 2, 3, 4};

        Constructor<LedgerDescriptorImpl> constructor = LedgerDescriptorImpl.class
                .getDeclaredConstructor(byte[].class, long.class, LedgerStorage.class);
        constructor.setAccessible(true);

        ledgerDescriptor = constructor.newInstance(masterKey, TEST_LEDGER_ID, ledgerStorage);

        checkAccessMethod = LedgerDescriptorImpl.class.getDeclaredMethod("checkAccess", byte[].class);
        checkAccessMethod.setAccessible(true);
    }

    @Test
    @DisplayName("Test checkAccess with correct master key")
    public void testCheckAccessWithCorrectKey() throws Exception {  // Reso public e aggiunto throws
        assertDoesNotThrow(() -> checkAccessMethod.invoke(ledgerDescriptor, masterKey));
    }
}