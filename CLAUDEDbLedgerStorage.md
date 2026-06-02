Act as a Senior QA Engineer. Your goal is to increase the Strength Score for `org.apache.openjpa.util.ProxyManagerImpl` 

### Context:

- Target Class: `src/main/java/org/apache/bookkeeper/bookie/storage/ldb/DbLedgerStorage.java`

- Existing Tests: `src/test/java/org/apache/bookkeeper/bookie/storage/ldb/Isw2DbLedgerStorage*`

- Current Status: Test strength is at 44%% with 10 uncovered mutations.



### Task:

1. Analyze `DbLedgerStorage.java` and the existing `src/test/java/org/apache/bookkeeper/bookie/storage/ldb/Isw2DbLedgerStorage*`.

2. Identify the "surviving mutants" logic, read the pit reports in `\\wsl.localhost\Ubuntu\home\francesco_astolfi\isw2-2026\bookkeeper\bookkeeper-server\target\pit-reports\org.apache.bookkeeper.bookie.storage.ldb\DbLedgerStorage.java.html`

3. Generate new JUnit 5 test cases focusing ONLY on:
    - Method: ` initialize(...), getEntry(log,long), addEntry(ByteBuff)`





### Constraints:

- Use JUnit 5 assertions (`org.junit.jupiter.api.Assertions`).

- Follow the naming convention `test[Scenario]_[ExpectedBehavior]`.

- Provide only the Java code for the new test methods or a complete test class if more efficient.

- Ensure the tests address boundary conditions (e.g., max size, load factor thresholds).



After generating the tests, tell me which specific PIT mutators (e.g., NEGATE_CONDITIONALS, VOID_METHOD_CALLS) each new test is designed to kill.

Generate the tests on pattern class src/test/java/org/apache/bookkeeper/bookie/storage/ldb//Isw2LLM2DbLedgerStorage
