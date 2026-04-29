/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.GarbageCollectionStatus;
import org.apache.bookkeeper.bookie.LedgerStorage.StorageState;
import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;


/**
 * Unit tests for {@link DbLedgerStorage}.
 *
 * <p>Uses Mockito to stub {@link SingleDirectoryDbLedgerStorage} so tests are
 * isolated from RocksDB and the file-system.
 */

class Isw2LLMDbLedgerStorageTest {

    // -----------------------------------------------------------------------
    // Helpers / shared fixtures
    // -----------------------------------------------------------------------

    /**
     * Minimal concrete subclass that injects mock
     * {@link SingleDirectoryDbLedgerStorage} instances instead of building real
     * ones. This avoids touching disk / RocksDB in unit tests.
     */
    private static class TestableDbLedgerStorage extends DbLedgerStorage {

        private final List<SingleDirectoryDbLedgerStorage> injectedList;

        TestableDbLedgerStorage(SingleDirectoryDbLedgerStorage... stores) {
            this.injectedList = Arrays.asList(stores);
        }

        /** Bypass initialize(); wire the list directly. */
        void wireStorages() throws Exception {
            var f = DbLedgerStorage.class.getDeclaredField("ledgerStorageList");
            f.setAccessible(true);
            f.set(this, injectedList);

            var n = DbLedgerStorage.class.getDeclaredField("numberOfDirs");
            n.setAccessible(true);
            n.setInt(this, injectedList.size());
        }
    }

    // -----------------------------------------------------------------------
    // getLongVariableOrDefault
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("getLongVariableOrDefault()")
    class GetLongVariableOrDefault {

        private ServerConfiguration conf;

        @BeforeEach
        void setUp() {
            conf = new ServerConfiguration();
        }

        @Test
        @DisplayName("returns default when property is absent")
        void returnsDefaultWhenAbsent() {
            long result = DbLedgerStorage.getLongVariableOrDefault(conf, "nonExistentKey", 42L);
            assertEquals(42L, result);
        }

        @Test
        @DisplayName("returns Number value when property is a Number")
        void returnsNumberValueDirectly() {
            conf.setProperty("myKey", 99L);
            long result = DbLedgerStorage.getLongVariableOrDefault(conf, "myKey", 0L);
            assertEquals(99L, result);
        }

        @Test
        @DisplayName("returns default when property is an empty string")
        void returnsDefaultForEmptyString() {
            conf.setProperty("myKey", "");
            long result = DbLedgerStorage.getLongVariableOrDefault(conf, "myKey", 7L);
            assertEquals(7L, result);
        }

        @Test
        @DisplayName("parses non-empty string to long")
        void parsesStringToLong() {
            conf.setProperty("myKey", "123");
            long result = DbLedgerStorage.getLongVariableOrDefault(conf, "myKey", 0L);
            assertEquals(123L, result);
        }
    }

    // -----------------------------------------------------------------------
    // getBooleanVariableOrDefault
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("getBooleanVariableOrDefault()")
    class GetBooleanVariableOrDefault {

        private ServerConfiguration conf;

        @BeforeEach
        void setUp() {
            conf = new ServerConfiguration();
        }

        @Test
        @DisplayName("returns default when property is absent")
        void returnsDefaultWhenAbsent() {
            boolean result = DbLedgerStorage.getBooleanVariableOrDefault(conf, "noKey", true);
            assertTrue(result);
        }

        @Test
        @DisplayName("returns Boolean value when property is a Boolean")
        void returnsBooleanDirectly() {
            conf.setProperty("flag", Boolean.TRUE);
            assertTrue(DbLedgerStorage.getBooleanVariableOrDefault(conf, "flag", false));

            conf.setProperty("flag", Boolean.FALSE);
            assertFalse(DbLedgerStorage.getBooleanVariableOrDefault(conf, "flag", true));
        }

        @Test
        @DisplayName("returns default when property is an empty string")
        void returnsDefaultForEmptyString() {
            conf.setProperty("flag", "");
            assertFalse(DbLedgerStorage.getBooleanVariableOrDefault(conf, "flag", false));
        }

        @ParameterizedTest
        @CsvSource({"true,true", "false,false", "TRUE,true", "FALSE,false"})
        @DisplayName("parses recognised boolean strings correctly")
        void parsesBooleanStrings(String raw, boolean expected) {
            conf.setProperty("flag", raw);
            assertEquals(expected, DbLedgerStorage.getBooleanVariableOrDefault(conf, "flag", !expected));
        }
    }

    // -----------------------------------------------------------------------
    // getListOfEntriesOfLedger – always throws UnsupportedOperationException
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("getListOfEntriesOfLedger()")
    class GetListOfEntriesOfLedger {

        @Test
        @DisplayName("always throws UnsupportedOperationException")
        void alwaysThrows() throws Exception {
            SingleDirectoryDbLedgerStorage store = mock(SingleDirectoryDbLedgerStorage.class);
            TestableDbLedgerStorage subject = new TestableDbLedgerStorage(store);
            subject.wireStorages();

            assertThrows(UnsupportedOperationException.class,
                    () -> subject.getListOfEntriesOfLedger(1L));
        }
    }

    // -----------------------------------------------------------------------
    // Delegation – operations that forward to the correct shard
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Shard delegation")
    class ShardDelegation {

        SingleDirectoryDbLedgerStorage shard0;

        SingleDirectoryDbLedgerStorage shard1;

        TestableDbLedgerStorage subject;

        @BeforeEach
        void setUp() throws Exception {
            shard0 = mock(SingleDirectoryDbLedgerStorage.class);
            shard1 = mock(SingleDirectoryDbLedgerStorage.class);
            subject = new TestableDbLedgerStorage(shard0, shard1);
            subject.wireStorages();
        }

        @Test
        @DisplayName("ledgerExists() delegates to the correct shard")
        void ledgerExistsDelegates() throws IOException {
            // ledgerId 0 → shard0, ledgerId 1 → shard1
            when(shard0.ledgerExists(0L)).thenReturn(true);
            when(shard1.ledgerExists(1L)).thenReturn(false);

            assertTrue(subject.ledgerExists(0L));
            assertFalse(subject.ledgerExists(1L));
            verify(shard0).ledgerExists(0L);
            verify(shard1).ledgerExists(1L);
        }

        @Test
        @DisplayName("setFenced() delegates to the correct shard and returns its result")
        void setFencedDelegates() throws IOException {
            when(shard0.setFenced(0L)).thenReturn(true);
            assertTrue(subject.setFenced(0L));
            verify(shard0).setFenced(0L);
            verify(shard1, never()).setFenced(anyLong());
        }

        @Test
        @DisplayName("isFenced() delegates to the correct shard")
        void isFencedDelegates() throws IOException, BookieException {
            when(shard1.isFenced(1L)).thenReturn(true);
            assertTrue(subject.isFenced(1L));
            verify(shard1).isFenced(1L);
        }

        @Test
        @DisplayName("deleteLedger() delegates to the correct shard")
        void deleteLedgerDelegates() throws IOException {
            doNothing().when(shard0).deleteLedger(0L);
            subject.deleteLedger(0L);
            verify(shard0).deleteLedger(0L);
        }

        @Test
        @DisplayName("addEntry() routes by ledgerId embedded in ByteBuf")
        void addEntryRoutesByLedgerId() throws IOException, BookieException {
            // Encode ledgerId=1 (→ shard1) in the first 8 bytes
            ByteBuf buf = Unpooled.buffer(16);
            buf.writeLong(1L);  // ledgerId
            buf.writeLong(0L);  // entryId (padding)

            when(shard1.addEntry(buf)).thenReturn(42L);

            long result = subject.addEntry(buf);

            assertEquals(42L, result);
            verify(shard1).addEntry(buf);
            verify(shard0, never()).addEntry(any());
            buf.release();
        }

        @Test
        @DisplayName("getEntry() delegates to the correct shard")
        void getEntryDelegates() throws IOException, BookieException {
            ByteBuf expected = Unpooled.buffer(8);
            when(shard0.getEntry(0L, 5L)).thenReturn(expected);

            ByteBuf result = subject.getEntry(0L, 5L);

            assertNotNull(result);
            assertEquals(expected, result);
            expected.release();
        }

        @Test
        @DisplayName("setMasterKey() and readMasterKey() use the same shard")
        void masterKeyRoundTrip() throws IOException, BookieException {
            byte[] key = "secret".getBytes();
            when(shard0.readMasterKey(0L)).thenReturn(key);

            subject.setMasterKey(0L, key);
            byte[] result = subject.readMasterKey(0L);

            verify(shard0).setMasterKey(0L, key);
            assertEquals(key, result);
        }

        @Test
        @DisplayName("getLastAddConfirmed() delegates to correct shard")
        void lastAddConfirmedDelegates() throws IOException, BookieException {
            when(shard1.getLastAddConfirmed(1L)).thenReturn(99L);
            assertEquals(99L, subject.getLastAddConfirmed(1L));
        }

        @Test
        @DisplayName("setExplicitLac() and getExplicitLac() delegate to correct shard")
        void explicitLacDelegates() throws IOException, BookieException {
            ByteBuf lac = Unpooled.buffer(8);
            when(shard0.getExplicitLac(0L)).thenReturn(lac);

            subject.setExplicitLac(0L, lac);
            ByteBuf result = subject.getExplicitLac(0L);

            verify(shard0).setExplicitLac(0L, lac);
            assertEquals(lac, result);
            lac.release();
        }
    }

    // -----------------------------------------------------------------------
    // Fan-out operations – broadcast to ALL shards
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Fan-out operations")
    class FanOut {

        SingleDirectoryDbLedgerStorage shard0;

        SingleDirectoryDbLedgerStorage shard1;

        TestableDbLedgerStorage subject;

        @BeforeEach
        void setUp() throws Exception {
            shard0 = mock(SingleDirectoryDbLedgerStorage.class);
            shard1 = mock(SingleDirectoryDbLedgerStorage.class);
            subject = new TestableDbLedgerStorage(shard0, shard1);
            subject.wireStorages();
        }

        @Test
        @DisplayName("start() calls start() on every shard")
        void startBroadcasts() {
            subject.start();
            verify(shard0).start();
            verify(shard1).start();
        }

        @Test
        @DisplayName("flush() calls flush() on every shard")
        void flushBroadcasts() throws IOException {
            subject.flush();
            verify(shard0).flush();
            verify(shard1).flush();
        }

        @Test
        @DisplayName("checkpoint() calls checkpoint() on every shard")
        void checkpointBroadcasts() throws IOException {
            Checkpoint cp = mock(Checkpoint.class);
            subject.checkpoint(cp);
            verify(shard0).checkpoint(cp);
            verify(shard1).checkpoint(cp);
        }

        @Test
        @DisplayName("forceGC() calls forceGC() on every shard")
        void forceGCBroadcasts() {
            subject.forceGC();
            verify(shard0).forceGC();
            verify(shard1).forceGC();
        }

        @Test
        @DisplayName("forceGC(major, minor) passes flags to every shard")
        void forceGCWithFlagsBroadcasts() {
            subject.forceGC(true, false);
            verify(shard0).forceGC(true, false);
            verify(shard1).forceGC(true, false);
        }

        @Test
        @DisplayName("suspendMinorGC() broadcasts to all shards")
        void suspendMinorGCBroadcasts() {
            subject.suspendMinorGC();
            verify(shard0).suspendMinorGC();
            verify(shard1).suspendMinorGC();
        }

        @Test
        @DisplayName("resumeMinorGC() broadcasts to all shards")
        void resumeMinorGCBroadcasts() {
            subject.resumeMinorGC();
            verify(shard0).resumeMinorGC();
            verify(shard1).resumeMinorGC();
        }

        @Test
        @DisplayName("suspendMajorGC() broadcasts to all shards")
        void suspendMajorGCBroadcasts() {
            subject.suspendMajorGC();
            verify(shard0).suspendMajorGC();
            verify(shard1).suspendMajorGC();
        }

        @Test
        @DisplayName("resumeMajorGC() broadcasts to all shards")
        void resumeMajorGCBroadcasts() {
            subject.resumeMajorGC();
            verify(shard0).resumeMajorGC();
            verify(shard1).resumeMajorGC();
        }

        @Test
        @DisplayName("registerLedgerDeletionListener() registers on all shards")
        void registerListenerBroadcasts() {
            var listener = mock(org.apache.bookkeeper.bookie.LedgerStorage.LedgerDeletionListener.class);
            subject.registerLedgerDeletionListener(listener);
            verify(shard0).registerLedgerDeletionListener(listener);
            verify(shard1).registerLedgerDeletionListener(listener);
        }

        @Test
        @DisplayName("setStateManager() propagates to all shards")
        void setStateManagerBroadcasts() {
            StateManager sm = mock(StateManager.class);
            subject.setStateManager(sm);
            verify(shard0).setStateManager(sm);
            verify(shard1).setStateManager(sm);
        }

        @Test
        @DisplayName("setCheckpointSource() propagates to all shards")
        void setCheckpointSourceBroadcasts() {
            CheckpointSource cs = mock(CheckpointSource.class);
            subject.setCheckpointSource(cs);
            verify(shard0).setCheckpointSource(cs);
            verify(shard1).setCheckpointSource(cs);
        }

        @Test
        @DisplayName("setCheckpointer() propagates to all shards")
        void setCheckpointerBroadcasts() {
            Checkpointer cp = mock(Checkpointer.class);
            subject.setCheckpointer(cp);
            verify(shard0).setCheckpointer(cp);
            verify(shard1).setCheckpointer(cp);
        }

        @Test
        @DisplayName("entryLocationCompact() calls compact on all shards")
        void entryLocationCompactBroadcasts() {
            subject.entryLocationCompact();
            verify(shard0).entryLocationCompact();
            verify(shard1).entryLocationCompact();
        }

        @Test
        @DisplayName("shutdown() shuts down every shard even when no executors are present")
        void shutdownBroadcasts() throws InterruptedException {
            subject.shutdown();
            verify(shard0).shutdown();
            verify(shard1).shutdown();
        }
    }

    // -----------------------------------------------------------------------
    // Aggregate predicates
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Aggregate predicates")
    class AggregatePredicates {

        SingleDirectoryDbLedgerStorage shard0;

        SingleDirectoryDbLedgerStorage shard1;

        TestableDbLedgerStorage subject;

        @BeforeEach
        void setUp() throws Exception {
            shard0 = mock(SingleDirectoryDbLedgerStorage.class);
            shard1 = mock(SingleDirectoryDbLedgerStorage.class);
            subject = new TestableDbLedgerStorage(shard0, shard1);
            subject.wireStorages();
        }

        @Test
        @DisplayName("isInForceGC() returns true when any shard is in GC")
        void isInForceGCReturnsTrueWhenAny() {
            when(shard0.isInForceGC()).thenReturn(false);
            when(shard1.isInForceGC()).thenReturn(true);
            assertTrue(subject.isInForceGC());
        }

        @Test
        @DisplayName("isInForceGC() returns false when no shard is in GC")
        void isInForceGCReturnsFalseWhenNone() {
            when(shard0.isInForceGC()).thenReturn(false);
            when(shard1.isInForceGC()).thenReturn(false);
            assertFalse(subject.isInForceGC());
        }

        @Test
        @DisplayName("isMajorGcSuspended() returns true only when ALL shards agree")
        void isMajorGcSuspendedRequiresAll() {
            when(shard0.isMajorGcSuspended()).thenReturn(true);
            when(shard1.isMajorGcSuspended()).thenReturn(false);
            assertFalse(subject.isMajorGcSuspended());

            when(shard1.isMajorGcSuspended()).thenReturn(true);
            assertTrue(subject.isMajorGcSuspended());
        }

        @Test
        @DisplayName("isMinorGcSuspended() returns true only when ALL shards agree")
        void isMinorGcSuspendedRequiresAll() {
            when(shard0.isMinorGcSuspended()).thenReturn(true);
            when(shard1.isMinorGcSuspended()).thenReturn(true);
            assertTrue(subject.isMinorGcSuspended());

            when(shard1.isMinorGcSuspended()).thenReturn(false);
            assertFalse(subject.isMinorGcSuspended());
        }

        @Test
        @DisplayName("isFlushRequired() returns true only when ALL shards require flush")
        void isFlushRequiredRequiresAll() {
            when(shard0.isFlushRequired()).thenReturn(true);
            when(shard1.isFlushRequired()).thenReturn(true);
            assertTrue(subject.isFlushRequired());

            when(shard1.isFlushRequired()).thenReturn(false);
            assertFalse(subject.isFlushRequired());
        }

        @Test
        @DisplayName("isEntryLocationCompacting() returns true when any shard is compacting")
        void isEntryLocationCompactingReturnsTrueWhenAny() {
            when(shard0.isEntryLocationCompacting()).thenReturn(false);
            when(shard1.isEntryLocationCompacting()).thenReturn(true);
            assertTrue(subject.isEntryLocationCompacting());
        }
    }

    // -----------------------------------------------------------------------
    // entryLocationCompact(List<String>) – selective compaction
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("entryLocationCompact(List<String>)")
    class SelectiveEntryLocationCompact {

        SingleDirectoryDbLedgerStorage shard0;

        SingleDirectoryDbLedgerStorage shard1;

        TestableDbLedgerStorage subject;

        @BeforeEach
        void setUp() throws Exception {
            shard0 = mock(SingleDirectoryDbLedgerStorage.class);
            shard1 = mock(SingleDirectoryDbLedgerStorage.class);
            subject = new TestableDbLedgerStorage(shard0, shard1);
            subject.wireStorages();

            when(shard0.getEntryLocationDBPath()).thenReturn(List.of("/data/db0"));
            when(shard1.getEntryLocationDBPath()).thenReturn(List.of("/data/db1"));
        }

        @Test
        @DisplayName("compacts only the shard whose path is in the list")
        void compactsOnlyMatchingShard() {
            subject.entryLocationCompact(List.of("/data/db1"));
            verify(shard0, never()).entryLocationCompact();
            verify(shard1).entryLocationCompact();
        }

        @Test
        @DisplayName("compacts all matching shards when multiple paths provided")
        void compactsAllMatchingShards() {
            subject.entryLocationCompact(List.of("/data/db0", "/data/db1"));
            verify(shard0).entryLocationCompact();
            verify(shard1).entryLocationCompact();
        }

        @Test
        @DisplayName("compacts nothing when no path matches")
        void compactsNothingWhenNoMatch() {
            subject.entryLocationCompact(List.of("/data/unknown"));
            verify(shard0, never()).entryLocationCompact();
            verify(shard1, never()).entryLocationCompact();
        }
    }

    // -----------------------------------------------------------------------
    // isEntryLocationCompacting(List<String>) – per-location status
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("isEntryLocationCompacting(List<String>)")
    class PerLocationCompactingStatus {

        SingleDirectoryDbLedgerStorage shard0;

        SingleDirectoryDbLedgerStorage shard1;

        TestableDbLedgerStorage subject;

        @BeforeEach
        void setUp() throws Exception {
            shard0 = mock(SingleDirectoryDbLedgerStorage.class);
            shard1 = mock(SingleDirectoryDbLedgerStorage.class);
            subject = new TestableDbLedgerStorage(shard0, shard1);
            subject.wireStorages();

            when(shard0.getEntryLocationDBPath()).thenReturn(List.of("/data/db0"));
            when(shard1.getEntryLocationDBPath()).thenReturn(List.of("/data/db1"));
        }

        @Test
        @DisplayName("returns correct status for each requested location")
        void returnsCorrectStatuses() {
            when(shard0.isEntryLocationCompacting()).thenReturn(true);
            when(shard1.isEntryLocationCompacting()).thenReturn(false);

            Map<String, Boolean> result = subject.isEntryLocationCompacting(
                    List.of("/data/db0", "/data/db1"));

            assertEquals(2, result.size());
            assertTrue(result.get("/data/db0"));
            assertFalse(result.get("/data/db1"));
        }

        @Test
        @DisplayName("returns empty map when no location matches")
        void returnsEmptyMapWhenNoMatch() {
            Map<String, Boolean> result = subject.isEntryLocationCompacting(List.of("/unknown"));
            assertTrue(result.isEmpty());
        }
    }

    // -----------------------------------------------------------------------
    // getEntryLocationDBPath – aggregates all shard paths
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("getEntryLocationDBPath()")
    class GetEntryLocationDBPath {

        SingleDirectoryDbLedgerStorage shard0;

        SingleDirectoryDbLedgerStorage shard1;

        TestableDbLedgerStorage subject;

        @BeforeEach
        void setUp() throws Exception {
            shard0 = mock(SingleDirectoryDbLedgerStorage.class);
            shard1 = mock(SingleDirectoryDbLedgerStorage.class);
            subject = new TestableDbLedgerStorage(shard0, shard1);
            subject.wireStorages();
        }

        @Test
        @DisplayName("returns the union of all shard paths")
        void returnsUnionOfAllPaths() {
            when(shard0.getEntryLocationDBPath()).thenReturn(List.of("/db/0"));
            when(shard1.getEntryLocationDBPath()).thenReturn(List.of("/db/1"));

            List<String> paths = subject.getEntryLocationDBPath();

            assertEquals(2, paths.size());
            assertTrue(paths.containsAll(List.of("/db/0", "/db/1")));
        }
    }

    // -----------------------------------------------------------------------
    // getGarbageCollectionStatus – one entry per shard
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("getGarbageCollectionStatus()")
    class GetGarbageCollectionStatus {

        SingleDirectoryDbLedgerStorage shard0;

        SingleDirectoryDbLedgerStorage shard1;

        TestableDbLedgerStorage subject;

        @BeforeEach
        void setUp() throws Exception {
            shard0 = mock(SingleDirectoryDbLedgerStorage.class);
            shard1 = mock(SingleDirectoryDbLedgerStorage.class);
            subject = new TestableDbLedgerStorage(shard0, shard1);
            subject.wireStorages();
        }

        @Test
        @DisplayName("returns one GarbageCollectionStatus per shard")
        void returnsOneStatusPerShard() {
            GarbageCollectionStatus gcStatus0 = mock(GarbageCollectionStatus.class);
            GarbageCollectionStatus gcStatus1 = mock(GarbageCollectionStatus.class);

            when(shard0.getGarbageCollectionStatus()).thenReturn(List.of(gcStatus0));
            when(shard1.getGarbageCollectionStatus()).thenReturn(List.of(gcStatus1));

            List<GarbageCollectionStatus> statuses = subject.getGarbageCollectionStatus();

            assertEquals(2, statuses.size());
            assertTrue(statuses.contains(gcStatus0));
            assertTrue(statuses.contains(gcStatus1));
        }
    }

    // -----------------------------------------------------------------------
    // StorageState flag operations – routed through STORAGE_FLAGS_KEY shard
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("StorageState flag operations")
    class StorageStateFlags {

        SingleDirectoryDbLedgerStorage shard0; // STORAGE_FLAGS_KEY=0L maps here

        SingleDirectoryDbLedgerStorage shard1;

        TestableDbLedgerStorage subject;

        @BeforeEach
        void setUp() throws Exception {
            shard0 = mock(SingleDirectoryDbLedgerStorage.class);
            shard1 = mock(SingleDirectoryDbLedgerStorage.class);
            subject = new TestableDbLedgerStorage(shard0, shard1);
            subject.wireStorages();
        }

        @Test
        @DisplayName("getStorageStateFlags() delegates to shard for key 0")
        void getStorageStateFlagsDelegates() throws IOException {
            EnumSet<StorageState> expected = EnumSet.of(StorageState.NEEDS_INTEGRITY_CHECK);
            when(shard0.getStorageStateFlags()).thenReturn(expected);

            assertEquals(expected, subject.getStorageStateFlags());
            verify(shard0).getStorageStateFlags();
        }

        @Test
        @DisplayName("setStorageStateFlag() delegates to shard for key 0")
        void setStorageStateFlagDelegates() throws IOException {
            subject.setStorageStateFlag(StorageState.NEEDS_INTEGRITY_CHECK);
            verify(shard0).setStorageStateFlag(StorageState.NEEDS_INTEGRITY_CHECK);
        }

        @Test
        @DisplayName("clearStorageStateFlag() delegates to shard for key 0")
        void clearStorageStateFlagDelegates() throws IOException {
            subject.clearStorageStateFlag(StorageState.NEEDS_INTEGRITY_CHECK);
            verify(shard0).clearStorageStateFlag(StorageState.NEEDS_INTEGRITY_CHECK);
        }
    }

    // -----------------------------------------------------------------------
    // Limbo state
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Limbo state operations")
    class LimboState {

        SingleDirectoryDbLedgerStorage shard0;

        SingleDirectoryDbLedgerStorage shard1;

        TestableDbLedgerStorage subject;

        @BeforeEach
        void setUp() throws Exception {
            shard0 = mock(SingleDirectoryDbLedgerStorage.class);
            shard1 = mock(SingleDirectoryDbLedgerStorage.class);
            subject = new TestableDbLedgerStorage(shard0, shard1);
            subject.wireStorages();
        }

        @Test
        @DisplayName("setLimboState() delegates to the correct shard")
        void setLimboStateDelegates() throws IOException {
            subject.setLimboState(0L);
            verify(shard0).setLimboState(0L);
        }

        @Test
        @DisplayName("hasLimboState() delegates to the correct shard")
        void hasLimboStateDelegates() throws IOException {
            when(shard1.hasLimboState(1L)).thenReturn(true);
            assertTrue(subject.hasLimboState(1L));
        }

        @Test
        @DisplayName("clearLimboState() delegates to the correct shard")
        void clearLimboStateDelegates() throws IOException {
            subject.clearLimboState(0L);
            verify(shard0).clearLimboState(0L);
        }
    }

    // -----------------------------------------------------------------------
    // flush() – propagates IOException from any shard
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Error propagation")
    class ErrorPropagation {

        SingleDirectoryDbLedgerStorage shard0;

        SingleDirectoryDbLedgerStorage shard1;

        TestableDbLedgerStorage subject;

        @BeforeEach
        void setUp() throws Exception {
            shard0 = mock(SingleDirectoryDbLedgerStorage.class);
            shard1 = mock(SingleDirectoryDbLedgerStorage.class);
            subject = new TestableDbLedgerStorage(shard0, shard1);
            subject.wireStorages();
        }

        @Test
        @DisplayName("flush() propagates IOException thrown by a shard")
        void flushPropagatesException() throws IOException {
            doThrow(new IOException("disk error")).when(shard0).flush();
            assertThrows(IOException.class, () -> subject.flush());
        }

        @Test
        @DisplayName("checkpoint() propagates IOException thrown by a shard")
        void checkpointPropagatesException() throws IOException {
            Checkpoint cp = mock(Checkpoint.class);
            doThrow(new IOException("disk error")).when(shard1).checkpoint(cp);
            assertThrows(IOException.class, () -> subject.checkpoint(cp));
        }

        @Test
        @DisplayName("readMasterKey() propagates BookieException from shard")
        void readMasterKeyPropagatesBookieException() throws IOException, BookieException {
            when(shard0.readMasterKey(0L))
                    .thenThrow(BookieException.create(BookieException.Code.UnauthorizedAccessException));
            assertThrows(BookieException.class, () -> subject.readMasterKey(0L));
        }
    }

    // -----------------------------------------------------------------------
    // getLedgerStorageList – visible for testing
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("getLedgerStorageList()")
    class GetLedgerStorageList {

        SingleDirectoryDbLedgerStorage shard0;

        SingleDirectoryDbLedgerStorage shard1;

        TestableDbLedgerStorage subject;

        @BeforeEach
        void setUp() throws Exception {
            shard0 = mock(SingleDirectoryDbLedgerStorage.class);
            shard1 = mock(SingleDirectoryDbLedgerStorage.class);
            subject = new TestableDbLedgerStorage(shard0, shard1);
            subject.wireStorages();
        }

        @Test
        @DisplayName("returns all injected shards")
        void returnsAllShards() {
            List<SingleDirectoryDbLedgerStorage> list = subject.getLedgerStorageList();
            assertEquals(2, list.size());
            assertTrue(list.contains(shard0));
            assertTrue(list.contains(shard1));
        }
    }

    // -----------------------------------------------------------------------
    // readLedgerIndexEntries – null guard checks
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("readLedgerIndexEntries() null-guard checks")
    class ReadLedgerIndexEntriesNullGuards {

        @Test
        @DisplayName("throws NullPointerException when ServerConfiguration is null")
        void throwsWhenConfIsNull() {
            assertThrows(NullPointerException.class,
                    () -> DbLedgerStorage.readLedgerIndexEntries(
                            1L,
                            null,
                            mock(SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor.class)));
        }

        @Test
        @DisplayName("throws NullPointerException when processor is null")
        void throwsWhenProcessorIsNull() {
            assertThrows(NullPointerException.class,
                    () -> DbLedgerStorage.readLedgerIndexEntries(
                            1L,
                            new ServerConfiguration(),
                            null));
        }
    }
}