/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.maria;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.source.SourceRecord;
import io.debezium.connector.maria.ParallelSnapshotReader.ParallelHaltingPredicate;

/**
 * @author Moira Tagle
 */
public class ParallelSnapshotReaderTest {

    @Test(groups = {"parallel"})
    public void startStartsBothReaders() {
        BinlogReader mockOldBinlogReader = mock(BinlogReader.class);
        SnapshotReader mockNewSnapshotReader = mock(SnapshotReader.class);
        BinlogReader mockNewBinlogReader = mock(BinlogReader.class);

        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(mockOldBinlogReader, mockNewSnapshotReader, mockNewBinlogReader);

        parallelSnapshotReader.start();

        AssertJUnit.assertSame(parallelSnapshotReader.state(), Reader.State.RUNNING);

        verify(mockOldBinlogReader).start();
        verify(mockNewSnapshotReader).start();
        // chained reader will only start the snapshot reader
    }

    @Test(groups = {"parallel"})
    public void pollCombinesBothReadersPolls() throws InterruptedException {
        BinlogReader mockOldBinlogReader = mock(BinlogReader.class);
        SnapshotReader mockNewSnapshotReader = mock(SnapshotReader.class);
        BinlogReader mockNewBinlogReader = mock(BinlogReader.class);

        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(mockOldBinlogReader, mockNewSnapshotReader, mockNewBinlogReader);

        SourceRecord oldBinlogSourceRecord = mock(SourceRecord.class);
        List<SourceRecord> oldBinlogRecords = new ArrayList<>();
        oldBinlogRecords.add(oldBinlogSourceRecord);

        SourceRecord newSnapshotSourceRecord = mock(SourceRecord.class);
        List<SourceRecord> newSnapshotRecords = new ArrayList<>();
        newSnapshotRecords.add(newSnapshotSourceRecord);

        when(mockOldBinlogReader.isRunning()).thenReturn(true);
        when(mockOldBinlogReader.poll()).thenReturn(oldBinlogRecords);
        when(mockNewSnapshotReader.poll()).thenReturn(newSnapshotRecords);

        // this needs to happen so that the chained reader can be polled.
        parallelSnapshotReader.start();

        List<SourceRecord> parallelRecords = parallelSnapshotReader.poll();

        AssertJUnit.assertEquals(2, parallelRecords.size());
        AssertJUnit.assertTrue(parallelRecords.contains(oldBinlogSourceRecord));
        AssertJUnit.assertTrue(parallelRecords.contains(newSnapshotSourceRecord));
    }

    @Test(groups = {"parallel"})
    public void pollReturnsNewIfOldReaderIsStopped() throws InterruptedException {
        BinlogReader mockOldBinlogReader = mock(BinlogReader.class);
        SnapshotReader mockNewSnapshotReader = mock(SnapshotReader.class);
        BinlogReader mockNewBinlogReader = mock(BinlogReader.class);

        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(mockOldBinlogReader, mockNewSnapshotReader, mockNewBinlogReader);

        SourceRecord newSnapshotSourceRecord = mock(SourceRecord.class);
        List<SourceRecord> newSnapshotRecords = new ArrayList<>();
        newSnapshotRecords.add(newSnapshotSourceRecord);

        // if the old reader is polled when it's stopped it will throw an exception.
        when(mockOldBinlogReader.isRunning()).thenReturn(false);
        when(mockOldBinlogReader.poll()).thenThrow(new InterruptedException());

        when(mockNewSnapshotReader.poll()).thenReturn(newSnapshotRecords);

        // this needs to happen so that the chained reader runs correctly.
        parallelSnapshotReader.start();

        List<SourceRecord> parallelRecords = parallelSnapshotReader.poll();

        AssertJUnit.assertEquals(1, parallelRecords.size());
        AssertJUnit.assertTrue(parallelRecords.contains(newSnapshotSourceRecord));
    }

    // this test and the next don't appear to be halting. Something with the chained reader maybe.
    @Test(groups = {"parallel"})
    public void pollReturnsOldIfNewReaderIsStopped() throws InterruptedException {
        BinlogReader mockOldBinlogReader = mock(BinlogReader.class);
        SnapshotReader mockNewSnapshotReader = mock(SnapshotReader.class);
        BinlogReader mockNewBinlogReader = mock(BinlogReader.class);

        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(mockOldBinlogReader, mockNewSnapshotReader, mockNewBinlogReader);

        SourceRecord oldBinlogSourceRecord = mock(SourceRecord.class);
        List<SourceRecord> oldBinlogRecords = new ArrayList<>();
        oldBinlogRecords.add(oldBinlogSourceRecord);

        when(mockOldBinlogReader.isRunning()).thenReturn(true);
        when(mockOldBinlogReader.poll()).thenReturn(oldBinlogRecords);

        // cheap way to have the new reader be stopped is to just not start it; so don't start the parallel reader

        List<SourceRecord> parallelRecords = parallelSnapshotReader.poll();

        AssertJUnit.assertEquals(1, parallelRecords.size());
        AssertJUnit.assertTrue(parallelRecords.contains(oldBinlogSourceRecord));
    }

    @Test(groups = {"parallel"})
    public void pollReturnsNullIfBothReadersAreStopped() throws InterruptedException {
        BinlogReader mockOldBinlogReader = mock(BinlogReader.class);
        SnapshotReader mockNewSnapshotReader = mock(SnapshotReader.class);
        BinlogReader mockNewBinlogReader = mock(BinlogReader.class);

        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(mockOldBinlogReader, mockNewSnapshotReader, mockNewBinlogReader);

        when(mockOldBinlogReader.isRunning()).thenReturn(false);
        when(mockOldBinlogReader.poll()).thenThrow(new InterruptedException());

        when(mockNewBinlogReader.poll()).thenReturn(null);

        // cheap way to have the new reader be stopped is to just not start it; so don't start the parallel reader

        List<SourceRecord> parallelRecords = parallelSnapshotReader.poll();

        AssertJUnit.assertEquals(null, parallelRecords);
    }

    @Test(groups = {"parallel"})
    public void testStopStopsBothReaders() {
        BinlogReader mockOldBinlogReader = mock(BinlogReader.class);
        SnapshotReader mockNewSnapshotReader = mock(SnapshotReader.class);
        BinlogReader mockNewBinlogReader = mock(BinlogReader.class);

        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(mockOldBinlogReader, mockNewSnapshotReader, mockNewBinlogReader);

        parallelSnapshotReader.start();
        parallelSnapshotReader.stop();

        AssertJUnit.assertTrue(parallelSnapshotReader.state() == Reader.State.STOPPED);

        verify(mockOldBinlogReader).stop();
        verify(mockNewSnapshotReader).stop();
    }


    @Test(groups = {"parallel"})
    public void testHaltingPredicateHonorsTimeRange() {
        // verify that halting predicate does nothing and changes no state if the
        // document's timestamp is outside of the time range.

        AtomicBoolean thisReaderNearEnd = new AtomicBoolean(false);
        AtomicBoolean otherReaderNearEnd = new AtomicBoolean(false);

        Duration duration = Duration.ofMinutes(5);

        ParallelHaltingPredicate parallelHaltingPredicate = new ParallelHaltingPredicate(thisReaderNearEnd, otherReaderNearEnd, duration);

        boolean testResult = parallelHaltingPredicate.accepts(createSourceRecordWithTimestamp(Instant.now().minus(duration.multipliedBy(2))));

        AssertJUnit.assertTrue(testResult);

        AssertJUnit.assertFalse(thisReaderNearEnd.get());
        AssertJUnit.assertFalse(otherReaderNearEnd.get());
    }

    @Test(groups = {"parallel"})
    public void testHaltingPredicateFlipsthisReaderNearEnd() {
        // verify that the halting predicate flips the `this reader` boolean if the
        // document's timestamp is within the time range, but still returns false.


        AtomicBoolean thisReaderNearEnd = new AtomicBoolean(false);
        AtomicBoolean otherReaderNearEnd = new AtomicBoolean(false);

        Duration duration = Duration.ofMinutes(5);

        ParallelHaltingPredicate parallelHaltingPredicate = new ParallelHaltingPredicate(thisReaderNearEnd, otherReaderNearEnd, duration);

        boolean testResult = parallelHaltingPredicate.accepts(createSourceRecordWithTimestamp(Instant.now()));

        AssertJUnit.assertTrue(testResult);

        AssertJUnit.assertTrue(thisReaderNearEnd.get());
        AssertJUnit.assertFalse(otherReaderNearEnd.get());
    }

    @Test(groups = {"parallel"})
    public void testHaltingPredicateHalts() {
        // verify that the halting predicate returns false if both the 'this' and
        // 'other' reader are near the end of the binlog.

        AtomicBoolean thisReaderNearEnd = new AtomicBoolean(false);
        AtomicBoolean otherReaderNearEnd = new AtomicBoolean(true);

        Duration duration = Duration.ofMinutes(5);

        ParallelHaltingPredicate parallelHaltingPredicate =
            new ParallelHaltingPredicate(thisReaderNearEnd, otherReaderNearEnd, duration);

        boolean testResult =
            parallelHaltingPredicate.accepts(createSourceRecordWithTimestamp(Instant.now()));

        AssertJUnit.assertFalse(testResult);

        AssertJUnit.assertTrue(thisReaderNearEnd.get());
        AssertJUnit.assertTrue(otherReaderNearEnd.get());
    }

    /**
     * Create an "offset" containing a single timestamp element with the given value.
     * Needed because {@link ParallelSnapshotReader.ParallelHaltingPredicate} halts based on how
     * close the record's timestamp is to the present time.
     * @param tsSec the timestamp in the resulting offset.
     * @return an "offset" containing the given timestamp.
     */
    private SourceRecord createSourceRecordWithTimestamp(Instant ts) {
        Map<String, ?> offset = Collections.singletonMap(SourceInfo.TIMESTAMP_KEY, ts.getEpochSecond());
        return new SourceRecord(null, offset, null, null, null);
    }
}
