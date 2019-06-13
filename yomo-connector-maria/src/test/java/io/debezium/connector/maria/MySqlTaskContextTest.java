/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.maria;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.connector.maria.MySqlConnectorConfig.GtidNewChannelPosition;
import io.debezium.connector.maria.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.connector.maria.MySqlConnectorConfig.SnapshotMode;
import io.debezium.doc.FixFor;
import io.debezium.document.Document;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.KafkaDatabaseHistory;
import io.debezium.util.Testing;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;

/**
 * @author Randall Hauch
 *
 */
public class MySqlTaskContextTest {

    protected static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-context.txt").toAbsolutePath();

    protected int serverId;
    protected String serverName;
    protected String databaseName;

    protected Configuration config;
    protected MySqlTaskContext context;
    protected HashMap<String, Object> dbConnInfo;

    @BeforeMethod(groups = {"test", "context"})
    public void beforeEach() {
        dbConnInfo = getConnInfo();
        serverId = 18965;
        serverName = "logical_server_name";
        databaseName = "connector_test_ro";
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @AfterMethod(groups = {"test", "context"})
    public void afterEach() {
        if (context != null) {
            try {
                context.shutdown();
            } finally {
                context = null;
                Testing.Files.delete(DB_HISTORY_PATH);
            }
        }
    }

    protected Configuration.Builder simpleConfig() {
        return Configuration.create()
                            .with(MySqlConnectorConfig.HOSTNAME, dbConnInfo.get("hostname").toString())
                            .with(MySqlConnectorConfig.PORT, dbConnInfo.get("port").toString())
                            .with(MySqlConnectorConfig.USER, dbConnInfo.get("user").toString())
                            .with(MySqlConnectorConfig.PASSWORD, dbConnInfo.get("password").toString())
                            .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED)
                            .with(MySqlConnectorConfig.SERVER_ID, serverId)
                            .with(MySqlConnectorConfig.SERVER_NAME, serverName)
                            .with(MySqlConnectorConfig.DATABASE_WHITELIST, databaseName)
                            .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                            .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH);
    }

    //No need database for test
    @Test(groups = {"context"})
    public void shouldCreateTaskFromConfigurationWithNeverSnapshotMode() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                               .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build(), false, null);
        context.start();

        assertThat("" + context.snapshotMode().getValue()).isEqualTo(SnapshotMode.NEVER.getValue());
        assertThat(context.isSnapshotAllowedWhenNeeded()).isEqualTo(false);
        assertThat(context.isSnapshotNeverAllowed()).isEqualTo(true);
    }

    @Test(groups = {"context"})
    public void shouldCreateTaskFromConfigurationWithWhenNeededSnapshotMode() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.WHEN_NEEDED)
                               .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build(), false, null);
        context.start();

        assertThat("" + context.snapshotMode().getValue()).isEqualTo(SnapshotMode.WHEN_NEEDED.getValue());
        assertThat(context.isSnapshotAllowedWhenNeeded()).isEqualTo(true);
        assertThat(context.isSnapshotNeverAllowed()).isEqualTo(false);
    }

    @Test(groups = {"context"})
    public void shouldUseGtidSetIncludes() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.GTID_SOURCE_INCLUDES, "a,b,c,d.*")
                               .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build(), false, null);
        context.start();

        Predicate<String> filter = context.gtidSourceFilter();
        assertThat(filter).isNotNull();
        assertThat(filter.test("a")).isTrue();
        assertThat(filter.test("b")).isTrue();
        assertThat(filter.test("c")).isTrue();
        assertThat(filter.test("d")).isTrue();
        assertThat(filter.test("d1")).isTrue();
        assertThat(filter.test("d2")).isTrue();
        assertThat(filter.test("d1234xdgfe")).isTrue();
        assertThat(filter.test("a1")).isFalse();
        assertThat(filter.test("a2")).isFalse();
        assertThat(filter.test("b1")).isFalse();
        assertThat(filter.test("c1")).isFalse();
        assertThat(filter.test("e")).isFalse();
    }

    @Test(groups = {"context"}, enabled = false)
    public void shouldUseGtidSetIncludesLiteralUuids() throws Exception {
        String gtidStr = "036d85a9-64e5-11e6-9b48-42010af0000c:1-2,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41";
        config = simpleConfig().with(MySqlConnectorConfig.GTID_SOURCE_INCLUDES,
                                     "036d85a9-64e5-11e6-9b48-42010af0000c,7145bf69-d1ca-11e5-a588-0242ac110004")
                               .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build(), false, null);
        context.start();

        Predicate<String> filter = context.gtidSourceFilter();
        assertThat(filter).isNotNull();
        assertThat(filter.test("036d85a9-64e5-11e6-9b48-42010af0000c")).isTrue();
        assertThat(filter.test("7145bf69-d1ca-11e5-a588-0242ac110004")).isTrue();
        assertThat(filter.test("036d85a9-64e5-11e6-9b48-42010af0000c-extra")).isFalse();
        assertThat(filter.test("7145bf69-d1ca-11e5-a588-0242ac110004-extra")).isFalse();
        assertThat(filter.test("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isFalse();

//        GtidSet original = new GtidSet(gtidStr);
//        assertThat(original.forServerWithId("036d85a9-64e5-11e6-9b48-42010af0000c")).isNotNull();
//        assertThat(original.forServerWithId("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isNotNull();
//        assertThat(original.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004")).isNotNull();
//
//        GtidSet filtered = original.retainAll(filter);
//        assertThat(filtered.forServerWithId("036d85a9-64e5-11e6-9b48-42010af0000c")).isNotNull();
//        assertThat(filtered.forServerWithId("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isNull();
//        assertThat(filtered.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004")).isNotNull();
    }

    @Test(groups = {"context"},enabled = false)
    public void shouldUseGtidSetxcludesLiteralUuids() throws Exception {
        String gtidStr = "036d85a9-64e5-11e6-9b48-42010af0000c:1-2,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41";
        config = simpleConfig().with(MySqlConnectorConfig.GTID_SOURCE_EXCLUDES,
                                     "7c1de3f2-3fd2-11e6-9cdc-42010af000bc")
                               .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build(), false, null);
        context.start();

        Predicate<String> filter = context.gtidSourceFilter();
        assertThat(filter).isNotNull();
        assertThat(filter.test("036d85a9-64e5-11e6-9b48-42010af0000c")).isTrue();
        assertThat(filter.test("7145bf69-d1ca-11e5-a588-0242ac110004")).isTrue();
        assertThat(filter.test("036d85a9-64e5-11e6-9b48-42010af0000c-extra")).isTrue();
        assertThat(filter.test("7145bf69-d1ca-11e5-a588-0242ac110004-extra")).isTrue();
        assertThat(filter.test("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isFalse();

//        GtidSet original = new GtidSet(gtidStr);
//        assertThat(original.forServerWithId("036d85a9-64e5-11e6-9b48-42010af0000c")).isNotNull();
//        assertThat(original.forServerWithId("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isNotNull();
//        assertThat(original.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004")).isNotNull();
//
//        GtidSet filtered = original.retainAll(filter);
//        assertThat(filtered.forServerWithId("036d85a9-64e5-11e6-9b48-42010af0000c")).isNotNull();
//        assertThat(filtered.forServerWithId("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isNull();
//        assertThat(filtered.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004")).isNotNull();
    }

    @Test(groups = {"context"},enabled = false)
    public void shouldNotAllowBothGtidSetIncludesAndExcludes() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.GTID_SOURCE_INCLUDES,
                                     "036d85a9-64e5-11e6-9b48-42010af0000c,7145bf69-d1ca-11e5-a588-0242ac110004")
                               .with(MySqlConnectorConfig.GTID_SOURCE_EXCLUDES,
                                     "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41")
                               .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build(), false, null);
        boolean valid = config.validateAndRecord(MySqlConnectorConfig.ALL_FIELDS, msg -> {});
        assertThat(valid).isFalse();
    }

    @Test(groups = {"context"},enabled = false)
    public void shouldFilterAndMergeGtidSet() throws Exception {
        String gtidStr = "036d85a9-64e5-11e6-9b48-42010af0000c:1-2,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:5-41";
        String availableServerGtidStr = "036d85a9-64e5-11e6-9b48-42010af0000c:1-20,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200,"
                + "123e4567-e89b-12d3-a456-426655440000:1-41";
        String purgedServerGtidStr = "";

        config = simpleConfig().with(MySqlConnectorConfig.GTID_SOURCE_INCLUDES,
                                     "036d85a9-64e5-11e6-9b48-42010af0000c")
                               .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build(), false, null);
        context.start();
        context.source().setCompletedGtidSet(gtidStr);

//        GtidSet mergedGtidSet = context.filterGtidSet(new GtidSet(availableServerGtidStr), new GtidSet(purgedServerGtidStr));
//        assertThat(mergedGtidSet).isNotNull();
//        GtidSet.UUIDSet uuidSet1 = mergedGtidSet.forServerWithId("036d85a9-64e5-11e6-9b48-42010af0000c");
//        GtidSet.UUIDSet uuidSet2 = mergedGtidSet.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004");
//        GtidSet.UUIDSet uuidSet3 = mergedGtidSet.forServerWithId("123e4567-e89b-12d3-a456-426655440000");
//        GtidSet.UUIDSet uuidSet4 = mergedGtidSet.forServerWithId("7c1de3f2-3fd2-11e6-9cdc-42010af000bc");
//
//        assertThat(uuidSet1.getIntervals()).isEqualTo(Arrays.asList(new GtidSet.Interval(1, 2)));
//        assertThat(uuidSet2.getIntervals()).isEqualTo(Arrays.asList(new GtidSet.Interval(1, 3200)));
//        assertThat(uuidSet3.getIntervals()).isEqualTo(Arrays.asList(new GtidSet.Interval(1, 41)));
//        assertThat(uuidSet4).isNull();
    }

    @Test(groups = {"context"},enabled = false)
    @FixFor("DBZ-923")
    public void shouldMergeToFirstAvailableGtidSetPositions() throws Exception {
        String gtidStr = "036d85a9-64e5-11e6-9b48-42010af0000c:1-2,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:5-41";

        String availableServerGtidStr = "036d85a9-64e5-11e6-9b48-42010af0000c:1-20,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200,"
                + "123e4567-e89b-12d3-a456-426655440000:1-41";

        String purgedServerGtidStr = "7145bf69-d1ca-11e5-a588-0242ac110004:1-1234";

        config = simpleConfig()
                .with(MySqlConnectorConfig.GTID_SOURCE_INCLUDES, "036d85a9-64e5-11e6-9b48-42010af0000c")
                .with(MySqlConnectorConfig.GTID_NEW_CHANNEL_POSITION, GtidNewChannelPosition.EARLIEST)
                .build();

        context = new MySqlTaskContext(config, new Filters.Builder(config).build(), false, null);
        context.start();
        context.source().setCompletedGtidSet(gtidStr);

//        GtidSet mergedGtidSet = context.filterGtidSet(new GtidSet(availableServerGtidStr), new GtidSet(purgedServerGtidStr));
//        assertThat(mergedGtidSet).isNotNull();
//        GtidSet.UUIDSet uuidSet1 = mergedGtidSet.forServerWithId("036d85a9-64e5-11e6-9b48-42010af0000c");
//        GtidSet.UUIDSet uuidSet2 = mergedGtidSet.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004");
//        GtidSet.UUIDSet uuidSet3 = mergedGtidSet.forServerWithId("123e4567-e89b-12d3-a456-426655440000");
//        GtidSet.UUIDSet uuidSet4 = mergedGtidSet.forServerWithId("7c1de3f2-3fd2-11e6-9cdc-42010af000bc");
//
//        assertThat(uuidSet1.getIntervals()).isEqualTo(Arrays.asList(new GtidSet.Interval(1, 2)));
//        assertThat(uuidSet2.getIntervals()).isEqualTo(Arrays.asList(new GtidSet.Interval(1, 1234)));
//        assertThat(uuidSet3).isNull();
//        assertThat(uuidSet4).isNull();
    }

    @Test(groups = {"context"},enabled = false)
    public void shouldComparePositionsWithDifferentFields() {
        String lastGtidStr = "01261278-6ade-11e6-b36a-42010af00790:1-400944168,"
                + "30efb117-e42a-11e6-ba9e-42010a28002e:1-9,"
                + "4d1a4918-44ba-11e6-bf12-42010af0040b:1-11604379,"
                + "621dc2f6-803b-11e6-acc1-42010af000a4:1-7963838,"
                + "716ec46f-d522-11e5-bb56-0242ac110004:1-35850702,"
                + "c627b2bc-9647-11e6-a886-42010af0044a:1-10426868,"
                + "d079cbb3-750f-11e6-954e-42010af00c28:1-11544291:11544293-11885648";
        config = simpleConfig().with(MySqlConnectorConfig.GTID_SOURCE_EXCLUDES, "96c2072e-e428-11e6-9590-42010a28002d")
                               .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build(), false, null);
        context.start();
        context.source().setCompletedGtidSet(lastGtidStr);
        HistoryRecordComparator comparator = context.dbSchema().historyComparator();

        String server = "mysql-server-1";
        HistoryRecord rec1 = historyRecord(server, "mysql-bin.000008", 380941551, "01261278-6ade-11e6-b36a-42010af00790:1-378422946,"
                + "4d1a4918-44ba-11e6-bf12-42010af0040b:1-11002284,"
                + "716ec46f-d522-11e5-bb56-0242ac110004:1-34673215,"
                + "96c2072e-e428-11e6-9590-42010a28002d:1-3,"
                + "c627b2bc-9647-11e6-a886-42010af0044a:1-9541144", 0, 0, true);
        HistoryRecord rec2 = historyRecord(server, "mysql-bin.000016", 645115324, "01261278-6ade-11e6-b36a-42010af00790:1-400944168,"
                + "30efb117-e42a-11e6-ba9e-42010a28002e:1-9,"
                + "4d1a4918-44ba-11e6-bf12-42010af0040b:1-11604379,"
                + "621dc2f6-803b-11e6-acc1-42010af000a4:1-7963838,"
                + "716ec46f-d522-11e5-bb56-0242ac110004:1-35850702,"
                + "c627b2bc-9647-11e6-a886-42010af0044a:1-10426868,"
                + "d079cbb3-750f-11e6-954e-42010af00c28:1-11544291:11544293-11885648", 2, 1, false);

        assertThat(comparator.isAtOrBefore(rec1, rec2)).isTrue();
        assertThat(comparator.isAtOrBefore(rec2, rec1)).isFalse();
    }

    @Test(groups = {"context"},enabled = false)
    public void shouldIgnoreDatabaseHistoryProperties() throws Exception {
        config = simpleConfig().with(KafkaDatabaseHistory.TOPIC, "dummytopic")
                               .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build(), false, null);
        context.start();

        context.getConnectionContext().jdbc().config().forEach((k, v) -> {
            assertThat(k).doesNotMatch("^history");
        });
    }

    protected HistoryRecord historyRecord(String serverName, String binlogFilename, int position, String gtids,
                                          int event, int row, boolean snapshot) {
        Document source = Document.create(SourceInfo.SERVER_NAME_KEY, serverName);
        Document pos = Document.create(SourceInfo.BINLOG_FILENAME_OFFSET_KEY, binlogFilename,
                                       SourceInfo.BINLOG_POSITION_OFFSET_KEY, position);
        if (row >= 0) {
            pos = pos.set(SourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY, row);
        }
        if (event >= 0) {
            pos = pos.set(SourceInfo.EVENTS_TO_SKIP_OFFSET_KEY, event);
        }
        if (gtids != null && gtids.trim().length() != 0) {
            pos = pos.set(SourceInfo.GTID_SET_KEY, gtids);
        }
        if (snapshot) {
            pos = pos.set(SourceInfo.SNAPSHOT_KEY, true);
        }
        return new HistoryRecord(Document.create(HistoryRecord.Fields.SOURCE, source,
                                                 HistoryRecord.Fields.POSITION, pos));
    }
    
    private HashMap<String, Object> getConnInfo() {
    	ResourceBundle bundle = ResourceBundle.getBundle("jdbc");
    	String prefix = "jdbc.mysql.replication.";
    	Map<String, Object> dbConnInfo = new HashMap<String, Object>();

    	if (bundle.getString(prefix + "database.hostname"     ) != null) dbConnInfo.put("hostname", bundle.getString(prefix + "database.hostname"));
    	if (bundle.getString(prefix + "database.port"         ) != null) dbConnInfo.put("port"    , bundle.getString(prefix + "database.port"));
    	if (bundle.getString(prefix + "database.superUsername") != null) dbConnInfo.put("user"    , bundle.getString(prefix + "database.superUsername"));
    	if (bundle.getString(prefix + "database.superPassword") != null) dbConnInfo.put("password", bundle.getString(prefix + "database.superPassword"));
    	
    	return (HashMap<String, Object>) dbConnInfo;
    }

}
