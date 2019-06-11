/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.maria;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

public class MySqlDateTimeInKeyIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-decimal-column.txt")
                                                             .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("pkdb", "datetime_key_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @BeforeMethod(groups = {"test"})
	public void beforeEach() throws SQLException, InterruptedException  {
        stopConnector();
        DATABASE.setConnInfo("jdbc");
        try (MySQLConnection db = MySQLConnection.forTestDatabase("mysql", DATABASE.getConnInfo());) {
            try (JdbcConnection connection = db.connect()) {
                final Connection jdbc = connection.connection();
               
                final Statement statement = jdbc.createStatement();
                statement.executeUpdate("reset master");  
            }
        }
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @AfterMethod(groups = {"test"})
	public void afterEach() {
        try {
            stopConnector();
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
            DATABASE.dropDB();
        }
    }

    @Test(groups = "test")
    @FixFor("DBZ-1194")
    public void shouldAcceptAllZeroDatetimeInPrimaryKey() throws SQLException, InterruptedException {	
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
        		.with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", DATABASE.getConnInfo().get("hostname").toString()))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", DATABASE.getConnInfo().get("port").toString()))
                .with(MySqlConnectorConfig.USER, DATABASE.getConnInfo().get("user").toString())
                .with(MySqlConnectorConfig.PASSWORD, DATABASE.getConnInfo().get("password").toString())
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // Testing.Debug.enable();
        final int numDatabase = 3; // Snapshot: one drop + one create,  incremental: one create
        final int numTables = 3;   // Snapshot: one drop + one create,  incremental: one create
        final int numInserts = 1;
        final int numOthers = 2; // one SET and one USE
        SourceRecords records = consumeRecordsByTopic(numDatabase + numTables + numInserts + numOthers);

        assertThat(records).isNotNull();
        records.forEach(this::validate);

        List<SourceRecord> changes = records.recordsForTopic(DATABASE.topicForTable("dbz_1194_datetime_key_test"));
        assertThat(changes).hasSize(1);

        assertKey(changes);

        try (final Connection conn = MySQLConnection.forTestDatabase(DATABASE.getDatabaseName(), DATABASE.getConnInfo()).connection()) {
            conn.createStatement().execute("SET sql_mode='';");
            conn.createStatement().execute("INSERT INTO dbz_1194_datetime_key_test VALUES (default, '0000-00-00 00:00:00', '0000-00-00', '00:00:00')");
        }
        records = consumeRecordsByTopic(1);
        
        assertThat(records).isNotNull();
        records.forEach(this::validate);

        changes = records.recordsForTopic(DATABASE.topicForTable("dbz_1194_datetime_key_test"));
        assertThat(changes).hasSize(1);

        assertKey(changes);

        stopConnector();
    }

    private void assertKey(List<SourceRecord> changes) {
        Struct key = (Struct) changes.get(0).key();
        assertThat(key.getInt64("dtval")).isZero();
        assertThat(key.getInt64("tval")).isZero();
        assertThat(key.getInt32("dval")).isZero();
    }
}
