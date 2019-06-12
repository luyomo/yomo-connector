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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

/**
 * @author Gunnar Morling
 */
public class MySqlFixedLengthBinaryColumnIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-binary-column.txt")
                                                             .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("binarycolumnit", "binary_column_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @BeforeMethod(groups = {"test", "column"})
	public void beforeEach() throws SQLException, InterruptedException {
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

    @AfterMethod(groups = {"test", "column"})
	public void afterEach() {
        try {
            stopConnector();
            DATABASE.dropDB();
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test(groups = {"column"})
    @FixFor("DBZ-254")
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
        		.with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", DATABASE.getConnInfo().get("hostname").toString()))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", DATABASE.getConnInfo().get("port").toString()))
                .with(MySqlConnectorConfig.USER, DATABASE.getConnInfo().get("user").toString())
                .with(MySqlConnectorConfig.PASSWORD, DATABASE.getConnInfo().get("password").toString())
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        //Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 1;
        int numInserts = 4;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numInserts);
        stopConnector();
        assertThat(records).isNotNull();
        List<SourceRecord> dmls = records.recordsForTopic(DATABASE.topicForTable("dbz_254_binary_column_test"));
        assertThat(dmls).hasSize(4);

        // source value has a trailing "00" which is not distinguishable from
        SourceRecord insert = dmls.get(0);
        Struct after = (Struct) ((Struct) insert.value()).get("after");
        assertThat(encodeToBase64String((ByteBuffer) after.get("file_uuid"))).isEqualTo("ZRrtCDkPSJOy8TaSPnt0AA==");

        insert = dmls.get(1);
        after = (Struct) ((Struct) insert.value()).get("after");
        assertThat(encodeToBase64String((ByteBuffer) after.get("file_uuid"))).isEqualTo("ZRrtCDkPSJOy8TaSPnt0qw==");

        // the value which isn't using the full length of the BINARY column is right-padded with 0x00 (zero bytes) - converted to AA in Base64
        insert = dmls.get(2);
        after = (Struct) ((Struct) insert.value()).get("after");
        assertThat(encodeToBase64String((ByteBuffer) after.get("file_uuid"))).isEqualTo("ZRrtCDkPSJOy8TaSPnt0AA==");

        // the value which isn't using the full length of the BINARY column is right-padded with 0x00 (zero bytes)
        insert = dmls.get(3);
        after = (Struct) ((Struct) insert.value()).get("after");
        assertThat(encodeToBase64String((ByteBuffer) after.get("file_uuid"))).isEqualTo("AAAAAAAAAAAAAAAAAAAAAA==");

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
    }

    private String encodeToBase64String(ByteBuffer bytes) {
        return Base64.getEncoder().encodeToString(bytes.array());
    }
}
