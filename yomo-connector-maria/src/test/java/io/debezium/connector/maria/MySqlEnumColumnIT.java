/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.maria;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.kafka.connect.data.Schema;
import org.junit.Rule;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.maria.junit.SkipForLegacyParser;
import io.debezium.connector.maria.junit.SkipTestForLegacyParser;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Chris Cranford
 */
@SkipForLegacyParser
public class MySqlEnumColumnIT extends AbstractConnectorTest {
    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-enum-column.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("enumcolumnit", "enum_column_test").withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Rule
    public final TestRule skip = new SkipTestForLegacyParser();

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
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test(groups = {"column"})
    @FixFor("DBZ-1203")
    public void shouldAlterEnumColumnCharacterSet() throws Exception {

        config = DATABASE.defaultConfig()
        		.with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", DATABASE.getConnInfo().get("hostname").toString()))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", DATABASE.getConnInfo().get("port").toString()))
                .with(MySqlConnectorConfig.USER, DATABASE.getConnInfo().get("user").toString())
                .with(MySqlConnectorConfig.PASSWORD, DATABASE.getConnInfo().get("password").toString())
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with(MySqlConnectorConfig.TABLE_WHITELIST, DATABASE.qualifiedTableName("test_stations_10"))
                .build();

        start(MySqlConnector.class, config);

        // There are 5 records to account for the following operations
        // CREATE DATABASE
        // CREATE TABLE
        // INSERT
        // ALTER TABLE
        // INSERT
        SourceRecords records = consumeRecordsByTopic( 5 );

        Schema schemaBeforeAlter = records.allRecordsInOrder().get(2).valueSchema().field("after").schema();
        Schema schemaAfterAlter = records.allRecordsInOrder().get(4).valueSchema().field("after").schema();

        String allowedBeforeAlter = schemaBeforeAlter.field("type").schema().parameters().get("allowed");
        String allowedAfterAlter = schemaAfterAlter.field("type").schema().parameters().get("allowed");

        assertThat(allowedBeforeAlter).isEqualTo("station,post_office");
        assertThat(allowedAfterAlter).isEqualTo("station,post_office,plane,ahihi_dongok,now,test,a\\,b,c\\,'d,g\\,'h");
        stopConnector();
    }
}
