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

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Chris Cranford
 */
public class MySqlTimestampColumnIT extends AbstractConnectorTest {
    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-timestamp-column.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("timestampcolumnit", "timestamp_column_test").withDbHistoryPath(DB_HISTORY_PATH);

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
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test(groups = {"column"})
    @FixFor("DBZ-1243")
    public void shouldAlterEnumColumnCharacterSet() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with(MySqlConnectorConfig.TABLE_WHITELIST, DATABASE.qualifiedTableName("t_user_black_list"))
                .build();

        start(MySqlConnector.class, config);

        // There should be 5 records that imply create database, create table, alter table, insert row, update row.
        // If the ddl parser fails, there will only be 3; the insert/update won't occur.
        SourceRecords records = consumeRecordsByTopic(5);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(5);

        stopConnector();
    }
}
