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

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

/**
 * @author Gunnar Morling
 */
public class MySqlTableMaintenanceStatementsIT extends AbstractConnectorTest {


    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-table-maintenance.txt")
                                                             .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("tablemaintenanceit", "table_maintenance_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @BeforeMethod(groups = {"test","stmt"})
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

    @AfterMethod(groups = {"test","stmt"})
	public void afterEach() {
        try {
            stopConnector();
            DATABASE.dropDB();
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test(groups = {"stmt"})
    @FixFor("DBZ-253")
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
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
        int numTableMaintenanceStatements = 3;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numTableMaintenanceStatements);
        System.out.println(records.allRecordsInOrder());
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(numCreateDatabase + numCreateTables + numTableMaintenanceStatements);
        assertThat(records.databaseNames()).containsOnly(DATABASE.getDatabaseName());
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(
            numCreateDatabase + numCreateTables + numTableMaintenanceStatements);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
    }
}
