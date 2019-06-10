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
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import io.debezium.config.Configuration;
import io.debezium.connector.maria.MySQLConnection.MySqlVersion;
import io.debezium.connector.maria.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.relational.history.KafkaDatabaseHistory;
import io.debezium.util.Testing;

/**
 * @author Jiri Pechanec, Randall Hauch
 */
public class BinlogReaderBufferIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("myServer1", "connector_test")
            .withDbHistoryPath(DB_HISTORY_PATH);
    private final UniqueDatabase RO_DATABASE = new UniqueDatabase("myServer2", "connector_test_ro", DATABASE)
            .withDbHistoryPath(DB_HISTORY_PATH);
    private static Map<String, Object> externalConfig = new HashMap<String, Object>();

    private Configuration config;

    @BeforeMethod(groups = {"test"})
	public void beforeEach() throws SQLException, InterruptedException {
    	ResourceBundle bundle = ResourceBundle.getBundle("jdbc");
    	String prefix = "jdbc.mysql.replication.";

    	if (bundle.getString(prefix + "database.hostname"     ) != null) externalConfig.put("hostname", bundle.getString(prefix + "database.hostname"));
    	if (bundle.getString(prefix + "database.port"         ) != null) externalConfig.put("port"    , bundle.getString(prefix + "database.port"));
    	if (bundle.getString(prefix + "database.superUsername") != null) externalConfig.put("user"    , bundle.getString(prefix + "database.superUsername"));
    	if (bundle.getString(prefix + "database.superPassword") != null) externalConfig.put("password", bundle.getString(prefix + "database.superPassword"));
    	
    	
        stopConnector();
        DATABASE.createAndInitialize(externalConfig);
//        RO_DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
//        Testing.Files.delete(DB_HISTORY_PATH);
        
        
    	try (MySQLConnection db = MySQLConnection.forTestDatabase(DATABASE.getDatabaseName(), externalConfig);) {
            try (JdbcConnection connection = db.connect()) {
                final Connection jdbc = connection.connection();
               
                final Statement statement = jdbc.createStatement();
                statement.executeUpdate("reset master");
            }
        }
    }

    @AfterMethod(groups = {"test"})
	public void afterEach() throws SQLException, InterruptedException {
        try {
            stopConnector();
            
            DATABASE.dropDB(externalConfig);
            
            
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test(groups = {"test"})
    public void shouldCorrectlyManageRollback() throws SQLException, InterruptedException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
      config = Configuration.create()
      .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", externalConfig.get("hostname").toString()))
      .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", externalConfig.get("port").toString()))
      .with(MySqlConnectorConfig.USER, externalConfig.get("user").toString())
      .with(MySqlConnectorConfig.PASSWORD, externalConfig.get("password").toString())
      .with(MySqlConnectorConfig.SERVER_ID, 18765)
      .with(MySqlConnectorConfig.SERVER_NAME, DATABASE.getServerName())
      .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED)
      .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
      .with(MySqlConnectorConfig.DATABASE_WHITELIST, DATABASE.getDatabaseName())
      .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
      .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
      .with(MySqlConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 10_000)
      .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
      .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement
        
        // ---------------------------------------------------------------------------------------------------------------
        // Transaction with rollback
        // supported only for non-GTID setup
        // ---------------------------------------------------------------------------------------------------------------
        if (replicaIsMaster) {
            try (MySQLConnection db = MySQLConnection.forTestDatabase(DATABASE.getDatabaseName(), externalConfig);) {
                try (JdbcConnection connection = db.connect()) {
                	System.out.println("**************************************************************");
                    final Connection jdbc = connection.connection();
                    connection.setAutoCommit(false);
                    final Statement statement = jdbc.createStatement();
                    statement.executeUpdate("CREATE TEMPORARY TABLE tmp_ids (a int)");
                    statement.executeUpdate("INSERT INTO tmp_ids VALUES(5)");
                    jdbc.commit();
                    statement.executeUpdate("DROP TEMPORARY TABLE tmp_ids");
                    statement.executeUpdate("UPDATE products SET weight=100.12 WHERE id=109");
                    jdbc.rollback();
                    connection.query("SELECT * FROM products", rs -> {
                        if (Testing.Print.isEnabled()) {
                            connection.print(rs);
                        }
                    });
                    connection.setAutoCommit(true);
                }
            }

            // The rolled-back transaction should be skipped
            //Thread.sleep(5000);

            // Try to read records and verify that there is none
            assertNoRecordsToConsume();
            assertEngineIsRunning();

            Testing.print("*** Done with rollback TX");
        }
    }

    @Test(groups = {"test"})
    public void shouldProcessSavepoint() throws SQLException, InterruptedException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        config = Configuration.create()
                              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", externalConfig.get("hostname").toString()))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", externalConfig.get("port").toString()))
                              .with(MySqlConnectorConfig.USER, externalConfig.get("user").toString())
                              .with(MySqlConnectorConfig.PASSWORD, externalConfig.get("password").toString())
                              .with(MySqlConnectorConfig.SERVER_ID, 18765)
                              .with(MySqlConnectorConfig.SERVER_NAME, DATABASE.getServerName())
                              .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED)
                              .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, DATABASE.getDatabaseName())
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement
        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Transaction with savepoint
        // ---------------------------------------------------------------------------------------------------------------
        try (MySQLConnection db = MySQLConnection.forTestDatabase(DATABASE.getDatabaseName(), externalConfig);) {
            try (JdbcConnection connection = db.connect()) {
                final Connection jdbc = connection.connection();
                connection.setAutoCommit(false);
                final Statement statement = jdbc.createStatement();
                statement.executeUpdate("INSERT INTO customers VALUES(default, 'first', 'first', 'first')");
                jdbc.setSavepoint();
                statement.executeUpdate("INSERT INTO customers VALUES(default, 'second', 'second', 'second')");
                jdbc.commit();
                connection.query("SELECT * FROM customers", rs -> {
                    if (Testing.Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
                connection.setAutoCommit(true);
            }
        }

        // 2 INSERTS + SAVEPOINT
        records = consumeRecordsByTopic(2 + 1);
        assertThat(records.topics().size()).isEqualTo(1 + 1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("customers"))).hasSize(2);
        assertThat(records.allRecordsInOrder()).hasSize(3);
        Testing.print("*** Done with savepoint TX");
    }

    @Test(groups = {"test"})
    public void shouldProcessLargeTransaction() throws SQLException, InterruptedException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        config = Configuration.create()
        		              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", externalConfig.get("hostname").toString()))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", externalConfig.get("port").toString()))
                              .with(MySqlConnectorConfig.USER, externalConfig.get("user").toString())
                              .with(MySqlConnectorConfig.PASSWORD, externalConfig.get("password").toString())
                              .with(MySqlConnectorConfig.SERVER_ID, 18765)
                              .with(MySqlConnectorConfig.SERVER_NAME, DATABASE.getServerName())
                              .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED)
                              .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, DATABASE.getDatabaseName())
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 9)
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement

        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Transaction with many events
        // ---------------------------------------------------------------------------------------------------------------
        try (MySQLConnection db = MySQLConnection.forTestDatabase(DATABASE.getDatabaseName(), externalConfig);) {
            final int numRecords = 40;
            try (JdbcConnection connection = db.connect()) {
            	//System.out.println("+++++++++++++++++++++++++++++++++");
                final Connection jdbc = connection.connection();
                connection.setAutoCommit(false);
                final Statement statement = jdbc.createStatement();
                for (int i = 0; i < numRecords; i++) {
                    statement.executeUpdate(String.format(
                            "INSERT INTO customers\n" + "VALUES (default,\"%s\",\"%s\",\"%s\")", i, i, i)
                    );
                }
                jdbc.commit();
                
                connection.query("SELECT * FROM customers", rs -> {
                    if (Testing.Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
                connection.setAutoCommit(true);
            }
            //TimeUnit.SECONDS.sleep(10);
            //System.out.println("----------------------------");
            

            // All records should be present only once
            records = consumeRecordsByTopic(numRecords);
            int recordIndex = 0;
            for (SourceRecord r: records.allRecordsInOrder()) {
                Struct envelope = (Struct) r.value();
                assertThat(envelope.getString("op")).isEqualTo(("c"));
                assertThat(envelope.getStruct("after").getString("email")).isEqualTo(Integer.toString(recordIndex++));
            }
            assertThat(records.topics().size()).isEqualTo(1);

            Testing.print("*** Done with large TX");
        }
    }

    @FixFor("DBZ-411")
    @Test(groups = {"test"})
    public void shouldProcessRolledBackSavepoint() throws SQLException, InterruptedException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        config = Configuration.create()
        		              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", externalConfig.get("hostname").toString()))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", externalConfig.get("port").toString()))
                              .with(MySqlConnectorConfig.USER, externalConfig.get("user").toString())
                              .with(MySqlConnectorConfig.PASSWORD, externalConfig.get("password").toString())
                              .with(MySqlConnectorConfig.SERVER_ID, 18765)
                              .with(MySqlConnectorConfig.SERVER_NAME, DATABASE.getServerName())
                              .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED)
                              .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, DATABASE.getDatabaseName())
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement
        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Transaction with rollback to savepoint
        // supported only for non-GTID setup
        // ---------------------------------------------------------------------------------------------------------------
        if (replicaIsMaster) {
            try (MySQLConnection db = MySQLConnection.forTestDatabase(DATABASE.getDatabaseName(), externalConfig);) {
                try (JdbcConnection connection = db.connect()) {
                    final Connection jdbc = connection.connection();
                    connection.setAutoCommit(false);
                    final Statement statement = jdbc.createStatement();
                    statement.executeUpdate("CREATE TEMPORARY TABLE tmp_ids (a int)");
                    statement.executeUpdate("INSERT INTO tmp_ids VALUES(5)");
                    jdbc.commit();
                    statement.executeUpdate("DROP TEMPORARY TABLE tmp_ids");
                    statement.executeUpdate("INSERT INTO customers VALUES(default, 'first', 'first', 'first')");
                    final Savepoint savepoint = jdbc.setSavepoint();
                    statement.executeUpdate("INSERT INTO customers VALUES(default, 'second', 'second', 'second')");
                    jdbc.rollback(savepoint);
                    jdbc.commit();
                    connection.query("SELECT * FROM customers", rs -> {
                        if (Testing.Print.isEnabled()) {
                            connection.print(rs);
                        }
                    });
                    connection.setAutoCommit(true);
                }
            }

            records = consumeRecordsByTopic(2);
            assertThat(records.topics().size()).isEqualTo(1 + 1);
            assertThat(records.recordsForTopic(DATABASE.topicForTable("customers"))).hasSize(1);
            assertThat(records.allRecordsInOrder()).hasSize(2);
            Testing.print("*** Done with savepoint TX");
        }
    }
}
