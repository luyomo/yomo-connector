/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.maria;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.BeforeMethod;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import io.debezium.util.Testing;

import org.testng.annotations.BeforeMethod;

/**
 * @author Randall Hauch
 *
 */
public class MySqlTaskContextIT extends MySqlTaskContextTest {
	@BeforeMethod(groups = {"test", "context"})
	public void beforeEach() {
        super.beforeEach();
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
	
    @Test(groups = {"context"})
    public void shouldCreateTaskFromConfiguration() throws Exception {
        config = simpleConfig().build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
        context.start();
        assertThat(context.config()).isSameAs(config);

        assertThat(context.getClock()).isNotNull();
        assertThat(context.dbSchema()).isNotNull();
        assertThat(context.getConnectionContext().jdbc()).isNotNull();
        assertThat(context.getConnectionContext().logger()).isNotNull();
        assertThat(context.makeRecord()).isNotNull();
        assertThat(context.source()).isNotNull();
        assertThat(context.topicSelector()).isNotNull();

        assertThat(context.getConnectionContext().hostname()).isEqualTo(dbConnInfo.get("hostname").toString());
        assertThat(context.getConnectionContext().port()).isEqualTo(Integer.parseInt(dbConnInfo.get("port").toString()));
        assertThat(context.getConnectionContext().username()).isEqualTo(dbConnInfo.get("user").toString());
        assertThat(context.getConnectionContext().password()).isEqualTo(dbConnInfo.get("password").toString());
        assertThat(context.serverId()).isEqualTo(serverId);
        assertThat(context.getConnectorConfig().getLogicalName()).isEqualTo(serverName);

        assertThat("" + context.includeSchemaChangeRecords()).isEqualTo(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES.defaultValueAsString());
        assertThat("" + context.includeSqlQuery()).isEqualTo(MySqlConnectorConfig.INCLUDE_SQL_QUERY.defaultValueAsString());
        assertThat("" + context.getConnectorConfig().getMaxBatchSize()).isEqualTo(MySqlConnectorConfig.MAX_BATCH_SIZE.defaultValueAsString());
        assertThat("" + context.getConnectorConfig().getMaxQueueSize()).isEqualTo(MySqlConnectorConfig.MAX_QUEUE_SIZE.defaultValueAsString());
        assertThat("" + context.getConnectorConfig().getPollInterval().toMillis()).isEqualTo(MySqlConnectorConfig.POLL_INTERVAL_MS.defaultValueAsString());
        assertThat("" + context.snapshotMode().getValue()).isEqualTo(MySqlConnectorConfig.SNAPSHOT_MODE.defaultValueAsString());

        // Snapshot default is 'initial' ...
        assertThat(context.isSnapshotAllowedWhenNeeded()).isEqualTo(false);
        assertThat(context.isSnapshotNeverAllowed()).isEqualTo(false);

        // JDBC connection is automatically created by MySqlTaskContext when it reads database variables
        assertConnectedToJdbc();
    }

    @Test(groups = {"context"})
    public void shouldCloseJdbcConnectionOnShutdown() throws Exception {
        config = simpleConfig().build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
        context.start();

        // JDBC connection is automatically created by MySqlTaskContext when it reads database variables
        assertConnectedToJdbc();

        context.shutdown();
        assertNotConnectedToJdbc();
    }

    protected void assertCanConnectToJdbc() throws SQLException {
        AtomicInteger count = new AtomicInteger();
        context.getConnectionContext().jdbc().query("SHOW DATABASES", rs -> {
            while (rs.next()) {
                count.incrementAndGet();
            }
        });
        assertThat(count.get()).isGreaterThan(0);
    }

    protected void assertConnectedToJdbc() throws SQLException {
        assertThat(context.getConnectionContext().jdbc().isConnected()).isTrue();
    }

    protected void assertNotConnectedToJdbc() throws SQLException {
        assertThat(context.getConnectionContext().jdbc().isConnected()).isFalse();
    }
}
