/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.maria;

import org.apache.kafka.connect.errors.ConnectException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ConnectionIT {
	private final UniqueDatabase DATABASE = new UniqueDatabase("readbinlog", "readbinlog_test");
	
	@BeforeClass(groups = {"test"})
	public void setUp() {
		DATABASE.setConnInfo("jdbc");
	}
	
	@AfterClass(groups = {"test"})
	public void afterClass() {
		DATABASE.dropDB();
	}

    @Test(groups = {"base"})
    public void shouldConnectToDefaultDatabase() throws SQLException {
        try (MySQLConnection conn = MySQLConnection.forTestDatabase("mysql", DATABASE.getConnInfo());) {
            conn.connect();
        }
    }

    @Test(groups = {"base"})
    public void shouldDoStuffWithDatabase() throws SQLException {
        DATABASE.createAndInitialize();
        try (MySQLConnection conn = MySQLConnection.forTestDatabase(DATABASE.getDatabaseName(), DATABASE.getConnInfo());) {
            conn.connect();
            // Set up the table as one transaction and wait to see the events ...
            conn.execute("DROP TABLE IF EXISTS person",
                         "CREATE TABLE person ("
                                 + "  name VARCHAR(255) primary key,"
                                 + "  birthdate DATE NULL,"
                                 + "  age INTEGER NULL DEFAULT 10,"
                                 + "  salary DECIMAL(5,2),"
                                 + "  bitStr BIT(18)"
                                 + ")");
            conn.execute("SELECT * FROM person");
            try (ResultSet rs = conn.connection().getMetaData().getColumns("readbinlog_test", null, null, null)) {
                //if ( Testing.Print.isEnabled() ) conn.print(rs);
            }
        }
    }

	@Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = ".*Unknown database.*", groups = {"base"})
    public void shouldConnectToEmptyDatabase() throws SQLException {
        try (MySQLConnection conn = MySQLConnection.forTestDatabase("emptydb",DATABASE.getConnInfo());) {
            conn.connect();
        }
    }
}
