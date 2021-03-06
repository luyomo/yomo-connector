/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.maria;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.fest.assertions.Assertions.assertThat;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import io.debezium.connector.maria.antlr.MySqlAntlrDdlParser;
import io.debezium.doc.FixFor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * @author Jiri Pechanec <jpechane@redhat.com>
 */
public class MysqlAntlrDefaultValueTest extends AbstractMysqlDefaultValueTest {

    {
        parserProducer = MySqlAntlrDdlParser::new;
    }
    
    @BeforeClass(groups = {"test", "defaultValue"})
    public void setUp() {
        super.setUp();
    }

    @Test(groups = {"defaultValue"})
    @FixFor("DBZ-870")
    public void shouldAcceptZeroAsDefaultValueForDateColumn() {
        String ddl = "CREATE TABLE data(id INT, nullable_date date default 0, not_nullable_date date not null default 0, PRIMARY KEY (id))";
        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "data"));

        assertThat(table.columnWithName("nullable_date").hasDefaultValue()).isTrue();

        // zero date should be mapped to null for nullable column
        assertThat(table.columnWithName("nullable_date").defaultValue()).isNull();

        assertThat(table.columnWithName("not_nullable_date").hasDefaultValue()).isTrue();

        // zero date should be mapped to epoch for non-nullable column (expecting Date, as this test is using "connect"
        // mode)
        assertThat(table.columnWithName("not_nullable_date").defaultValue()).isEqualTo(getEpochDate());
    }

    private Date getEpochDate() {
        return Date.from(LocalDate.of(1970, 1, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    }

    @Test(groups = {"defaultValue"})
    @FixFor("DBZ-1204")
    public void shouldAcceptBooleanAsDefaultValue() {
        String ddl = "CREATE TABLE data(id INT, "
                        + "bval BOOLEAN DEFAULT TRUE, "
                        + "tival1 TINYINT(1) DEFAULT FALSE, "
                        + "tival2 TINYINT(1) DEFAULT 3, "
                        + "tival3 TINYINT(2) DEFAULT TRUE, "
                        + "tival4 TINYINT(2) DEFAULT 18, "
                        + "PRIMARY KEY (id))";

        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "data"));

        assertThat((Boolean) table.columnWithName("bval").defaultValue()).isTrue();
        assertThat((Short) table.columnWithName("tival1").defaultValue()).isZero();
        assertThat((Short) table.columnWithName("tival2").defaultValue()).isEqualTo((short) 3);
        assertThat((Short) table.columnWithName("tival3").defaultValue()).isEqualTo((short) 1);
        assertThat((Short) table.columnWithName("tival4").defaultValue()).isEqualTo((short) 18);
    }

    @Test(groups = {"defaultValue"})
    @FixFor("DBZ-1249")
    public void shouldAcceptBitSetDefaultValue() {
        String ddl = "CREATE TABLE user_subscribe (id bigint(20) unsigned NOT NULL AUTO_INCREMENT, content bit(24) DEFAULT b'111111111111101100001110', PRIMARY KEY (id)) ENGINE=InnoDB";

        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "user_subscribe"));

        final byte[] defVal = (byte[]) table.columnWithName("content").defaultValue();
        assertThat(Byte.toUnsignedInt((defVal[0]))).isEqualTo(0b00001110);
        assertThat(Byte.toUnsignedInt((defVal[1]))).isEqualTo(0b11111011);
        assertThat(Byte.toUnsignedInt((defVal[2]))).isEqualTo(0b11111111);
    }
    
    @Test(groups = {"defaultValue"})
    public void parseUnsignedTinyintDefaultValue() {
    	super.parseUnsignedTinyintDefaultValue();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseUnsignedSmallintDefaultValue() {
        super.parseUnsignedSmallintDefaultValue();	
    }
    
    @Test(groups = {"defaultValue"})
    public void parseUnsignedMediumintDefaultValue() {
    	super.parseUnsignedMediumintDefaultValue();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseUnsignedIntDefaultValue() {
    	super.parseUnsignedIntDefaultValue();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseUnsignedBigIntDefaultValueToLong() {
    	super.parseUnsignedBigIntDefaultValueToLong();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseUnsignedBigIntDefaultValueToBigDecimal() {
    	super.parseUnsignedBigIntDefaultValueToBigDecimal();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseStringDefaultValue() {
    	super.parseStringDefaultValue();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseBitDefaultValue() {
    	super.parseBitDefaultValue();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseBooleanDefaultValue() {
    	super.parseBooleanDefaultValue();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseNumberDefaultValue() {
    	super.parseNumberDefaultValue();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseRealDefaultValue() {
    	super.parseRealDefaultValue();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseNumericAndDecimalToDoubleDefaultValue() {
    	super.parseNumericAndDecimalToDoubleDefaultValue();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseNumericAndDecimalToDecimalDefaultValue() {
    	super.parseNumericAndDecimalToDecimalDefaultValue();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseTimeDefaultValue() {
    	super.parseTimeDefaultValue();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseDateDefaultValue() {
    	super.parseDateDefaultValue();
    }
    
    @Test(groups = {"defaultValue"})
    public void parseAlterTableTruncatedDefaulDateTime() {
    	super.parseAlterTableTruncatedDefaulDateTime();
    }
}
