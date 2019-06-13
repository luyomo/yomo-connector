/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.maria;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.debezium.connector.maria.antlr.MySqlAntlrDdlParser;

/**
 * @author laomei
 */
public class MysqlDefaultValueTest extends AbstractMysqlDefaultValueTest {

    {
        parserProducer = (converters) -> {
            return new MySqlAntlrDdlParser(converters);
        };
    }
    
    @BeforeClass(groups = {"test", "defaultValue"})
    public void setUp() {
        super.setUp();
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
