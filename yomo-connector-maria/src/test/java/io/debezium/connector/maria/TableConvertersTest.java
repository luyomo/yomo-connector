/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.maria;

import org.testng.annotations.Test;
import org.junit.Ignore;
import org.testng.Assert;

/**
 * @author Randall Hauch
 */
@Ignore
public class TableConvertersTest {

    @Test(enabled=false)
    public void shouldHandleMetadataEventToUpdateTables() {
        Assert.fail("Not yet implemented");
    }

    @Test(enabled=false)
    public void shouldProduceSourceRecorForMetadataEventWhenConfigured() {
        Assert.fail("Not yet implemented");
    }

    @Test(enabled=false)
    public void shouldProduceSourceRecorForInsertEvent() {
        Assert.fail("Not yet implemented");
    }

    @Test(enabled=false)
    public void shouldProduceSourceRecorForUpdateEvent() {
        Assert.fail("Not yet implemented");
    }

    @Test(enabled=false)
    public void shouldProduceSourceRecorForDeleteEvent() {
        Assert.fail("Not yet implemented");
    }

}
