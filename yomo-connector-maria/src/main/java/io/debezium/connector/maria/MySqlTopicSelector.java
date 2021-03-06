/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.maria;

import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * Factory for this connector's {@link TopicSelector}.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public class MySqlTopicSelector {

    /**
     * Get the default topic selector logic, which uses a '.' delimiter character when needed.
     *
     * @param prefix the name of the prefix to be used for all topics; may not be null and must not terminate in the
     *            {@code delimiter}
     * @param heartbeatPrefix the name of the prefix to be used for all heartbeat topics; may not be null and must not terminate in the
     *            {@code delimiter}
     * @return the topic selector; never null
     */
    static TopicSelector<TableId> defaultSelector(String prefix, String heartbeatPrefix) {
        return TopicSelector.defaultSelector(prefix, heartbeatPrefix, ".",
                (t, pref, delimiter) -> String.join(delimiter, pref, t.catalog(), t.table()));
    }
}
