/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.maria.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used together with the {@link SkipTestForLegacyParser} JUnit rule, that allows
 * tests to be skipped if the legacy parser is used.
 */
@Retention( RetentionPolicy.RUNTIME)
@Target( {ElementType.METHOD, ElementType.TYPE})
public @interface SkipForLegacyParser {
}
