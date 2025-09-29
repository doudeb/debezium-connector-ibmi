/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

public class As400ConnectorIT extends AbstractAsyncEngineConnectorTest {

    private static final String TABLE = "TESTT";

    @Before
    public void before() throws SQLException {
        initializeConnectorTestFramework();
        TestHelper.testConnection().execute(
                "DELETE FROM " + TABLE,
                "INSERT INTO " + TABLE + " VALUES (1, 'first')");
    }

    @Test
    public void shouldSnapshotAndStream() throws Exception {
        Testing.Print.enable();
        final var config = TestHelper.defaultConfig(TABLE);

        start(As400RpcConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        var records = consumeRecordsByTopic(1);

        TestHelper.testConnection().execute(
                "INSERT INTO " + TABLE + " VALUES (2, 'second')",
                "INSERT INTO " + TABLE + " VALUES (3, 'third')");

        records = consumeRecordsByTopic(2);

        assertNoRecordsToConsume();
        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    public void shouldIncludeRrnInSource() throws Exception {
        Testing.Print.enable();
        final Configuration config = Configuration.copy(TestHelper.defaultConfig(TABLE))
                .with(As400ConnectorConfig.INCLUDE_RRN_IN_SOURCE, true)
                .build();

        start(As400RpcConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        var records = consumeRecordsByTopic(1);
        assertEquals(1, records.allRecordsInOrder().size());

        final SourceRecord snapshotRecord = records.allRecordsInOrder().get(0);
        final Struct value = (Struct) snapshotRecord.value();
        final Struct source = value.getStruct("source");
        assertNotNull("RRN should be present in snapshot record", source.get("rrn"));

        TestHelper.testConnection().execute(
                "INSERT INTO " + TABLE + " VALUES (2, 'second')");

        records = consumeRecordsByTopic(1);
        assertEquals(1, records.allRecordsInOrder().size());
        final SourceRecord streamingRecord = records.allRecordsInOrder().get(0);
        final Struct streamingValue = (Struct) streamingRecord.value();
        final Struct streamingSource = streamingValue.getStruct("source");
        assertNotNull("RRN should be present in streaming record", streamingSource.get("rrn"));

        assertNoRecordsToConsume();
        stopConnector();
        assertConnectorNotRunning();
    }
}
