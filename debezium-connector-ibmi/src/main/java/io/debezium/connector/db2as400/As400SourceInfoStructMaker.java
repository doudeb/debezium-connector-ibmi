/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

public class As400SourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;
    private final As400ConnectorConfig connectorConfig;

    public As400SourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        init(connector, version, connectorConfig);
        this.connectorConfig = (As400ConnectorConfig) connectorConfig;

        var schemaBuilder = commonSchemaBuilder()
                .name("io.debezium.connector.db2as400.Source");

        // Check if we should include RRN field in source schema
        boolean includeRrn = false;
        try {
            includeRrn = this.connectorConfig != null && this.connectorConfig.isIncludeRrnInSource();
        }
        catch (Exception e) {
            // Fall back to false if there's any issue reading the config
            includeRrn = false;
        }

        if (includeRrn) {
            schemaBuilder.field(SourceInfo.RRN_KEY, Schema.OPTIONAL_INT64_SCHEMA);
        }

        // TODO add in table info
        // .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
        // .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
        // TODO add in offset

        schema = schemaBuilder.build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct ret = super.commonStruct(sourceInfo);

        // Only add RRN if the schema actually contains the RRN field AND we have a value
        if (schema.field(SourceInfo.RRN_KEY) != null && sourceInfo.getRelativeRecordNumber() != null) {
            ret.put(SourceInfo.RRN_KEY, sourceInfo.getRelativeRecordNumber());
        }

        // .put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.getTableId().schema())
        // .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.getTableId().table());

        return ret;
    }
}
