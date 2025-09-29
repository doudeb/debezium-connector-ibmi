/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.db2as400.As400OffsetContext.Loader;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

public class As400SnapshotChangeEventSource
        extends RelationalSnapshotChangeEventSource<As400Partition, As400OffsetContext> {
    private static final Logger log = LoggerFactory.getLogger(As400SnapshotChangeEventSource.class);

    private final As400ConnectorConfig connectorConfig;
    private final As400JdbcConnection jdbcConnection;
    private final As400RpcConnection rpcConnection;
    private final As400DatabaseSchema schema;

    public As400SnapshotChangeEventSource(As400ConnectorConfig connectorConfig, As400RpcConnection rpcConnection,
                                          MainConnectionProvidingConnectionFactory<As400JdbcConnection> jdbcConnectionFactory,
                                          As400DatabaseSchema schema, EventDispatcher<As400Partition, TableId> dispatcher, Clock clock,
                                          SnapshotProgressListener<As400Partition> snapshotProgressListener,
                                          NotificationService<As400Partition, As400OffsetContext> notificationService,
                                          SnapshotterService snapshotterService) {

        super(connectorConfig, jdbcConnectionFactory, schema, dispatcher, clock, snapshotProgressListener,
                notificationService, snapshotterService);

        this.connectorConfig = connectorConfig;
        this.rpcConnection = rpcConnection;
        this.jdbcConnection = jdbcConnectionFactory.mainConnection();
        this.schema = schema;
    }

    @Override
    public SnapshotResult<As400OffsetContext> execute(ChangeEventSourceContext context, As400Partition partition,
                                                      As400OffsetContext previousOffset, SnapshottingTask snapshottingTask)
            throws InterruptedException {
        if (snapshottingTask.shouldSkipSnapshot()) {
            log.info("snapshotting skipped but fetching structure");
            final RelationalSnapshotContext<As400Partition, As400OffsetContext> ctx;
            try {
                ctx = (RelationalSnapshotContext<As400Partition, As400OffsetContext>) prepare(partition, false);
                determineTables(ctx, snapshottingTask);
                readTableStructure(context, ctx, previousOffset, snapshottingTask);
            }
            catch (final Exception e) {
                throw new RuntimeException("Failed to initialize snapshot context.", e);
            }
            log.info("finished fetching structure");
        }
        return super.execute(context, partition, previousOffset, snapshottingTask);
    }

    void determineTables(RelationalSnapshotContext<As400Partition, As400OffsetContext> ctx,
                         SnapshottingTask snapshottingTask)
            throws Exception {
        final Set<TableId> allTableIds = getAllTableIds(ctx);
        final Set<Pattern> dataCollectionsToBeSnapshotted = getDataCollectionPattern(
                snapshottingTask.getDataCollections());

        final Set<TableId> snapshottedTableIds = determineDataCollectionsToBeSnapshotted(allTableIds,
                dataCollectionsToBeSnapshotted)
                .collect(Collectors.toSet());

        final Set<TableId> capturedTables = new HashSet<>();
        final Set<TableId> capturedSchemaTables = new HashSet<>();

        for (final TableId tableId : allTableIds) {
            if (connectorConfig.getTableFilters().eligibleForSchemaDataCollectionFilter().isIncluded(tableId)) {
                capturedSchemaTables.add(tableId);
            }
        }

        for (final TableId tableId : snapshottedTableIds) {
            if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                capturedTables.add(tableId);
            }
        }

        ctx.capturedTables = capturedTables;
        ctx.capturedSchemaTables = capturedSchemaTables.stream().sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext)
            throws Exception {
        final Set<TableId> tables = jdbcConnection.readTableNames(jdbcConnection.getRealDatabaseName(),
                connectorConfig.getSchema(), null, new String[]{ "TABLE" });
        return tables;
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext)
            throws Exception {
        // TODO lock tables
    }

    @Override
    protected void determineSnapshotOffset(
                                           RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext,
                                           As400OffsetContext previousOffset)
            throws Exception {
        if (previousOffset != null && previousOffset.isPositionSet() && !snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot()) {
            snapshotContext.offset = previousOffset;
        }
        else {
            final Instant now = Instant.now();
            final JournalPosition position = rpcConnection.getCurrentPosition();
            // set last entry to processed, so we don't process it again
            final JournalProcessedPosition processedPos = new JournalProcessedPosition(position, now, true);
            snapshotContext.offset = new As400OffsetContext(connectorConfig, processedPos);
        }
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext,
                                      As400OffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws Exception {
        final Set<String> schemas = snapshotContext.capturedTables.stream().map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of
        // captured tables;
        // while the passed table name filter alone would skip all non-included tables,
        // reading the schema
        // would take much longer that way
        for (final String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            log.info("Reading structure of schema '{}'", schema);

            jdbcConnection.readSchema(snapshotContext.tables, // use snapshotContext.capturedSchemaTables?
                    jdbcConnection.getRealDatabaseName(), schema,
                    connectorConfig.getTableFilters().eligibleDataCollectionFilter(), null, false);

            try {
                jdbcConnection.getAllSystemNames(schema);
            }
            catch (final Exception e) {
                log.warn("failure fetching table names", e);
            }
        }

        for (final TableId id : snapshotContext.capturedTables) {
            final Table table = snapshotContext.tables.forTable(id);
            if (table == null) {
                log.error("table schema not found for {}", id, new Exception("missing table definition"));
            }
            else {
                schema.addSchema(table);
            }
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(
                                              RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext)
            throws Exception {
        // TODO unlock tables
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(
                                                    RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext, Table table) {
        return SchemaChangeEvent.of(SchemaChangeEventType.CREATE, snapshotContext.partition, snapshotContext.offset,
                snapshotContext.catalogName, table.id().schema(), null, table, true);
    }

    @Override
    protected Optional<String> getSnapshotSelect(
                                                 RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext, TableId tableId,
                                                 List<String> columns) {
        if (connectorConfig.isIncludeRrnInSource()) {
            return Optional.of(String.format("SELECT RRN(%s) AS RRN, %s.* FROM %s.%s %s",
                    tableId.table(), tableId.table(), tableId.schema(), tableId.table(), tableId.table()));
        }
        else {
            return Optional.of(String.format("SELECT * FROM %s.%s", tableId.schema(), tableId.table()));
        }
    }

    @Override
    public SnapshottingTask getSnapshottingTask(As400Partition partition, As400OffsetContext previousOffset) {
        final List<String> dataCollectionsToBeSnapshotted = connectorConfig.getDataCollectionsToBeSnapshotted();
        final Map<DataCollectionId, String> snapshotSelectOverridesByTable = connectorConfig.getSnapshotSelectOverridesByTable();

        // found a previous offset and the earlier snapshot has completed
        if (previousOffset != null && previousOffset.isSnapshotComplete()) {
            // when control tables in place
            if (!previousOffset.hasNewTables()) {
                log.info(
                        "A previous offset indicating a completed snapshot has been found. Neither schema nor data will be snapshotted.");
                return new SnapshottingTask(false, false, dataCollectionsToBeSnapshotted,
                        snapshotSelectOverridesByTable, false);
            }
        }

        log.info("No previous offset has been found");
        if (this.connectorConfig.getSnapshotMode().includeData()) {
            log.info("According to the connector configuration both schema and data will be snapshotted");
        }
        else {
            log.info("According to the connector configuration only schema will be snapshotted");
        }

        return new SnapshottingTask(this.connectorConfig.getSnapshotMode().includeData(),
                this.connectorConfig.getSnapshotMode().includeData(), dataCollectionsToBeSnapshotted,
                snapshotSelectOverridesByTable, false);
    }

    @Override
    protected SnapshotContext<As400Partition, As400OffsetContext> prepare(As400Partition partition, boolean onDemand) throws Exception {
        return new RelationalSnapshotContext<>(partition, jdbcConnection.getRealDatabaseName(), onDemand);
    }

    @Override
    protected As400OffsetContext copyOffset(
                                            RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }

    @Override
    protected void doCreateDataEventsForTable(ChangeEventSourceContext sourceContext,
                                              RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext,
                                              As400OffsetContext offsetContext,
                                              SnapshotReceiver<As400Partition> snapshotReceiver,
                                              Table table, boolean generateBinlogEvent, boolean selectAllOp,
                                              int rowCount, int readCount, String sqlStatement,
                                              OptionalLong totalCount,
                                              Set<TableId> tableIds,
                                              JdbcConnection jdbcConnection)
            throws InterruptedException, SQLException {

        // If RRN is not included or it's not our special RRN-enabled query, use default processing
        if (!connectorConfig.isIncludeRrnInSource() ||
                !sqlStatement.contains("SELECT RRN(")) {
            super.doCreateDataEventsForTable(sourceContext, snapshotContext, offsetContext,
                    snapshotReceiver, table, generateBinlogEvent, selectAllOp, rowCount, readCount,
                    sqlStatement, totalCount, tableIds, jdbcConnection);
            return;
        }

        log.info("Processing snapshot data for table {} with RRN extraction", table.id());

        try (Statement statement = jdbcConnection.connection().createStatement();
                ResultSet rs = resultSetForDataEvents(sqlStatement, statement)) {
            long rowsProcessed = 0;

            while (rs.next() && !sourceContext.isRunning() == false) {
                // Extract RRN value from first column
                Long rrnValue = null;
                try {
                    Object rrnObj = rs.getObject(1); // RRN is first column in our SELECT
                    if (rrnObj != null) {
                        rrnValue = ((Number) rrnObj).longValue();
                    }
                }
                catch (Exception e) {
                    log.warn("Failed to extract RRN value for table {}: {}", table.id(), e.getMessage());
                }

                // Create a new offset context for this row and set the RRN
                As400OffsetContext rowOffsetContext = copyOffset(snapshotContext);
                if (rrnValue != null) {
                    rowOffsetContext.setRelativeRecordNumber(rrnValue);
                }

                // Create the change record with custom data handling
                // We need to skip the RRN column (first column) when creating the change record
                try {
                    // Get column values starting from column 2 (skipping RRN)
                    Object[] columnValues = new Object[table.columns().size()];
                    for (int i = 0; i < table.columns().size(); i++) {
                        columnValues[i] = rs.getObject(i + 2); // +2 because RRN is column 1, and JDBC is 1-indexed
                    }

                    // Create and emit the change record using the standard Debezium approach
                    As400ChangeRecordEmitter changeRecordEmitter = new As400ChangeRecordEmitter(
                            snapshotContext.partition,
                            rowOffsetContext,
                            io.debezium.data.Envelope.Operation.READ,
                            null, // old values
                            columnValues, // new values
                            clock,
                            connectorConfig);

                    // Use the standard change record emitter approach - need table schema
                    changeRecordEmitter.emitChangeRecords(schema.schemaFor(table.id()), snapshotReceiver);

                    rowsProcessed++;
                    if (rowsProcessed % 1000 == 0) {
                        log.info("Processed {} rows for table {}", rowsProcessed, table.id());
                    }

                }
                catch (Exception e) {
                    log.error("Failed to process row {} for table {}: {}", rowsProcessed, table.id(), e.getMessage(), e);
                    throw new SQLException("Failed to process snapshot row", e);
                }
            }

            log.info("Completed snapshot processing for table {} with {} rows and RRN extraction",
                    table.id(), rowsProcessed);

        }
        catch (SQLException e) {
            log.error("Error processing snapshot data for table {}: {}", table.id(), e.getMessage(), e);
            throw e;
        }
    }
}
