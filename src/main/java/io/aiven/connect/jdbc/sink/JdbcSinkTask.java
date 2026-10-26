/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.connect.jdbc.sink;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.dialect.DatabaseDialects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(JdbcSinkTask.class);

    // SQLSTATEs meaning the database rejected the content of a record: data exception (class 22),
    // integrity constraint violation (class 23) and datatype mismatch (42804).
    private static final List<String> INVALID_DATA_SQLSTATE_PREFIXES = List.of("22", "23", "42804");

    DatabaseDialect dialect;
    JdbcSinkConfig config;
    JdbcDbWriter writer;
    int remainingRetries;
    // Null when the connector has neither a dead letter queue nor error logging configured.
    ErrantRecordReporter reporter;

    @Override
    public void start(final Map<String, String> props) {
        log.info("Starting JDBC Sink task");
        config = new JdbcSinkConfig(props);
        initWriter();
        remainingRetries = config.maxRetries;
        reporter = errantRecordReporter();
    }

    private ErrantRecordReporter errantRecordReporter() {
        try {
            return context.errantRecordReporter();
        } catch (final NoSuchMethodError | NoClassDefFoundError e) {
            log.warn("The Connect runtime predates Kafka 2.6 and cannot report errant records; "
                + "records that fail to write will fail the task");
            return null;
        }
    }

    void initWriter() {
        final String dialectName = config.getDialectName();
        if (dialectName != null && !dialectName.trim().isEmpty()) {
            dialect = DatabaseDialects.create(dialectName, config);
        } else {
            final String connectionUrl = config.getConnectionUrl();
            dialect = DatabaseDialects.findBestFor(connectionUrl, config);
        }
        final DbStructure dbStructure = new DbStructure(dialect);
        log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
        writer = new JdbcDbWriter(config, dialect, dbStructure);
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        log.debug(
            "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
                + "database...",
            recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );
        try {
            writer.write(records);
        } catch (final SQLException sqle) {
            log.warn(
                "Write of {} records failed, remainingRetries={}",
                records.size(),
                remainingRetries,
                sqle
            );
            String sqleAllMessages = "";
            for (final Throwable e : sqle) {
                sqleAllMessages += e + System.lineSeparator();
            }
            if (remainingRetries == 0) {
                if (reporter == null) {
                    throw new ConnectException(new SQLException(sqleAllMessages));
                }
                writeOneByOneReportingFailures(records);
            } else {
                writer.closeQuietly();
                initWriter();
                remainingRetries--;
                context.timeout(config.retryBackoffMs);
                throw new RetriableException(new SQLException(sqleAllMessages));
            }
        }
        remainingRetries = config.maxRetries;
    }

    /**
     * Writes the records one at a time and hands each one the database rejects as invalid data
     * to the errant record reporter, so a single bad record no longer fails the whole task. The
     * reporter applies {@code errors.tolerance}: with {@code none} it still fails the task. Any
     * other failure, such as a permission error or the database being unreachable, fails the
     * task instead of being reported. The writer rolls a rejected record back, so the next
     * record reuses the same connection.
     *
     * <p>A table change that makes every insert fail as invalid data, such as a column whose
     * type changed, sends every record to the dead letter queue while the task keeps running:
     * the SQLSTATE cannot tell a bad record from a changed table.
     */
    private void writeOneByOneReportingFailures(final Collection<SinkRecord> records) {
        writer.closeQuietly();
        initWriter();
        for (final SinkRecord record : records) {
            try {
                writer.write(Collections.singletonList(record));
            } catch (final SQLException sqle) {
                if (!isInvalidData(sqle)) {
                    throw new ConnectException(sqle);
                }
                log.warn("Reporting record {}-{}-{} as errant: {}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset(), sqle.getMessage());
                reporter.report(record, sqle);
            }
        }
    }

    /**
     * Whether the database rejected the content of the record rather than failing for a reason
     * that would affect any record. The SQLSTATE may sit on any exception of the chain, for
     * example on the one chained to a {@link java.sql.BatchUpdateException}.
     */
    private static boolean isInvalidData(final SQLException sqle) {
        for (final Throwable e : sqle) {
            if (e instanceof SQLException) {
                final String sqlState = ((SQLException) e).getSQLState();
                if (sqlState != null && INVALID_DATA_SQLSTATE_PREFIXES.stream().anyMatch(sqlState::startsWith)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> map) {
        // Not necessary
    }

    public void stop() {
        log.info("Stopping task");
        try {
            writer.closeQuietly();
        } finally {
            try {
                if (dialect != null) {
                    dialect.close();
                }
            } catch (final Throwable t) {
                log.warn("Error while closing the {} dialect: ", dialect.name(), t);
            } finally {
                dialect = null;
            }
        }
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

}
