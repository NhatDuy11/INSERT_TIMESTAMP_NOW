error id: file:///E:/INSERT_TIMESTAMP_NOW/src/main/java/com/github/kafka/connect/smt/InsertSinkTimestamp.java:java/time/ZoneOffset#
file:///E:/INSERT_TIMESTAMP_NOW/src/main/java/com/github/kafka/connect/smt/InsertSinkTimestamp.java
empty definition using pc, found symbol in pc: java/time/ZoneOffset#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 3421
uri: file:///E:/INSERT_TIMESTAMP_NOW/src/main/java/com/github/kafka/connect/smt/InsertSinkTimestamp.java
text:
```scala
package com.github.kafka.connect.smt;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Kafka Connect SMT — <b>InsertSinkTimestamp</b>
 *
 * <p>
 * Appends a new field to every record that holds the <b>wall-clock UTC
 * instant</b>
 * at which the Sink Connector processed the record.
 *
 * <h2>Output type per mode</h2>
 * <ul>
 * <li><b>Schema-ful (Struct)</b>: field is a Kafka Connect {@link Timestamp}
 * logical type
 * ({@code org.apache.kafka.connect.data.Timestamp}), which maps to a native
 * {@code TIMESTAMP} column in Apache Iceberg / Dremio — no string casting
 * needed.</li>
 * <li><b>Schemaless (Map)</b>: field is a {@link String} formatted as
 * {@code yyyy-MM-dd HH:mm:ss.SSS} (UTC). This produces a human-readable,
 * SQL-friendly timestamp that most downstream sinks can parse directly.</li>
 * </ul>
 *
 * <h2>Schema cache</h2>
 * <p>
 * Evolved schemas (original schema + new timestamp field) are cached in a
 * high-throughput, lock-free Caffeine LRU cache
 * (default capacity = {@value #SCHEMA_CACHE_CAPACITY}).
 * Caffeine uses an efficient W-TinyLFU eviction policy and is safe for
 * concurrent access from multiple Connector task threads without external
 * locking.
 * Call {@link #clearSchemaCache()} to flush explicitly if needed.
 *
 * <h2>Configuration</h2>
 *
 * <pre>
 *   transforms=addSinkTs
 *   transforms.addSinkTs.type=com.github.kafka.connect.smt.InsertSinkTimestamp$Value
 *   transforms.addSinkTs.sink.timestamp.field=sink_timestamp
 * </pre>
 *
 * <h2>Inner classes</h2>
 * <ul>
 * <li>{@link Value} — transforms the record <em>value</em> (most common)</li>
 * <li>{@link Key} — transforms the record <em>key</em></li>
 * </ul>
 *
 * @param <R> {@link ConnectRecord} subtype
 */
public abstract class InsertSinkTimestamp<R extends ConnectRecord<R>>
        implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(InsertSinkTimestamp.class);

    // -----------------------------------------------------------------------
    // Config keys & defaults
    // -----------------------------------------------------------------------

    static final String FIELD_CONFIG = "sink.timestamp.field";

    /** Maximum number of distinct source schemas to keep in the Caffeine cache. */
    static final int SCHEMA_CACHE_CAPACITY = 64;

    /**
     * Timestamp formatter for schemaless (Map) records.
     * Produces strings like {@code 2026-03-12 14:05:28.123} (UTC).
     * {@link DateTimeFormatter} is immutable and thread-safe.
     */
    private static final DateTimeFormatter SINK_TS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneOffs@@et.UTC);

    // -----------------------------------------------------------------------
    // Config definition
    // -----------------------------------------------------------------------

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Name of the field to insert.\n"
                            + "  - Schema-ful records: field type = Kafka Connect Timestamp logical type "
                            + "(UTC epoch-ms → Iceberg TIMESTAMP).\n"
                            + "  - Schemaless records: field type = String, "
                            + "formatted as 'yyyy-MM-dd HH:mm:ss.SSS' (UTC).");

    // -----------------------------------------------------------------------
    // Runtime state
    // -----------------------------------------------------------------------

    /** Configured field name. */
    private String fieldName;

    /**
     * Caffeine LRU cache: maps the <em>original</em> source {@link Schema} to the
     * evolved {@link Schema} that includes the new timestamp field.
     *
     * <p>
     * Bounded by {@link #SCHEMA_CACHE_CAPACITY} entries. Caffeine's internal
     * implementation is non-blocking and highly concurrent — no external
     * {@code synchronized} blocks needed.
     *
     * <p>
     * Using {@link Schema} reference equality as key is safe here because
     * Kafka Connect's converters reuse the same Schema object for
     * consecutive records of the same topic/version.
     */
    private Cache<Schema, Schema> schemaCache;

    // -----------------------------------------------------------------------
    // Transformation lifecycle
    // -----------------------------------------------------------------------

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(FIELD_CONFIG);

        schemaCache = Caffeine.newBuilder()
                .maximumSize(SCHEMA_CACHE_CAPACITY)
                .recordStats() // enables hit/miss metrics (useful for monitoring)
                .build();

        log.info("[InsertSinkTimestamp] configured — field='{}', schema-cache-capacity={}",
                fieldName, SCHEMA_CACHE_CAPACITY);
    }

    @Override
    public R apply(R record) {
        // Best practice: tombstone records (null value/key) must be passed through
        // unchanged. Stamping a timestamp on a tombstone would corrupt the
        // compaction semantics and could cause NullPointerExceptions downstream.
        if (operatingValue(record) == null) {
            log.debug("[InsertSinkTimestamp] tombstone record on topic '{}' — passing through unchanged",
                    record.topic());
            return record;
        }

        // Capture wall-clock instant as early as possible after null-guard
        final Instant sinkInstant = Instant.now();

        if (operatingSchema(record) == null) {
            return applySchemaless(record, sinkInstant);
        } else {
            return applyWithSchema(record, Date.from(sinkInstant));
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        clearSchemaCache();
    }

    // -----------------------------------------------------------------------
    // Public utility
    // -----------------------------------------------------------------------

    /**
     * Clears the internal Caffeine schema cache.
     * Call this if the upstream schema changes and you want to free cached entries
     * immediately rather than waiting for automatic eviction.
     */
    public void clearSchemaCache() {
        if (schemaCache != null) {
            schemaCache.invalidateAll();
            log.debug("[InsertSinkTimestamp] schema cache invalidated");
        }
    }

    // -----------------------------------------------------------------------
    // Abstract hooks — implemented by Key / Value inner classes
    // -----------------------------------------------------------------------

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /**
     * Handles <b>schema-ful</b> (Struct) records.
     *
     * <p>
     * The evolved schema — original fields + new optional Timestamp field — is
     * looked up
     * from the Caffeine cache keyed by the original {@link Schema} reference.
     * If absent, it is built and inserted automatically by
     * {@code get(key, loader)}.
     *
     * <h3>Bug guards</h3>
     * <ul>
     * <li><b>ClassCastException guard</b>: verifies the value is actually a
     * {@link Struct}
     * before casting. Non-Struct schema-ful records (unusual but possible with some
     * connectors) are passed through unchanged.</li>
     * <li><b>Idempotency guard</b>: when copying original fields into the new
     * Struct, the
     * {@code fieldName} field is skipped. This prevents a {@code DataException} if
     * the
     * SMT is applied twice (SMT chain or topic re-read): the original value's type
     * may
     * differ from the Timestamp type we defined in the evolved schema.</li>
     * </ul>
     */
    private R applyWithSchema(R record, Date sinkInstant) {
        final Schema originalSchema = operatingSchema(record);
        final Object rawValue = operatingValue(record);

        // Bug fix #1 — ClassCastException guard
        // Schema is non-null but value might not be a Struct (e.g. some rare connectors
        // emit a schema alongside a Map or primitive). Pass through safely.
        if (!(rawValue instanceof Struct)) {
            log.warn("[InsertSinkTimestamp] schema-ful value on topic '{}' is not a Struct (actual: {}) "
                    + "— passing through unchanged",
                    record.topic(),
                    rawValue == null ? "null" : rawValue.getClass().getName());
            return record;
        }
        final Struct originalStruct = (Struct) rawValue;

        // Warn if fieldName collides with an existing field (at most once per schema
        // version
        // because the schema object is cached and this check happens only on cache
        // miss)
        if (originalSchema.field(fieldName) != null) {
            log.warn("[InsertSinkTimestamp] field '{}' already exists in schema '{}' — "
                    + "overwriting with sink timestamp. Consider renaming via sink.timestamp.field.",
                    fieldName, originalSchema.name());
        }

        // Caffeine.get(key, loader) is atomic and non-blocking for concurrent callers
        // with different keys. For the same key, Caffeine serialises the loader call
        // to avoid duplicate schema builds (similar to
        // ConcurrentHashMap.computeIfAbsent).
        final Schema newSchema = schemaCache.get(originalSchema, this::makeUpdatedSchema);

        // Populate new Struct
        final Struct newStruct = new Struct(newSchema);
        for (Field field : originalSchema.fields()) {
            // Bug fix #2 — Idempotency / DataException guard
            // Skip fieldName in the copy loop. If this SMT was applied previously
            // (e.g. SMT chain or topic re-read), the original schema may already
            // contain fieldName with a type incompatible with our Timestamp definition.
            // Copying that mismatched value into the new Struct would throw DataException.
            if (!field.name().equals(fieldName)) {
                newStruct.put(field.name(), originalStruct.get(field));
            }
        }
        // Always write the fresh sink timestamp
        newStruct.put(fieldName, sinkInstant);

        return newRecord(record, newSchema, newStruct);
    }

    /**
     * Handles <b>schemaless</b> (Map) records.
     *
     * <p>
     * Inserts the sink timestamp as a <b>formatted String</b>
     * ({@code yyyy-MM-dd HH:mm:ss.SSS}, UTC) under the configured field name.
     * A String is chosen instead of a raw {@link Date} because schemaless JSON
     * converters would otherwise serialize {@code Date} as an opaque
     * epoch-millisecond
     * long value, which is not human-readable and harder to index in downstream
     * systems.
     *
     * <p>
     * The result map is a {@link LinkedHashMap} to preserve field insertion order,
     * which matters for JDBC/Iceberg sinks that may map columns positionally.
     *
     * <p>
     * If the operating value is not a {@link Map} (e.g. a raw primitive or array
     * from an unusual connector), the record is passed through unchanged with a
     * warning.
     */
    @SuppressWarnings("unchecked")
    private R applySchemaless(R record, Instant sinkInstant) {
        final Object rawValue = operatingValue(record);

        // Defensive type check — schemaless records should carry a Map, but
        // some connectors may produce other types. Pass through to avoid
        // ClassCastException.
        if (!(rawValue instanceof Map)) {
            log.warn("[InsertSinkTimestamp] schemaless value on topic '{}' is not a Map (actual: {}) "
                    + "— passing through unchanged",
                    record.topic(),
                    rawValue == null ? "null" : rawValue.getClass().getName());
            return record;
        }

        final Map<String, Object> original = (Map<String, Object>) rawValue;

        // LinkedHashMap preserves insertion order, important for positional sink
        // mappings
        final Map<String, Object> updated = new LinkedHashMap<>(original);

        // Format timestamp as human-readable UTC string, e.g. "2026-03-12 14:05:28.123"
        updated.put(fieldName, SINK_TS_FORMATTER.format(sinkInstant));

        return newRecord(record, null, updated);
    }

    /**
     * Builds a new evolved {@link Schema} by copying every field from the
     * original schema and appending a new <b>optional</b> {@link Timestamp}
     * logical-type field at the end.
     *
     * <p>
     * Preservation strategy:
     * <ul>
     * <li><b>name, doc, version</b>: copied verbatim so downstream schema
     * registries
     * and JDBC sinks continue to recognise the schema identity.</li>
     * <li><b>parameters</b>: copied in full. {@link Schema#parameters()} holds
     * critical
     * metadata injected by Debezium (e.g. column type hints like
     * {@code __debezium.source.column.type=VARCHAR}) and Confluent Schema Registry
     * (e.g. {@code io.confluent.connect.avro.Avro.ConnectDefault}). Dropping these
     * parameters can prevent the sink connector from correctly mapping target
     * columns.
     * </li>
     * <li><b>fieldName collision</b>: if the original schema already contains a
     * field
     * with {@code fieldName}, it is skipped and replaced by our fresh Timestamp
     * definition — preventing duplicate-field errors and DataExceptions on
     * idempotent re-application of the SMT.</li>
     * </ul>
     *
     * <p>
     * The timestamp field is marked {@code optional()} for backwards-compatibility:
     * records that arrived before the SMT was engaged still validate correctly.
     */
    private Schema makeUpdatedSchema(Schema originalSchema) {
        final SchemaBuilder builder = SchemaBuilder.struct();

        if (originalSchema.name() != null)
            builder.name(originalSchema.name());
        if (originalSchema.doc() != null)
            builder.doc(originalSchema.doc());
        if (originalSchema.version() != null)
            builder.version(originalSchema.version());

        // Bug fix #3 — Preserve schema parameters (Debezium type hints, Avro defaults,
        // etc.)
        // Dropping parameters can break column mapping in JDBC / Iceberg sinks.
        if (originalSchema.parameters() != null) {
            builder.parameters(originalSchema.parameters());
        }

        // Copy all existing fields (skip fieldName if it already exists — we'll
        // redefine it
        // below to ensure type is always Timestamp regardless of the original type).
        for (Field field : originalSchema.fields()) {
            if (!field.name().equals(fieldName)) {
                builder.field(field.name(), field.schema());
            }
        }

        // Append the Kafka Connect Timestamp logical type (→ Iceberg TIMESTAMP)
        // optional() = backwards-compatible with pre-SMT records
        builder.field(fieldName, Timestamp.builder().optional().build());

        Schema evolved = builder.build();
        log.debug("[InsertSinkTimestamp] built evolved schema '{}' with field '{}'",
                evolved.name(), fieldName);
        return evolved;
    }

    // =========================================================================
    // Public inner classes — Value and Key variants
    // =========================================================================

    /**
     * Transforms the record <b>value</b>.
     *
     * <pre>
     * transforms.addSinkTs.type = com.github.kafka.connect.smt.InsertSinkTimestamp$Value
     * </pre>
     */
    public static final class Value<R extends ConnectRecord<R>>
            extends InsertSinkTimestamp<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    updatedSchema,
                    updatedValue,
                    record.timestamp());
        }
    }

    /**
     * Transforms the record <b>key</b>.
     *
     * <pre>
     * transforms.addSinkTs.type = com.github.kafka.connect.smt.InsertSinkTimestamp$Key
     * </pre>
     */
    public static final class Key<R extends ConnectRecord<R>>
            extends InsertSinkTimestamp<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    updatedSchema,
                    updatedValue,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp());
        }
    }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: java/time/ZoneOffset#