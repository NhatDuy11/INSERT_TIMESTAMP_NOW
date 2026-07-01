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
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Kafka Connect SMT — <b>InsertSinkTimestamp</b>
 *
 * <p>
 * Appends a new field to every record that holds the <b>wall-clock UTC instant</b>
 * at which the Sink Connector processed the record.
 *
 * <h2>Output type per mode</h2>
 * <ul>
 * <li><b>Schema-ful (Struct)</b>: field is a Kafka Connect {@link Timestamp} logical type
 * ({@code org.apache.kafka.connect.data.Timestamp}), which maps to a native
 * {@code TIMESTAMP} column in Apache Iceberg / Dremio — no string casting needed.</li>
 * <li><b>Schemaless (Map)</b>: field is a raw {@code long} epoch-millis (UTC).
 * A numeric epoch is emitted rather than a formatted string so downstream sinks
 * receive an unambiguous instant; if a formatted string is required, chain Kafka
 * Connect's built-in {@code TimestampConverter} SMT after this one.</li>
 * </ul>
 *
 * <h2>Schema cache</h2>
 * <p>
 * Evolved schemas (original schema + new timestamp field) are cached in a
 * Caffeine cache keyed by the original {@link Schema} reference identity.
 * The cache uses {@code weakKeys + softValues}: entries are held only as long as
 * the source schema is still in use by the Connect framework, and JVM GC can
 * reclaim values under memory pressure. This makes the cache self-sizing —
 * no capacity constant is needed regardless of how many topics or schema
 * versions are in flight.
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

    // -----------------------------------------------------------------------
    // Config definition
    // -----------------------------------------------------------------------

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH,
                    "Name of the field to insert.\n"
                            + "  - Schema-ful records: field type = Kafka Connect Timestamp logical type "
                            + "(UTC epoch-ms → Iceberg TIMESTAMP).\n"
                            + "  - Schemaless records: field type = long, "
                            + "raw epoch-millis (UTC).");

    // -----------------------------------------------------------------------
    // Runtime state
    // -----------------------------------------------------------------------

    /** Configured field name. */
    private String fieldName;

    /**
     * Self-sizing Caffeine cache: maps the <em>original</em> source {@link Schema} to the
     * evolved {@link Schema} that includes the new timestamp field.
     *
     * <p>
     * Uses {@code weakKeys}: Caffeine uses identity equality for lookup (correct — Connect
     * reuses the same Schema object for consecutive records of the same topic/version) and
     * automatically evicts entries whose source schema is no longer referenced by the
     * Connect framework (e.g. after a schema migration or topic removal).
     *
     * <p>
     * Uses {@code softValues}: the JVM can reclaim evolved-schema entries under memory
     * pressure before triggering an OOM, making the cache safe with any number of topics.
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
                .weakKeys()
                .softValues()
                .build();

        log.info("[InsertSinkTimestamp] configured — field='{}'", fieldName);
    }

    @Override
    public R apply(R record) {
        // Tombstone records (null value/key) must pass through unchanged.
        // Stamping a timestamp on a tombstone corrupts compaction semantics.
        if (operatingValue(record) == null) {
            return record;
        }

        // Single clock read; avoid Instant allocation on the schema-ful path.
        final long epochMs = System.currentTimeMillis();

        if (operatingSchema(record) == null) {
            return applySchemaless(record, epochMs);
        } else {
            return applyWithSchema(record, new Date(epochMs));
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // weakKeys cache entries are GC-managed; explicit invalidation is unnecessary
        // and risks a thundering-herd of schema rebuilds if Connect reuses this instance.
        schemaCache = null;
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
     * The evolved schema — original fields + new optional Timestamp field — is looked up
     * from the Caffeine cache keyed by the original {@link Schema} reference.
     * If absent, it is built and inserted automatically by {@code get(key, loader)}.
     *
     * <h3>Guards</h3>
     * <ul>
     * <li><b>ClassCastException guard</b>: verifies the value is actually a {@link Struct}
     * before casting. Non-Struct schema-ful records are passed through unchanged.</li>
     * <li><b>Idempotency guard</b>: when copying original fields into the new Struct, the
     * {@code fieldName} field is skipped. This prevents a {@code DataException} if the SMT
     * is applied twice (SMT chain or topic re-read).</li>
     * </ul>
     */
    private R applyWithSchema(R record, Date sinkDate) {
        final Schema originalSchema = operatingSchema(record);
        final Object rawValue = operatingValue(record);

        if (!(rawValue instanceof Struct)) {
            log.warn("[InsertSinkTimestamp] schema-ful value on topic '{}' is not a Struct (actual: {}) "
                    + "— passing through unchanged",
                    record.topic(),
                    rawValue == null ? "null" : rawValue.getClass().getName());
            return record;
        }
        final Struct originalStruct = (Struct) rawValue;

        // get(key, loader) is atomic: Caffeine serialises the loader for the same key,
        // preventing duplicate schema builds across concurrent task threads.
        final Schema newSchema = schemaCache.get(originalSchema, this::makeUpdatedSchema);

        final Struct newStruct = new Struct(newSchema);
        for (Field field : originalSchema.fields()) {
            // Skip fieldName: if SMT was applied previously the original type may differ
            // from the Timestamp type in the evolved schema → DataException on put().
            if (!field.name().equals(fieldName)) {
                newStruct.put(field.name(), originalStruct.get(field));
            }
        }
        newStruct.put(fieldName, sinkDate);

        return newRecord(record, newSchema, newStruct);
    }

    /**
     * Handles <b>schemaless</b> (Map) records.
     *
     * <p>
     * Inserts the sink timestamp as a raw {@code long} epoch-millis (UTC) under the
     * configured field name. A numeric epoch is emitted rather than a formatted string;
     * if a formatted string is required, chain Kafka Connect's built-in
     * {@code TimestampConverter} SMT after this one.
     *
     * <p>
     * The result map is a {@link LinkedHashMap} to preserve field insertion order,
     * which matters for JDBC/Iceberg sinks that may map columns positionally.
     */
    @SuppressWarnings("unchecked")
    private R applySchemaless(R record, long epochMs) {
        final Object rawValue = operatingValue(record);

        if (!(rawValue instanceof Map)) {
            log.warn("[InsertSinkTimestamp] schemaless value on topic '{}' is not a Map (actual: {}) "
                    + "— passing through unchanged",
                    record.topic(),
                    rawValue == null ? "null" : rawValue.getClass().getName());
            return record;
        }

        final Map<String, Object> original = (Map<String, Object>) rawValue;
        final Map<String, Object> updated = new LinkedHashMap<>(original);
        updated.put(fieldName, epochMs);

        return newRecord(record, null, updated);
    }

    /**
     * Builds a new evolved {@link Schema} by copying every field from the original schema
     * and appending a new <b>optional</b> {@link Timestamp} logical-type field at the end.
     * Called only on cache miss (i.e. first encounter of each distinct source schema).
     *
     * <p>Preservation strategy:
     * <ul>
     * <li><b>name, doc, version</b>: copied verbatim so downstream schema registries and
     * JDBC sinks continue to recognise the schema identity.</li>
     * <li><b>parameters</b>: copied in full — holds critical metadata injected by Debezium
     * (column type hints) and Confluent Schema Registry (Avro defaults). Dropping these
     * breaks column mapping in JDBC / Iceberg sinks.</li>
     * <li><b>fieldName collision</b>: if the original schema already contains a field with
     * {@code fieldName}, it is replaced by our fresh Timestamp definition — preventing
     * duplicate-field errors on idempotent re-application of the SMT.</li>
     * </ul>
     *
     * <p>The timestamp field is marked {@code optional()} for backwards-compatibility:
     * records that arrived before the SMT was engaged still validate correctly.
     */
    private Schema makeUpdatedSchema(Schema originalSchema) {
        if (originalSchema.field(fieldName) != null) {
            log.warn("[InsertSinkTimestamp] field '{}' already exists in schema '{}' — "
                    + "overwriting with sink timestamp. Consider renaming via sink.timestamp.field.",
                    fieldName, originalSchema.name());
        }

        final SchemaBuilder builder = SchemaBuilder.struct();

        if (originalSchema.name() != null)
            builder.name(originalSchema.name());
        if (originalSchema.doc() != null)
            builder.doc(originalSchema.doc());
        if (originalSchema.version() != null)
            builder.version(originalSchema.version());

        if (originalSchema.parameters() != null) {
            builder.parameters(originalSchema.parameters());
        }

        for (Field field : originalSchema.fields()) {
            if (!field.name().equals(fieldName)) {
                builder.field(field.name(), field.schema());
            }
        }

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
