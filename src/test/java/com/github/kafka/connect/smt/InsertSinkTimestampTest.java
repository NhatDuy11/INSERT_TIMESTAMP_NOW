package com.github.kafka.connect.smt;


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Unit tests for {@link InsertSinkTimestamp}.
 *
 * <p>
 * Covers:
 * <ul>
 * <li>Value transform — schema-ful (Struct) records</li>
 * <li>Value transform — schemaless (Map) records</li>
 * <li>Key transform — schema-ful records</li>
 * <li>Custom field name configuration</li>
 * <li>Schema cache LRU eviction</li>
 * <li>Schema metadata preservation (name, version, doc)</li>
 * <li>clearSchemaCache() utility</li>
 * <li>close() lifecycle</li>
 * <li>Tombstone records (null value) — schemaless and schema-ful</li>
 * </ul>
 */
class InsertSinkTimestampTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static final String TOPIC = "test-topic";
    private static final String DEFAULT_FIELD = "sink_timestamp";
    private static final String CUSTOM_FIELD = "my_ts";
    private static final int PARTITION = 0;

    /** Builds a minimal {@link SinkRecord} with a Struct value. */
    private static SinkRecord sinkRecordWithSchema(Schema schema, Struct value) {
        return new SinkRecord(TOPIC, PARTITION,
                Schema.STRING_SCHEMA, "key",
                schema, value,
                0L);
    }

    /** Builds a {@link SinkRecord} with a Map value (schemaless). */
    private static SinkRecord sinkRecordSchemaless(Map<String, Object> value) {
        return new SinkRecord(TOPIC, PARTITION,
                null, null,
                null, value,
                0L);
    }

    /** Builds a {@link SinkRecord} where the KEY carries the schema/value. */
    private static SinkRecord sinkRecordWithKeySchema(Schema keySchema, Struct keyValue) {
        return new SinkRecord(TOPIC, PARTITION,
                keySchema, keyValue,
                Schema.STRING_SCHEMA, "value",
                0L);
    }

    /** A simple source schema with two fields: id (INT32) and name (STRING). */
    private static Schema simpleSchema() {
        return SchemaBuilder.struct()
                .name("com.example.Event")
                .version(1)
                .doc("Test schema")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
    }

    private static Struct simpleStruct(Schema schema) {
        return new Struct(schema)
                .put("id", 42)
                .put("name", "hello");
    }

    // -----------------------------------------------------------------------
    // Value transform tests
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Value transform")
    class ValueTransformTests {

        private InsertSinkTimestamp.Value<SinkRecord> smt;

        @BeforeEach
        void setUp() {
            smt = new InsertSinkTimestamp.Value<>();
            Map<String, String> cfg = new HashMap<>();
            cfg.put(InsertSinkTimestamp.FIELD_CONFIG, DEFAULT_FIELD);
            smt.configure(cfg);
        }

        @AfterEach
        void tearDown() {
            smt.close();
        }

        // -------------------------------------------------------------------

        @Test
        @DisplayName("should add sink_timestamp field to schema-ful record")
        void shouldAddTimestampFieldToSchemaRecord() {
            Schema schema = simpleSchema();
            Struct struct = simpleStruct(schema);
            SinkRecord input = sinkRecordWithSchema(schema, struct);

            SinkRecord output = smt.apply(input);

            Schema outSchema = output.valueSchema();
            Struct outStruct = (Struct) output.value();

            // New field must exist with Timestamp logical type
            Field tsField = outSchema.field(DEFAULT_FIELD);
            assertThat(tsField).isNotNull();
            assertThat(tsField.schema().name()).isEqualTo(Timestamp.LOGICAL_NAME);

            // Original fields preserved
            assertThat(outStruct.get("id")).isEqualTo(42);
            assertThat(outStruct.get("name")).isEqualTo("hello");

            // Timestamp value is a Date close to now
            java.util.Date ts = (java.util.Date) outStruct.get(DEFAULT_FIELD);
            assertThat(ts).isNotNull();
            assertThat(ts.toInstant())
                    .isCloseTo(java.time.Instant.now(), within(5, SECONDS));
        }

        @Test
        @DisplayName("should add field to schemaless (Map) record as formatted String")
        void shouldAddTimestampFieldToSchemalessRecord() {
            Map<String, Object> map = new HashMap<>();
            map.put("id", 10);
            map.put("name", "world");
            SinkRecord input = sinkRecordSchemaless(map);

            SinkRecord output = smt.apply(input);

            assertThat(output.valueSchema()).isNull();

            @SuppressWarnings("unchecked")
            Map<String, Object> outMap = (Map<String, Object>) output.value();

            assertThat(outMap).containsKey(DEFAULT_FIELD);
            assertThat(outMap.get("id")).isEqualTo(10);
            assertThat(outMap.get("name")).isEqualTo("world");

            // Schemaless timestamp is now a String in format yyyy-MM-dd HH:mm:ss.SSS (UTC)
            Object tsValue = outMap.get(DEFAULT_FIELD);
            assertThat(tsValue).isInstanceOf(String.class);

            String tsStr = (String) tsValue;
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            LocalDateTime parsed = LocalDateTime.parse(tsStr, fmt);  // must not throw
            assertThat(parsed).isNotNull();

            // Value should be close to now (within 5 seconds)
            assertThat(parsed.toInstant(java.time.ZoneOffset.UTC))
                    .isCloseTo(java.time.Instant.now(), within(5, SECONDS));
        }

        @Test
        @DisplayName("schemaless non-Map value should be passed through unchanged with warning")
        void schemalessNonMapValueShouldPassThrough() {
            // Create a SinkRecord whose value is a raw String (not a Map)
            SinkRecord input = new org.apache.kafka.connect.sink.SinkRecord(
                    "test-topic", 0, null, null, null, "raw-string-value", 0L);

            SinkRecord output = smt.apply(input);

            // Must be the exact same object — no transformation applied
            assertThat(output).isSameAs(input);
        }

        @Test
        @DisplayName("should preserve schema name, version and doc")
        void shouldPreserveSchemaMetadata() {
            Schema schema = simpleSchema();
            SinkRecord input = sinkRecordWithSchema(schema, simpleStruct(schema));
            SinkRecord output = smt.apply(input);

            Schema outSchema = output.valueSchema();
            assertThat(outSchema.name()).isEqualTo("com.example.Event");
            assertThat(outSchema.version()).isEqualTo(1);
            assertThat(outSchema.doc()).isEqualTo("Test schema");
        }

        @Test
        @DisplayName("timestamp field should be optional in the evolved schema")
        void timestampFieldShouldBeOptional() {
            Schema schema = simpleSchema();
            SinkRecord input = sinkRecordWithSchema(schema, simpleStruct(schema));
            SinkRecord output = smt.apply(input);

            Field tsField = output.valueSchema().field(DEFAULT_FIELD);
            assertThat(tsField.schema().isOptional()).isTrue();
        }

        @Test
        @DisplayName("same evolved schema object reused from cache on consecutive records")
        void shouldReuseEvolvedSchemaFromCache() {
            Schema schema = simpleSchema();
            SinkRecord r1 = smt.apply(sinkRecordWithSchema(schema, simpleStruct(schema)));
            SinkRecord r2 = smt.apply(sinkRecordWithSchema(schema, simpleStruct(schema)));

            // The evolved schema instance should be the exact same object (cache hit)
            assertThat(r1.valueSchema()).isSameAs(r2.valueSchema());
        }

        @Test
        @DisplayName("different source schemas produce different evolved schemas")
        void shouldBuildSeparateEvolvedSchemasForDifferentSources() {
            Schema s1 = SchemaBuilder.struct().field("a", Schema.INT32_SCHEMA).build();
            Schema s2 = SchemaBuilder.struct().field("b", Schema.STRING_SCHEMA).build();

            Struct struct1 = new Struct(s1).put("a", 1);
            Struct struct2 = new Struct(s2).put("b", "x");

            SinkRecord r1 = smt.apply(sinkRecordWithSchema(s1, struct1));
            SinkRecord r2 = smt.apply(sinkRecordWithSchema(s2, struct2));

            assertThat(r1.valueSchema()).isNotSameAs(r2.valueSchema());
            assertThat(r1.valueSchema().field(DEFAULT_FIELD)).isNotNull();
            assertThat(r2.valueSchema().field(DEFAULT_FIELD)).isNotNull();
        }

        @Test
        @DisplayName("clearSchemaCache should empty the cache without errors")
        void clearSchemaCacheShouldWork() {
            Schema s = simpleSchema();
            smt.apply(sinkRecordWithSchema(s, simpleStruct(s))); // populate cache
            smt.clearSchemaCache();

            // After clear a fresh schema object is built — both records should still work
            SinkRecord out = smt.apply(sinkRecordWithSchema(s, simpleStruct(s)));
            assertThat(out.valueSchema().field(DEFAULT_FIELD)).isNotNull();
        }

        // -------------------------------------------------------------------
        // Tombstone (null value) pass-through tests
        // -------------------------------------------------------------------

        @Test
        @DisplayName("schemaless tombstone (null value) should be passed through unchanged")
        void schemalessNullValueShouldPassThrough() {
            SinkRecord input = sinkRecordSchemaless(null);
            SinkRecord output = smt.apply(input);

            // Must be the exact same object — no transformation applied
            assertThat(output).isSameAs(input);
        }

        @Test
        @DisplayName("schema-ful tombstone (null Struct value) should be passed through unchanged")
        void schemafulNullValueShouldPassThrough() {
            // A schema-ful tombstone has a schema but a null struct value
            Schema schema = simpleSchema();
            SinkRecord input = sinkRecordWithSchema(schema, null);
            SinkRecord output = smt.apply(input);

            // Must be the exact same object — no timestamp field added, no exception
            assertThat(output).isSameAs(input);
        }
    }

    // -----------------------------------------------------------------------
    // Custom field name tests
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Custom field name configuration")
    class CustomFieldNameTests {

        private InsertSinkTimestamp.Value<SinkRecord> smt;

        @BeforeEach
        void setUp() {
            smt = new InsertSinkTimestamp.Value<>();
            Map<String, String> cfg = new HashMap<>();
            cfg.put(InsertSinkTimestamp.FIELD_CONFIG, CUSTOM_FIELD);
            smt.configure(cfg);
        }

        @AfterEach
        void tearDown() {
            smt.close();
        }

        @Test
        @DisplayName("should use the configured custom field name in schema-ful record")
        void shouldUseCustomFieldNameWithSchema() {
            Schema schema = simpleSchema();
            SinkRecord output = smt.apply(sinkRecordWithSchema(schema, simpleStruct(schema)));

            assertThat(output.valueSchema().field(CUSTOM_FIELD)).isNotNull();
            assertThat(output.valueSchema().field(DEFAULT_FIELD)).isNull();
            assertThat(((Struct) output.value()).get(CUSTOM_FIELD)).isNotNull();
        }

        @Test
        @DisplayName("should use the configured custom field name in schemaless record")
        void shouldUseCustomFieldNameWithoutSchema() {
            Map<String, Object> map = Map.of("x", 1);
            SinkRecord output = smt.apply(sinkRecordSchemaless(map));

            @SuppressWarnings("unchecked")
            Map<String, Object> outMap = (Map<String, Object>) output.value();
            assertThat(outMap).containsKey(CUSTOM_FIELD);
            assertThat(outMap).doesNotContainKey(DEFAULT_FIELD);
        }
    }

    // -----------------------------------------------------------------------
    // Key transform tests
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Key transform")
    class KeyTransformTests {

        private InsertSinkTimestamp.Key<SinkRecord> smt;

        @BeforeEach
        void setUp() {
            smt = new InsertSinkTimestamp.Key<>();
            Map<String, String> cfg = new HashMap<>();
            cfg.put(InsertSinkTimestamp.FIELD_CONFIG, DEFAULT_FIELD);
            smt.configure(cfg);
        }

        @AfterEach
        void tearDown() {
            smt.close();
        }

        @Test
        @DisplayName("should add timestamp field to the record KEY")
        void shouldAddTimestampToKey() {
            Schema keySchema = SchemaBuilder.struct()
                    .field("user_id", Schema.INT64_SCHEMA)
                    .build();
            Struct keyValue = new Struct(keySchema).put("user_id", 99L);

            SinkRecord input = sinkRecordWithKeySchema(keySchema, keyValue);
            SinkRecord output = smt.apply(input);

            Schema outKeySchema = output.keySchema();
            Struct outKeyStruct = (Struct) output.key();

            assertThat(outKeySchema.field(DEFAULT_FIELD)).isNotNull();
            assertThat(outKeyStruct.get("user_id")).isEqualTo(99L);
            assertThat(outKeyStruct.get(DEFAULT_FIELD)).isNotNull();

            // Value must remain unchanged
            assertThat(output.valueSchema()).isEqualTo(Schema.STRING_SCHEMA);
            assertThat(output.value()).isEqualTo("value");
        }
    }

    // -----------------------------------------------------------------------
    // Schema cache LRU eviction test
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Schema cache LRU eviction")
    class LruEvictionTests {

        private InsertSinkTimestamp.Value<SinkRecord> smt;

        @BeforeEach
        void setUp() {
            smt = new InsertSinkTimestamp.Value<>();
            Map<String, String> cfg = new HashMap<>();
            cfg.put(InsertSinkTimestamp.FIELD_CONFIG, DEFAULT_FIELD);
            smt.configure(cfg);
        }

        @AfterEach
        void tearDown() {
            smt.close();
        }

        @Test
        @DisplayName("cache should handle 300 distinct schemas without error (self-sizing weakKeys cache)")
        void shouldNotErrorWithManyDistinctSchemas() {
            // 300 > any previous hardcoded limit; weakKeys+softValues cache is unbounded by design
            for (int i = 0; i < 300; i++) {
                Schema s = SchemaBuilder.struct().field("f" + i, Schema.INT32_SCHEMA).build();
                Struct struct = new Struct(s).put("f" + i, i);
                SinkRecord out = smt.apply(sinkRecordWithSchema(s, struct));
                assertThat(out.valueSchema().field(DEFAULT_FIELD)).isNotNull();
            }
        }
    }

    // -----------------------------------------------------------------------
    // ConfigDef / config() test
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Configuration")
    class ConfigurationTests {

        @Test
        @DisplayName("config() should return the static CONFIG_DEF")
        void configMethodShouldReturnConfigDef() {
            InsertSinkTimestamp.Value<SinkRecord> smt = new InsertSinkTimestamp.Value<>();
            assertThat(smt.config()).isSameAs(InsertSinkTimestamp.CONFIG_DEF);
        }

        @Test
        @DisplayName("config() should expose FIELD_CONFIG key in CONFIG_DEF")
        void configDefShouldContainFieldConfig() {
            assertThat(InsertSinkTimestamp.CONFIG_DEF.configKeys())
                    .containsKey(InsertSinkTimestamp.FIELD_CONFIG);
        }
    }

    // -----------------------------------------------------------------------
    // Bug-fix regression tests
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Bug-fix regression tests")
    class BugFixRegressionTests {

        private InsertSinkTimestamp.Value<SinkRecord> smt;

        @BeforeEach
        void setUp() {
            smt = new InsertSinkTimestamp.Value<>();
            Map<String, String> cfg = new HashMap<>();
            cfg.put(InsertSinkTimestamp.FIELD_CONFIG, DEFAULT_FIELD);
            smt.configure(cfg);
        }

        @AfterEach
        void tearDown() {
            smt.close();
        }

        // -------------------------------------------------------------------
        // Bug fix #1 — ClassCastException guard (schema-ful non-Struct value)
        // -------------------------------------------------------------------

        @Test
        @DisplayName("Bug #1: schema-ful non-Struct value should pass through unchanged (no ClassCastException)")
        void schemafulNonStructValueShouldPassThrough() {
            // Unusual: record has a schema but the value is a raw String (not a Struct).
            // Before the fix this threw ClassCastException at (Struct) operatingValue(record).
            SinkRecord input = new SinkRecord(
                    TOPIC, PARTITION,
                    null, null,
                    Schema.STRING_SCHEMA, "not-a-struct",
                    0L);

            SinkRecord output = smt.apply(input);

            // Must be the exact same object — no transformation applied
            assertThat(output).isSameAs(input);
        }

        // -------------------------------------------------------------------
        // Bug fix #2 — DataException / Idempotency crash
        // -------------------------------------------------------------------

        @Test
        @DisplayName("Bug #2: applying SMT twice on the same record must not throw DataException")
        void applyingSmtTwiceMustNotThrowDataException() {
            Schema schema = simpleSchema();
            Struct struct = simpleStruct(schema);
            SinkRecord first = sinkRecordWithSchema(schema, struct);

            // First application — adds sink_timestamp as Timestamp
            SinkRecord afterFirst = smt.apply(first);

            // Second application — sink_timestamp already exists in the evolved schema.
            // The copy loop must skip fieldName to avoid type mismatch DataException.
            SinkRecord afterSecond = smt.apply(afterFirst);

            assertThat(afterSecond.valueSchema().field(DEFAULT_FIELD)).isNotNull();
            assertThat(((Struct) afterSecond.value()).get(DEFAULT_FIELD)).isNotNull();
            assertThat(afterSecond.valueSchema().field(DEFAULT_FIELD).schema().name())
                    .isEqualTo(Timestamp.LOGICAL_NAME);
        }

        // -------------------------------------------------------------------
        // Bug fix #3 — Schema parameters preservation
        // -------------------------------------------------------------------

        @Test
        @DisplayName("Bug #3: evolved schema must preserve all original schema parameters")
        void evolvedSchemaMustPreserveParameters() {
            Schema schemaWithParams = SchemaBuilder.struct()
                    .name("com.example.DebeziumEvent")
                    .version(1)
                    .parameter("__debezium.source.column.type", "VARCHAR")
                    .parameter("__debezium.source.column.length", "255")
                    .field("id", Schema.INT32_SCHEMA)
                    .field("payload", Schema.STRING_SCHEMA)
                    .build();

            Struct struct = new Struct(schemaWithParams)
                    .put("id", 1)
                    .put("payload", "data");

            SinkRecord input = sinkRecordWithSchema(schemaWithParams, struct);
            SinkRecord output = smt.apply(input);

            Schema outSchema = output.valueSchema();
            assertThat(outSchema.parameters()).isNotNull();
            assertThat(outSchema.parameters())
                    .containsEntry("__debezium.source.column.type", "VARCHAR")
                    .containsEntry("__debezium.source.column.length", "255");

            assertThat(outSchema.field(DEFAULT_FIELD)).isNotNull();
        }
    }
}
