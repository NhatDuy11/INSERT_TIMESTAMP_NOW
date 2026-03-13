# InsertSinkTimestamp — Kafka Connect SMT

> **Single Message Transform (SMT)** tự động đóng dấu thời gian tại thời điểm **Sink Connector nhận được record** (wall-clock UTC), giúp bạn biết chính xác khi nào dữ liệu đến được đích (Iceberg, JDBC, S3...).

---

## 📋 Mục lục

- [Tính năng](#-tính-năng)
- [Cách hoạt động](#-cách-hoạt-động)
- [Yêu cầu](#-yêu-cầu)
- [Build & Cài đặt](#-build--cài-đặt)
- [Cấu hình](#-cấu-hình)
- [Ví dụ sử dụng](#-ví-dụ-sử-dụng)
- [Output theo từng chế độ](#-output-theo-từng-chế-độ)
- [Các trường hợp đặc biệt](#-các-trường-hợp-đặc-biệt)
- [Lưu ý Production & Kubernetes](#-lưu-ý-production--kubernetes)
- [Chạy Tests](#-chạy-tests)

---

## ✨ Tính năng

| Tính năng | Mô tả |
|---|---|
| **Tự động đóng dấu sink-time** | Ghi lại thời điểm record được Sink Connector xử lý — không phải thời điểm message được produce lên Kafka |
| **Schema-ful (Struct)** | Field timestamp dạng Kafka Connect `Timestamp` logical type → map thẳng sang cột `TIMESTAMP` trong Apache Iceberg / Dremio |
| **Schemaless (Map/JSON)** | Field timestamp dạng `String` format `yyyy-MM-dd HH:mm:ss.SSS` (UTC) — human-readable, dễ index |
| **Tombstone safe** | Record `null` value (delete marker) được pass-through nguyên vẹn, không làm hỏng Kafka compaction |
| **Idempotent** | Áp dụng SMT nhiều lần (SMT chain, re-read) không gây `DataException` |
| **Caffeine LRU cache** | Schema evolved được cache lock-free với Caffeine, throughput cao, an toàn đa luồng |
| **Debezium / Avro compatible** | Giữ nguyên `parameters()` của schema gốc (Debezium column type hints, Confluent Avro defaults) |

---

## ⚙️ Cách hoạt động

```
Kafka Topic
    │
    ▼
┌─────────────────────────────────┐
│   InsertSinkTimestamp SMT       │
│                                 │
│  1. Kiểm tra tombstone          │
│     └─ null value → pass-through│
│                                 │
│  2. Lấy Instant.now() (UTC)     │
│                                 │
│  3a. Schema-ful (Struct):       │
│      ├─ Tra cứu Caffeine cache  │
│      ├─ Tạo evolved schema nếu  │
│      │   chưa có (copy fields + │
│      │   thêm Timestamp field)  │
│      └─ Ghi Date vào Struct     │
│                                 │
│  3b. Schemaless (Map):          │
│      └─ Ghi String              │
│         "yyyy-MM-dd HH:mm:ss.SSS│
│          " vào Map              │
└─────────────────────────────────┘
    │
    ▼
Sink Connector (Iceberg / JDBC / S3 ...)
```

---

## 📦 Yêu cầu

| Thành phần | Phiên bản tối thiểu |
|---|---|
| Java | 11 |
| Apache Kafka Connect | 3.x |
| Maven | 3.6+ |
| Caffeine | 3.1.8 (bundled vào JAR) |

---

## 🔨 Build & Cài đặt

### 1. Build JAR

```bash
mvn clean package -DskipTests
```

JAR xuất ra tại: `target/insert-sink-timestamp-smt-1.0.0.jar`

### 2. Cài vào Kafka Connect

Copy JAR vào thư mục plugin của Kafka Connect Worker:

```bash
# Ví dụ với Confluent Platform
cp target/insert-sink-timestamp-smt-1.0.0.jar \
   /usr/share/java/kafka-connect-plugins/

# Ví dụ với Docker / Kubernetes
# → Mount JAR vào /usr/share/confluent-hub-components/ hoặc
#   đường dẫn CONNECT_PLUGIN_PATH của bạn
```

Restart Kafka Connect Worker sau khi copy.

### 3. Kubernetes — ConfigMap / Init Container

```yaml
# Ví dụ mount JAR qua init container
initContainers:
  - name: install-smt
    image: busybox
    command: ["sh", "-c", "cp /plugins/insert-sink-timestamp-smt-1.0.0.jar /kafka/plugins/"]
    volumeMounts:
      - name: plugin-volume
        mountPath: /kafka/plugins
```

---

## 🔧 Cấu hình

### Parameters

| Key | Bắt buộc | Mô tả |
|---|---|---|
| `transforms` | ✅ | Tên alias cho transform (ví dụ: `addSinkTs`) |
| `transforms.<alias>.type` | ✅ | Class đầy đủ (xem bên dưới) |
| `transforms.<alias>.sink.timestamp.field` | ✅ | Tên field sẽ được insert vào record |

### Class name

| Muốn transform | Class |
|---|---|
| Record **value** (thường dùng nhất) | `com.github.kafka.connect.smt.InsertSinkTimestamp$Value` |
| Record **key** | `com.github.kafka.connect.smt.InsertSinkTimestamp$Key` |

---

## 📝 Ví dụ sử dụng

### Cấu hình Sink Connector cơ bản

```properties
# Trong cấu hình Sink Connector
transforms=addSinkTs
transforms.addSinkTs.type=com.github.kafka.connect.smt.InsertSinkTimestamp$Value
transforms.addSinkTs.sink.timestamp.field=sink_timestamp
```

### Kết hợp nhiều SMT (SMT Chain)

```properties
# Ví dụ: Flatten JSON trước, rồi thêm sink timestamp
transforms=flatten,addSinkTs

transforms.flatten.type=org.apache.kafka.connect.transforms.Flatten$Value
transforms.flatten.delimiter=_

transforms.addSinkTs.type=com.github.kafka.connect.smt.InsertSinkTimestamp$Value
transforms.addSinkTs.sink.timestamp.field=sink_timestamp
```

### Cấu hình với Iceberg Sink (Tabular / AWS Glue)

```json
{
  "name": "iceberg-sink-banking",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "topics": "banking.transactions",
    "iceberg.catalog.type": "hive",
    "transforms": "addSinkTs",
    "transforms.addSinkTs.type": "com.github.kafka.connect.smt.InsertSinkTimestamp$Value",
    "transforms.addSinkTs.sink.timestamp.field": "sink_timestamp"
  }
}
```

### Cấu hình với JDBC Sink

```json
{
  "name": "jdbc-sink-banking",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.url": "jdbc:postgresql://db:5432/bankdb",
    "topics": "banking.accounts",
    "auto.create": "true",
    "transforms": "addSinkTs",
    "transforms.addSinkTs.type": "com.github.kafka.connect.smt.InsertSinkTimestamp$Value",
    "transforms.addSinkTs.sink.timestamp.field": "sink_loaded_at"
  }
}
```

### Chỉ đóng dấu trên Key

```properties
transforms=addSinkTsKey
transforms.addSinkTsKey.type=com.github.kafka.connect.smt.InsertSinkTimestamp$Key
transforms.addSinkTsKey.sink.timestamp.field=key_sink_ts
```

---

## 📤 Output theo từng chế độ

### Schema-ful record (Avro / Protobuf / JSON Schema)

**Input Struct:**
```
id:   INT32  = 42
name: STRING = "Nguyen Van A"
```

**Output Struct (evolved schema):**
```
id:             INT32     = 42
name:           STRING    = "Nguyen Van A"
sink_timestamp: TIMESTAMP = 2026-03-12T14:00:00.123Z   ← UTC epoch-ms (Date)
```

> Field `sink_timestamp` kiểu `org.apache.kafka.connect.data.Timestamp` → Iceberg/Dremio nhận diện thành cột `TIMESTAMP` native. Để hiển thị theo giờ Việt Nam trong Dremio:
> ```sql
> SET TIME ZONE 'Asia/Ho_Chi_Minh';
> SELECT sink_timestamp FROM banking.transactions;
> ```

---

### Schemaless record (JSON converter không có schema)

**Input Map:**
```json
{"id": 42, "name": "Nguyen Van A"}
```

**Output Map:**
```json
{
  "id": 42,
  "name": "Nguyen Van A",
  "sink_timestamp": "2026-03-12 21:00:00.123"
}
```

> Field `sink_timestamp` là **String** format `yyyy-MM-dd HH:mm:ss.SSS` (UTC).  
> Human-readable, dễ parse bởi mọi downstream system.

---

## 🛡️ Các trường hợp đặc biệt

### Tombstone record (null value)

Record có value `null` (Kafka compaction delete marker) được **pass-through nguyên vẹn**. SMT không chèn timestamp vào tombstone, tránh làm hỏng compaction semantics.

```
Input:  key="user-123"  value=null  ← tombstone
Output: key="user-123"  value=null  ← không đổi gì
```

### Record đã có sẵn field trùng tên (`sink_timestamp`)

Nếu field `sink_timestamp` đã tồn tại trong schema gốc (ví dụ SMT được áp dụng 2 lần trong SMT chain):
- **Schema**: field cũ bị thay thế bằng định nghĩa `Timestamp` mới.
- **Giá trị**: luôn ghi **sink timestamp mới nhất**.
- **Không crash** `DataException`.
- Log warning được emit để bạn phát hiện cấu hình trùng lặp.

### Value schema-ful nhưng value không phải Struct

Một số connector hiếm gặp emit schema kèm với primitive/array value. SMT sẽ **pass-through** và log warning thay vì crash `ClassCastException`.

---

## 🚀 Lưu ý Production & Kubernetes

### NTP Sync

`Instant.now()` phụ thuộc đồng hồ hệ thống. Đảm bảo K8s nodes sync NTP chính xác (sai lệch > 1s có thể ảnh hưởng event ordering).

### Timezone trong container

SMT luôn ghi **UTC**. Không dùng `ZoneId.systemDefault()` để tránh phụ thuộc vào timezone của container image.  
Xử lý timezone tại query layer (Dremio `SET TIME ZONE`, Spark session timezone...).

### Caffeine Schema Cache

- **Capacity**: 64 schema variants per SMT instance.
- **Eviction**: W-TinyLFU — tự động khi vượt capacity.
- **Thread safety**: lock-free, an toàn với nhiều task threads chia sẻ cùng SMT instance.
- **Monitoring**: cache stats được record (`recordStats()`), có thể expose qua JMX/Micrometer.

### Memory footprint

Mỗi Connector task có một SMT instance riêng:
```
Tổng schemas trong memory ≈ SCHEMA_CACHE_CAPACITY (64) × số_tasks
```
Với 20 tasks → ~1280 schema objects. Thường không đáng kể với banking workload.

### Debezium compatibility

Schema `parameters()` (Debezium column type hints như `__debezium.source.column.type=VARCHAR`) được **giữ nguyên** trong evolved schema, đảm bảo sink connector vẫn map đúng kiểu cột đích.

---

## 🧪 Chạy Tests

```bash
# Chạy toàn bộ unit tests
mvn clean test

# Chạy một test class cụ thể
mvn test -Dtest=InsertSinkTimestampTest

# Chạy một test method cụ thể
mvn test -Dtest="InsertSinkTimestampTest#shouldAddTimestampFieldToSchemaRecord"
```

### Test coverage

| Test Group | Nội dung |
|---|---|
| `ValueTransformTests` | Schema-ful Struct, schemaless Map, tombstone pass-through, schema metadata, cache reuse |
| `CustomFieldNameTests` | Tên field tùy chỉnh với schema-ful và schemaless |
| `KeyTransformTests` | Transform trên record key |
| `LruEvictionTests` | Caffeine cache không lỗi khi vượt capacity |
| `ConfigurationTests` | `config()`, `SCHEMA_CACHE_CAPACITY` constant |
| `BugFixRegressionTests` | ClassCastException guard, DataException idempotency, schema parameters preservation |

---

## 📁 Cấu trúc dự án

```
insert-sink-timestamp-smt/
├── pom.xml
└── src/
    ├── main/java/com/github/kafka/connect/smt/
    │   └── InsertSinkTimestamp.java        # SMT chính (Value + Key variants)
    └── test/java/com/github/kafka/connect/smt/
        └── InsertSinkTimestampTest.java    # Unit tests (JUnit 5 + AssertJ)
```

---

## 📄 License

Apache License 2.0
