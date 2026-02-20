//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hms_constants.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
namespace hms {

// Table format identifiers
namespace format {
constexpr const char *DELTA = "delta";
constexpr const char *ICEBERG = "iceberg";
constexpr const char *PARQUET = "parquet";
constexpr const char *AVRO = "avro";
constexpr const char *CSV = "csv";
constexpr const char *ORC = "orc";
} // namespace format

// Hive type strings
namespace hive_type {
constexpr const char *TINYINT = "tinyint";
constexpr const char *SMALLINT = "smallint";
constexpr const char *INT = "int";
constexpr const char *INTEGER = "integer";
constexpr const char *BIGINT = "bigint";
constexpr const char *LONG = "long";
constexpr const char *FLOAT = "float";
constexpr const char *DOUBLE = "double";
constexpr const char *BOOLEAN = "boolean";
constexpr const char *STRING = "string";
constexpr const char *VARCHAR = "varchar";
constexpr const char *CHAR = "char";
constexpr const char *BINARY = "binary";
constexpr const char *DATE = "date";
constexpr const char *TIMESTAMP = "timestamp";
constexpr const char *VOID = "void";
constexpr const char *BYTE = "byte";
constexpr const char *TINYINT_ALT = "tinyint";
constexpr const char *SMALLINT_ALT = "smallint";
constexpr const char *ARRAY_PREFIX = "array<";
constexpr const char *MAP_PREFIX = "map<";
constexpr const char *STRUCT_PREFIX = "struct<";
constexpr const char *DECIMAL_PREFIX = "decimal(";
} // namespace hive_type

// Spark schema parameter keys
namespace spark_param {
constexpr const char *PROVIDER = "spark.sql.sources.provider";
constexpr const char *SCHEMA = "spark.sql.sources.schema";
constexpr const char *SCHEMA_NUM_PARTS = "spark.sql.sources.schema.numParts";
constexpr const char *SCHEMA_PART_TEMPLATE = "spark.sql.sources.schema.part.%d";
} // namespace spark_param

// Table type parameters
namespace table_type {
constexpr const char *TABLE_TYPE = "table_type";
constexpr const char *ICEBERG = "ICEBERG";
constexpr const char *DELTA = "DELTA";
} // namespace table_type

// Storage format classes
namespace storage_format {
// Parquet
constexpr const char *PARQUET_INPUT = "Parquet";
constexpr const char *PARQUET_SERDE = "Parquet";
constexpr const char *PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
constexpr const char *PARQUET_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
constexpr const char *PARQUET_SERDE_LIB = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

// Avro
constexpr const char *AVRO_INPUT = "Avro";
constexpr const char *AVRO_SERDE = "Avro";

// Iceberg
constexpr const char *ICEBERG_INPUT_FORMAT = "HiveIcebergInputFormat";

// CSV/Text
constexpr const char *TEXT_INPUT = "TextInputFormat";
constexpr const char *LAZY_SIMPLE_SERDE = "LazySimpleSerDe";
constexpr const char *TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
constexpr const char *TEXT_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
constexpr const char *TEXT_SERDE_LIB = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

// Delta
constexpr const char *DELTA_SERDE = "Delta";
} // namespace storage_format

// Hive SerDe parameters
namespace serde_param {
constexpr const char *FIELD_DELIM = "field.delim";
} // namespace serde_param

// Table scan function names
namespace scan_function {
constexpr const char *DELTA_SCAN = "delta_scan";
constexpr const char *ICEBERG_SCAN = "iceberg_scan";
constexpr const char *PARQUET_SCAN = "parquet_scan";
constexpr const char *READ_AVRO = "read_avro";
constexpr const char *READ_CSV = "read_csv";
} // namespace scan_function

// URL schemes
namespace url {
constexpr const char *S3 = "s3://";
constexpr const char *S3A = "s3a://";
constexpr const char *HTTP = "http://";
constexpr const char *HTTPS = "https://";
constexpr const char *FILE = "file://";
} // namespace url

// Table path suffixes
namespace path {
constexpr const char *PLACEHOLDER = "__PLACEHOLDER__";
constexpr const char *PLACEHOLDER_SUFFIX = "-__PLACEHOLDER__";
} // namespace path

// Table types
namespace table {
constexpr const char *MANAGED_TABLE = "MANAGED_TABLE";
constexpr const char *EXTERNAL_TABLE = "EXTERNAL_TABLE";
} // namespace table

// Constants
namespace constants {
constexpr int MAX_SCHEMA_PARTS = 1000;
constexpr char DEFAULT_HIVE_DELIMITER = '\x01';
} // namespace constants

} // namespace hms
} // namespace duckdb
