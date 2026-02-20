// HMS Utilities - Type conversion and schema parsing for Hive Metastore integration
//
// YYJSON DEPENDENCY:
// This file uses the yyjson library to parse Spark DataType JSON schemas stored in HMS table parameters.
// Spark stores table schemas as JSON (spark.sql.sources.schema.*) which is richer than standard Hive
// column definitions, especially for complex types (struct, array, map) and Spark-specific types.
//
// This dependency CANNOT be replaced by Thrift Table objects because:
// 1. Spark stores type metadata in JSON format in table parameters, not in Hive's sd.cols
// 2. The JSON schema includes nullability, metadata, and nested type information
// 3. Standard Hive type strings are less expressive than Spark's type system
//
// The JSON parsing is centralized in this file and is required for full Spark-created table compatibility.

#include "hms_utils.hpp"
#include "hms_constants.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/hms_schema_entry.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"

namespace duckdb {

using duckdb_yyjson::yyjson_arr_size;
using duckdb_yyjson::yyjson_doc;
using duckdb_yyjson::yyjson_doc_free;
using duckdb_yyjson::yyjson_doc_get_root;
using duckdb_yyjson::yyjson_get_bool;
using duckdb_yyjson::yyjson_get_int;
using duckdb_yyjson::yyjson_get_str;
using duckdb_yyjson::yyjson_is_arr;
using duckdb_yyjson::yyjson_is_bool;
using duckdb_yyjson::yyjson_is_int;
using duckdb_yyjson::yyjson_is_obj;
using duckdb_yyjson::yyjson_is_str;
using duckdb_yyjson::yyjson_obj_get;
using duckdb_yyjson::yyjson_read;
using duckdb_yyjson::yyjson_val;

// Helper class for RAII JSON document management
struct YyjsonDocDeleter {
	void operator()(yyjson_doc *doc) const {
		if (doc) {
			yyjson_doc_free(doc);
		}
	}
};

// Parse a Spark DataType JSON representation into a DuckDB LogicalType
//
// Spark stores schemas in HMS table parameters as JSON objects following the Spark DataType format.
// Example: {"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}}]}
//
// This function recursively parses:
// - Simple types: integer, long, float, double, string, boolean, date, timestamp, binary, decimal
// - Complex types: struct, array, map
// - User-defined types (UDT) with fallback to VARCHAR
//
// The yyjson library is used for high-performance, zero-copy JSON parsing.
static LogicalType ParseSparkDataType(yyjson_val *type_val) {
	if (!type_val) {
		return LogicalType::VARCHAR; // Fallback for null values
	}

	if (yyjson_is_str(type_val)) {
		string type_str = yyjson_get_str(type_val);
		if (type_str == hms::hive_type::INTEGER) {
			return LogicalType::INTEGER;
		} else if (type_str == hms::hive_type::LONG) {
			return LogicalType::BIGINT;
		} else if (type_str == hms::hive_type::FLOAT) {
			return LogicalType::FLOAT;
		} else if (type_str == hms::hive_type::DOUBLE) {
			return LogicalType::DOUBLE;
		} else if (type_str == hms::hive_type::STRING) {
			return LogicalType::VARCHAR;
		} else if (type_str == hms::hive_type::BOOLEAN) {
			return LogicalType::BOOLEAN;
		} else if (type_str == hms::hive_type::DATE) {
			return LogicalType::DATE;
		} else if (type_str == hms::hive_type::TIMESTAMP) {
			return LogicalType::TIMESTAMP;
		} else if (type_str == hms::hive_type::BINARY) {
			return LogicalType::BLOB;
		} else if (type_str == hms::hive_type::VOID) {
			return LogicalType::SQLNULL;
		} else if (type_str == hms::hive_type::BYTE || type_str == hms::hive_type::TINYINT_ALT) {
			return LogicalType::TINYINT;
		} else if (type_str == hms::hive_type::SMALLINT_ALT || type_str == hms::hive_type::SMALLINT) {
			return LogicalType::SMALLINT;
		}
		// Fallback for unknown simple types
		return LogicalType::VARCHAR;
	} else if (yyjson_is_obj(type_val)) {
		// Complex type
		auto type_name_val = yyjson_obj_get(type_val, "type");
		if (yyjson_is_str(type_name_val)) {
			string type_name = yyjson_get_str(type_name_val);
			if (type_name == "struct") {
				auto fields_val = yyjson_obj_get(type_val, "fields");
				if (yyjson_is_arr(fields_val)) {
					child_list_t<LogicalType> children;
					size_t idx, max;
					yyjson_val *field;
					yyjson_arr_foreach(fields_val, idx, max, field) {
						if (!yyjson_is_obj(field)) {
							continue; // Skip malformed field entries
						}
						auto name_val = yyjson_obj_get(field, "name");
						auto child_type_val = yyjson_obj_get(field, "type");
						if (yyjson_is_str(name_val) && child_type_val) {
							string name = yyjson_get_str(name_val);
							LogicalType child_type = ParseSparkDataType(child_type_val);
							children.push_back(make_pair(name, child_type));
						}
					}
					// Only return struct if we have at least one child
					if (!children.empty()) {
						return LogicalType::STRUCT(children);
					}
				}
			} else if (type_name == "array") {
				auto element_type_val = yyjson_obj_get(type_val, "elementType");
				if (element_type_val) {
					LogicalType element_type = ParseSparkDataType(element_type_val);
					return LogicalType::LIST(element_type);
				}
			} else if (type_name == "map") {
				auto key_type_val = yyjson_obj_get(type_val, "keyType");
				auto value_type_val = yyjson_obj_get(type_val, "valueType");
				if (key_type_val && value_type_val) {
					LogicalType key_type = ParseSparkDataType(key_type_val);
					LogicalType value_type = ParseSparkDataType(value_type_val);
					return LogicalType::MAP(key_type, value_type);
				}
			} else if (type_name == "udt") {
				// User Defined Type - fallback to VARCHAR
				return LogicalType::VARCHAR;
			} else if (StringUtil::StartsWith(type_name, "decimal")) {
				// Handle decimal types in object form
				auto precision_val = yyjson_obj_get(type_val, "precision");
				auto scale_val = yyjson_obj_get(type_val, "scale");
				if (yyjson_is_int(precision_val) && yyjson_is_int(scale_val)) {
					int64_t precision_int = yyjson_get_int(precision_val);
					int64_t scale_int = yyjson_get_int(scale_val);
					// Validate bounds before casting to uint8_t
					if (precision_int >= 0 && precision_int <= 255 && scale_int >= 0 && scale_int <= 255) {
						uint8_t precision = static_cast<uint8_t>(precision_int);
						uint8_t scale = static_cast<uint8_t>(scale_int);
						return LogicalType::DECIMAL(precision, scale);
					}
				}
			}
		}
	}
	return LogicalType::VARCHAR;
}

//! Parse Spark schema from table parameters
//! Handles both single-part schema (spark.sql.sources.schema) and
//! multi-part schema (spark.sql.sources.schema.part.N)
bool HMSUtils::ParseSparkSchema(const map<string, string> &parameters, vector<HMSAPIColumnDefinition> &columns) {
	string full_json;

	// Check if schema is directly available in "spark.sql.sources.schema"
	auto schema_it = parameters.find(hms::spark_param::SCHEMA);
	if (schema_it != parameters.end()) {
		full_json = schema_it->second;
	} else {
		// Look for spark.sql.sources.schema.numParts
		auto it = parameters.find(hms::spark_param::SCHEMA_NUM_PARTS);
		if (it == parameters.end()) {
			return false; // No schema information found
		}

		int num_parts;
		try {
			num_parts = std::stoi(it->second);
		} catch (const std::exception &) {
			// Invalid numParts value
			return false;
		}

		constexpr int MAX_SCHEMA_PARTS = hms::constants::MAX_SCHEMA_PARTS; // Sanity limit for schema part count
		if (num_parts <= 0 || num_parts > MAX_SCHEMA_PARTS) {
			// Sanity check: reject invalid or suspiciously large part counts
			return false;
		}

		// Reconstruct the JSON string from parts
		full_json.reserve(1024UL * static_cast<size_t>(num_parts)); // Reserve space to avoid reallocations
		for (int i = 0; i < num_parts; ++i) {
			string key = StringUtil::Format(hms::spark_param::SCHEMA_PART_TEMPLATE, i);
			auto part_it = parameters.find(key);
			if (part_it == parameters.end()) {
				return false; // Missing schema part
			}
			full_json += part_it->second;
		}
	}

	if (full_json.empty()) {
		return false; // No schema content
	}

	// Parse JSON with yyjson
	yyjson_doc *doc = yyjson_read(full_json.c_str(), full_json.length(), 0);
	if (!doc) {
		return false; // JSON parsing failed
	}

	yyjson_val *root = yyjson_doc_get_root(doc);
	if (!yyjson_is_obj(root)) {
		yyjson_doc_free(doc);
		return false;
	}

	// Spark schema root is a StructType, so it has "type": "struct" and "fields": [...]
	// Or sometimes it's just the object containing "fields" directly if it's the schema definition
	// Actually, based on the snippet: schema = DataType.fromJson(json) -> StructType
	// The JSON usually looks like {"type":"struct","fields":[...]}

	auto type_val = yyjson_obj_get(root, "type");
	bool is_struct = false;
	if (yyjson_is_str(type_val) && string(yyjson_get_str(type_val)) == "struct") {
		is_struct = true;
	}

	yyjson_val *fields = yyjson_obj_get(root, "fields");
	if (!fields && !is_struct) {
		// Maybe it's not wrapped in a struct object, but usually it is.
		yyjson_doc_free(doc);
		return false;
	}

	if (!yyjson_is_arr(fields)) {
		yyjson_doc_free(doc);
		return false;
	}

	vector<HMSAPIColumnDefinition> new_columns;
	new_columns.reserve(yyjson_arr_size(fields)); // Reserve space for efficiency

	size_t idx, max;
	yyjson_val *field;
	yyjson_arr_foreach(fields, idx, max, field) {
		if (!yyjson_is_obj(field)) {
			continue; // Skip malformed field entries
		}

		HMSAPIColumnDefinition col;

		// Parse field name (required)
		auto name_val = yyjson_obj_get(field, "name");
		if (!yyjson_is_str(name_val)) {
			continue; // Skip fields without valid names
		}
		col.name = yyjson_get_str(name_val);

		// Parse field type (required)
		auto type_val = yyjson_obj_get(field, "type");
		if (!type_val) {
			continue; // Skip fields without type information
		}
		LogicalType duck_type = ParseSparkDataType(type_val);
		col.type = duck_type.ToString();

		// Parse nullable flag (optional, defaults to true)
		auto nullable_val = yyjson_obj_get(field, "nullable");
		bool nullable = true;
		if (yyjson_is_bool(nullable_val)) {
			nullable = yyjson_get_bool(nullable_val);
		}
		// Note: nullable flag is parsed but not currently stored in HMSAPIColumnDefinition

		// Parse metadata (optional)
		auto metadata_val = yyjson_obj_get(field, "metadata");
		if (yyjson_is_obj(metadata_val)) {
			auto comment_val = yyjson_obj_get(metadata_val, "comment");
			if (yyjson_is_str(comment_val)) {
				col.comment = yyjson_get_str(comment_val);
			}
		}

		new_columns.push_back(std::move(col));
	}

	yyjson_doc_free(doc);

	if (!new_columns.empty()) {
		columns = std::move(new_columns);
		return true;
	}

	return false;
}

//! Split a string by delimiter while respecting nested angle brackets
//! Used for parsing complex Hive types like map<string,array<int>>
static vector<string> SplitNested(const string &input, char delimiter) {
	vector<string> result;
	if (input.empty()) {
		return result;
	}

	int nested_level = 0;
	size_t start = 0;

	for (size_t i = 0; i < input.length(); ++i) {
		char current_char = input[i];
		if (current_char == '<') {
			nested_level++;
		} else if (current_char == '>') {
			nested_level--;
			if (nested_level < 0) {
				// Malformed input: more closing than opening brackets
				nested_level = 0;
			}
		} else if (current_char == delimiter && nested_level == 0) {
			string part = input.substr(start, i - start);
			StringUtil::Trim(part);
			if (!part.empty()) {
				result.push_back(part);
			}
			start = i + 1;
		}
	}

	// Add the last part
	string part = input.substr(start);
	StringUtil::Trim(part);
	if (!part.empty()) {
		result.push_back(part);
	}

	return result;
}

LogicalType HMSUtils::TypeToLogicalType(ClientContext &context, const string &type_text) {
	string type_text_trimmed = type_text;
	StringUtil::Trim(type_text_trimmed);
	auto type_text_lower = StringUtil::Lower(type_text_trimmed);

	if (type_text_lower == hms::hive_type::TINYINT) {
		return LogicalType::TINYINT;
	} else if (type_text_lower == hms::hive_type::SMALLINT) {
		return LogicalType::SMALLINT;
	} else if (type_text_lower == hms::hive_type::BIGINT) {
		return LogicalType::BIGINT;
	} else if (type_text_lower == hms::hive_type::INT) {
		return LogicalType::INTEGER;
	} else if (type_text_lower == hms::hive_type::LONG) {
		return LogicalType::BIGINT;
	} else if (type_text_lower == hms::hive_type::STRING ||
	           StringUtil::StartsWith(type_text_lower, hms::hive_type::VARCHAR) ||
	           StringUtil::StartsWith(type_text_lower, hms::hive_type::CHAR)) {
		return LogicalType::VARCHAR;
	} else if (type_text_lower == hms::hive_type::DOUBLE) {
		return LogicalType::DOUBLE;
	} else if (type_text_lower == hms::hive_type::FLOAT) {
		return LogicalType::FLOAT;
	} else if (type_text_lower == hms::hive_type::BOOLEAN) {
		return LogicalType::BOOLEAN;
	} else if (type_text_lower == hms::hive_type::TIMESTAMP) {
		return LogicalType::TIMESTAMP;
	} else if (type_text_lower == hms::hive_type::BINARY) {
		return LogicalType::BLOB;
	} else if (type_text_lower == hms::hive_type::DATE) {
		return LogicalType::DATE;
	} else if (type_text_lower == hms::hive_type::VOID) {
		return LogicalType::SQLNULL;
	} else if (StringUtil::StartsWith(type_text_lower, hms::hive_type::DECIMAL_PREFIX)) {
		constexpr size_t DECIMAL_PREFIX_LEN = 8; // Length of "decimal("
		size_t spec_end = type_text_trimmed.find(')');
		if (spec_end != string::npos && spec_end > DECIMAL_PREFIX_LEN) {
			size_t sep = type_text_trimmed.find(',', DECIMAL_PREFIX_LEN);
			if (sep != string::npos && sep > DECIMAL_PREFIX_LEN && sep < spec_end) {
				auto prec_str = type_text_trimmed.substr(DECIMAL_PREFIX_LEN, sep - DECIMAL_PREFIX_LEN);
				auto scale_str = type_text_trimmed.substr(sep + 1, spec_end - sep - 1);
				StringUtil::Trim(prec_str);
				StringUtil::Trim(scale_str);
				try {
					uint8_t prec = Cast::Operation<string_t, uint8_t>(prec_str);
					uint8_t scale = Cast::Operation<string_t, uint8_t>(scale_str);
					return LogicalType::DECIMAL(prec, scale);
				} catch (const std::exception &) {
					// Invalid decimal specification, fall through to error at end
				}
			}
		}
	} else if (StringUtil::StartsWith(type_text_lower, hms::hive_type::ARRAY_PREFIX)) {
		constexpr size_t ARRAY_PREFIX_LEN = 6; // Length of "array<"
		size_t type_end = type_text_trimmed.rfind('>');
		if (type_end != string::npos && type_end > ARRAY_PREFIX_LEN) {
			auto child_type_str = type_text_trimmed.substr(ARRAY_PREFIX_LEN, type_end - ARRAY_PREFIX_LEN);
			StringUtil::Trim(child_type_str);
			if (!child_type_str.empty()) {
				auto child_type = HMSUtils::TypeToLogicalType(context, child_type_str);
				return LogicalType::LIST(child_type);
			}
		}
	} else if (StringUtil::StartsWith(type_text_lower, hms::hive_type::MAP_PREFIX)) {
		constexpr size_t MAP_PREFIX_LEN = 4; // Length of "map<"
		size_t type_end = type_text_trimmed.rfind('>');
		if (type_end != string::npos && type_end > MAP_PREFIX_LEN) {
			auto children_str = type_text_trimmed.substr(MAP_PREFIX_LEN, type_end - MAP_PREFIX_LEN);
			StringUtil::Trim(children_str);
			auto parts = SplitNested(children_str, ',');
			if (parts.size() != 2) {
				throw NotImplementedException(
				    "Invalid map specification: expected 2 types (key, value), found %d in '%s'",
				    static_cast<int>(parts.size()), type_text_trimmed.c_str());
			}
			auto key_type = HMSUtils::TypeToLogicalType(context, parts[0]);
			auto value_type = HMSUtils::TypeToLogicalType(context, parts[1]);
			return LogicalType::MAP(key_type, value_type);
		}
	} else if (StringUtil::StartsWith(type_text_lower, hms::hive_type::STRUCT_PREFIX)) {
		constexpr size_t STRUCT_PREFIX_LEN = 7; // Length of "struct<"
		size_t type_end = type_text_trimmed.rfind('>');
		if (type_end != string::npos && type_end > STRUCT_PREFIX_LEN) {
			child_list_t<LogicalType> children;
			auto children_str = type_text_trimmed.substr(STRUCT_PREFIX_LEN, type_end - STRUCT_PREFIX_LEN);
			StringUtil::Trim(children_str);

			if (!children_str.empty()) {
				auto parts = SplitNested(children_str, ',');
				for (const auto &part : parts) {
					if (part.empty()) {
						continue;
					}
					size_t type_sep = part.find(':');
					if (type_sep == string::npos) {
						throw NotImplementedException(
						    "Invalid struct field specification: expected 'name:type', got '%s'", part.c_str());
					}
					auto child_name = part.substr(0, type_sep);
					StringUtil::Trim(child_name);
					if (child_name.empty()) {
						throw NotImplementedException("Empty field name in struct specification: '%s'", part.c_str());
					}
					auto child_type_str = part.substr(type_sep + 1);
					StringUtil::Trim(child_type_str);
					auto child_type = HMSUtils::TypeToLogicalType(context, child_type_str);
					children.push_back({child_name, child_type});
				}
			}

			if (!children.empty()) {
				return LogicalType::STRUCT(children);
			}
		}
	}

	throw NotImplementedException("Unsupported or unrecognized Hive type: '%s'", type_text.c_str());
}

//! Map Avro-incompatible types to Avro-compatible equivalents
//!
//! The Avro format has limited type support compared to Hive/Parquet:
//! - DATE is stored as INT32 (days since Unix epoch 1970-01-01)
//! - TIMESTAMP is stored as INT64 (microseconds since Unix epoch)
//! - DECIMAL may have precision/scale limitations
//!
//! This function converts HMS schema types to match what the DuckDB Avro extension
//! actually returns when reading Avro files, preventing type mismatch errors.
//!
//! References:
//! - Avro spec: https://avro.apache.org/docs/++version++/specification/
//! - DuckDB Avro extension issue: https://github.com/duckdb/duckdb-avro/issues/6
LogicalType HMSUtils::MapTypeForAvro(const LogicalType &hms_type) {
	switch (hms_type.id()) {
	case LogicalTypeId::DATE:
		// Avro stores dates as INT32 (days since epoch)
		// The Avro extension currently returns INTEGER instead of DATE
		return LogicalType::INTEGER;

	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
		// Avro stores timestamps as INT64 (microseconds since epoch)
		// The Avro extension may return BIGINT instead of TIMESTAMP
		return LogicalType::BIGINT;

	case LogicalTypeId::LIST: {
		// Recursively map element types
		auto &child_type = ListType::GetChildType(hms_type);
		auto mapped_child = MapTypeForAvro(child_type);
		return LogicalType::LIST(mapped_child);
	}

	case LogicalTypeId::STRUCT: {
		// Recursively map struct field types
		auto &child_types = StructType::GetChildTypes(hms_type);
		child_list_t<LogicalType> mapped_children;
		for (const auto &child : child_types) {
			auto mapped_child_type = MapTypeForAvro(child.second);
			mapped_children.push_back(make_pair(child.first, mapped_child_type));
		}
		return LogicalType::STRUCT(mapped_children);
	}

	case LogicalTypeId::MAP: {
		// Recursively map key and value types
		auto &key_type = MapType::KeyType(hms_type);
		auto &value_type = MapType::ValueType(hms_type);
		auto mapped_key = MapTypeForAvro(key_type);
		auto mapped_value = MapTypeForAvro(value_type);
		return LogicalType::MAP(mapped_key, mapped_value);
	}

	default:
		// Other types (INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR, etc.) are compatible
		return hms_type;
	}
}

string HMSUtils::LogicalTypeToHiveType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return hms::hive_type::TINYINT;
	case LogicalTypeId::SMALLINT:
		return hms::hive_type::SMALLINT;
	case LogicalTypeId::INTEGER:
		return hms::hive_type::INT;
	case LogicalTypeId::BIGINT:
		return hms::hive_type::BIGINT;
	case LogicalTypeId::FLOAT:
		return hms::hive_type::FLOAT;
	case LogicalTypeId::DOUBLE:
		return hms::hive_type::DOUBLE;
	case LogicalTypeId::BOOLEAN:
		return hms::hive_type::BOOLEAN;
	case LogicalTypeId::VARCHAR:
		return hms::hive_type::STRING;
	case LogicalTypeId::BLOB:
		return hms::hive_type::BINARY;
	case LogicalTypeId::DATE:
		return hms::hive_type::DATE;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
		return hms::hive_type::TIMESTAMP;
	case LogicalTypeId::LIST: {
		auto &child = ListType::GetChildType(type);
		return string(hms::hive_type::ARRAY_PREFIX) + LogicalTypeToHiveType(child) + ">";
	}
	case LogicalTypeId::STRUCT: {
		auto &children = StructType::GetChildTypes(type);
		string res = hms::hive_type::STRUCT_PREFIX;
		bool first = true;
		for (auto &c : children) {
			if (!first) {
				res += ",";
			}
			res += c.first + ":" + LogicalTypeToHiveType(c.second);
			first = false;
		}
		res += ">";
		return res;
	}
	case LogicalTypeId::MAP: {
		auto &kt = MapType::KeyType(type);
		auto &vt = MapType::ValueType(type);
		return string(hms::hive_type::MAP_PREFIX) + LogicalTypeToHiveType(kt) + "," + LogicalTypeToHiveType(vt) + ">";
	}
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		return StringUtil::Format("decimal(%d,%d)", width, scale);
	}
	default:
		return hms::hive_type::STRING; // Fallback
	}
}

Apache::Hadoop::Hive::Table HMSUtils::BuildThriftTable(ClientContext &context, HMSSchemaEntry &schema,
                                                       BoundCreateTableInfo &info, const string &format,
                                                       const string &warehouse_location) {
	Apache::Hadoop::Hive::Table table;
	auto &base = info.Base();
	table.tableName = base.table;
	table.dbName = schema.name;

	// Storage descriptor
	Apache::Hadoop::Hive::StorageDescriptor sd;

	// Columns
	sd.cols.clear();
	for (auto it = base.columns.Logical().begin(); it != base.columns.Logical().end(); ++it) {
		auto &col = *it;
		Apache::Hadoop::Hive::FieldSchema fs;
		fs.name = col.GetName();
		// Map DuckDB LogicalType to Hive type string
		fs.type = HMSUtils::LogicalTypeToHiveType(col.GetType());
		fs.comment = col.Comment().ToString();
		sd.cols.push_back(fs);
	}

	// Default to Parquet if unspecified
	string fmt = StringUtil::Lower(format);
	StringUtil::Trim(fmt);
	if (fmt.empty()) {
		fmt = hms::format::PARQUET;
	}

	if (fmt == hms::format::PARQUET) {
		sd.inputFormat = hms::storage_format::PARQUET_INPUT_FORMAT;
		sd.outputFormat = hms::storage_format::PARQUET_OUTPUT_FORMAT;
		Apache::Hadoop::Hive::SerDeInfo serde;
		serde.serializationLib = hms::storage_format::PARQUET_SERDE_LIB;
		sd.serdeInfo = serde;
	} else if (fmt == hms::format::CSV) {
		// Minimal CSV mapping (not fully compatible with Spark)
		sd.inputFormat = hms::storage_format::TEXT_INPUT_FORMAT;
		sd.outputFormat = hms::storage_format::TEXT_OUTPUT_FORMAT;
		Apache::Hadoop::Hive::SerDeInfo serde;
		serde.serializationLib = hms::storage_format::TEXT_SERDE_LIB;
		sd.serdeInfo = serde;
	} else if (fmt == hms::format::DELTA) {
		// Delta is not supported to be managed by Hive metastore directly here
		throw NotImplementedException("CREATE TABLE USING DELTA is not supported by this HMS extension");
	} else {
		throw NotImplementedException("Unsupported table format for CREATE TABLE USING: %s", format.c_str());
	}

	// Copy table properties from CreateInfo tags if present (e.g., LOCATION)
	for (auto &kv : base.tags) {
		table.parameters[kv.first] = kv.second;
	}

	// Determine storage location: explicit table parameter 'location' or warehouse default
	auto loc_it = table.parameters.find("location");
	if (loc_it != table.parameters.end() && !loc_it->second.empty()) {
		sd.location = loc_it->second;
	} else if (!warehouse_location.empty()) {
		// Build a default managed location under the warehouse: <warehouse>/<db>/<table>
		string default_loc = warehouse_location;
		if (!StringUtil::EndsWith(default_loc, "/")) {
			default_loc += "/";
		}
		default_loc += schema.name + "/" + base.table;
		sd.location = default_loc;
		table.parameters["location"] = sd.location;
	} else {
		throw BinderException("CREATE TABLE requires a LOCATION to be provided or a WAREHOUSE_LOCATION to be set when "
		                      "attaching the HMS catalog");
	}

	// Assign final storage descriptor (with location) to table
	table.sd = sd;

	// Set table type: MANAGED_TABLE if the location is under the configured warehouse, else EXTERNAL_TABLE
	if (!warehouse_location.empty()) {
		string wl = warehouse_location;
		if (!StringUtil::EndsWith(wl, "/")) {
			wl += "/";
		}
		if (StringUtil::StartsWith(sd.location, wl)) {
			table.tableType = hms::table::MANAGED_TABLE;
		} else {
			table.tableType = hms::table::EXTERNAL_TABLE;
		}
	} else {
		// No warehouse configured: treat as EXTERNAL
		table.tableType = hms::table::EXTERNAL_TABLE;
	}

	return table;
}

} // namespace duckdb
