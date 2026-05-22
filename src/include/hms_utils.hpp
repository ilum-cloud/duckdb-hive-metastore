//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hms_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "hms_api.hpp"

namespace duckdb {

class HMSSchemaEntry;

class HMSUtils {
public:
	static LogicalType TypeToLogicalType(ClientContext &context, const string &columnDefinition);

	//! Tries to parse Spark's JSON schema from table parameters
	//! Returns true if successful and populates columns, false otherwise
	static bool ParseSparkSchema(const map<string, string> &parameters, vector<HMSAPIColumnDefinition> &columns);

	// Convert DuckDB LogicalType to a Hive-compatible type string (e.g. int, bigint, string, array<int>)
	static string LogicalTypeToHiveType(const LogicalType &type);

	// duckdb-avro on DuckDB 1.4 does not resolve Avro `date` and `timestamp-micros`
	// logical types: read_avro returns INT32 / INT64 instead of DATE / TIMESTAMP, so the
	// HMS-declared catalog type must be downcast to match the scan output. Newer
	// (1.5.1+) duckdb-avro resolves these natively; on those versions this function
	// is unused.
	static LogicalType MapTypeForAvro(const LogicalType &hms_type);

	// Build a Thrift Table object from a BoundCreateTableInfo / CreateTableInfo
	static Apache::Hadoop::Hive::Table BuildThriftTable(ClientContext &context, HMSSchemaEntry &schema,
	                                                    BoundCreateTableInfo &info, const string &format,
	                                                    const string &warehouse_location);
};

} // namespace duckdb
