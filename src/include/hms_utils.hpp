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

	// Build a Thrift Table object from a BoundCreateTableInfo / CreateTableInfo
	static Apache::Hadoop::Hive::Table BuildThriftTable(ClientContext &context, HMSSchemaEntry &schema,
	                                                    BoundCreateTableInfo &info, const string &format,
	                                                    const string &warehouse_location);
};

} // namespace duckdb
