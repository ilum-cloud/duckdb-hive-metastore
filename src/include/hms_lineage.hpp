//===----------------------------------------------------------------------===//
// DuckDB Hive Metastore Extension
//
// File: hms_lineage.hpp
// Description: Lineage metadata structures for HMS tables.
//              This header is designed to be included by other extensions
//              (like openlineage) without pulling in Thrift dependencies.
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

namespace duckdb {

//! Structure containing lineage metadata for HMS tables
struct HMSLineageInfo {
	std::string fully_qualified_name; // catalog.schema.table
	std::string storage_location;     // s3://..., hdfs://..., file://..., etc.
	std::string table_type;           // MANAGED_TABLE, EXTERNAL_TABLE, etc.
	std::string catalog_name;         // HMS catalog name
	std::string schema_name;          // HMS schema (database) name
	std::string table_name;           // Table name

	HMSLineageInfo() = default;
	HMSLineageInfo(const std::string &catalog, const std::string &schema, const std::string &table)
	    : catalog_name(catalog), schema_name(schema), table_name(table) {
		fully_qualified_name = catalog + "." + schema + "." + table;
	}
};

} // namespace duckdb
