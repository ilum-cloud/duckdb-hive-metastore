#pragma once

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "hms_client.hpp"

namespace duckdb {

struct HMSAPISchema {
	string schema_name;
	string description;
};

struct HMSAPIColumnDefinition {
	string name;
	string type;
	string comment;
};

struct HMSAPITable {
	string name;
	string db_name;
	string table_type;
	string storage_location;
	string input_format;
	string output_format;
	string serialization_lib;
	map<string, string> serde_parameters;
	map<string, string> parameters;
	vector<HMSAPIColumnDefinition> columns;
	vector<HMSAPIColumnDefinition> partition_keys;

	//! Whether both describe the same DuckDB table. Compares everything the catalog entry and the scan are built from
	//! and ignores volatile table parameters such as statistics or the metadata file of the latest Iceberg commit.
	bool HasSameDefinition(const HMSAPITable &other) const;
};

//! One partition of a table, as the metastore stores it
struct HMSAPIPartition {
	//! Partition values, positionally aligned with HMSAPITable::partition_keys
	vector<string> values;
	//! The partition's own storage location, which may be anywhere
	string location;
};

class HMSAPI {
public:
	static vector<HMSAPISchema> GetSchemas(ClientContext &ctx, const string &endpoint);
	//! Names of all tables in a schema
	static vector<string> GetTableNames(ClientContext &ctx, const string &schema, const string &endpoint);
	//! Names of all tables in all schemas, keyed by schema name (a single metastore call)
	static case_insensitive_map_t<vector<string>> GetAllTableNames(ClientContext &ctx, const string &endpoint);
	//! Metadata of one table, or nullptr if the table does not exist
	static unique_ptr<HMSAPITable> GetTable(ClientContext &ctx, const string &schema, const string &table,
	                                        const string &endpoint);
	//! Metadata of the given tables; tables that do not exist are left out
	static vector<HMSAPITable> GetTables(ClientContext &ctx, const string &schema, const vector<string> &table_names,
	                                     const string &endpoint);
	static HMSAPITable FromThrift(const Apache::Hadoop::Hive::Table &table);

	//! Names of all partitions of a table; empty if it has none registered
	static vector<string> GetPartitionNames(ClientContext &ctx, const string &schema, const string &table,
	                                        const string &endpoint);
	//! The named partitions, with the location each one is stored at
	static vector<HMSAPIPartition> GetPartitions(ClientContext &ctx, const string &schema, const string &table,
	                                             const vector<string> &partition_names, const string &endpoint);

	// Create a table in HMS
	static void CreateTable(ClientContext &ctx, const Apache::Hadoop::Hive::Table &table, const string &endpoint);

	// Drop a table from HMS (metadata only; storage files are not removed).
	// Returns true if dropped, false if the table did not exist. Other failures throw.
	static bool DropTable(ClientContext &ctx, const string &db_name, const string &table_name, const string &endpoint);

	// Helper to get or create a client based on endpoint
	static unique_ptr<HMSClient> GetClient(const string &endpoint);
};

} // namespace duckdb
