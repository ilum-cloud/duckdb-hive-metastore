#pragma once

#include "duckdb.hpp"
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
};

class HMSAPI {
public:
	static vector<HMSAPISchema> GetSchemas(ClientContext &ctx, const string &endpoint);
	static vector<HMSAPITable> GetTablesInSchema(ClientContext &ctx, const string &schema, const string &endpoint);

	// Create a table in HMS
	static void CreateTable(ClientContext &ctx, const Apache::Hadoop::Hive::Table &table, const string &endpoint);

	// Helper to get or create a client based on endpoint
	static unique_ptr<HMSClient> GetClient(const string &endpoint);
};

} // namespace duckdb
