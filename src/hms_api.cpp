#include "hms_api.hpp"

namespace duckdb {

unique_ptr<HMSClient> HMSAPI::GetClient(const string &endpoint) {
	// Parse host and port from endpoint
	// Expected format: "thrift://hostname:port" or "hostname:port"
	string parsed_endpoint = endpoint;
	string prefix = "thrift://";
	if (StringUtil::StartsWith(parsed_endpoint, prefix)) {
		parsed_endpoint = parsed_endpoint.substr(prefix.length());
	}

	auto parts = StringUtil::Split(parsed_endpoint, ":");
	if (parts.size() != 2) {
		throw InvalidInputException("Invalid HMS endpoint format. Expected 'hostname:port', got: %s", endpoint);
	}

	string host = parts[0];
	int port;
	try {
		port = std::stoi(parts[1]);
	} catch (const std::invalid_argument &) {
		throw InvalidInputException("Invalid port in HMS endpoint: %s", parts[1]);
	} catch (const std::out_of_range &) {
		throw InvalidInputException("Port number out of range in HMS endpoint: %s", parts[1]);
	}

	auto client = make_uniq<HMSClient>(host, port);
	client->Open();
	return client;
}

vector<HMSAPISchema> HMSAPI::GetSchemas(ClientContext &ctx, const string &endpoint) {
	auto client = GetClient(endpoint);
	auto db_names = client->GetAllDatabases();

	vector<HMSAPISchema> schemas;
	for (const auto &name : db_names) {
		// We could fetch details for each DB, but listing names is often enough for "ScanSchemas"
		// and fetching details one-by-one might be slow if there are many DBs.
		// For now, let's just use the names.
		HMSAPISchema schema;
		schema.schema_name = name;
		schemas.push_back(schema);
	}
	return schemas;
}

vector<HMSAPITable> HMSAPI::GetTablesInSchema(ClientContext &ctx, const string &schema, const string &endpoint) {
	auto client = GetClient(endpoint);
	auto table_names = client->GetAllTables(schema);

	// Fetch details for all tables (potentially in batches if GetTableObjects supports it)
	// The current HMSClient::GetTableObjects implementation takes a list of names.
	auto hive_tables = client->GetTableObjects(schema, table_names);

	vector<HMSAPITable> result;
	for (const auto &ht : hive_tables) {
		HMSAPITable t;
		t.name = ht.tableName;
		t.db_name = ht.dbName;
		t.table_type = ht.tableType;
		t.storage_location = ht.sd.location;
		t.input_format = ht.sd.inputFormat;
		t.output_format = ht.sd.outputFormat;
		t.serialization_lib = ht.sd.serdeInfo.serializationLib;
		t.serde_parameters = ht.sd.serdeInfo.parameters;
		t.parameters = ht.parameters;

		// For Iceberg tables, use metadata_location parameter instead of sd.location
		// Iceberg stores the actual metadata file path in table properties
		// However, iceberg_scan expects the table root directory, not the metadata file
		auto metadata_location_it = ht.parameters.find("metadata_location");
		if (metadata_location_it != ht.parameters.end()) {
			// This is an Iceberg table with explicit metadata location
			string metadata_path = metadata_location_it->second;

			// Extract table root: remove /metadata/... from the path
			// e.g., s3://bucket/path/table/metadata/v1.metadata.json -> s3://bucket/path/table
			auto metadata_dir_pos = metadata_path.find("/metadata/");
			if (metadata_dir_pos != string::npos) {
				t.storage_location = metadata_path.substr(0, metadata_dir_pos);
			} else {
				// Fallback: use the metadata_location as-is if we can't find /metadata/
				t.storage_location = metadata_path;
			}
		}

		for (const auto &col : ht.sd.cols) {
			HMSAPIColumnDefinition c;
			c.name = col.name;
			c.type = col.type;
			c.comment = col.comment;
			t.columns.push_back(c);
		}

		for (const auto &pk : ht.partitionKeys) {
			HMSAPIColumnDefinition c;
			c.name = pk.name;
			c.type = pk.type;
			c.comment = pk.comment;
			t.partition_keys.push_back(c);
		}

		result.push_back(t);
	}

	return result;
}

void HMSAPI::CreateTable(ClientContext &ctx, const Apache::Hadoop::Hive::Table &table, const string &endpoint) {
	auto client = GetClient(endpoint);
	client->CreateTable(table);
}

} // namespace duckdb
