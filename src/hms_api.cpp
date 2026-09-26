#include "hms_api.hpp"
#include "hms_constants.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

// Table objects are fetched in batches: one get_table_objects_by_name call for thousands of tables can exceed the
// metastore's message size limits.
static constexpr idx_t TABLE_OBJECTS_BATCH_SIZE = 100;

// Partitions are fetched in batches as well: a table can have hundreds of thousands of them, and each one carries a
// full storage descriptor.
static constexpr idx_t PARTITION_BATCH_SIZE = 200;

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
	DUCKDB_LOG_DEBUG(ctx, "hive_metastore rpc=get_all_databases");
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

vector<string> HMSAPI::GetTableNames(ClientContext &ctx, const string &schema, const string &endpoint) {
	DUCKDB_LOG_DEBUG(ctx, "hive_metastore rpc=get_all_tables db=%s", schema);
	auto client = GetClient(endpoint);
	return client->GetAllTables(schema);
}

case_insensitive_map_t<vector<string>> HMSAPI::GetAllTableNames(ClientContext &ctx, const string &endpoint) {
	DUCKDB_LOG_DEBUG(ctx, "hive_metastore rpc=get_table_meta");
	auto client = GetClient(endpoint);
	case_insensitive_map_t<vector<string>> result;
	for (const auto &table : client->GetTableMeta("*", "*")) {
		result[table.dbName].push_back(table.tableName);
	}
	return result;
}

unique_ptr<HMSAPITable> HMSAPI::GetTable(ClientContext &ctx, const string &schema, const string &table,
                                         const string &endpoint) {
	DUCKDB_LOG_DEBUG(ctx, "hive_metastore rpc=get_table db=%s table=%s", schema, table);
	auto client = GetClient(endpoint);
	Apache::Hadoop::Hive::Table thrift_table;
	if (!client->TryGetTable(schema, table, thrift_table)) {
		return nullptr;
	}
	return make_uniq<HMSAPITable>(FromThrift(thrift_table));
}

vector<HMSAPITable> HMSAPI::GetTables(ClientContext &ctx, const string &schema, const vector<string> &table_names,
                                      const string &endpoint) {
	vector<HMSAPITable> result;
	if (table_names.empty()) {
		return result;
	}
	auto client = GetClient(endpoint);
	for (idx_t offset = 0; offset < table_names.size(); offset += TABLE_OBJECTS_BATCH_SIZE) {
		auto end = MinValue<idx_t>(offset + TABLE_OBJECTS_BATCH_SIZE, table_names.size());
		vector<string> batch;
		for (idx_t i = offset; i < end; i++) {
			batch.push_back(table_names[i]);
		}
		DUCKDB_LOG_DEBUG(ctx, "hive_metastore rpc=get_table_objects_by_name db=%s tables=%d", schema, batch.size());
		for (const auto &thrift_table : client->GetTableObjects(schema, batch)) {
			result.push_back(FromThrift(thrift_table));
		}
	}
	return result;
}

vector<string> HMSAPI::GetPartitionNames(ClientContext &ctx, const string &schema, const string &table,
                                         const string &endpoint) {
	DUCKDB_LOG_DEBUG(ctx, "hive_metastore rpc=get_partition_names db=%s table=%s", schema, table);
	auto client = GetClient(endpoint);
	return client->GetPartitionNames(schema, table);
}

vector<HMSAPIPartition> HMSAPI::GetPartitions(ClientContext &ctx, const string &schema, const string &table,
                                              const vector<string> &partition_names, const string &endpoint) {
	vector<HMSAPIPartition> result;
	if (partition_names.empty()) {
		return result;
	}
	auto client = GetClient(endpoint);
	for (idx_t offset = 0; offset < partition_names.size(); offset += PARTITION_BATCH_SIZE) {
		auto end = MinValue<idx_t>(offset + PARTITION_BATCH_SIZE, partition_names.size());
		vector<string> batch;
		for (idx_t i = offset; i < end; i++) {
			batch.push_back(partition_names[i]);
		}
		DUCKDB_LOG_DEBUG(ctx, "hive_metastore rpc=get_partitions_by_names db=%s table=%s partitions=%d", schema, table,
		                 batch.size());
		for (const auto &partition : client->GetPartitionsByNames(schema, table, batch)) {
			// The response carries no partition name and is not required to preserve the request order, so a
			// caller that wants one composes it from the table's partition keys and these values.
			HMSAPIPartition converted;
			converted.values = vector<string>(partition.values.begin(), partition.values.end());
			converted.location = partition.sd.location;
			result.push_back(std::move(converted));
		}
	}
	return result;
}

HMSAPITable HMSAPI::FromThrift(const Apache::Hadoop::Hive::Table &ht) {
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
	return t;
}

static bool SameColumns(const vector<HMSAPIColumnDefinition> &left, const vector<HMSAPIColumnDefinition> &right) {
	if (left.size() != right.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.size(); i++) {
		if (left[i].name != right[i].name || left[i].type != right[i].type) {
			return false;
		}
	}
	return true;
}

// The table parameters that format detection, schema resolution or the scan read
static map<string, string> DefinitionParameters(const map<string, string> &parameters) {
	map<string, string> result;
	for (const auto &entry : parameters) {
		if (entry.first == hms::spark_param::PROVIDER || entry.first == hms::table_type::TABLE_TYPE ||
		    entry.first == "location" || entry.first == "path" ||
		    StringUtil::StartsWith(entry.first, hms::spark_param::SCHEMA)) {
			result.insert(entry);
		}
	}
	return result;
}

bool HMSAPITable::HasSameDefinition(const HMSAPITable &other) const {
	return name == other.name && db_name == other.db_name && table_type == other.table_type &&
	       storage_location == other.storage_location && input_format == other.input_format &&
	       output_format == other.output_format && serialization_lib == other.serialization_lib &&
	       serde_parameters == other.serde_parameters && SameColumns(columns, other.columns) &&
	       SameColumns(partition_keys, other.partition_keys) &&
	       DefinitionParameters(parameters) == DefinitionParameters(other.parameters);
}

void HMSAPI::CreateTable(ClientContext &ctx, const Apache::Hadoop::Hive::Table &table, const string &endpoint) {
	DUCKDB_LOG_DEBUG(ctx, "hive_metastore rpc=create_table db=%s table=%s", table.dbName, table.tableName);
	auto client = GetClient(endpoint);
	client->CreateTable(table);
}

bool HMSAPI::DropTable(ClientContext &ctx, const string &db_name, const string &table_name, const string &endpoint) {
	DUCKDB_LOG_DEBUG(ctx, "hive_metastore rpc=drop_table db=%s table=%s", db_name, table_name);
	auto client = GetClient(endpoint);
	// delete_data=false: HMS extension manages the catalog only, not the underlying
	// storage. The user controls file lifecycle via their object store.
	return client->DropTable(db_name, table_name, /*delete_data=*/false);
}

} // namespace duckdb
