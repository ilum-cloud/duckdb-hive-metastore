#include "storage/hms_catalog.hpp"
#include "storage/hms_multi_file_reader.hpp"
#include "storage/hms_schema_entry.hpp"
#include "storage/hms_table_entry.hpp"
#include "hms_api.hpp"
#include "hms_constants.hpp"
#include "hms_format_detector.hpp"
#include "hms_path_utils.hpp"
#include "storage/hms_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/string_util.hpp"
#include "hms_utils.hpp"

namespace duckdb {

// Helper function to ensure the required extension is loaded for Delta/Iceberg tables
static void AutoLoadExtensionIfNeeded(ClientContext &context, const hms::FormatDetectionResult &format) {
	if (!hms::FormatDetector::RequiresExtension(format.format)) {
		return;
	}

	const char *extension_name = hms::FormatDetector::GetExtensionName(format.format);
	if (extension_name) {
		ExtensionHelper::TryAutoLoadExtension(context, extension_name);
		// Extension loading failed, but we don't want to prevent the table from being discovered
		// The actual data access will fail later with a more informative error
	}
}

HMSTableEntry::HMSTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = false;
	// Copy tags from CreateTableInfo to CatalogEntry
	// This ensures inter-extension communication (e.g., with OpenLineage) works
	for (auto &tag : info.tags) {
		this->tags[tag.first] = tag.second;
	}
}

unique_ptr<HMSTableEntry> HMSTableEntry::Build(ClientContext &context, Catalog &catalog, SchemaCatalogEntry &schema,
                                               HMSAPITable table) {
	auto table_name = table.name;
	try {
		CreateTableInfo info;
		info.table = table_name;
		auto schema_source = HMSSchemaSource::HMS_COLUMNS;

		// Try to discover dynamic schema for Parquet/Delta/Iceberg tables
		vector<ColumnDefinition> discovered_columns;
		if (DiscoverDynamicSchema(context, catalog, schema, table, discovered_columns)) {
			// Use the discovered schema from Parquet/Delta/Iceberg
			schema_source = HMSSchemaSource::FILES;
			for (auto &col : discovered_columns) {
				info.columns.AddColumn(std::move(col));
			}
		} else {
			// Try to parse Spark schema for other tables
			vector<HMSAPIColumnDefinition> spark_columns;
			if (HMSUtils::ParseSparkSchema(table.parameters, spark_columns)) {
				schema_source = HMSSchemaSource::SPARK_SCHEMA;
				for (auto &col : spark_columns) {
					// col.type is already a DuckDB LogicalType string (e.g. "INTEGER", "STRUCT(...)")
					auto logical_type = TransformStringToLogicalType(col.type, context);
					info.columns.AddColumn(ColumnDefinition(col.name, logical_type));
				}
			} else {
				// Fallback to standard HMS columns
				for (auto &col : table.columns) {
					auto logical_type = HMSUtils::TypeToLogicalType(context, col.type);
					info.columns.AddColumn(ColumnDefinition(col.name, logical_type));
				}
			}
		}

		// A partitioned table keeps its partition values in the metastore, not in the data files, so the scan fills
		// those columns itself. Add them here, in the order the metastore declares them (Hive puts them last).
		if (InjectsPartitionColumns(table, hms::FormatDetector::Detect(table))) {
			for (auto &partition_key : table.partition_keys) {
				auto partition_type = HMSUtils::TypeToLogicalType(context, partition_key.type);
				if (info.columns.ColumnExists(partition_key.name)) {
					auto &existing = info.columns.GetColumnMutable(partition_key.name);
					existing.SetType(partition_type);
				} else {
					info.columns.AddColumn(ColumnDefinition(partition_key.name, partition_type));
				}
			}
		}

		// Add HMS metadata as tags for inter-extension communication (e.g., with OpenLineage)
		// This allows other extensions to access HMS metadata without code dependencies
		info.tags["hms_storage_location"] = table.storage_location;
		info.tags["hms_table_type"] = table.table_type;
		info.tags["hms_input_format"] = table.input_format;
		info.tags["hms_output_format"] = table.output_format;
		info.tags["hms_serialization_lib"] = table.serialization_lib;

		auto entry = make_uniq<HMSTableEntry>(catalog, schema, info);
		entry->internal = schema.internal;
		entry->schema_source = schema_source;
		entry->table_data = make_uniq<HMSAPITable>(std::move(table));
		return entry;
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERRUPT) {
			throw;
		}
		error.Throw(
		    StringUtil::Format("Failed to load table \"%s.%s\" from Hive Metastore: ", schema.name, table_name));
	}
}

bool HMSTableEntry::HasSameColumns(const HMSTableEntry &other) const {
	auto &columns = GetColumns();
	auto &other_columns = other.GetColumns();
	if (columns.LogicalColumnCount() != other_columns.LogicalColumnCount()) {
		return false;
	}
	for (idx_t i = 0; i < columns.LogicalColumnCount(); i++) {
		auto &column = columns.GetColumn(LogicalIndex(i));
		auto &other_column = other_columns.GetColumn(LogicalIndex(i));
		if (column.Name() != other_column.Name() || column.Type() != other_column.Type()) {
			return false;
		}
	}
	return true;
}

bool HMSTableEntry::InjectsPartitionColumns(const HMSAPITable &table, const hms::FormatDetectionResult &format) {
	// Delta and Iceberg keep their own partition metadata and are read by their own extensions. CSV and Avro are not
	// covered yet: their partition columns are missing today as well, and their scans need separate work.
	return !table.partition_keys.empty() && !format.IsDelta() && !format.IsIceberg() && format.IsParquet();
}

//! The name the metastore uses for a partition, rebuilt from the partition keys and values
static string PartitionDisplayName(const HMSAPITable &table, const vector<string> &values) {
	string result;
	for (idx_t i = 0; i < table.partition_keys.size() && i < values.size(); i++) {
		if (!result.empty()) {
			result += "/";
		}
		result += table.partition_keys[i].name + "=" + values[i];
	}
	return result;
}

static Value PartitionValue(const string &raw, const LogicalType &type, const string &column,
                            const string &partition_name, const string &table_name) {
	// What Hive stores for a row whose partition column is NULL. Unlike a path segment this value is not escaped,
	// and the literal string "NULL" is a value of its own.
	if (raw == "__HIVE_DEFAULT_PARTITION__") {
		return Value(type);
	}
	Value result;
	string error;
	if (!Value(raw).DefaultTryCastAs(type, result, &error)) {
		throw InvalidInputException(
		    "Partition \"%s\" of table \"%s\": cannot read the value '%s' of partition column \"%s\" as %s",
		    partition_name, table_name, raw, column, type.ToString());
	}
	return result;
}

shared_ptr<const HMSPartitionPlan> HMSTableEntry::GetPartitionPlan(ClientContext &context) {
	auto &hms_catalog = catalog.Cast<HMSCatalog>();
	auto generation = hms_catalog.GetCacheGeneration();
	{
		lock_guard<mutex> guard(partition_lock);
		if (partition_plan && partition_plan_generation == generation &&
		    std::chrono::steady_clock::now() - partition_plan_loaded_at < hms_catalog.GetMetadataCacheTTL()) {
			return partition_plan;
		}
	}

	auto format = hms::FormatDetector::Detect(*table_data);
	auto plan = make_shared_ptr<HMSPartitionPlan>();
	plan->source = HMSPartitionSource::PATH;
	for (auto &partition_key : table_data->partition_keys) {
		plan->names.push_back(partition_key.name);
		plan->types.push_back(HMSUtils::TypeToLogicalType(context, partition_key.type));
	}

	auto mode = hms_catalog.GetPartitionMode();
	if (mode != HMSPartitionMode::PATH) {
		vector<HMSAPIPartition> partitions;
		try {
			auto partition_names = HMSAPI::GetPartitionNames(context, schema.name, name, hms_catalog.endpoint);
			partitions = HMSAPI::GetPartitions(context, schema.name, name, partition_names, hms_catalog.endpoint);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			lock_guard<mutex> guard(partition_lock);
			if (error.Type() == ExceptionType::INTERRUPT || !partition_plan) {
				throw;
			}
			// The metastore could not be reached: keep scanning the partitions we know until the TTL expires again
			DUCKDB_LOG_WARNING(context,
			                   "hive_metastore: failed to refresh the partitions of \"%s.%s\", using the cached "
			                   "partition list: %s",
			                   schema.name, name, error.RawMessage());
			partition_plan_loaded_at = std::chrono::steady_clock::now();
			partition_plan_generation = generation;
			return partition_plan;
		}
		for (auto &partition : partitions) {
			HMSScanPartition scan_partition;
			scan_partition.name = PartitionDisplayName(*table_data, partition.values);
			if (partition.location.empty() || partition.values.size() != table_data->partition_keys.size()) {
				DUCKDB_LOG_WARNING(context,
				                   "hive_metastore: skipping partition \"%s\" of \"%s.%s\": it has no location or "
				                   "does not match the partition columns",
				                   scan_partition.name, schema.name, name);
				continue;
			}
			// A partition location gets the same treatment as a table location: placeholder stripping, s3a/oss/cos
			// rewriting and the http endpoint handling
			auto path_result = hms::PathUtils::NormalizeScanPath(partition.location, *table_data, format);
			scan_partition.location = path_result.scan_path;
			scan_partition.scan_location = hms::PathUtils::BuildPartitionGlobPattern(path_result.scan_path, format);
			scan_partition.fallback_scan_location =
			    hms::PathUtils::BuildPartitionFallbackGlobPattern(path_result.scan_path);
			if (path_result.needs_s3_config) {
				plan->needs_s3_config = true;
				plan->s3_endpoint = path_result.s3_endpoint;
			}
			for (idx_t i = 0; i < table_data->partition_keys.size(); i++) {
				scan_partition.values.push_back(
				    PartitionValue(partition.values[i], plan->types[i], plan->names[i], scan_partition.name, name));
			}
			plan->partitions.push_back(std::move(scan_partition));
		}
		if (!plan->partitions.empty()) {
			plan->source = HMSPartitionSource::HMS;
		}
	}

	if (plan->source != HMSPartitionSource::HMS && mode == HMSPartitionMode::HMS) {
		throw InvalidInputException("Table \"%s.%s\" declares partition columns but has no partition registered in "
		                            "the Hive Metastore (PARTITION_MODE 'hms')",
		                            schema.name, name);
	}

	lock_guard<mutex> guard(partition_lock);
	partition_plan = std::move(plan);
	partition_plan_loaded_at = std::chrono::steady_clock::now();
	partition_plan_generation = generation;
	return partition_plan;
}

unique_ptr<BaseStatistics> HMSTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

optional_ptr<Catalog> HMSTableEntry::GetInternalCatalog() {
	if (!internal_attached_database) {
		return nullptr;
	}
	return internal_attached_database->GetCatalog();
}

void HMSTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                          ClientContext &) {
	throw NotImplementedException("BindUpdateConstraints");
}

bool HMSTableEntry::DiscoverDynamicSchema(ClientContext &context, Catalog &catalog, SchemaCatalogEntry &schema,
                                          HMSAPITable &table_data, vector<ColumnDefinition> &columns) {
	// Detect format
	auto format_result = hms::FormatDetector::Detect(table_data);

	if (!format_result.IsDelta() && !format_result.IsIceberg() && !format_result.IsParquet()) {
		return false; // Not a dynamic schema table
	}

	// Autoload the required extension
	AutoLoadExtensionIfNeeded(context, format_result);

	// Normalize the scan path
	auto path_result = hms::PathUtils::NormalizeScanPath(table_data.storage_location, table_data, format_result);

	// Get the scan function for this table type
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &system_schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);

	TableFunction scan_function;
	const char *scan_func_name = hms::FormatDetector::GetScanFunctionName(format_result.format);
	if (!scan_func_name) {
		return false;
	}

	auto catalog_entry = system_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, scan_func_name);
	if (!catalog_entry) {
		return false; // Extension not loaded
	}
	auto &function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	scan_function = function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

	// Build glob pattern for directory-based scans (similar to GetScanFunction)
	string scan_path = path_result.scan_path;
	scan_path = hms::PathUtils::BuildGlobPattern(scan_path, format_result, format_result.is_partitioned);

	// Bind the function to discover the schema
	try {
		vector<Value> inputs = {Value(scan_path)};
		named_parameter_map_t param_map;
		// Discover the columns the files actually hold. Partition columns are added from the metastore afterwards,
		// which also keeps the result independent of how the partition directories happen to be named.
		param_map["hive_partitioning"] = Value::BOOLEAN(false);
		vector<LogicalType> return_types;
		vector<string> names;
		TableFunctionRef empty_ref;

		TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, scan_function,
		                                  empty_ref);

		auto bind_result = scan_function.bind(context, bind_input, return_types, names);

		// Convert the discovered types and names to column definitions
		if (return_types.size() != names.size() || return_types.empty()) {
			return false;
		}

		columns.clear();
		for (idx_t i = 0; i < names.size(); i++) {
			columns.push_back(ColumnDefinition(names[i], return_types[i]));
		}

		return true;
	} catch (const std::exception &ex) {
		// An interrupted query must stop, not fall back to the HMS schema
		if (ErrorData(ex).Type() == ExceptionType::INTERRUPT) {
			throw;
		}
		// Binding failed - this is expected when the extension is not loaded
		// or the table path is invalid. Return false to fall back to HMS schema.
		// Note: We catch by const reference to avoid slicing and to potentially
		// log the error message in debug builds.
		return false;
	}
}

TableFunction HMSTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &db = DatabaseInstance::GetDatabase(context);

	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &system_schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);

	if (!table_data) {
		throw InternalException("HMSTableEntry::GetScanFunction called with null table_data for table '%s'", name);
	}

	// Detect table format
	auto format_result = hms::FormatDetector::Detect(*table_data);

	// Autoload the required extension
	AutoLoadExtensionIfNeeded(context, format_result);

	// Get the scan function name
	const char *scan_func_name = hms::FormatDetector::GetScanFunctionName(format_result.format);
	if (!scan_func_name) {
		throw NotImplementedException("Table '%s' has unsupported format: %s / %s", table_data->name,
		                              table_data->input_format, table_data->serialization_lib);
	}

	// Get the scan function
	auto catalog_entry = system_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, scan_func_name);
	if (!catalog_entry) {
		string extension_name = scan_func_name;
		// Remove "_scan" suffix for error message
		auto scan_pos = extension_name.find("_scan");
		if (scan_pos != string::npos) {
			extension_name = extension_name.substr(0, scan_pos);
		}
		throw InvalidInputException("Function '%s' not found. The '%s' extension failed to autoload.", scan_func_name,
		                            extension_name.c_str());
	}
	auto &function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	TableFunction scan_function = function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

	// Normalize the scan path
	auto path_result = hms::PathUtils::NormalizeScanPath(table_data->storage_location, *table_data, format_result);
	string scan_path = path_result.scan_path;

	// Configure S3 if needed (for MinIO compatibility)
	if (path_result.needs_s3_config) {
		Value endpoint_val = Value(path_result.s3_endpoint);
		Value use_ssl_val = Value(false);
		Value url_style_val = Value("path");
		context.db->config.SetOption("s3_endpoint", endpoint_val);
		context.db->config.SetOption("s3_use_ssl", use_ssl_val);
		context.db->config.SetOption("s3_url_style", url_style_val);
	}

	// Build glob pattern for directory-based scans
	scan_path = hms::PathUtils::BuildGlobPattern(scan_path, format_result, format_result.is_partitioned);

	// Set the path as input to table function
	vector<Value> inputs = {Value(scan_path)};

	if (table_data->storage_location.find("file://") != 0) {
		// S3 credentials handling would go here.
	}
	named_parameter_map_t param_map;

	// For Iceberg tables, add allow_moved_paths for better path handling
	if (format_result.IsIceberg()) {
		param_map["allow_moved_paths"] = Value::BOOLEAN(true);
	}

	// For partitioned tables, scan the partitions the metastore registered, at the locations it recorded, and fill
	// the partition columns from the values it holds. DuckDB's own hive partitioning is not used: it can only read
	// values out of key=value directory names and rejects any other layout.
	if (InjectsPartitionColumns(*table_data, format_result)) {
		auto partition_plan = GetPartitionPlan(context);
		if (partition_plan->needs_s3_config) {
			Value endpoint_val = Value(partition_plan->s3_endpoint);
			Value use_ssl_val = Value(false);
			Value url_style_val = Value("path");
			context.db->config.SetOption("s3_endpoint", endpoint_val);
			context.db->config.SetOption("s3_use_ssl", use_ssl_val);
			context.db->config.SetOption("s3_url_style", url_style_val);
		}
		scan_function.function_info = make_shared_ptr<HMSScanFunctionInfo>(partition_plan);
		scan_function.get_multi_file_reader = HMSMultiFileReader::CreateInstance;
	}

	// For CSV/Text tables, we must provide the schema to avoid type mismatch crashes
	// and to ensure correct parsing (Hive tables usually have no header).
	if (!format_result.IsDelta() && !format_result.IsIceberg() && format_result.IsCSV()) {
		child_list_t<Value> struct_children;

		// Try to parse Spark schema first for CSV tables, as HMS type definitions for CSV
		// (via USING CSV) might be incorrect (e.g. all array<string> or similar).
		vector<HMSAPIColumnDefinition> columns;
		bool has_spark_schema = HMSUtils::ParseSparkSchema(table_data->parameters, columns);

		if (has_spark_schema) {
			// Successfully parsed Spark schema, use it.
			// The types in 'columns' are already DuckDB LogicalType strings from ParseSparkSchema
			for (const auto &col : columns) {
				struct_children.push_back(make_pair(col.name, Value(col.type)));
			}
		} else {
			// Fallback to standard HMS columns
			for (const auto &col : table_data->columns) {
				// Convert HMS type to DuckDB LogicalType string
				auto duckdb_type = HMSUtils::TypeToLogicalType(context, col.type);
				struct_children.push_back(make_pair(col.name, Value(duckdb_type.ToString())));
			}
		}
		param_map["columns"] = Value::STRUCT(std::move(struct_children));

		// Handle delimiter and other CSV options
		// Default Hive delimiter is \001 (Ctrl-A)
		string delim = string(1, hms::constants::DEFAULT_HIVE_DELIMITER);
		auto it = table_data->serde_parameters.find(hms::serde_param::FIELD_DELIM);
		if (it != table_data->serde_parameters.end()) {
			delim = it->second;
		}

		bool is_default_hive_delim = (delim == string(1, hms::constants::DEFAULT_HIVE_DELIMITER));

		bool is_spark_csv = false;
		auto csv_provider_it = table_data->parameters.find(hms::spark_param::PROVIDER);
		if (csv_provider_it != table_data->parameters.end() &&
		    StringUtil::CIEquals(csv_provider_it->second, hms::format::CSV)) {
			is_spark_csv = true;
		}

		// Logic to determine CSV parsing mode:
		// 1. If it's a Spark CSV table (provider=csv), we enable auto_detect.
		//    If the delimiter is default (\x01), we ignore it to let the sniffer find the real one (likely comma).
		// 2. If it has a Spark schema AND a non-default delimiter (e.g. comma), it's likely a compatible CSV table.
		//    We enable auto_detect.
		// 3. Otherwise (Standard Hive table, usually LazySimpleSerDe), we disable auto_detect and enforce strict
		// parsing.

		if (is_spark_csv || (has_spark_schema && !is_default_hive_delim)) {
			// Spark CSV or compatible (e.g. comma separated)
			// Enable auto_detect to allow sniffing of quotes, headers, etc.
			param_map["auto_detect"] = Value::BOOLEAN(true);

			if (it != table_data->serde_parameters.end()) {
				// Set sep if explicitly defined, UNLESS it's the default hive delimiter for a Spark CSV
				// (because Spark CSVs often leave SerDe delim as default \x01 while actual file is comma)
				if (!is_spark_csv || !is_default_hive_delim) {
					param_map["sep"] = Value(delim);
				}
			}
		} else {
			// Strict Hive behavior (LazySimpleSerDe) or Default Hive
			param_map["header"] = Value::BOOLEAN(false); // Hive tables usually have no header
			param_map["sep"] = Value(delim);
			param_map["quote"] = Value("");  // Disable quoting
			param_map["escape"] = Value(""); // Disable escaping

			// Explicitly disable auto detection for strict Hive tables
			param_map["auto_detect"] = Value::BOOLEAN(false);
		}

		// If strict mode is failing, we might want to relax it, but for now let's try with correct delimiters
		param_map["null_padding"] = Value::BOOLEAN(true);  // Hive treats missing columns as null
		param_map["ignore_errors"] = Value::BOOLEAN(true); // Best effort
	}

	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, scan_function,
	                                  empty_ref);

	auto result = scan_function.bind(context, bind_input, return_types, names);
	bind_data = std::move(result);

	// Create a wrapper function with a custom name to preserve HMS table identity
	// This allows the OpenLineage optimizer to identify this as an HMS table
	// The metadata is now read from table entry tags instead of being encoded here
	TableFunction hms_function = scan_function;
	string fully_qualified_name = catalog.GetName() + "." + schema.name + "." + name;
	// URL encode to safely handle special characters like '##'
	string encoded_name = StringUtil::URLEncode(fully_qualified_name);
	hms_function.name = "hms_scan##" + encoded_name;
	hms_function.verify_serialization = false; // Custom dynamic function cannot be serialized

	return hms_function;
}

virtual_column_map_t HMSTableEntry::GetVirtualColumns() const {
	//! FIXME: requires changes in core to be able to delegate this
	return TableCatalogEntry::GetVirtualColumns();
}

vector<column_t> HMSTableEntry::GetRowIdColumns() const {
	//! FIXME: requires changes in core to be able to delegate this
	return TableCatalogEntry::GetRowIdColumns();
}

TableStorageInfo HMSTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	// TODO fill info
	return result;
}

HMSLineageInfo HMSTableEntry::GetLineageInfo() const {
	HMSLineageInfo info;

	// Set basic table information
	info.catalog_name = catalog.GetName();
	info.schema_name = schema.name;
	info.table_name = name;
	info.fully_qualified_name = info.catalog_name + "." + info.schema_name + "." + info.table_name;

	// Extract HMS-specific metadata if available
	if (table_data) {
		info.storage_location = table_data->storage_location;
		info.table_type = table_data->table_type;
	}

	return info;
}

} // namespace duckdb
