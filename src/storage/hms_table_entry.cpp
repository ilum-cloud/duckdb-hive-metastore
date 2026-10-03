#include "storage/hms_catalog.hpp"
#include "storage/hms_csv_options.hpp"
#include "storage/hms_multi_file_reader.hpp"
#include "storage/hms_partition_resolver.hpp"
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
#include "duckdb/common/operator/cast_operators.hpp"
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
	// Delta and Iceberg keep their own partition metadata and are read by their own extensions
	return !table.partition_keys.empty() && !format.IsDelta() && !format.IsIceberg() &&
	       (format.IsParquet() || format.IsCSV() || format.IsAvro());
}

shared_ptr<HMSPartitionCache> HMSTableEntry::GetPartitionCache(ClientContext &context) {
	lock_guard<mutex> guard(partition_cache_lock);
	if (partition_cache) {
		return partition_cache;
	}
	HMSPartitionSchema partition_schema;
	partition_schema.database = schema.name;
	partition_schema.table = name;
	for (auto &partition_key : table_data->partition_keys) {
		partition_schema.names.push_back(partition_key.name);
		partition_schema.hms_types.push_back(partition_key.type);
		partition_schema.types.push_back(HMSUtils::TypeToLogicalType(context, partition_key.type));
	}
	partition_schema.format = hms::FormatDetector::Detect(*table_data);
	partition_schema.table_data = make_shared_ptr<HMSAPITable>(*table_data);
	auto path_result =
	    hms::PathUtils::NormalizeScanPath(table_data->storage_location, *table_data, partition_schema.format);
	partition_schema.table_location = path_result.scan_path;
	while (StringUtil::EndsWith(partition_schema.table_location, "/")) {
		partition_schema.table_location.pop_back();
	}
	partition_schema.root_glob = hms::PathUtils::BuildGlobPattern(path_result.scan_path, partition_schema.format,
	                                                              partition_schema.format.is_partitioned);
	partition_cache = make_shared_ptr<HMSPartitionCache>(catalog.Cast<HMSCatalog>(), std::move(partition_schema));
	return partition_cache;
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
		// The partitions are not fetched here: the scan selects them once the filters are known
		auto partition_cache = GetPartitionCache(context);
		// The scan must produce exactly this entry's columns: the catalog maps them to the scan by position
		vector<string> column_names;
		vector<LogicalType> column_types;
		for (auto &column : GetColumns().Logical()) {
			column_names.push_back(column.Name());
			column_types.push_back(column.Type());
		}
		// Parquet and Avro files name their columns, so the scan produces the table's columns and matches each file to
		// them by name. CSV files do not: the CSV scan takes its columns from the columns parameter set below, which is
		// built from the same list as the table's, and gets the partition columns appended.
		auto bind_to_table_columns = !format_result.IsCSV();
		scan_function.function_info = make_shared_ptr<HMSScanFunctionInfo>(
		    std::move(partition_cache), std::move(column_names), std::move(column_types), bind_to_table_columns);
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

		// The partition columns are not in the files of a partitioned table: the partition reader appends them. Spark's
		// schema lists them, last, so leave them out of what the files are parsed as.
		case_insensitive_set_t partition_columns;
		if (InjectsPartitionColumns(*table_data, format_result)) {
			for (const auto &partition_key : table_data->partition_keys) {
				partition_columns.insert(partition_key.name);
			}
		}
		if (has_spark_schema) {
			// Successfully parsed Spark schema, use it.
			// The types in 'columns' are already DuckDB LogicalType strings from ParseSparkSchema
			for (const auto &col : columns) {
				if (partition_columns.count(col.name)) {
					continue;
				}
				struct_children.push_back(make_pair(col.name, Value(col.type)));
			}
		} else {
			// Fallback to standard HMS columns
			for (const auto &col : table_data->columns) {
				if (partition_columns.count(col.name)) {
					continue;
				}
				// Convert HMS type to DuckDB LogicalType string
				auto duckdb_type = HMSUtils::TypeToLogicalType(context, col.type);
				struct_children.push_back(make_pair(col.name, Value(duckdb_type.ToString())));
			}
		}
		param_map["columns"] = Value::STRUCT(std::move(struct_children));
		if (!partition_columns.empty()) {
			// The partition reader supplies the partition values. Left on, DuckDB's own detection would walk every file
			// while binding, before the filters that select the partitions are known.
			param_map["hive_partitioning"] = Value::BOOLEAN(false);
		}

		AddCSVReadOptions(context, *table_data, has_spark_schema, !partition_columns.empty(), param_map);
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
