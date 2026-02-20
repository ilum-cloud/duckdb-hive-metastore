#include "hms_api.hpp"
#include "hms_utils.hpp"

#include "storage/hms_catalog.hpp"
#include "storage/hms_table_set.hpp"
#include "storage/hms_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/hms_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

HMSTableSet::HMSTableSet(HMSSchemaEntry &schema) : HMSInSchemaSet(schema) {
}

static ColumnDefinition CreateColumnDefinition(ClientContext &context, HMSAPIColumnDefinition &coldef) {
	// HMS types (e.g. "int", "string") are compatible with HMSUtils::TypeToLogicalType parser
	return {coldef.name, HMSUtils::TypeToLogicalType(context, coldef.type)};
}

void HMSTableSet::LoadEntries(ClientContext &context) {
	auto &transaction = HMSTransaction::Get(context, catalog);

	auto &hms_catalog = catalog.Cast<HMSCatalog>();

	// TODO: handle out-of-order columns using position property

	auto tables = HMSAPI::GetTablesInSchema(context, schema.name, hms_catalog.endpoint);

	for (auto &table : tables) {
		// Validate that the table's database matches our schema name
		// This should always hold if the HMS API is working correctly
		if (schema.name != table.db_name) {
			// Log but continue - this is a data inconsistency issue, not a crash-worthy error
			// The HMS API returned a table for the wrong database
			continue;
		}
		CreateTableInfo info;
		info.table = table.name;

		// Check if this is an Avro table
		bool is_avro =
		    StringUtil::Contains(table.input_format, "Avro") || StringUtil::Contains(table.serialization_lib, "Avro");

		// Try to discover dynamic schema for Parquet/Delta/Iceberg tables
		vector<ColumnDefinition> discovered_columns;
		if (HMSTableEntry::DiscoverDynamicSchema(context, catalog, schema, table, discovered_columns)) {
			// Use the discovered schema from Parquet/Delta/Iceberg
			for (auto &col : discovered_columns) {
				info.columns.AddColumn(std::move(col));
			}
		} else {
			// Try to parse Spark schema for other tables
			vector<HMSAPIColumnDefinition> spark_columns;
			if (HMSUtils::ParseSparkSchema(table.parameters, spark_columns)) {
				for (auto &col : spark_columns) {
					// col.type is already a DuckDB LogicalType string (e.g. "INTEGER", "STRUCT(...)")
					auto logical_type = TransformStringToLogicalType(col.type, context);

					// Apply Avro type mapping if this is an Avro table
					if (is_avro) {
						logical_type = HMSUtils::MapTypeForAvro(logical_type);
					}

					info.columns.AddColumn(ColumnDefinition(col.name, logical_type));
				}
			} else {
				// Fallback to standard HMS columns
				for (auto &col : table.columns) {
					auto logical_type = HMSUtils::TypeToLogicalType(context, col.type);

					// Apply Avro type mapping if this is an Avro table
					if (is_avro) {
						logical_type = HMSUtils::MapTypeForAvro(logical_type);
					}

					info.columns.AddColumn(ColumnDefinition(col.name, logical_type));
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

		auto table_entry = make_uniq<HMSTableEntry>(catalog, schema, info);
		table_entry->table_data = make_uniq<HMSAPITable>(table);
		CreateEntry(std::move(table_entry));
	}
}

optional_ptr<CatalogEntry> HMSTableSet::RefreshTable(ClientContext &context, const string &table_name) {
	auto table_info = GetTableInfo(context, schema, table_name);
	if (!table_info) {
		throw IOException("Failed to fetch table info for '%s.%s': table info is null", schema.name.c_str(),
		                  table_name.c_str());
	}
	auto table_entry = make_uniq<HMSTableEntry>(catalog, schema, *table_info);
	auto table_ptr = table_entry.get();
	CreateEntry(std::move(table_entry));
	return table_ptr;
}

unique_ptr<HMSTableInfo> HMSTableSet::GetTableInfo(ClientContext &context, HMSSchemaEntry &schema,
                                                   const string &table_name) {
	auto &hms_catalog = catalog.Cast<HMSCatalog>();
	auto client = HMSAPI::GetClient(hms_catalog.endpoint);

	Apache::Hadoop::Hive::Table ht;
	try {
		ht = client->GetTable(schema.name, table_name);
	} catch (const std::exception &ex) {
		throw IOException("Failed to fetch table info for '%s.%s': %s", schema.name.c_str(), table_name.c_str(),
		                  ex.what());
	}

	auto result = make_uniq<HMSTableInfo>(schema, table_name);
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

	result->create_info->table = table_name;
	result->create_info->columns = CreateTableInfo().columns; // initialize empty then fill

	result->create_info->sql = "";

	// Replace tags by constructing a fresh map from parameters
	result->create_info->tags = InsertionOrderPreservingMap<string>();
	for (auto &kv : t.parameters) {
		result->create_info->tags[kv.first] = kv.second;
	}

	result->create_info->comment = Value();

	// attach table_data BEFORE schema resolution (needed for format detection)
	result->table_data = make_uniq<HMSAPITable>(std::move(t));

	// Resolve schema using the same logic as LoadEntries
	// Check if this is an Avro table
	bool is_avro = StringUtil::Contains(result->table_data->input_format, "Avro") ||
	               StringUtil::Contains(result->table_data->serialization_lib, "Avro");

	// Try to discover dynamic schema for Parquet/Delta/Iceberg tables
	vector<ColumnDefinition> discovered_columns;
	if (HMSTableEntry::DiscoverDynamicSchema(context, catalog, schema, *result->table_data, discovered_columns)) {
		// Use the discovered schema from Parquet/Delta/Iceberg
		result->create_info->columns = CreateTableInfo().columns; // clear and re-fill
		for (auto &col : discovered_columns) {
			result->create_info->columns.AddColumn(std::move(col));
		}
	} else {
		// Try to parse Spark schema for other tables
		vector<HMSAPIColumnDefinition> spark_columns;
		if (HMSUtils::ParseSparkSchema(result->table_data->parameters, spark_columns)) {
			result->create_info->columns = CreateTableInfo().columns; // clear and re-fill
			for (auto &col : spark_columns) {
				// col.type is already a DuckDB LogicalType string (e.g. "INTEGER", "STRUCT(...)")
				auto logical_type = TransformStringToLogicalType(col.type, context);

				// Apply Avro type mapping if this is an Avro table
				if (is_avro) {
					logical_type = HMSUtils::MapTypeForAvro(logical_type);
				}

				result->create_info->columns.AddColumn(ColumnDefinition(col.name, logical_type));
			}
		} else {
			// Fallback to standard HMS columns
			result->create_info->columns = CreateTableInfo().columns; // clear and re-fill
			for (auto &c : result->table_data->columns) {
				auto logical_type = HMSUtils::TypeToLogicalType(context, c.type);

				// Apply Avro type mapping if this is an Avro table
				if (is_avro) {
					logical_type = HMSUtils::MapTypeForAvro(logical_type);
				}

				result->create_info->columns.AddColumn(ColumnDefinition(c.name, logical_type));
			}
		}
	}

	// Add HMS metadata as tags for inter-extension communication (e.g., with OpenLineage)
	// This allows other extensions to access HMS metadata without code dependencies
	result->create_info->tags["hms_storage_location"] = result->table_data->storage_location;
	result->create_info->tags["hms_table_type"] = result->table_data->table_type;
	result->create_info->tags["hms_input_format"] = result->table_data->input_format;
	result->create_info->tags["hms_output_format"] = result->table_data->output_format;
	result->create_info->tags["hms_serialization_lib"] = result->table_data->serialization_lib;

	// ensure create_info has columns
	result->create_info->table = table_name;

	// Return the HMSTableInfo containing create_info and table_data

	return result;
}

optional_ptr<CatalogEntry> HMSTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
	auto &base = info.Base();

	// Basic checks: reject unsupported features
	if (!info.query && base.columns.empty()) {
		throw BinderException("CREATE TABLE must specify columns or be CREATE TABLE AS");
	}

	if (base.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT ||
	    base.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		throw NotImplementedException("ON CONFLICT clauses are not supported for HMS CREATE TABLE");
	}

	// Extract format: prefer tag 'format', fallback to provider tag or empty (default PARQUET)
	string format;
	auto fmt_it = base.tags.find("format");
	if (fmt_it != base.tags.end()) {
		format = fmt_it->second;
	} else {
		auto prov_it = base.tags.find("provider");
		if (prov_it != base.tags.end()) {
			format = prov_it->second;
		}
	}

	// Require either an explicit 'location' tag or a warehouse_location configured on the HMS catalog
	auto &hms_catalog = catalog.Cast<HMSCatalog>();
	auto loc_tag_it = base.tags.find("location");
	if (loc_tag_it == base.tags.end() || loc_tag_it->second.empty()) {
		if (hms_catalog.warehouse_location.empty()) {
			throw BinderException("CREATE TABLE requires a LOCATION to be provided or the HMS catalog to be attached "
			                      "with a WAREHOUSE_LOCATION");
		}
	}

	// Build Thrift Table
	auto thrift_table = HMSUtils::BuildThriftTable(context, schema, info, format, hms_catalog.warehouse_location);

	// Call HMS API to create
	HMSAPI::CreateTable(context, thrift_table, hms_catalog.endpoint);

	// Fetch table info first to ensure we have complete data before creating entry
	// This avoids creating an incomplete entry if GetTableInfo fails
	auto table_info = GetTableInfo(context, schema, base.table);
	if (!table_info) {
		throw IOException("Failed to fetch table info after creating table '%s'", base.table);
	}

	// Register the new table entry in the catalog
	CreateTableInfo create_info;
	create_info.table = base.table;
	// Populate columns using public ColumnList API
	for (auto it = base.columns.Logical().begin(); it != base.columns.Logical().end(); ++it) {
		create_info.columns.AddColumn((*it).Copy());
	}

	auto table_entry = make_uniq<HMSTableEntry>(catalog, schema, create_info);
	// Set table_data from the fetched table info
	if (table_info->table_data) {
		table_entry->table_data = make_uniq<HMSAPITable>(*table_info->table_data);
	}

	auto ptr = table_entry.get();
	CreateEntry(std::move(table_entry));
	return ptr;
}

void HMSTableSet::AlterTable(ClientContext &context, RenameTableInfo &info) {
	throw NotImplementedException("HMSTableSet::AlterTable");
}

void HMSTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info) {
	throw NotImplementedException("HMSTableSet::AlterTable");
}

void HMSTableSet::AlterTable(ClientContext &context, AddColumnInfo &info) {
	throw NotImplementedException("HMSTableSet::AlterTable");
}

void HMSTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info) {
	throw NotImplementedException("HMSTableSet::AlterTable");
}

void HMSTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter) {
	throw NotImplementedException("HMSTableSet::AlterTable");
}

} // namespace duckdb
