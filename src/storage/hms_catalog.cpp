#include "storage/hms_catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/database_size.hpp"
#include "storage/hms_schema_entry.hpp"
#include "storage/hms_transaction.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "storage/hms_table_entry.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

HMSCatalog::HMSCatalog(AttachedDatabase &db_p, const string &internal_name, AttachOptions &attach_options,
                       string endpoint_p, const string &default_schema, const string &warehouse_location,
                       string catalog_name_p)
    : Catalog(db_p), internal_name(internal_name), access_mode(attach_options.access_mode),
      endpoint(std::move(endpoint_p)), warehouse_location(warehouse_location), schemas(*this),
      default_schema(default_schema), catalog_name(std::move(catalog_name_p)) {
}

HMSCatalog::~HMSCatalog() = default;

void HMSCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> HMSCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		DropInfo try_drop;
		try_drop.type = CatalogType::SCHEMA_ENTRY;
		try_drop.name = info.schema;
		try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
		try_drop.cascade = false;
		schemas.DropEntry(transaction.GetContext(), try_drop);
	}
	return schemas.CreateSchema(transaction.GetContext(), info);
}

void HMSCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	return schemas.DropEntry(context, info);
}

void HMSCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<HMSSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> HMSCatalog::LookupSchema(CatalogTransaction transaction,
                                                          const EntryLookupInfo &schema_lookup,
                                                          OnEntryNotFound if_not_found) {
	if (schema_lookup.GetEntryName() == DEFAULT_SCHEMA && default_schema != DEFAULT_SCHEMA) {
		if (default_schema.empty()) {
			throw InvalidInputException(
			    "Default schema for catalog '%s' not found. This means auto-detection of default schema failed. Please "
			    "specify a DEFAULT_SCHEMA on ATTACH: `ATTACH '..' (TYPE hive_metastore, DEFAULT_SCHEMA 'my_schema')`",
			    GetName());
		}
		return GetSchema(transaction, default_schema, if_not_found);
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_lookup.GetEntryName());
	if (!entry) {
		if (if_not_found != OnEntryNotFound::RETURN_NULL) {
			throw BinderException("Schema with name \"%s\" not found", schema_lookup.GetEntryName());
		}
		return nullptr;
	}
	return &entry->Cast<SchemaCatalogEntry>();
}

bool HMSCatalog::InMemory() {
	return false;
}

string HMSCatalog::GetDBPath() {
	return internal_name;
}

string HMSCatalog::GetDefaultSchema() const {
	return default_schema;
}

DatabaseSize HMSCatalog::GetDatabaseSize(ClientContext &context) {
	if (default_schema.empty()) {
		throw InvalidInputException("Attempting to fetch the database size - but no database was provided "
		                            "in the connection string");
	}
	DatabaseSize size;
	return size;
}

void HMSCatalog::ClearCache() {
	schemas.ClearEntries();
}

PhysicalOperator &HMSCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("HMSCatalog PlanCreateTableAs");
}

PhysicalOperator &HMSCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                         optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("HMSCatalog PlanInsert");
}

PhysicalOperator &HMSCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                         PhysicalOperator &plan) {
	throw NotImplementedException("HMSCatalog PlanDelete");
}

PhysicalOperator &HMSCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op) {
	throw NotImplementedException("HMSCatalog PlanDelete");
}

PhysicalOperator &HMSCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                         PhysicalOperator &plan) {
	throw NotImplementedException("HMSCatalog PlanUpdate");
}

unique_ptr<LogicalOperator> HMSCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                        unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("HMSCatalog BindCreateIndex");
}

} // namespace duckdb
