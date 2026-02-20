#include "storage/hms_schema_set.hpp"
#include "storage/hms_catalog.hpp"
#include "hms_api.hpp"
#include "storage/hms_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

HMSSchemaSet::HMSSchemaSet(Catalog &catalog) : HMSCatalogSet(catalog) {
}

static bool IsInternalTable(const string &catalog, const string &schema) {
	if (schema == "information_schema") {
		return true;
	}
	return false;
}

void HMSSchemaSet::LoadEntries(ClientContext &context) {

	auto &hms_catalog = catalog.Cast<HMSCatalog>();

	// Use HMS API to fetch schemas
	auto schemas = HMSAPI::GetSchemas(context, hms_catalog.endpoint);

	for (const auto &schema : schemas) {
		CreateSchemaInfo info;
		info.schema = schema.schema_name;
		info.internal = IsInternalTable(hms_catalog.catalog_name, schema.schema_name);
		auto schema_entry = make_uniq<HMSSchemaEntry>(catalog, info);

		schema_entry->schema_data = make_uniq<HMSAPISchema>(schema);
		CreateEntry(std::move(schema_entry));
	}
}

optional_ptr<CatalogEntry> HMSSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	throw NotImplementedException("Schema creation");
}

} // namespace duckdb
