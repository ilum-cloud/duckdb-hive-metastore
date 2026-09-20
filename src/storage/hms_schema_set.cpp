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

vector<string> HMSSchemaSet::ListEntryNames(ClientContext &context) {
	vector<string> names;
	for (const auto &schema : HMSAPI::GetSchemas(context, GetHMSCatalog().endpoint)) {
		names.push_back(schema.schema_name);
	}
	return names;
}

void HMSSchemaSet::LoadEntries(ClientContext &context, const vector<pair<string, optional_ptr<CatalogEntry>>> &requests,
                               const std::function<void(const string &name, HMSLoadResult result)> &on_loaded) {
	auto &hms_catalog = GetHMSCatalog();
	for (auto &request : requests) {
		if (request.second) {
			// A schema entry holds nothing but its name; reusing it also keeps its cached tables
			on_loaded(request.first, HMSLoadResult::KeepCached());
			continue;
		}
		CreateSchemaInfo info;
		info.schema = request.first;
		info.internal = IsInternalTable(hms_catalog.catalog_name, request.first);
		auto schema_entry = make_uniq<HMSSchemaEntry>(catalog, info);

		HMSAPISchema schema;
		schema.schema_name = request.first;
		schema_entry->schema_data = make_uniq<HMSAPISchema>(schema);
		on_loaded(request.first, HMSLoadResult::NewEntry(std::move(schema_entry)));
	}
}

optional_ptr<CatalogEntry> HMSSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	throw NotImplementedException("Schema creation");
}

} // namespace duckdb
