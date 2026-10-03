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

unique_ptr<HMSSchemaEntry> HMSSchemaSet::MakeSchemaEntry(const string &name) {
	CreateSchemaInfo info;
	info.schema = name;
	info.internal = IsInternalTable(GetHMSCatalog().catalog_name, name);
	auto schema_entry = make_uniq<HMSSchemaEntry>(catalog, info);
	HMSAPISchema schema;
	schema.schema_name = name;
	schema_entry->schema_data = make_uniq<HMSAPISchema>(schema);
	return schema_entry;
}

HMSLoadResult HMSSchemaSet::LoadEntry(ClientContext &context, const string &name, optional_ptr<CatalogEntry> cached) {
	// One get_database call. Resolving a schema used to list every database, which a metastore with thousands of them
	// (or one that checks permissions per database) answers slowly, while a query only needs the one it names.
	auto schema = HMSAPI::GetSchema(context, name, GetHMSCatalog().endpoint);
	if (!schema) {
		return HMSLoadResult::Missing();
	}
	if (cached) {
		// A schema entry holds nothing but its name; reusing it also keeps its cached tables
		return HMSLoadResult::KeepCached();
	}
	return HMSLoadResult::NewEntry(MakeSchemaEntry(schema->schema_name));
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
	// The names come from the listing, so the databases exist: nothing more to ask the metastore
	for (auto &request : requests) {
		if (request.second) {
			// A schema entry holds nothing but its name; reusing it also keeps its cached tables
			on_loaded(request.first, HMSLoadResult::KeepCached());
			continue;
		}
		on_loaded(request.first, HMSLoadResult::NewEntry(MakeSchemaEntry(request.first)));
	}
}

optional_ptr<CatalogEntry> HMSSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	throw NotImplementedException("Schema creation");
}

} // namespace duckdb
