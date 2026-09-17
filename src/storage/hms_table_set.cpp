#include "hms_api.hpp"
#include "hms_format_detector.hpp"
#include "hms_utils.hpp"

#include "storage/hms_catalog.hpp"
#include "storage/hms_table_set.hpp"
#include "storage/hms_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/hms_schema_entry.hpp"

namespace duckdb {

HMSTableSet::HMSTableSet(HMSSchemaEntry &schema) : HMSCatalogSet(schema.ParentCatalog()), schema(schema) {
}

HMSLoadResult HMSTableSet::LoadEntry(ClientContext &context, const string &name, optional_ptr<CatalogEntry> cached) {
	auto table = HMSAPI::GetTable(context, schema.name, name, GetHMSCatalog().endpoint);
	if (!table) {
		return HMSLoadResult::Missing();
	}
	return Revalidate(context, std::move(*table), cached);
}

vector<string> HMSTableSet::ListEntryNames(ClientContext &context) {
	return HMSAPI::GetTableNames(context, schema.name, GetHMSCatalog().endpoint);
}

void HMSTableSet::LoadEntries(ClientContext &context, const vector<pair<string, optional_ptr<CatalogEntry>>> &requests,
                              const std::function<void(const string &name, HMSLoadResult result)> &on_loaded) {
	vector<string> names;
	case_insensitive_map_t<optional_ptr<CatalogEntry>> cached_entries;
	for (auto &request : requests) {
		names.push_back(request.first);
		cached_entries[request.first] = request.second;
	}
	auto tables = HMSAPI::GetTables(context, schema.name, names, GetHMSCatalog().endpoint);
	for (auto &table : tables) {
		if (context.interrupted) {
			throw InterruptException();
		}
		auto cached = cached_entries.find(table.name);
		if (cached == cached_entries.end() || !StringUtil::CIEquals(table.db_name, schema.name)) {
			// Not requested, or returned for another database
			continue;
		}
		on_loaded(cached->first, Revalidate(context, std::move(table), cached->second));
	}
}

HMSLoadResult HMSTableSet::Revalidate(ClientContext &context, HMSAPITable table, optional_ptr<CatalogEntry> cached) {
	optional_ptr<HMSTableEntry> cached_table;
	if (cached) {
		cached_table = &cached->Cast<HMSTableEntry>();
	}
	bool same_definition =
	    cached_table && cached_table->table_data && cached_table->table_data->HasSameDefinition(table);
	auto format = hms::FormatDetector::Detect(table);
	bool schema_from_files = format.IsParquet() || format.IsDelta() || format.IsIceberg();
	if (same_definition && !schema_from_files) {
		// The columns come from the metastore definition, which did not change
		return HMSLoadResult::KeepCached();
	}
	// Discover the schema again: files can change without the metastore definition changing
	unique_ptr<HMSTableEntry> rebuilt;
	try {
		rebuilt = HMSTableEntry::Build(context, catalog, schema, std::move(table));
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERRUPT) {
			throw;
		}
		return HMSLoadResult::Failed(std::move(error));
	}
	if (same_definition) {
		if (rebuilt->HasSameColumns(*cached_table)) {
			return HMSLoadResult::KeepCached();
		}
		if (cached_table->schema_source == HMSSchemaSource::FILES && rebuilt->schema_source != HMSSchemaSource::FILES) {
			// Discovery failed this time (e.g. the object store was unreachable) while the definition is unchanged:
			// keep the columns discovered earlier
			return HMSLoadResult::KeepCached();
		}
	}
	return HMSLoadResult::NewEntry(std::move(rebuilt));
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
	auto &hms_catalog = GetHMSCatalog();
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

	// Register the table as the metastore stored it, rather than with the user-declared columns, so subsequent
	// queries see exactly what a re-attach would resolve
	auto table = HMSAPI::GetTable(context, schema.name, base.table, hms_catalog.endpoint);
	if (!table) {
		throw IOException("Failed to fetch table info after creating table '%s'", base.table);
	}
	return PutEntry(context, HMSTableEntry::Build(context, catalog, schema, std::move(*table)));
}

void HMSTableSet::DropEntry(ClientContext &context, DropInfo &info) {
	bool dropped = HMSAPI::DropTable(context, schema.name, info.name, GetHMSCatalog().endpoint);
	// The table is gone from the metastore either way (e.g. dropped by another process after it was cached)
	EvictEntry(context, info.name);
	if (!dropped && info.if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw CatalogException("Table '%s.%s' does not exist", schema.name, info.name);
	}
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
