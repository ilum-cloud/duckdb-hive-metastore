#include "storage/hms_catalog_set.hpp"
#include "storage/hms_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/hms_schema_entry.hpp"

namespace duckdb {

HMSCatalogSet::HMSCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false), last_load_time() {
}

optional_ptr<CatalogEntry> HMSCatalogSet::GetEntry(ClientContext &context, const string &name) {
	// Check if we need to load entries (either first time or cache expired)
	bool need_to_load = false;
	{
		lock_guard<mutex> l(entry_lock);
		if (!is_loaded) {
			need_to_load = true;
		} else {
			// Check if cache is stale (older than TTL)
			auto now = std::chrono::steady_clock::now();
			auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_load_time).count();
			if (elapsed >= CACHE_TTL_SECONDS) {
				need_to_load = true;
			}
		}
	}

	// Load entries if needed (CreateEntry will acquire the lock)
	if (need_to_load) {
		try {
			LoadEntries(context);
			lock_guard<mutex> l(entry_lock);
			is_loaded = true;
			last_load_time = std::chrono::steady_clock::now();
		} catch (std::exception &e) {
			// HMS is unavailable or errored - mark as loaded with empty entries
			// This allows queries to return empty results instead of failing
			lock_guard<mutex> l(entry_lock);
			is_loaded = true;
			last_load_time = std::chrono::steady_clock::now();
		}
	}

	// Now lookup the entry
	lock_guard<mutex> l(entry_lock);
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	return entry->second.get();
}

void HMSCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("HMSCatalogSet::DropEntry");
}

void HMSCatalogSet::EraseEntryInternal(const string &name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(name);
}

void HMSCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	// Check if we need to load entries (either first time or cache expired)
	bool need_to_load = false;
	{
		lock_guard<mutex> l(entry_lock);
		if (!is_loaded) {
			need_to_load = true;
		} else {
			// Check if cache is stale (older than TTL)
			auto now = std::chrono::steady_clock::now();
			auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_load_time).count();
			if (elapsed >= CACHE_TTL_SECONDS) {
				need_to_load = true;
			}
		}
	}

	// Load entries if needed (CreateEntry will acquire the lock)
	if (need_to_load) {
		try {
			LoadEntries(context);
			lock_guard<mutex> l(entry_lock);
			is_loaded = true;
			last_load_time = std::chrono::steady_clock::now();
		} catch (std::exception &e) {
			// HMS is unavailable or errored - mark as loaded with empty entries
			// This allows queries to return empty results instead of failing
			lock_guard<mutex> l(entry_lock);
			is_loaded = true;
			last_load_time = std::chrono::steady_clock::now();
		}
	}

	// Now scan the entries
	lock_guard<mutex> l(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> HMSCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	if (!entry) {
		throw InternalException("HMSCatalogSet::CreateEntry called with null entry");
	}
	lock_guard<mutex> l(entry_lock);
	auto result = entry.get();
	if (result->name.empty()) {
		throw InternalException("HMSCatalogSet::CreateEntry called with empty name");
	}
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}

void HMSCatalogSet::ClearEntries() {
	lock_guard<mutex> l(entry_lock);
	entries.clear();
	is_loaded = false;
}

HMSInSchemaSet::HMSInSchemaSet(HMSSchemaEntry &schema) : HMSCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> HMSInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	if (!entry) {
		throw InternalException("HMSInSchemaSet::CreateEntry called with null entry");
	}
	if (!entry->internal) {
		entry->internal = schema.internal;
	}
	return HMSCatalogSet::CreateEntry(std::move(entry));
}

} // namespace duckdb
