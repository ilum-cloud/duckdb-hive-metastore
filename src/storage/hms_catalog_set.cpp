#include "storage/hms_catalog_set.hpp"
#include "storage/hms_catalog.hpp"
#include "storage/hms_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

#include <algorithm>

namespace duckdb {

HMSLoadResult HMSLoadResult::Missing() {
	return HMSLoadResult();
}

HMSLoadResult HMSLoadResult::KeepCached() {
	HMSLoadResult result;
	result.kind = Kind::KEEP_CACHED;
	return result;
}

HMSLoadResult HMSLoadResult::NewEntry(unique_ptr<CatalogEntry> entry) {
	HMSLoadResult result;
	result.kind = Kind::NEW_ENTRY;
	result.entry = std::move(entry);
	return result;
}

HMSLoadResult HMSLoadResult::Failed(ErrorData error) {
	HMSLoadResult result;
	result.kind = Kind::FAILED;
	result.error = std::move(error);
	return result;
}

HMSInFlightLoad::HMSInFlightLoad() : owner(std::this_thread::get_id()), state(State::RUNNING) {
}

bool HMSInFlightLoad::IsOwner() const {
	return owner == std::this_thread::get_id();
}

void HMSInFlightLoad::Finish(HMSLookupOutcome outcome_p) {
	lock_guard<mutex> guard(lock);
	outcome = std::move(outcome_p);
	state = State::FINISHED;
	done.notify_all();
}

void HMSInFlightLoad::Finish(vector<reference<CatalogEntry>> listing_p) {
	lock_guard<mutex> guard(lock);
	listing = std::move(listing_p);
	state = State::FINISHED;
	done.notify_all();
}

void HMSInFlightLoad::Fail(ErrorData error_p) {
	lock_guard<mutex> guard(lock);
	error = std::move(error_p);
	state = State::FAILED;
	done.notify_all();
}

void HMSInFlightLoad::Abort() {
	lock_guard<mutex> guard(lock);
	state = State::ABORTED;
	done.notify_all();
}

HMSInFlightLoad::State HMSInFlightLoad::Wait(ClientContext &context) {
	unique_lock<mutex> guard(lock);
	while (state == State::RUNNING) {
		if (context.interrupted) {
			throw InterruptException();
		}
		// Wake up regularly to notice interrupts
		done.wait_for(guard, std::chrono::milliseconds(50));
	}
	return state;
}

HMSCatalogSet::HMSCatalogSet(Catalog &catalog) : catalog(catalog) {
}

HMSCatalog &HMSCatalogSet::GetHMSCatalog() {
	return catalog.Cast<HMSCatalog>();
}

optional_ptr<CatalogEntry> HMSCatalogSet::GetEntry(ClientContext &context, const string &name) {
	auto &transaction = HMSTransaction::Get(context, catalog);
	optional_ptr<CatalogEntry> result;
	if (transaction.TryGetEntry(*this, name, result)) {
		return result;
	}
	auto outcome = Lookup(context, name);
	if (outcome.error.HasError()) {
		outcome.error.Throw();
	}
	return transaction.RecordEntry(*this, name, outcome.entry);
}

void HMSCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("HMSCatalogSet::DropEntry");
}

void HMSCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	auto &transaction = HMSTransaction::Get(context, catalog);
	vector<reference<CatalogEntry>> scanned;
	if (!transaction.TryGetScan(*this, scanned)) {
		scanned = transaction.RecordScan(*this, List(context));
	}
	// No locks are held here, so callbacks can look up entries again
	for (auto &entry : scanned) {
		callback(entry.get());
	}
}

HMSLoadResult HMSCatalogSet::LoadEntry(ClientContext &context, const string &name, optional_ptr<CatalogEntry> cached) {
	throw InternalException("HMSCatalogSet: entries of this set cannot be loaded individually");
}

optional_ptr<CatalogEntry> HMSCatalogSet::PutEntry(ClientContext &context, unique_ptr<CatalogEntry> entry) {
	if (!entry) {
		throw InternalException("HMSCatalogSet::PutEntry called with null entry");
	}
	auto name = entry->name;
	optional_ptr<CatalogEntry> result = entry.get();
	{
		lock_guard<mutex> guard(lock);
		SyncWithCatalogCache();
		retained.push_back(std::move(entry));
		auto &cached = entries[name];
		cached.entry = result;
		cached.exists = true;
		cached.error = ErrorData();
		cached.checked_at = std::chrono::steady_clock::now();
		// Loads that started before this change must not overwrite it
		generation++;
	}
	HMSTransaction::Get(context, catalog).OverrideEntry(*this, name, result);
	return result;
}

void HMSCatalogSet::EvictEntry(ClientContext &context, const string &name) {
	{
		lock_guard<mutex> guard(lock);
		SyncWithCatalogCache();
		auto &cached = entries[name];
		cached.exists = false;
		cached.error = ErrorData();
		cached.checked_at = std::chrono::steady_clock::now();
		generation++;
	}
	HMSTransaction::Get(context, catalog).OverrideEntry(*this, name, nullptr);
}

HMSLookupOutcome HMSCatalogSet::Lookup(ClientContext &context, const string &name) {
	if (!SupportsPointLookup()) {
		for (auto &entry : List(context)) {
			if (StringUtil::CIEquals(entry.get().name, name)) {
				HMSLookupOutcome outcome;
				outcome.entry = &entry.get();
				return outcome;
			}
		}
		return HMSLookupOutcome();
	}
	while (true) {
		shared_ptr<HMSInFlightLoad> load;
		CachedEntry cached;
		idx_t start_generation = 0;
		{
			lock_guard<mutex> guard(lock);
			SyncWithCatalogCache();
			auto entry = entries.find(name);
			if (entry != entries.end() && IsFresh(entry->second.checked_at)) {
				return OutcomeOf(entry->second);
			}
			if (entry == entries.end() && listing_valid && IsFresh(listed_at)) {
				// A fresh listing does not contain the name
				return HMSLookupOutcome();
			}
			auto running = loading.find(name);
			if (running != loading.end() && !running->second->IsOwner()) {
				load = running->second;
			} else {
				if (running == loading.end()) {
					load = make_shared_ptr<HMSInFlightLoad>();
					loading[name] = load;
				}
				// Otherwise this thread is already loading the name further up its stack: load it again, unshared
				if (entry != entries.end()) {
					cached = entry->second;
				}
				start_generation = generation;
			}
		}
		if (load && !load->IsOwner()) {
			switch (load->Wait(context)) {
			case HMSInFlightLoad::State::FINISHED:
				return load->outcome;
			case HMSInFlightLoad::State::FAILED:
				load->error.Throw();
			default:
				// The load was abandoned: try again
				continue;
			}
		}
		try {
			auto outcome = LoadAndCommit(context, name, cached, start_generation);
			if (load) {
				FinishLoad(name, load);
				load->Finish(outcome);
			}
			return outcome;
		} catch (std::exception &ex) {
			if (load) {
				FinishLoad(name, load);
				ErrorData error(ex);
				if (error.Type() == ExceptionType::INTERRUPT) {
					load->Abort();
				} else {
					load->Fail(std::move(error));
				}
			}
			throw;
		}
	}
}

HMSLookupOutcome HMSCatalogSet::LoadAndCommit(ClientContext &context, const string &name, const CachedEntry &cached,
                                              idx_t start_generation) {
	HMSLoadResult result;
	try {
		result = LoadEntry(context, name, cached.entry);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERRUPT || !cached.exists || cached.error.HasError()) {
			throw;
		}
		// The metastore could not be reached: keep using the cached version until the TTL expires again
		DUCKDB_LOG_WARNING(context, "hive_metastore: failed to refresh \"%s\", using cached metadata: %s", name,
		                   error.RawMessage());
		result = HMSLoadResult::KeepCached();
	}
	return Commit(name, std::move(result), cached.entry, start_generation);
}

HMSLookupOutcome HMSCatalogSet::Commit(const string &name, HMSLoadResult result, optional_ptr<CatalogEntry> cached,
                                       idx_t start_generation) {
	CachedEntry updated;
	updated.entry = cached;
	lock_guard<mutex> guard(lock);
	switch (result.kind) {
	case HMSLoadResult::Kind::MISSING:
		break;
	case HMSLoadResult::Kind::KEEP_CACHED:
		if (!cached) {
			throw InternalException("HMSCatalogSet: no cached version of \"%s\" to keep", name);
		}
		updated.exists = true;
		break;
	case HMSLoadResult::Kind::NEW_ENTRY:
		updated.entry = result.entry.get();
		updated.exists = true;
		retained.push_back(std::move(result.entry));
		break;
	case HMSLoadResult::Kind::FAILED:
		updated.exists = true;
		updated.error = std::move(result.error);
		break;
	}
	updated.checked_at = std::chrono::steady_clock::now();
	if (generation == start_generation) {
		entries[name] = updated;
	}
	return OutcomeOf(updated);
}

vector<reference<CatalogEntry>> HMSCatalogSet::List(ClientContext &context) {
	while (true) {
		shared_ptr<HMSInFlightLoad> load;
		{
			lock_guard<mutex> guard(lock);
			SyncWithCatalogCache();
			if (listing_valid && IsFresh(listed_at)) {
				return CachedListing();
			}
			if (listing_load && !listing_load->IsOwner()) {
				load = listing_load;
			} else if (!listing_load) {
				load = make_shared_ptr<HMSInFlightLoad>();
				listing_load = load;
			}
			// Otherwise this thread is already listing further up its stack: list again, unshared
		}
		if (load && !load->IsOwner()) {
			switch (load->Wait(context)) {
			case HMSInFlightLoad::State::FINISHED:
				return load->listing;
			case HMSInFlightLoad::State::FAILED:
				load->error.Throw();
			default:
				continue;
			}
		}
		try {
			auto listing = LoadListing(context);
			if (load) {
				FinishListingLoad(load);
				load->Finish(listing);
			}
			return listing;
		} catch (std::exception &) {
			if (load) {
				FinishListingLoad(load);
				load->Abort();
			}
			throw;
		}
	}
}

vector<reference<CatalogEntry>> HMSCatalogSet::LoadListing(ClientContext &context) {
	auto start = std::chrono::steady_clock::now();
	idx_t start_generation;
	{
		lock_guard<mutex> guard(lock);
		start_generation = generation;
	}
	vector<string> names;
	try {
		names = ListEntryNames(context);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERRUPT) {
			throw;
		}
		// The metastore could not be reached: list what is cached. Listings back duckdb_tables() and
		// information_schema for every attached catalog, so they should not fail.
		DUCKDB_LOG_WARNING(context, "hive_metastore: failed to list entries, using cached metadata: %s",
		                   error.RawMessage());
		lock_guard<mutex> guard(lock);
		return CachedListing();
	}

	// Reuse fresh entries, wait for entries that other threads are loading, and load the rest here
	vector<pair<string, optional_ptr<CatalogEntry>>> requests;
	case_insensitive_map_t<shared_ptr<HMSInFlightLoad>> own_loads;
	vector<pair<string, shared_ptr<HMSInFlightLoad>>> other_loads;
	auto listing_checked_at = start;
	{
		lock_guard<mutex> guard(lock);
		for (auto &name : names) {
			auto entry = entries.find(name);
			if (entry != entries.end() && entry->second.exists && IsFresh(entry->second.checked_at)) {
				listing_checked_at = MinValue(listing_checked_at, entry->second.checked_at);
				continue;
			}
			auto running = loading.find(name);
			if (running != loading.end()) {
				if (!running->second->IsOwner()) {
					other_loads.emplace_back(name, running->second);
				}
				continue;
			}
			auto load = make_shared_ptr<HMSInFlightLoad>();
			loading[name] = load;
			own_loads[name] = load;
			requests.emplace_back(name, entry != entries.end() ? entry->second.entry : nullptr);
		}
	}

	case_insensitive_map_t<optional_ptr<CatalogEntry>> cached_versions;
	for (auto &request : requests) {
		cached_versions[request.first] = request.second;
	}
	case_insensitive_map_t<HMSLookupOutcome> outcomes;
	auto complete = [&](const string &name, HMSLoadResult result) {
		auto own_load = own_loads.find(name);
		if (own_load == own_loads.end() || outcomes.find(name) != outcomes.end()) {
			return;
		}
		auto outcome = Commit(name, std::move(result), cached_versions[name], start_generation);
		if (outcome.error.HasError()) {
			DUCKDB_LOG_WARNING(context, "hive_metastore: skipping \"%s\": %s", name, outcome.error.RawMessage());
		}
		outcomes[name] = outcome;
		FinishLoad(name, own_load->second);
		own_load->second->Finish(outcome);
	};
	try {
		LoadEntries(context, requests, complete);
		// Listed entries whose metadata was not returned were dropped in the meantime
		for (auto &request : requests) {
			complete(request.first, HMSLoadResult::Missing());
		}
	} catch (std::exception &) {
		for (auto &own_load : own_loads) {
			if (outcomes.find(own_load.first) == outcomes.end()) {
				FinishLoad(own_load.first, own_load.second);
				own_load.second->Abort();
			}
		}
		throw;
	}
	for (auto &other_load : other_loads) {
		if (other_load.second->Wait(context) == HMSInFlightLoad::State::FINISHED) {
			outcomes[other_load.first] = other_load.second->outcome;
		}
	}

	vector<reference<CatalogEntry>> listing;
	lock_guard<mutex> guard(lock);
	if (generation == start_generation) {
		// Entries that are no longer listed were dropped
		case_insensitive_set_t listed(names.begin(), names.end());
		for (auto &entry : entries) {
			if (entry.second.exists && listed.find(entry.first) == listed.end()) {
				entry.second.exists = false;
				entry.second.error = ErrorData();
				entry.second.checked_at = start;
			}
		}
		listing_valid = true;
		listed_at = listing_checked_at;
	}
	for (auto &name : names) {
		auto outcome = outcomes.find(name);
		if (outcome != outcomes.end()) {
			if (outcome->second.entry) {
				listing.push_back(*outcome->second.entry);
			}
			continue;
		}
		auto entry = entries.find(name);
		if (entry != entries.end() && entry->second.exists && !entry->second.error.HasError() && entry->second.entry) {
			listing.push_back(*entry->second.entry);
		}
	}
	return listing;
}

void HMSCatalogSet::FinishLoad(const string &name, const shared_ptr<HMSInFlightLoad> &load) {
	lock_guard<mutex> guard(lock);
	auto running = loading.find(name);
	if (running != loading.end() && running->second == load) {
		loading.erase(running);
	}
}

void HMSCatalogSet::FinishListingLoad(const shared_ptr<HMSInFlightLoad> &load) {
	lock_guard<mutex> guard(lock);
	if (listing_load == load) {
		listing_load.reset();
	}
}

bool HMSCatalogSet::IsFresh(std::chrono::steady_clock::time_point checked_at) {
	return checked_at > invalidated_at &&
	       std::chrono::steady_clock::now() - checked_at < GetHMSCatalog().GetMetadataCacheTTL();
}

void HMSCatalogSet::SyncWithCatalogCache() {
	auto current = GetHMSCatalog().GetCacheGeneration();
	if (current == catalog_cache_generation) {
		return;
	}
	// hms_clear_cache() was called: everything cached so far is stale
	catalog_cache_generation = current;
	generation++;
	listing_valid = false;
	invalidated_at = std::chrono::steady_clock::now();
}

vector<reference<CatalogEntry>> HMSCatalogSet::CachedListing() {
	vector<reference<CatalogEntry>> result;
	for (auto &entry : entries) {
		if (entry.second.exists && !entry.second.error.HasError() && entry.second.entry) {
			result.push_back(*entry.second.entry);
		}
	}
	std::sort(result.begin(), result.end(),
	          [](const reference<CatalogEntry> &left, const reference<CatalogEntry> &right) {
		          return left.get().name < right.get().name;
	          });
	return result;
}

HMSLookupOutcome HMSCatalogSet::OutcomeOf(const CachedEntry &cached) {
	HMSLookupOutcome outcome;
	if (!cached.exists) {
		return outcome;
	}
	if (cached.error.HasError()) {
		outcome.error = cached.error;
	} else {
		outcome.entry = cached.entry;
	}
	return outcome;
}

} // namespace duckdb
