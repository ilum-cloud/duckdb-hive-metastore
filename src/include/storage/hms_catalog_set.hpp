//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include <chrono>
#include <condition_variable>
#include <thread>

namespace duckdb {
struct DropInfo;
class HMSCatalog;
class HMSSchemaEntry;
class HMSTransaction;

//! The result of loading one entry from the metastore
struct HMSLoadResult {
	enum class Kind : uint8_t {
		//! The entry does not exist in the metastore
		MISSING,
		//! The entry exists and the cached version is still accurate
		KEEP_CACHED,
		//! The entry exists and a new version was built
		NEW_ENTRY,
		//! The entry exists but could not be built (e.g. it has a column type DuckDB cannot map)
		FAILED
	};

	Kind kind = Kind::MISSING;
	unique_ptr<CatalogEntry> entry;
	ErrorData error;

	static HMSLoadResult Missing();
	static HMSLoadResult KeepCached();
	static HMSLoadResult NewEntry(unique_ptr<CatalogEntry> entry);
	static HMSLoadResult Failed(ErrorData error);
};

//! What a name resolves to: an entry, nothing (the entry does not exist), or the error that loading it raised
struct HMSLookupOutcome {
	optional_ptr<CatalogEntry> entry;
	ErrorData error;
};

//! A metastore load that concurrent lookups wait on instead of repeating it
class HMSInFlightLoad {
public:
	enum class State : uint8_t { RUNNING, FINISHED, FAILED, ABORTED };

	HMSInFlightLoad();

	//! Whether the calling thread performs this load
	bool IsOwner() const;
	void Finish(HMSLookupOutcome outcome);
	void Finish(vector<reference<CatalogEntry>> listing);
	//! The metastore could not be reached; waiters raise the same error
	void Fail(ErrorData error);
	//! The load was abandoned (e.g. its query was interrupted); waiters retry it themselves
	void Abort();
	//! Waits until the load is done and returns how it ended; throws if the waiting query is interrupted
	State Wait(ClientContext &context);

	//! Set when FINISHED, read-only afterwards
	HMSLookupOutcome outcome;
	vector<reference<CatalogEntry>> listing;
	//! Set when FAILED, read-only afterwards
	ErrorData error;

private:
	const std::thread::id owner;
	mutex lock;
	std::condition_variable done;
	State state;
};

//! Cache of catalog entries loaded from the Hive Metastore.
//!
//! Lookups load only the requested entry; scans load the whole set. A cached entry is revalidated against the
//! metastore once it is older than the catalog's metadata cache TTL, and replaced only if its definition changed.
//! Concurrent loads of the same entry (or of the listing) happen once. Entry versions are never freed while the
//! catalog is attached, because bound queries, prepared statements and other transactions may still reference them.
class HMSCatalogSet {
public:
	explicit HMSCatalogSet(Catalog &catalog);
	virtual ~HMSCatalogSet() = default;

	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);
	virtual void DropEntry(ClientContext &context, DropInfo &info);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);

protected:
	//! Whether a single entry can be loaded by name; if not, lookups load the listing
	virtual bool SupportsPointLookup() const = 0;
	//! Loads one entry. `cached` is its most recent version, if any. Failures to reach the metastore throw.
	virtual HMSLoadResult LoadEntry(ClientContext &context, const string &name, optional_ptr<CatalogEntry> cached);
	//! Lists the names of all entries. Failures to reach the metastore throw.
	virtual vector<string> ListEntryNames(ClientContext &context) = 0;
	//! Loads the requested entries (name and most recent version, if any) and reports each through `on_loaded`.
	//! Entries that no longer exist may be left unreported. Failures to reach the metastore throw.
	virtual void LoadEntries(ClientContext &context, const vector<pair<string, optional_ptr<CatalogEntry>>> &requests,
	                         const std::function<void(const string &name, HMSLoadResult result)> &on_loaded) = 0;

	//! Registers an entry created through this catalog; the calling transaction sees it right away
	optional_ptr<CatalogEntry> PutEntry(ClientContext &context, unique_ptr<CatalogEntry> entry);
	//! Registers that an entry was dropped through this catalog
	void EvictEntry(ClientContext &context, const string &name);

	HMSCatalog &GetHMSCatalog();

protected:
	Catalog &catalog;

private:
	struct CachedEntry {
		//! The most recent version; kept after the entry disappears so it can be reused if it comes back unchanged
		optional_ptr<CatalogEntry> entry;
		//! Whether the entry exists in the metastore
		bool exists = false;
		//! Set when the entry exists but could not be loaded
		ErrorData error;
		std::chrono::steady_clock::time_point checked_at;
	};

	HMSLookupOutcome Lookup(ClientContext &context, const string &name);
	HMSLookupOutcome LoadAndCommit(ClientContext &context, const string &name, const CachedEntry &cached,
	                               idx_t start_generation);
	HMSLookupOutcome Commit(const string &name, HMSLoadResult result, optional_ptr<CatalogEntry> cached,
	                        idx_t start_generation);
	vector<reference<CatalogEntry>> List(ClientContext &context);
	vector<reference<CatalogEntry>> LoadListing(ClientContext &context);
	void FinishLoad(const string &name, const shared_ptr<HMSInFlightLoad> &load);
	void FinishListingLoad(const shared_ptr<HMSInFlightLoad> &load);

	//! The following require `lock` to be held
	bool IsFresh(std::chrono::steady_clock::time_point checked_at);
	void SyncWithCatalogCache();
	vector<reference<CatalogEntry>> CachedListing();

	static HMSLookupOutcome OutcomeOf(const CachedEntry &cached);

	mutex lock;
	case_insensitive_map_t<CachedEntry> entries;
	//! Owns every entry version built by this set
	vector<unique_ptr<CatalogEntry>> retained;
	case_insensitive_map_t<shared_ptr<HMSInFlightLoad>> loading;
	shared_ptr<HMSInFlightLoad> listing_load;
	bool listing_valid = false;
	std::chrono::steady_clock::time_point listed_at;
	//! Entries checked at or before this point are stale regardless of their age
	std::chrono::steady_clock::time_point invalidated_at;
	//! Incremented whenever cached state is invalidated; loads that started earlier do not update the cache
	idx_t generation = 0;
	//! The catalog-wide cache generation (see HMSCatalog::ClearCache) this set last invalidated itself for
	idx_t catalog_cache_generation = 0;
};

} // namespace duckdb
