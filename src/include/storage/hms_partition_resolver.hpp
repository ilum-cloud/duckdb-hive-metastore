//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_partition_resolver.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/hms_partition_options.hpp"
#include "storage/hms_partition_plan.hpp"
#include "hms_format_detector.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/planner/expression.hpp"

#include <chrono>

namespace duckdb {

class HMSCatalog;
struct HMSAPIPartition;
struct MultiFilePushdownInfo;

//! What scanning a partitioned table needs to know about its partitioning; fixed for the life of the table entry
struct HMSPartitionSchema {
	string database;
	string table;
	//! Partition key names as the metastore stores them, their metastore types and their DuckDB types
	vector<string> names;
	vector<string> hms_types;
	vector<LogicalType> types;
	hms::FormatDetectionResult format;
	//! The table's metastore record, which normalizing partition locations needs
	shared_ptr<const HMSAPITable> table_data;
	//! The table location, normalized like partition locations, without a trailing slash
	string table_location;
	//! The glob of the files under the table location, read when the table has no partition registered
	string root_glob;
};

//! How the partitions of a scan were selected
enum class HMSPartitionMethod : uint8_t {
	//! The complete partition list was cached, and pruned client-side
	CACHED_PLAN,
	//! The metastore evaluated a filter
	HMS_FILTER,
	//! The metastore's answer to the same filter was cached
	CACHED_HMS_FILTER,
	//! The partition names were listed and the filters evaluated on them
	PARTITION_NAMES,
	//! Every partition was fetched
	FULL,
	//! The metastore could not be reached; a complete list from before is used
	STALE_PLAN,
	//! No partition is registered: the files under the table location are read
	PATH
};

const char *HMSPartitionMethodName(HMSPartitionMethod method);

struct HMSPartitionSelection {
	HMSPartitionMethod method = HMSPartitionMethod::FULL;
	//! The partitions to scan; null for PATH
	shared_ptr<const HMSPartitionPlan> plan;
	//! The filter sent to the metastore, if any
	string hms_filter;
	//! Why a cheaper method was not used, if any
	string reason;
};

//! The partition metadata of one table entry: loaded on demand and reused for the catalog's metadata cache TTL, until
//! hms_clear_cache() bumps the cache generation. Shared by every scan of the entry; thread-safe. No lock is held
//! while the metastore is called.
class HMSPartitionCache {
public:
	HMSPartitionCache(HMSCatalog &catalog, HMSPartitionSchema schema);

	const HMSPartitionSchema &Schema() const {
		return schema;
	}
	HMSPartitionMode Mode() const;
	HMSPartitionFilterPushdown FilterPushdown() const;

	//! Every partition, or PATH when the table has none registered (errors in PARTITION_MODE 'hms')
	HMSPartitionSelection ResolveAll(ClientContext &context);
	//! The partitions that can satisfy the filters (each references partition columns of this scan only, through
	//! `partition_columns`: scan column index -> partition key index). The result may hold more partitions than
	//! match; the scan prunes them client-side on their real values.
	HMSPartitionSelection ResolveForFilters(ClientContext &context, const vector<unique_ptr<Expression>> &filters,
	                                        const MultiFilePushdownInfo &info,
	                                        const unordered_map<column_t, idx_t> &partition_columns);
	//! Every partition, to decide which partition a file belongs to. Throws when the metastore cannot be reached.
	shared_ptr<const HMSPartitionPlan> CompletePlan(ClientContext &context);

	//! Builds a plan from partitions the metastore returned: locations normalized, values cast, sorted by name
	shared_ptr<const HMSPartitionPlan> BuildPlan(ClientContext &context, vector<HMSAPIPartition> partitions,
	                                             bool complete) const;

private:
	using time_point = std::chrono::steady_clock::time_point;

	bool IsFresh(time_point loaded_at, idx_t loaded_generation, idx_t generation) const;
	//! Fetches every partition; caches it, along with the names
	shared_ptr<const HMSPartitionPlan> FetchComplete(ClientContext &context, idx_t generation);
	//! The names of every partition, cached
	shared_ptr<const vector<string>> GetNames(ClientContext &context, idx_t generation);
	//! Whether the table has a partition registered, from what is cached or else asking for one name
	bool HasPartitions(ClientContext &context, idx_t generation);
	//! The selection for a table with no partition registered
	HMSPartitionSelection NoPartitions(ClientContext &context);
	//! Selects through the metastore filter; false (with `reason`) when it cannot be used, `refused` telling whether
	//! the metastore answered with a refusal rather than could not be reached
	bool SelectByMetastoreFilter(ClientContext &context, idx_t generation, const string &filter,
	                             HMSPartitionSelection &selection, string &reason, bool &refused);
	//! The keys whose filters the metastore refused, one flag per partition key
	vector<bool> RefusedKeys(idx_t generation);
	//! Remembers that the metastore refused filters on `keys`, and adds them to `excluded`; false if none was new
	bool RefuseKeys(idx_t generation, const vector<idx_t> &keys, vector<bool> &excluded);
	//! Selects by evaluating the filters on the partition names, then fetching the partitions kept
	HMSPartitionSelection SelectByNames(ClientContext &context, idx_t generation,
	                                    const vector<unique_ptr<Expression>> &filters,
	                                    const MultiFilePushdownInfo &info,
	                                    const unordered_map<column_t, idx_t> &partition_columns);

	struct FilterResult {
		//! The partitions the metastore selected, or null when it refused the filter
		shared_ptr<const HMSPartitionPlan> plan;
		string rejection;
		time_point loaded_at;
		idx_t generation;
	};

	HMSCatalog &catalog;
	const HMSPartitionSchema schema;

	mutex lock;
	shared_ptr<const HMSPartitionPlan> complete_plan;
	time_point complete_plan_loaded_at;
	idx_t complete_plan_generation = 0;
	shared_ptr<const vector<string>> names;
	time_point names_loaded_at;
	idx_t names_generation = 0;
	//! -1 unknown, else whether the table has a partition registered
	int8_t has_partitions = -1;
	time_point has_partitions_loaded_at;
	idx_t has_partitions_generation = 0;
	//! The metastore's answers to recent filters, most recently used last
	unordered_map<string, FilterResult> filter_results;
	vector<string> filter_order;
	//! How many partitions the answers kept hold together
	idx_t filter_result_partitions = 0;
	//! The keys whose filters the metastore refused, left out of the filters sent until the TTL expires
	vector<bool> refused_keys;
	time_point refused_keys_loaded_at;
	idx_t refused_keys_generation = 0;
	//! Whether falling back to the table location for lack of registered partitions was already reported
	bool warned_no_partitions = false;
};

} // namespace duckdb
