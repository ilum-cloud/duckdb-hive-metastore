#include "storage/hms_partition_resolver.hpp"

#include "storage/hms_catalog.hpp"
#include "hms_api.hpp"
#include "hms_path_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include <algorithm>

namespace duckdb {

const char *HMSPartitionMethodName(HMSPartitionMethod method) {
	switch (method) {
	case HMSPartitionMethod::CACHED_PLAN:
		return "cached_plan";
	case HMSPartitionMethod::HMS_FILTER:
		return "hms_filter";
	case HMSPartitionMethod::CACHED_HMS_FILTER:
		return "cached_hms_filter";
	case HMSPartitionMethod::PARTITION_NAMES:
		return "partition_names";
	case HMSPartitionMethod::FULL:
		return "full";
	case HMSPartitionMethod::STALE_PLAN:
		return "stale_plan";
	case HMSPartitionMethod::PATH:
		return "path";
	default:
		return "unknown";
	}
}

HMSPartitionCache::HMSPartitionCache(HMSCatalog &catalog, HMSPartitionSchema schema_p)
    : catalog(catalog), schema(std::move(schema_p)) {
}

HMSPartitionMode HMSPartitionCache::Mode() const {
	return catalog.GetPartitionMode();
}

HMSPartitionFilterPushdown HMSPartitionCache::FilterPushdown() const {
	return catalog.GetPartitionFilterPushdown();
}

bool HMSPartitionCache::IsFresh(time_point loaded_at, idx_t loaded_generation, idx_t generation) const {
	return loaded_generation == generation &&
	       std::chrono::steady_clock::now() - loaded_at < catalog.GetMetadataCacheTTL();
}

//! The name the metastore uses for a partition, rebuilt from the partition keys and values
static string PartitionDisplayName(const HMSPartitionSchema &schema, const vector<string> &values) {
	string result;
	for (idx_t i = 0; i < schema.names.size() && i < values.size(); i++) {
		if (!result.empty()) {
			result += "/";
		}
		result += schema.names[i] + "=" + values[i];
	}
	return result;
}

static Value PartitionValue(const string &raw, const LogicalType &type, const string &column,
                            const string &partition_name, const string &table_name) {
	// What Hive stores for a row whose partition column is NULL. Unlike a path segment this value is not escaped,
	// and the literal string "NULL" is a value of its own.
	if (raw == "__HIVE_DEFAULT_PARTITION__") {
		return Value(type);
	}
	Value result;
	string error;
	if (!Value(raw).DefaultTryCastAs(type, result, &error)) {
		throw InvalidInputException(
		    "Partition \"%s\" of table \"%s\": cannot read the value '%s' of partition column \"%s\" as %s",
		    partition_name, table_name, raw, column, type.ToString());
	}
	return result;
}

//! A statistic from a partition's parameters, under the first of the keys present. Hive stores -1 when unknown, and 0
//! for a partition registered before its files were written, which it does not notice: take neither.
static optional_idx PartitionStatistic(const map<string, string> &parameters, const vector<string> &keys) {
	for (auto &key : keys) {
		auto entry = parameters.find(key);
		if (entry == parameters.end()) {
			continue;
		}
		int64_t value;
		if (TryCast::Operation<string_t, int64_t>(string_t(entry->second), value) && value > 0) {
			return optional_idx(UnsafeNumericCast<idx_t>(value));
		}
	}
	return optional_idx();
}

shared_ptr<const HMSPartitionPlan>
HMSPartitionCache::BuildPlan(ClientContext &context, vector<HMSAPIPartition> partitions, bool complete) const {
	auto plan = make_shared_ptr<HMSPartitionPlan>();
	plan->complete = complete;
	plan->table_location = schema.table_location;
	for (auto &partition : partitions) {
		HMSScanPartition scan_partition;
		scan_partition.name = PartitionDisplayName(schema, partition.values);
		if (partition.location.empty() || partition.values.size() != schema.names.size()) {
			DUCKDB_LOG_WARNING(context,
			                   "hive_metastore: skipping partition \"%s\" of \"%s.%s\": it has no location or does "
			                   "not match the partition columns",
			                   scan_partition.name, schema.database, schema.table);
			continue;
		}
		// A partition location gets the same treatment as a table location: placeholder stripping, s3a/oss/cos
		// rewriting and the http endpoint handling
		auto path_result = hms::PathUtils::NormalizeScanPath(partition.location, *schema.table_data, schema.format);
		scan_partition.location = path_result.scan_path;
		while (StringUtil::EndsWith(scan_partition.location, "/")) {
			scan_partition.location.pop_back();
		}
		scan_partition.scan_location = hms::PathUtils::BuildPartitionGlobPattern(path_result.scan_path, schema.format);
		scan_partition.fallback_scan_location =
		    hms::PathUtils::BuildPartitionFallbackGlobPattern(path_result.scan_path);
		if (path_result.needs_s3_config) {
			plan->needs_s3_config = true;
			plan->s3_endpoint = path_result.s3_endpoint;
		}
		scan_partition.row_count =
		    PartitionStatistic(partition.parameters, {"numRows", "spark.sql.statistics.numRows"});
		scan_partition.total_size =
		    PartitionStatistic(partition.parameters, {"totalSize", "spark.sql.statistics.totalSize"});
		scan_partition.file_count = PartitionStatistic(partition.parameters, {"numFiles"});
		for (idx_t i = 0; i < schema.names.size(); i++) {
			scan_partition.values.push_back(PartitionValue(partition.values[i], schema.types[i], schema.names[i],
			                                               scan_partition.name, schema.table));
		}
		plan->partitions.push_back(std::move(scan_partition));
	}
	// The metastore does not promise an order; sorted, the files of a scan come in the same order every time
	std::sort(plan->partitions.begin(), plan->partitions.end(),
	          [](const HMSScanPartition &a, const HMSScanPartition &b) { return a.name < b.name; });

	// What deciding how to list the partitions takes: which of them live under the table location, how many files
	// those hold, and which partitions sit at each location
	idx_t files_under_table_location = 0;
	bool file_counts_known = true;
	for (idx_t i = 0; i < plan->partitions.size(); i++) {
		auto &scan_partition = plan->partitions[i];
		plan->partitions_by_location[scan_partition.location].push_back(i);
		if (!plan->IsUnderTableLocation(scan_partition)) {
			continue;
		}
		plan->partitions_under_table_location++;
		if (scan_partition.file_count.IsValid()) {
			files_under_table_location += scan_partition.file_count.GetIndex();
		} else {
			file_counts_known = false;
		}
	}
	if (file_counts_known) {
		plan->files_under_table_location = files_under_table_location;
	}
	return std::move(plan);
}

//! Partition locations served over http:// need the S3 endpoint configured, as table locations do
static void ConfigureStorage(ClientContext &context, const HMSPartitionPlan &plan) {
	if (!plan.needs_s3_config) {
		return;
	}
	Value endpoint_val = Value(plan.s3_endpoint);
	Value use_ssl_val = Value(false);
	Value url_style_val = Value("path");
	context.db->config.SetOption("s3_endpoint", endpoint_val);
	context.db->config.SetOption("s3_use_ssl", use_ssl_val);
	context.db->config.SetOption("s3_url_style", url_style_val);
}

shared_ptr<const HMSPartitionPlan> HMSPartitionCache::FetchComplete(ClientContext &context, idx_t generation) {
	auto &endpoint = catalog.endpoint;
	auto names = HMSAPI::GetPartitionNames(context, schema.database, schema.table, endpoint);
	auto partitions = HMSAPI::GetPartitions(context, schema.database, schema.table, names, endpoint);
	auto plan = BuildPlan(context, std::move(partitions), true);
	lock_guard<mutex> guard(lock);
	complete_plan = plan;
	complete_plan_loaded_at = std::chrono::steady_clock::now();
	complete_plan_generation = generation;
	return plan;
}

HMSPartitionSelection HMSPartitionCache::NoPartitions(ClientContext &context) {
	if (Mode() == HMSPartitionMode::HMS) {
		throw InvalidInputException("Table \"%s.%s\" declares partition columns but has no partition registered in "
		                            "the Hive Metastore (PARTITION_MODE 'hms')",
		                            schema.database, schema.table);
	}
	bool warn = false;
	{
		lock_guard<mutex> guard(lock);
		warn = !warned_no_partitions;
		warned_no_partitions = true;
	}
	if (warn) {
		DUCKDB_LOG_WARNING(context,
		                   "hive_metastore: table \"%s.%s\" declares partition columns but has no partition registered "
		                   "in the Hive Metastore; reading every file under its location, with the partition values "
		                   "of key=value directory names, else those the files hold",
		                   schema.database, schema.table);
	}
	HMSPartitionSelection selection;
	selection.method = HMSPartitionMethod::PATH;
	return selection;
}

HMSPartitionSelection HMSPartitionCache::ResolveAll(ClientContext &context) {
	auto generation = catalog.GetCacheGeneration();
	HMSPartitionSelection selection;
	{
		lock_guard<mutex> guard(lock);
		if (complete_plan && IsFresh(complete_plan_loaded_at, complete_plan_generation, generation)) {
			selection.method = HMSPartitionMethod::CACHED_PLAN;
			selection.plan = complete_plan;
		}
	}
	if (!selection.plan) {
		try {
			selection.method = HMSPartitionMethod::FULL;
			selection.plan = FetchComplete(context, generation);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			lock_guard<mutex> guard(lock);
			if (error.Type() == ExceptionType::INTERRUPT || !complete_plan) {
				throw;
			}
			// The metastore could not be reached: keep scanning the partitions we know until the TTL expires again
			DUCKDB_LOG_WARNING(context,
			                   "hive_metastore: failed to refresh the partitions of \"%s.%s\", using the cached "
			                   "partition list: %s",
			                   schema.database, schema.table, error.RawMessage());
			complete_plan_loaded_at = std::chrono::steady_clock::now();
			complete_plan_generation = generation;
			selection.method = HMSPartitionMethod::STALE_PLAN;
			selection.plan = complete_plan;
			selection.reason = error.RawMessage();
		}
	}
	if (selection.plan->partitions.empty()) {
		return NoPartitions(context);
	}
	ConfigureStorage(context, *selection.plan);
	return selection;
}

HMSPartitionSelection HMSPartitionCache::ResolveForFilters(ClientContext &context,
                                                           const vector<unique_ptr<Expression>> &filters,
                                                           const MultiFilePushdownInfo &info,
                                                           const unordered_map<column_t, idx_t> &partition_columns) {
	return ResolveAll(context);
}

shared_ptr<const HMSPartitionPlan> HMSPartitionCache::CompletePlan(ClientContext &context) {
	auto generation = catalog.GetCacheGeneration();
	{
		lock_guard<mutex> guard(lock);
		if (complete_plan && IsFresh(complete_plan_loaded_at, complete_plan_generation, generation)) {
			return complete_plan;
		}
	}
	auto plan = FetchComplete(context, generation);
	ConfigureStorage(context, *plan);
	return plan;
}

} // namespace duckdb
