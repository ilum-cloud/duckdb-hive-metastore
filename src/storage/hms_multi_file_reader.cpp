#include "storage/hms_multi_file_reader.hpp"

#include "storage/hms_partition_filter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

#include <thread>

namespace duckdb {

//! How many files to list for a row estimate when the metastore has no statistics (DuckDB uses the same for globs)
static constexpr idx_t ESTIMATE_FILE_COUNT = 500;
//! The bytes per row DuckDB assumes for Parquet files it has not opened
static constexpr idx_t ESTIMATED_BYTES_PER_ROW = 10;
//! The rows per file DuckDB assumes at least, used when not even the file sizes are known
static constexpr idx_t MIN_ROWS_PER_FILE = 1000;
//! The keys one listing request returns at most (S3 and GCS; Azure returns more)
static constexpr idx_t KEYS_PER_LISTING_REQUEST = 1000;
//! How many partitions are listed at once when a filter selected the partitions
static constexpr idx_t PREFETCH_PARTITIONS = 16;
//! A filter's selection is listed together with one listing of the table location only from this many partitions on:
//! fewer cost a few rounds of parallel listings at most
static constexpr idx_t MIN_PARTITIONS_LISTED_TOGETHER = 256;

void HMSScanDiagnostics::Record(const HMSPartitionSelection &selection, idx_t selected_p) {
	lock_guard<mutex> guard(lock);
	recorded = true;
	method = selection.method;
	selected = selected_p;
	hms_filter = selection.hms_filter;
	reason = selection.reason;
}

InsertionOrderPreservingMap<string> HMSScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	result["Function"] = StringUtil::Upper(input.table_function.name);
	auto &info = input.table_function.function_info->Cast<HMSScanFunctionInfo>();
	if (info.cache->Mode() == HMSPartitionMode::PATH) {
		return result;
	}
	auto &diagnostics = *info.diagnostics;
	lock_guard<mutex> guard(diagnostics.lock);
	if (!diagnostics.recorded) {
		// No filter was about the partition columns alone: the scan selects every partition when it starts
		result["HMS Partitions"] = "all, selected when the scan starts";
		return result;
	}
	if (diagnostics.method == HMSPartitionMethod::PATH) {
		result["HMS Partitions"] = "none registered, the files under the table location are read";
	} else {
		result["HMS Partitions"] =
		    StringUtil::Format("%llu (%s)", diagnostics.selected, HMSPartitionMethodName(diagnostics.method));
	}
	if (!diagnostics.hms_filter.empty()) {
		result["HMS Partition Filter"] = diagnostics.hms_filter;
	}
	if (!diagnostics.reason.empty()) {
		result["HMS Partition Fallback"] = diagnostics.reason;
	}
	return result;
}

static void LogSelection(ClientContext &context, const HMSPartitionSchema &schema,
                         const HMSPartitionSelection &selection, idx_t selected) {
	DUCKDB_LOG_DEBUG(context,
	                 "hive_metastore partitions db=%s table=%s method=%s selected=%d filter=\"%s\" reason=\"%s\"",
	                 schema.database, schema.table, HMSPartitionMethodName(selection.method), selected,
	                 selection.hms_filter, selection.reason);
}

HMSPartitionFileList::HMSPartitionFileList(ClientContext &context, shared_ptr<HMSPartitionCache> cache_p,
                                           shared_ptr<HMSScanDiagnostics> diagnostics_p, FileGlobInput glob_input_p)
    : LazyMultiFileList(&context), context(context), cache(std::move(cache_p)), diagnostics(std::move(diagnostics_p)),
      glob_input(std::move(glob_input_p)), state(State::UNRESOLVED), binding(true) {
}

HMSPartitionFileList::HMSPartitionFileList(ClientContext &context, shared_ptr<HMSPartitionCache> cache_p,
                                           shared_ptr<HMSScanDiagnostics> diagnostics_p, FileGlobInput glob_input_p,
                                           shared_ptr<const HMSPartitionPlan> plan_p, vector<idx_t> partition_indexes_p,
                                           ListingMode mode, optional_idx table_partitions_p)
    : LazyMultiFileList(&context), context(context), cache(std::move(cache_p)), diagnostics(std::move(diagnostics_p)),
      glob_input(std::move(glob_input_p)), state(State::PARTITIONS), binding(false), plan(std::move(plan_p)),
      partition_indexes(std::move(partition_indexes_p)), listing_mode(mode), table_partitions(table_partitions_p) {
}

void HMSPartitionFileList::FinishBinding() const {
	lock_guard<mutex> guard(lock);
	binding = false;
}

bool HMSPartitionFileList::ReadsTableLocation() const {
	lock_guard<mutex> guard(lock);
	return state == State::TABLE_LOCATION;
}

void HMSPartitionFileList::UseSelectionLocked(const HMSPartitionSelection &selection) const {
	if (!selection.plan) {
		// No partition registered: the files under the table location, with values from their key=value paths
		state = State::TABLE_LOCATION;
		table_location_files =
		    make_shared_ptr<GlobMultiFileList>(context, vector<string> {cache->Schema().root_glob}, glob_input);
		table_location_files->InitializeScan(table_location_scan);
		return;
	}
	state = State::PARTITIONS;
	plan = selection.plan;
	partition_indexes.clear();
	for (idx_t i = 0; i < plan->partitions.size(); i++) {
		partition_indexes.push_back(i);
	}
	listing_mode = plan->complete ? ListingMode::TOGETHER_IF_CHEAPER : ListingMode::PER_PARTITION;
}

void HMSPartitionFileList::ResolveLocked() const {
	auto selection = cache->ResolveAll(context);
	auto selected = selection.plan ? selection.plan->partitions.size() : 0;
	diagnostics->Record(selection, selected);
	LogSelection(context, cache->Schema(), selection, selected);
	UseSelectionLocked(selection);
}

optional_ptr<const vector<idx_t>> HMSPartitionFileList::PartitionsOwning(const HMSPartitionPlan &plan,
                                                                         const string &path) {
	// Walk up from the file itself (a partition location can be a single file) to the first partition location
	auto scheme_end = path.find("://");
	idx_t min_length = scheme_end == string::npos ? 1 : scheme_end + 3;
	string location = path;
	while (StringUtil::EndsWith(location, "/")) {
		location.pop_back();
	}
	while (location.size() >= min_length) {
		auto entry = plan.partitions_by_location.find(location);
		if (entry != plan.partitions_by_location.end()) {
			return &entry->second;
		}
		auto slash = location.rfind('/');
		if (slash == string::npos || slash < min_length) {
			break;
		}
		location.resize(slash);
	}
	return nullptr;
}

bool HMSPartitionFileList::BelongsTo(const string &path, idx_t partition_index) const {
	auto owners = PartitionsOwning(*plan, path);
	return owners && std::find(owners->begin(), owners->end(), partition_index) != owners->end();
}

vector<OpenFileInfo> HMSPartitionFileList::ListPartition(idx_t partition_index) const {
	auto &partition = plan->partitions[partition_index];
	auto &fs = FileSystem::GetFileSystem(context);
	// One listing of everything under the location; the format's pattern picks the data files from it, else Hive's
	// pattern for the files it writes without an extension (`000000_0`). Globbing each pattern would list twice.
	auto pattern = partition.fallback_scan_location.empty() ? partition.scan_location : partition.location + "/**";
	auto files = fs.GlobFiles(pattern, FileGlobOptions::ALLOW_EMPTY);
	// A partition nested inside this one's location keeps its own files
	files.erase(std::remove_if(files.begin(), files.end(),
	                           [&](const OpenFileInfo &file) { return !BelongsTo(file.path, partition_index); }),
	            files.end());
	files = SelectDataFiles(partition, std::move(files));
	if (files.empty() && fs.FileExists(partition.location)) {
		// A partition location that points at a single file rather than a directory
		files.emplace_back(partition.location);
	}
	return files;
}

//! Whether a file lies directly in a partition's directory (or is the partition's location itself)
static bool IsDirectChild(const string &path, const string &location) {
	if (path == location) {
		return true;
	}
	auto prefix = location + "/";
	if (!StringUtil::StartsWith(path, prefix)) {
		return false;
	}
	return path.find('/', prefix.size()) == string::npos;
}

void HMSPartitionFileList::DropNestedPartitionFiles(idx_t partition_index, vector<OpenFileInfo> &files) const {
	auto &partition = plan->partitions[partition_index];
	bool deeper = false;
	for (auto &file : files) {
		if (!IsDirectChild(file.path, partition.location)) {
			deeper = true;
			break;
		}
	}
	if (!deeper) {
		return;
	}
	// A subdirectory may be the location of a partition the filter left out, which this partial plan does not know
	if (!ownership) {
		ownership = cache->CompletePlan(context);
		DUCKDB_LOG_DEBUG(
		    context,
		    "hive_metastore partitions db=%s table=%s fetched the complete partition list: partition \"%s\" "
		    "has files below its directory",
		    cache->Schema().database, cache->Schema().table, partition.name);
	}
	files.erase(std::remove_if(files.begin(), files.end(),
	                           [&](const OpenFileInfo &file) {
		                           auto owners = PartitionsOwning(*ownership, file.path);
		                           if (!owners || owners->empty()) {
			                           return false;
		                           }
		                           return ownership->partitions[owners->front()].location != partition.location;
	                           }),
	            files.end());
}

//! The file name part of a partition's glob pattern: what follows the last slash
static string FileNamePattern(const string &pattern) {
	auto slash = pattern.rfind('/');
	return slash == string::npos ? pattern : pattern.substr(slash + 1);
}

vector<OpenFileInfo> HMSPartitionFileList::SelectDataFiles(const HMSScanPartition &partition,
                                                           vector<OpenFileInfo> files) const {
	auto select = [&](const string &glob_pattern) {
		auto pattern = FileNamePattern(glob_pattern);
		vector<OpenFileInfo> result;
		for (auto &file : files) {
			auto slash = file.path.rfind('/');
			auto name = slash == string::npos ? file.path : file.path.substr(slash + 1);
			if (!name.empty() && Glob(name.c_str(), name.size(), pattern.c_str(), pattern.size())) {
				result.push_back(file);
			}
		}
		return result;
	};
	auto result = select(partition.scan_location);
	if (result.empty() && !partition.fallback_scan_location.empty()) {
		result = select(partition.fallback_scan_location);
	}
	return result;
}

void HMSPartitionFileList::ListTogetherIfCheaper() const {
	// Listing each partition costs a request per partition, which adds up to minutes for a full scan of a table with
	// thousands of them. One listing of the table location costs a request per thousand files instead, but also
	// lists the partitions not being read. So list together when that is fewer requests: exactly, when the metastore
	// knows how many files the partitions hold, else when most of the partitions under the location are read.
	vector<idx_t> positions;
	for (idx_t position = 0; position < partition_indexes.size(); position++) {
		if (plan->IsUnderTableLocation(plan->partitions[partition_indexes[position]])) {
			positions.push_back(position);
		}
	}
	if (positions.size() < 2) {
		return;
	}
	bool cheaper;
	if (plan->complete && plan->files_under_table_location.IsValid()) {
		auto files = plan->files_under_table_location.GetIndex();
		cheaper = (files + KEYS_PER_LISTING_REQUEST - 1) / KEYS_PER_LISTING_REQUEST < positions.size();
	} else if (plan->complete) {
		cheaper = positions.size() * 2 > plan->partitions_under_table_location;
	} else {
		// A filter's selection knows only its own partitions: compare with every partition the table has
		cheaper = table_partitions.IsValid() && positions.size() * 2 >= table_partitions.GetIndex();
	}
	if (!cheaper) {
		return;
	}
	unordered_map<idx_t, idx_t> position_of_partition;
	for (auto position : positions) {
		position_of_partition[partition_indexes[position]] = position;
	}
	listed_together.resize(partition_indexes.size());
	is_listed_together.resize(partition_indexes.size(), false);
	for (auto position : positions) {
		is_listed_together[position] = true;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &file : fs.GlobFiles(plan->table_location + "/**", FileGlobOptions::ALLOW_EMPTY)) {
		// Files of partitions not being read, and of directories no partition points at, are left out
		auto owners = PartitionsOwning(*plan, file.path);
		if (!owners) {
			continue;
		}
		for (auto owner : *owners) {
			auto entry = position_of_partition.find(owner);
			if (entry != position_of_partition.end()) {
				listed_together[entry->second].push_back(file);
			}
		}
	}
}

void HMSPartitionFileList::PrefetchFrom(idx_t position) const {
	// List the partitions not listed together with the table location several at once, so a wide selection does not
	// pay one request after another. The scan's other threads wait on the list lock meanwhile, so the work cannot go
	// to the task scheduler; plain threads do it.
	if (context.interrupted) {
		throw InterruptException();
	}
	auto end = MinValue<idx_t>(position + PREFETCH_PARTITIONS, partition_indexes.size());
	auto count = end - position;
	vector<vector<OpenFileInfo>> results(count);
	vector<ErrorData> errors(count);
	auto list = [&](idx_t i) {
		if (!is_listed_together.empty() && is_listed_together[position + i]) {
			return;
		}
		try {
			results[i] = ListPartition(partition_indexes[position + i]);
		} catch (std::exception &ex) {
			errors[i] = ErrorData(ex);
		}
	};
	vector<std::thread> threads;
	idx_t started = 0;
	if (count > 1) {
		try {
			for (; started < count; started++) {
				threads.emplace_back(list, started);
			}
		} catch (std::exception &) {
			// No more threads to be had: list the rest on this one
		}
	}
	for (idx_t i = started; i < count; i++) {
		list(i);
	}
	for (auto &thread : threads) {
		thread.join();
	}
	for (auto &error : errors) {
		if (error.HasError()) {
			error.Throw();
		}
	}
	if (!plan->complete) {
		for (idx_t i = 0; i < count; i++) {
			DropNestedPartitionFiles(partition_indexes[position + i], results[i]);
		}
	}
	prefetched = std::move(results);
	prefetched_from = position;
}

void HMSPartitionFileList::AddPartitionFiles(idx_t partition_index, vector<OpenFileInfo> files) const {
	auto &partition = plan->partitions[partition_index];
	auto &names = cache->Schema().names;
	child_list_t<Value> fields;
	for (idx_t i = 0; i < names.size(); i++) {
		fields.emplace_back(names[i], partition.values[i]);
	}
	auto values = Value::STRUCT(std::move(fields));
	std::sort(files.begin(), files.end());
	for (auto &file : files) {
		// Keep whatever the glob attached (file size, modification time, etag: the Parquet metadata cache uses them)
		// and record the partition's values, so the file carries them wherever the scan copies it
		auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		if (file.extended_info) {
			extended_info->options = file.extended_info->options;
		}
		extended_info->options[HMS_PARTITION_VALUES_KEY] = values;
		file.extended_info = std::move(extended_info);
		expanded_files.push_back(std::move(file));
	}
}

bool HMSPartitionFileList::ExpandNextPath() const {
	if (state == State::UNRESOLVED) {
		ResolveLocked();
	}
	if (state == State::TABLE_LOCATION) {
		OpenFileInfo file;
		if (!table_location_files->Scan(table_location_scan, file)) {
			return false;
		}
		expanded_files.push_back(std::move(file));
		return true;
	}
	if (next_partition >= partition_indexes.size()) {
		return false;
	}
	auto position = next_partition++;
	auto partition_index = partition_indexes[position];

	vector<OpenFileInfo> files;
	if (listing_mode == ListingMode::TOGETHER_IF_CHEAPER && !listing_decided) {
		listing_decided = true;
		ListTogetherIfCheaper();
	}
	if (!is_listed_together.empty() && is_listed_together[position]) {
		files = SelectDataFiles(plan->partitions[partition_index], std::move(listed_together[position]));
		if (!plan->complete) {
			DropNestedPartitionFiles(partition_index, files);
		}
	} else {
		if (position < prefetched_from || position >= prefetched_from + prefetched.size()) {
			PrefetchFrom(position);
		}
		files = std::move(prefetched[position - prefetched_from]);
	}
	AddPartitionFiles(partition_index, std::move(files));
	return true;
}

FileExpandResult HMSPartitionFileList::GetExpandResult() const {
	{
		lock_guard<mutex> guard(lock);
		if (state == State::UNRESOLVED && binding) {
			// Binding asks only whether there is more than one file, before the filters are known. Selecting the
			// partitions now would fetch all of them; the answer only tunes the scan, so assume several.
			return FileExpandResult::MULTIPLE_FILES;
		}
	}
	return LazyMultiFileList::GetExpandResult();
}

vector<OpenFileInfo> HMSPartitionFileList::GetDisplayFileList(optional_idx max_files) const {
	lock_guard<mutex> guard(lock);
	if (state == State::UNRESOLVED) {
		return {OpenFileInfo(cache->Schema().table_location)};
	}
	if (state == State::TABLE_LOCATION) {
		return table_location_files->GetDisplayFileList(max_files);
	}
	vector<OpenFileInfo> result;
	for (auto partition_index : partition_indexes) {
		if (max_files.IsValid() && result.size() >= max_files.GetIndex()) {
			break;
		}
		result.emplace_back(plan->partitions[partition_index].scan_location);
	}
	return result;
}

unique_ptr<NodeStatistics> HMSPartitionFileList::GetCardinality(ClientContext &context_p) const {
	{
		lock_guard<mutex> guard(lock);
		if (state == State::UNRESOLVED) {
			ResolveLocked();
		}
	}
	if (state == State::TABLE_LOCATION) {
		return table_location_files->GetCardinality(context_p);
	}
	// The scan binds to the table's columns without opening a data file, so DuckDB has no file to estimate from: it
	// would report 0 rows for a single file. Estimate here instead, first from the statistics the metastore holds
	// for the partitions left to scan (Hive records the row count, Hive and Spark the size in bytes)
	idx_t row_count = 0;
	idx_t total_size = 0;
	bool all_row_counts = true;
	bool all_sizes = true;
	for (auto partition_index : partition_indexes) {
		auto &partition = plan->partitions[partition_index];
		if (partition.row_count.IsValid()) {
			row_count += partition.row_count.GetIndex();
		} else {
			all_row_counts = false;
		}
		if (partition.total_size.IsValid()) {
			total_size += partition.total_size.GetIndex();
		} else {
			all_sizes = false;
		}
	}
	if (all_row_counts) {
		return make_uniq<NodeStatistics>(row_count);
	}
	idx_t file_count = 0;
	if (!all_sizes) {
		// No statistics: size up the data files instead. List partitions until enough files are known, as DuckDB
		// does for its own globs (the listed files are kept for the scan), and extrapolate to the rest.
		GetFileCount(ESTIMATE_FILE_COUNT);
		lock_guard<mutex> guard(lock);
		total_size = 0;
		bool all_file_sizes = true;
		for (auto &file : expanded_files) {
			if (!file.extended_info) {
				all_file_sizes = false;
				break;
			}
			auto &options = file.extended_info->options;
			auto entry = options.find("file_size");
			if (entry == options.end()) {
				all_file_sizes = false;
				break;
			}
			total_size += entry->second.GetValue<uint64_t>();
		}
		file_count = expanded_files.size();
		if (next_partition > 0 && next_partition < partition_indexes.size()) {
			total_size = total_size * partition_indexes.size() / next_partition;
			file_count = file_count * partition_indexes.size() / next_partition;
		}
		if (!all_file_sizes) {
			return make_uniq<NodeStatistics>(file_count * MIN_ROWS_PER_FILE);
		}
	}
	// The same assumption DuckDB makes for Parquet files it has not opened
	auto estimate = total_size / ESTIMATED_BYTES_PER_ROW;
	if (estimate == 0 && (total_size > 0 || file_count > 0)) {
		estimate = 1;
	}
	return make_uniq<NodeStatistics>(estimate);
}

unique_ptr<MultiFileList> HMSPartitionFileList::ComplexFilterPushdown(ClientContext &context_p,
                                                                      const MultiFileOptions &options,
                                                                      MultiFilePushdownInfo &info,
                                                                      vector<unique_ptr<Expression>> &filters) const {
	auto &schema = cache->Schema();
	if (filters.empty() || schema.names.empty()) {
		return nullptr;
	}
	auto partition_column_ids = PartitionColumnIds(info, schema.names);
	if (partition_column_ids.empty()) {
		return nullptr;
	}

	State current_state;
	shared_ptr<const HMSPartitionPlan> current_plan;
	vector<idx_t> current_indexes;
	{
		lock_guard<mutex> guard(lock);
		current_state = state;
		current_plan = plan;
		current_indexes = partition_indexes;
	}
	if (current_state == State::TABLE_LOCATION) {
		// The reader prunes the files under the table location on their key=value paths
		return nullptr;
	}

	HMSPartitionSelection selection;
	bool resolved_now = false;
	if (current_state == State::UNRESOLVED) {
		// Select the partitions now that the filters are known, if a filter is about partition columns alone. A filter
		// holding a prepared statement parameter is not: the statement is bound again with the value when executed.
		bool usable = false;
		for (auto &filter : filters) {
			if (filter->HasParameter()) {
				return nullptr;
			}
			if (ReferencesOnlyPartitionColumns(*filter, partition_column_ids, info.table_index)) {
				usable = true;
			}
		}
		if (!usable) {
			return nullptr;
		}
		selection = cache->ResolveForFilters(context_p, filters, info, partition_column_ids);
		if (!selection.plan) {
			diagnostics->Record(selection, 0);
			LogSelection(context_p, schema, selection, 0);
			auto result = make_uniq<HMSPartitionFileList>(context_p, cache, diagnostics, glob_input);
			lock_guard<mutex> guard(result->lock);
			result->binding = false;
			result->UseSelectionLocked(selection);
			return std::move(result);
		}
		current_plan = selection.plan;
		current_indexes.clear();
		for (idx_t i = 0; i < current_plan->partitions.size(); i++) {
			current_indexes.push_back(i);
		}
		resolved_now = true;
	}

	// Prune on the values the metastore holds for each partition
	vector<idx_t> kept;
	vector<bool> have_preserved_filter(filters.size(), false);
	vector<unique_ptr<Expression>> preserved_filters;
	unordered_set<idx_t> filters_applied;
	for (auto partition_index : current_indexes) {
		auto &partition = current_plan->partitions[partition_index];
		unordered_map<column_t, Value> partition_values;
		for (auto &entry : partition_column_ids) {
			partition_values[entry.first] = partition.values[entry.second];
		}
		bool prune = false;
		for (idx_t i = 0; i < filters.size(); i++) {
			auto result = EvaluatePartitionFilter(context_p, *filters[i], partition_values, info.table_index);
			if (result == PartitionFilterResult::NEEDS_ROWS) {
				// The filter needs more than the partition columns, so it must still run over the rows
				if (!have_preserved_filter[i]) {
					preserved_filters.push_back(filters[i]->Copy());
					have_preserved_filter[i] = true;
				}
			} else if (result == PartitionFilterResult::FAILS) {
				prune = true;
				if (filters_applied.insert(i).second) {
					if (!info.extra_info.file_filters.empty()) {
						info.extra_info.file_filters += " AND ";
					}
					info.extra_info.file_filters += filters[i]->ToString();
				}
			}
		}
		if (!prune) {
			kept.push_back(partition_index);
		}
	}
	if (resolved_now) {
		diagnostics->Record(selection, kept.size());
		LogSelection(context_p, schema, selection, kept.size());
	}
	if (!resolved_now && kept.size() == current_indexes.size()) {
		return nullptr;
	}
	if (kept.size() < current_indexes.size()) {
		if (current_plan->complete) {
			info.extra_info.total_files = current_plan->partitions.size();
			info.extra_info.filtered_files = kept.size();
		}
		filters = std::move(preserved_filters);
	}
	// Every partition of the table is read: one listing of the table location may be cheapest. A filter selected the
	// partitions: list each of them, unless they are most of the table's partitions (a weak filter), where one listing
	// of the table location takes far fewer requests.
	auto mode = ListingMode::PER_PARTITION;
	optional_idx table_partitions;
	if (current_plan->complete && kept.size() == current_plan->partitions.size()) {
		mode = ListingMode::TOGETHER_IF_CHEAPER;
	} else {
		idx_t under_table_location = 0;
		for (auto partition_index : kept) {
			if (current_plan->IsUnderTableLocation(current_plan->partitions[partition_index])) {
				under_table_location++;
			}
		}
		if (under_table_location >= MIN_PARTITIONS_LISTED_TOGETHER) {
			table_partitions = current_plan->complete ? optional_idx(current_plan->partitions_under_table_location)
			                                          : cache->PartitionCount(context_p);
			if (table_partitions.IsValid() && under_table_location * 2 >= table_partitions.GetIndex()) {
				mode = ListingMode::TOGETHER_IF_CHEAPER;
				DUCKDB_LOG_DEBUG(context_p,
				                 "hive_metastore listing db=%s table=%s the table location, once: %d of its %d "
				                 "partitions are selected",
				                 schema.database, schema.table, under_table_location, table_partitions.GetIndex());
			}
		}
	}
	return make_uniq<HMSPartitionFileList>(context_p, cache, diagnostics, glob_input, current_plan, std::move(kept),
	                                       mode, table_partitions);
}

HMSMultiFileReader::HMSMultiFileReader(shared_ptr<HMSPartitionCache> cache_p,
                                       shared_ptr<HMSScanDiagnostics> diagnostics_p, vector<string> column_names_p,
                                       vector<LogicalType> column_types_p, bool bind_to_table_columns_p)
    : cache(std::move(cache_p)), diagnostics(std::move(diagnostics_p)), column_names(std::move(column_names_p)),
      column_types(std::move(column_types_p)), bind_to_table_columns(bind_to_table_columns_p) {
	D_ASSERT(column_names.size() == column_types.size());
}

unique_ptr<MultiFileReader> HMSMultiFileReader::CreateInstance(const TableFunction &table_function) {
	if (!table_function.function_info) {
		throw InternalException("HMSMultiFileReader: the scan function carries no partition metadata");
	}
	auto &info = table_function.function_info->Cast<HMSScanFunctionInfo>();
	return make_uniq<HMSMultiFileReader>(info.cache, info.diagnostics, info.column_names, info.column_types,
	                                     info.bind_to_table_columns);
}

unique_ptr<MultiFileReader> HMSMultiFileReader::Copy() const {
	return make_uniq<HMSMultiFileReader>(cache, diagnostics, column_names, column_types, bind_to_table_columns);
}

shared_ptr<MultiFileList> HMSMultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                             const FileGlobInput &glob_input) {
	if (cache->Mode() == HMSPartitionMode::PATH) {
		return MultiFileReader::CreateFileList(context, paths, glob_input);
	}
	// The partitions are selected once the filters are known, so the metastore can select them
	return make_shared_ptr<HMSPartitionFileList>(context, cache, diagnostics, glob_input);
}

bool HMSMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                              vector<string> &names, MultiFileReaderBindData &bind_data) {
	// Produce the columns of the table entry, not those of whichever file happens to come first. The catalog hands
	// columns to the scan by position, and the entry's list comes from elsewhere (one file under the table location,
	// or the metastore), so binding on a data file can shift every column when files differ in order or content.
	// Each file is matched to these columns by name instead. A column a file does not have reads as NULL, which is
	// what Hive does for files written before ALTER TABLE ... ADD COLUMNS. Partition columns are part of the list;
	// FinalizeBind fills them with constants, so they are never looked up in a file.
	if (!bind_to_table_columns) {
		// Files that do not name their columns (CSV): the format binds its own way, BindOptions adds the partitions
		return false;
	}
	for (idx_t i = 0; i < column_names.size(); i++) {
		auto column = MultiFileColumnDefinition::CreateFromNameAndType(column_names[i], column_types[i]);
		column.default_expression = make_uniq<ConstantExpression>(Value(column_types[i]));
		bind_data.schema.push_back(std::move(column));
		names.push_back(column_names[i]);
		return_types.push_back(column_types[i]);
	}
	bind_data.mapping = MultiFileColumnMappingMode::BY_NAME;
	return true;
}

void HMSMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                                     vector<string> &names, MultiFileReaderBindData &bind_data) {
	// Partition values come from the metastore (or, in path mode, from this reader), never from DuckDB's hive
	// partitioning, which requires key=value directories and rejects anything else
	options.auto_detect_hive_partitioning = false;
	options.hive_partitioning = false;
	options.hive_types_schema.clear();
	MultiFileReader::BindOptions(options, files, return_types, names, bind_data);

	// When the format bound its own way (CSV), add the partition columns, in the order the metastore declares them,
	// as the table does. Bound to the table's columns, they are there already.
	auto &schema = cache->Schema();
	for (idx_t i = 0; i < schema.names.size(); i++) {
		bool found = false;
		for (idx_t col = 0; col < names.size(); col++) {
			if (StringUtil::CIEquals(names[col], schema.names[i])) {
				return_types[col] = schema.types[i];
				found = true;
				break;
			}
		}
		if (!found) {
			names.push_back(schema.names[i]);
			return_types.push_back(schema.types[i]);
		}
	}
	// Binding is the last step that runs before the filters are known
	auto partition_files = dynamic_cast<HMSPartitionFileList *>(&files);
	if (partition_files) {
		partition_files->FinishBinding();
	}
}

unique_ptr<MultiFileList> HMSMultiFileReader::ComplexFilterPushdown(ClientContext &context, MultiFileList &files,
                                                                    const MultiFileOptions &options,
                                                                    MultiFilePushdownInfo &info,
                                                                    vector<unique_ptr<Expression>> &filters) {
	auto partition_files = dynamic_cast<HMSPartitionFileList *>(&files);
	if (!partition_files) {
		// PARTITION_MODE 'path', or files under the table location pruned before
		return PrunePathFiles(context, files, info, filters);
	}
	auto result = partition_files->ComplexFilterPushdown(context, options, info, filters);
	auto &selected = result ? *result : files;
	auto selected_partition_files = dynamic_cast<HMSPartitionFileList *>(&selected);
	if (selected_partition_files && selected_partition_files->ReadsTableLocation()) {
		auto pruned = PrunePathFiles(context, selected, info, filters);
		if (pruned) {
			return pruned;
		}
	}
	return result;
}

unique_ptr<MultiFileList> HMSMultiFileReader::PrunePathFiles(ClientContext &context, MultiFileList &files,
                                                             MultiFilePushdownInfo &info,
                                                             vector<unique_ptr<Expression>> &filters) const {
	// Drop the files whose key=value path values cannot satisfy the filters, before they are opened. A value the path
	// does not carry may come from the file itself, so that says nothing, and every filter still runs over the rows
	// of the files kept.
	auto &schema = cache->Schema();
	if (filters.empty() || schema.names.empty()) {
		return nullptr;
	}
	auto partition_column_ids = PartitionColumnIds(info, schema.names);
	if (partition_column_ids.empty()) {
		return nullptr;
	}
	auto all_files = files.GetAllFiles();
	vector<OpenFileInfo> kept;
	unordered_set<idx_t> filters_applied;
	for (auto &file : all_files) {
		auto path_values = HivePartitioning::Parse(file.path);
		unordered_map<column_t, Value> values;
		for (auto &entry : partition_column_ids) {
			auto path_value = path_values.find(schema.names[entry.second]);
			if (path_value == path_values.end()) {
				continue;
			}
			try {
				values[entry.first] = HivePartitioning::GetValue(context, schema.names[entry.second],
				                                                 path_value->second, schema.types[entry.second]);
			} catch (std::exception &) {
				// A value that does not convert is reported when the file is read
			}
		}
		bool prune = false;
		for (idx_t i = 0; !values.empty() && i < filters.size(); i++) {
			if (EvaluatePartitionFilter(context, *filters[i], values, info.table_index) ==
			    PartitionFilterResult::FAILS) {
				prune = true;
				if (filters_applied.insert(i).second) {
					if (!info.extra_info.file_filters.empty()) {
						info.extra_info.file_filters += " AND ";
					}
					info.extra_info.file_filters += filters[i]->ToString();
				}
				break;
			}
		}
		if (!prune) {
			kept.push_back(file);
		}
	}
	if (kept.size() == all_files.size()) {
		return nullptr;
	}
	info.extra_info.total_files = all_files.size();
	info.extra_info.filtered_files = kept.size();
	return make_uniq<SimpleMultiFileList>(std::move(kept));
}

vector<Value> HMSMultiFileReader::ValuesForFile(ClientContext &context, const BaseFileReader &reader,
                                                vector<bool> &present) const {
	auto &schema = cache->Schema();
	present.assign(schema.names.size(), true);
	if (reader.file.extended_info) {
		auto entry = reader.file.extended_info->options.find(HMS_PARTITION_VALUES_KEY);
		if (entry != reader.file.extended_info->options.end()) {
			return StructValue::GetChildren(entry->second);
		}
	}
	// A file under the table location: take what its path encodes. A value it does not carry comes from the file,
	// which some writers store partition values in, and is NULL if the file does not have the column either.
	auto path_values = HivePartitioning::Parse(reader.GetFileName());
	vector<Value> values;
	for (idx_t i = 0; i < schema.names.size(); i++) {
		auto entry = path_values.find(schema.names[i]);
		if (entry == path_values.end()) {
			present[i] = false;
			values.push_back(Value(schema.types[i]));
			continue;
		}
		values.push_back(HivePartitioning::GetValue(context, schema.names[i], entry->second, schema.types[i]));
	}
	return values;
}

void HMSMultiFileReader::FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
                                      const MultiFileReaderBindData &options,
                                      const vector<MultiFileColumnDefinition> &global_columns,
                                      const vector<ColumnIndex> &global_column_ids, ClientContext &context,
                                      optional_ptr<MultiFileReaderGlobalState> global_state) {
	MultiFileReader::FinalizeBind(reader_data, file_options, options, global_columns, global_column_ids, context,
	                              global_state);
	auto &schema = cache->Schema();
	if (schema.names.empty() || !reader_data.reader) {
		return;
	}
	vector<bool> present;
	auto values = ValuesForFile(context, *reader_data.reader, present);
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto column_id = global_column_ids[i].GetPrimaryIndex();
		if (IsVirtualColumn(column_id) || column_id >= global_columns.size()) {
			continue;
		}
		for (idx_t k = 0; k < schema.names.size(); k++) {
			if (StringUtil::CIEquals(global_columns[column_id].name, schema.names[k])) {
				if (present[k]) {
					reader_data.constant_map.Add(MultiFileGlobalIndex(i), values[k]);
				}
				break;
			}
		}
	}
}

} // namespace duckdb
