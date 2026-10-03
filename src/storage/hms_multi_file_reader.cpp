#include "storage/hms_multi_file_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

//! How many files to list for a row estimate when the metastore has no statistics (DuckDB uses the same for globs)
static constexpr idx_t ESTIMATE_FILE_COUNT = 500;
//! The bytes per row DuckDB assumes for Parquet files it has not opened
static constexpr idx_t ESTIMATED_BYTES_PER_ROW = 10;
//! The rows per file DuckDB assumes at least, used when not even the file sizes are known
static constexpr idx_t MIN_ROWS_PER_FILE = 1000;
//! The keys one listing request returns at most (S3 and GCS; Azure returns more)
static constexpr idx_t KEYS_PER_LISTING_REQUEST = 1000;

HMSPartitionFileList::HMSPartitionFileList(ClientContext &context, shared_ptr<const HMSPartitionPlan> plan_p,
                                           vector<idx_t> partition_indexes_p)
    : LazyMultiFileList(&context), context(context), plan(std::move(plan_p)),
      partition_indexes(std::move(partition_indexes_p)) {
}

optional_ptr<const vector<idx_t>> HMSPartitionFileList::PartitionsOwning(const string &path) const {
	// Walk up from the file itself (a partition location can be a single file) to the first partition location
	auto scheme_end = path.find("://");
	idx_t min_length = scheme_end == string::npos ? 1 : scheme_end + 3;
	string location = path;
	while (StringUtil::EndsWith(location, "/")) {
		location.pop_back();
	}
	while (location.size() >= min_length) {
		auto entry = plan->partitions_by_location.find(location);
		if (entry != plan->partitions_by_location.end()) {
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
	auto owners = PartitionsOwning(path);
	return owners && std::find(owners->begin(), owners->end(), partition_index) != owners->end();
}

vector<OpenFileInfo> HMSPartitionFileList::ListPartition(idx_t partition_index) const {
	auto &partition = plan->partitions[partition_index];
	auto &fs = FileSystem::GetFileSystem(context);
	auto list = [&](const string &pattern) {
		auto files = fs.GlobFiles(pattern, FileGlobOptions::ALLOW_EMPTY);
		// A partition nested inside this one's location keeps its own files
		files.erase(std::remove_if(files.begin(), files.end(),
		                           [&](const OpenFileInfo &file) { return !BelongsTo(file.path, partition_index); }),
		            files.end());
		return files;
	};
	auto files = list(partition.scan_location);
	if (files.empty() && !partition.fallback_scan_location.empty()) {
		// Hive writes data files without an extension (`000000_0`), so the format's pattern matches nothing
		files = list(partition.fallback_scan_location);
	}
	if (files.empty() && fs.FileExists(partition.location)) {
		// A partition location that points at a single file rather than a directory
		files.emplace_back(partition.location);
	}
	return files;
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
	if (plan->files_under_table_location.IsValid()) {
		auto files = plan->files_under_table_location.GetIndex();
		cheaper = (files + KEYS_PER_LISTING_REQUEST - 1) / KEYS_PER_LISTING_REQUEST < positions.size();
	} else {
		cheaper = positions.size() * 2 > plan->partitions_under_table_location;
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
		auto owners = PartitionsOwning(file.path);
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

bool HMSPartitionFileList::ExpandNextPath() const {
	if (next_partition >= partition_indexes.size()) {
		return false;
	}
	if (!listing_decided) {
		listing_decided = true;
		ListTogetherIfCheaper();
	}
	auto position = next_partition++;
	auto partition_index = partition_indexes[position];
	auto &partition = plan->partitions[partition_index];

	vector<OpenFileInfo> files;
	if (!is_listed_together.empty() && is_listed_together[position]) {
		files = SelectDataFiles(partition, std::move(listed_together[position]));
	} else {
		files = ListPartition(partition_index);
	}
	std::sort(files.begin(), files.end());
	for (auto &file : files) {
		// Keep whatever the glob attached (file size, modification time, etag: the Parquet metadata cache uses them)
		// and record which partition the file belongs to
		auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		if (file.extended_info) {
			extended_info->options = file.extended_info->options;
		}
		extended_info->options[HMS_PARTITION_INDEX_KEY] = Value::UBIGINT(partition_index);
		file.extended_info = std::move(extended_info);
		expanded_files.push_back(std::move(file));
	}
	return true;
}

vector<OpenFileInfo> HMSPartitionFileList::GetDisplayFileList(optional_idx max_files) const {
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

// Replaces references to partition columns with the value this partition has for them, so the filter can be folded
static void ConvertPartitionColumnsToConstants(unique_ptr<Expression> &expr,
                                               const unordered_map<column_t, Value> &partition_values,
                                               idx_t table_index) {
	if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
		if (table_index != bound_colref.binding.table_index) {
			return;
		}
		auto lookup = partition_values.find(bound_colref.binding.column_index);
		if (lookup != partition_values.end()) {
			expr = make_uniq<BoundConstantExpression>(lookup->second);
		}
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		ConvertPartitionColumnsToConstants(child, partition_values, table_index);
	});
}

//! Which of the columns the scan produces are partition columns: scan column index -> index into plan.names
static unordered_map<column_t, idx_t> PartitionColumnIds(const MultiFilePushdownInfo &info,
                                                         const HMSPartitionPlan &plan) {
	unordered_map<column_t, idx_t> result;
	for (idx_t i = 0; i < info.column_ids.size(); i++) {
		auto column_id = info.column_ids[i];
		if (IsVirtualColumn(column_id) || column_id >= info.column_names.size()) {
			continue;
		}
		for (idx_t k = 0; k < plan.names.size(); k++) {
			if (StringUtil::CIEquals(info.column_names[column_id], plan.names[k])) {
				result[i] = k;
				break;
			}
		}
	}
	return result;
}

enum class PartitionFilterResult : uint8_t {
	//! The filter needs more than the partition values given, so it can only run over the rows
	NEEDS_ROWS,
	PASSES,
	FAILS
};

//! Evaluates a filter with the partition columns replaced by the values given
static PartitionFilterResult EvaluatePartitionFilter(ClientContext &context, const Expression &filter,
                                                     const unordered_map<column_t, Value> &values, idx_t table_index) {
	auto filter_copy = filter.Copy();
	ConvertPartitionColumnsToConstants(filter_copy, values, table_index);
	Value result;
	if (!filter_copy->IsScalar() || !filter_copy->IsFoldable() ||
	    !ExpressionExecutor::TryEvaluateScalar(context, *filter_copy, result)) {
		return PartitionFilterResult::NEEDS_ROWS;
	}
	return result.IsNull() || !result.GetValue<bool>() ? PartitionFilterResult::FAILS : PartitionFilterResult::PASSES;
}

unique_ptr<MultiFileList> HMSPartitionFileList::ComplexFilterPushdown(ClientContext &context_p,
                                                                      const MultiFileOptions &options,
                                                                      MultiFilePushdownInfo &info,
                                                                      vector<unique_ptr<Expression>> &filters) const {
	if (filters.empty() || plan->names.empty()) {
		return nullptr;
	}
	auto partition_column_ids = PartitionColumnIds(info, *plan);
	if (partition_column_ids.empty()) {
		return nullptr;
	}

	vector<idx_t> kept;
	vector<bool> have_preserved_filter(filters.size(), false);
	vector<unique_ptr<Expression>> preserved_filters;
	unordered_set<idx_t> filters_applied;
	for (auto partition_index : partition_indexes) {
		auto &partition = plan->partitions[partition_index];
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
				if (filters_applied.find(i) == filters_applied.end()) {
					info.extra_info.file_filters += filters[i]->ToString();
					filters_applied.insert(i);
				}
			}
		}
		if (!prune) {
			kept.push_back(partition_index);
		}
	}
	if (kept.size() == partition_indexes.size()) {
		return nullptr;
	}
	info.extra_info.total_files = partition_indexes.size();
	info.extra_info.filtered_files = kept.size();
	filters = std::move(preserved_filters);
	return make_uniq<HMSPartitionFileList>(context_p, plan, std::move(kept));
}

HMSMultiFileReader::HMSMultiFileReader(shared_ptr<const HMSPartitionPlan> plan_p, vector<string> column_names_p,
                                       vector<LogicalType> column_types_p, bool bind_to_table_columns_p)
    : plan(std::move(plan_p)), column_names(std::move(column_names_p)), column_types(std::move(column_types_p)),
      bind_to_table_columns(bind_to_table_columns_p) {
	D_ASSERT(column_names.size() == column_types.size());
}

unique_ptr<MultiFileReader> HMSMultiFileReader::CreateInstance(const TableFunction &table_function) {
	if (!table_function.function_info) {
		throw InternalException("HMSMultiFileReader: the scan function carries no partition plan");
	}
	auto &info = table_function.function_info->Cast<HMSScanFunctionInfo>();
	return make_uniq<HMSMultiFileReader>(info.plan, info.column_names, info.column_types, info.bind_to_table_columns);
}

unique_ptr<MultiFileReader> HMSMultiFileReader::Copy() const {
	return make_uniq<HMSMultiFileReader>(plan, column_names, column_types, bind_to_table_columns);
}

shared_ptr<MultiFileList> HMSMultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                             const FileGlobInput &glob_input) {
	if (plan->source != HMSPartitionSource::HMS) {
		return MultiFileReader::CreateFileList(context, paths, glob_input);
	}
	vector<idx_t> partition_indexes;
	for (idx_t i = 0; i < plan->partitions.size(); i++) {
		partition_indexes.push_back(i);
	}
	return make_shared_ptr<HMSPartitionFileList>(context, plan, std::move(partition_indexes));
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
	for (idx_t i = 0; i < plan->names.size(); i++) {
		bool found = false;
		for (idx_t col = 0; col < names.size(); col++) {
			if (StringUtil::CIEquals(names[col], plan->names[i])) {
				return_types[col] = plan->types[i];
				found = true;
				break;
			}
		}
		if (!found) {
			names.push_back(plan->names[i]);
			return_types.push_back(plan->types[i]);
		}
	}
}

unique_ptr<MultiFileList> HMSMultiFileReader::ComplexFilterPushdown(ClientContext &context, MultiFileList &files,
                                                                    const MultiFileOptions &options,
                                                                    MultiFilePushdownInfo &info,
                                                                    vector<unique_ptr<Expression>> &filters) {
	if (plan->source != HMSPartitionSource::PATH) {
		// The partition list prunes on the values the metastore holds
		return MultiFileReader::ComplexFilterPushdown(context, files, options, info, filters);
	}
	// Path mode: drop the files whose key=value path values cannot satisfy the filters, before they are opened. A value
	// the path does not carry may come from the file itself, so that says nothing, and every filter still runs over
	// the rows of the files kept.
	if (filters.empty() || plan->names.empty()) {
		return nullptr;
	}
	auto partition_column_ids = PartitionColumnIds(info, *plan);
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
			auto path_value = path_values.find(plan->names[entry.second]);
			if (path_value == path_values.end()) {
				continue;
			}
			try {
				values[entry.first] = HivePartitioning::GetValue(context, plan->names[entry.second], path_value->second,
				                                                 plan->types[entry.second]);
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
	present.assign(plan->names.size(), true);
	if (plan->source == HMSPartitionSource::HMS) {
		if (reader.file.extended_info) {
			auto entry = reader.file.extended_info->options.find(HMS_PARTITION_INDEX_KEY);
			if (entry != reader.file.extended_info->options.end()) {
				auto partition_index = entry->second.GetValue<uint64_t>();
				if (partition_index < plan->partitions.size()) {
					return plan->partitions[partition_index].values;
				}
			}
		}
		throw InternalException("HMSMultiFileReader: file \"%s\" does not belong to a partition", reader.GetFileName());
	}
	// Path mode: take what the file path encodes. A value it does not carry comes from the file, which some writers
	// store partition values in, and is NULL if the file does not have the column either.
	auto path_values = HivePartitioning::Parse(reader.GetFileName());
	vector<Value> values;
	for (idx_t i = 0; i < plan->names.size(); i++) {
		auto entry = path_values.find(plan->names[i]);
		if (entry == path_values.end()) {
			present[i] = false;
			values.push_back(Value(plan->types[i]));
			continue;
		}
		values.push_back(HivePartitioning::GetValue(context, plan->names[i], entry->second, plan->types[i]));
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
	if (plan->names.empty() || !reader_data.reader) {
		return;
	}
	vector<bool> present;
	auto values = ValuesForFile(context, *reader_data.reader, present);
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto column_id = global_column_ids[i].GetPrimaryIndex();
		if (IsVirtualColumn(column_id) || column_id >= global_columns.size()) {
			continue;
		}
		for (idx_t k = 0; k < plan->names.size(); k++) {
			if (StringUtil::CIEquals(global_columns[column_id].name, plan->names[k])) {
				if (present[k]) {
					reader_data.constant_map.Add(MultiFileGlobalIndex(i), values[k]);
				}
				break;
			}
		}
	}
}

} // namespace duckdb
