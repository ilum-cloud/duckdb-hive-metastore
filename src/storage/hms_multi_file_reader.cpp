#include "storage/hms_multi_file_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
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

HMSPartitionFileList::HMSPartitionFileList(ClientContext &context, shared_ptr<const HMSPartitionPlan> plan_p,
                                           vector<idx_t> partition_indexes_p)
    : LazyMultiFileList(&context), context(context), plan(std::move(plan_p)),
      partition_indexes(std::move(partition_indexes_p)) {
}

bool HMSPartitionFileList::ExpandNextPath() const {
	if (next_partition >= partition_indexes.size()) {
		return false;
	}
	auto partition_index = partition_indexes[next_partition++];
	auto &partition = plan->partitions[partition_index];

	auto &fs = FileSystem::GetFileSystem(context);
	auto files = fs.GlobFiles(partition.scan_location, FileGlobOptions::ALLOW_EMPTY);
	if (files.empty() && !partition.fallback_scan_location.empty()) {
		// Hive writes data files without an extension (`000000_0`), so the format's pattern matches nothing
		files = fs.GlobFiles(partition.fallback_scan_location, FileGlobOptions::ALLOW_EMPTY);
	}
	if (files.empty() && fs.FileExists(partition.location)) {
		// A partition location that points at a single file rather than a directory
		files.emplace_back(partition.location);
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

unique_ptr<MultiFileList> HMSPartitionFileList::ComplexFilterPushdown(ClientContext &context_p,
                                                                      const MultiFileOptions &options,
                                                                      MultiFilePushdownInfo &info,
                                                                      vector<unique_ptr<Expression>> &filters) const {
	if (filters.empty() || plan->names.empty()) {
		return nullptr;
	}
	// Which of the columns the scan produces are partition columns, and where each one sits in the plan
	unordered_map<column_t, idx_t> partition_column_ids;
	for (idx_t i = 0; i < info.column_ids.size(); i++) {
		auto column_id = info.column_ids[i];
		if (IsVirtualColumn(column_id) || column_id >= info.column_names.size()) {
			continue;
		}
		for (idx_t k = 0; k < plan->names.size(); k++) {
			if (StringUtil::CIEquals(info.column_names[column_id], plan->names[k])) {
				partition_column_ids[i] = k;
				break;
			}
		}
	}
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
			auto filter_copy = filters[i]->Copy();
			ConvertPartitionColumnsToConstants(filter_copy, partition_values, info.table_index);
			Value result;
			if (!filter_copy->IsScalar() || !filter_copy->IsFoldable() ||
			    !ExpressionExecutor::TryEvaluateScalar(context_p, *filter_copy, result)) {
				// The filter needs more than the partition columns, so it must still run over the rows
				if (!have_preserved_filter[i]) {
					preserved_filters.push_back(filters[i]->Copy());
					have_preserved_filter[i] = true;
				}
			} else if (result.IsNull() || !result.GetValue<bool>()) {
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
                                       vector<LogicalType> column_types_p)
    : plan(std::move(plan_p)), column_names(std::move(column_names_p)), column_types(std::move(column_types_p)) {
	D_ASSERT(column_names.size() == column_types.size());
}

unique_ptr<MultiFileReader> HMSMultiFileReader::CreateInstance(const TableFunction &table_function) {
	if (!table_function.function_info) {
		throw InternalException("HMSMultiFileReader: the scan function carries no partition plan");
	}
	auto &info = table_function.function_info->Cast<HMSScanFunctionInfo>();
	return make_uniq<HMSMultiFileReader>(info.plan, info.column_names, info.column_types);
}

unique_ptr<MultiFileReader> HMSMultiFileReader::Copy() const {
	return make_uniq<HMSMultiFileReader>(plan, column_names, column_types);
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
}

vector<Value> HMSMultiFileReader::ValuesForFile(ClientContext &context, const BaseFileReader &reader) const {
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
	// Path mode: read what the file path encodes, and leave the rest NULL instead of failing the query
	auto path_values = HivePartitioning::Parse(reader.GetFileName());
	vector<Value> values;
	for (idx_t i = 0; i < plan->names.size(); i++) {
		auto entry = path_values.find(plan->names[i]);
		if (entry == path_values.end()) {
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
	auto values = ValuesForFile(context, *reader_data.reader);
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto column_id = global_column_ids[i].GetPrimaryIndex();
		if (IsVirtualColumn(column_id) || column_id >= global_columns.size()) {
			continue;
		}
		for (idx_t k = 0; k < plan->names.size(); k++) {
			if (StringUtil::CIEquals(global_columns[column_id].name, plan->names[k])) {
				reader_data.constant_map.Add(MultiFileGlobalIndex(i), values[k]);
				break;
			}
		}
	}
}

} // namespace duckdb
