#include "storage/hms_multi_file_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

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

HMSMultiFileReader::HMSMultiFileReader(shared_ptr<const HMSPartitionPlan> plan_p) : plan(std::move(plan_p)) {
}

unique_ptr<MultiFileReader> HMSMultiFileReader::CreateInstance(const TableFunction &table_function) {
	if (!table_function.function_info) {
		throw InternalException("HMSMultiFileReader: the scan function carries no partition plan");
	}
	return make_uniq<HMSMultiFileReader>(table_function.function_info->Cast<HMSScanFunctionInfo>().plan);
}

unique_ptr<MultiFileReader> HMSMultiFileReader::Copy() const {
	return make_uniq<HMSMultiFileReader>(plan);
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

void HMSMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                                     vector<string> &names, MultiFileReaderBindData &bind_data) {
	// Partition values come from the metastore (or, in path mode, from this reader), never from DuckDB's hive
	// partitioning, which requires key=value directories and rejects anything else
	options.auto_detect_hive_partitioning = false;
	options.hive_partitioning = false;
	options.hive_types_schema.clear();
	MultiFileReader::BindOptions(options, files, return_types, names, bind_data);

	// The partition columns are not in the data files, so add them here, in the order the metastore declares them
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
