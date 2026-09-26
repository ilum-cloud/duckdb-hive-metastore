//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/hms_partition_plan.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! Key under which a file carries the partition it belongs to (an index into HMSPartitionPlan::partitions)
static constexpr const char *HMS_PARTITION_INDEX_KEY = "hms_partition_index";

//! Carries the partition plan from the table entry to the multi-file reader DuckDB creates while binding the scan
struct HMSScanFunctionInfo : public TableFunctionInfo {
	explicit HMSScanFunctionInfo(shared_ptr<const HMSPartitionPlan> plan_p) : plan(std::move(plan_p)) {
	}

	shared_ptr<const HMSPartitionPlan> plan;
};

//! The files of one partitioned table, one partition at a time. Each partition is globbed at its own location, so
//! partitions stored outside the table location are read as well, and every file remembers which partition it came
//! from.
class HMSPartitionFileList : public LazyMultiFileList {
public:
	HMSPartitionFileList(ClientContext &context, shared_ptr<const HMSPartitionPlan> plan,
	                     vector<idx_t> partition_indexes);

	//! Drops whole partitions whose metastore values cannot satisfy the filters, before their locations are listed
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) const override;
	//! Shows the partition locations rather than every expanded file
	vector<OpenFileInfo> GetDisplayFileList(optional_idx max_files = optional_idx()) const override;

protected:
	bool ExpandNextPath() const override;

private:
	ClientContext &context;
	shared_ptr<const HMSPartitionPlan> plan;
	//! The partitions still to scan, as indexes into plan->partitions
	vector<idx_t> partition_indexes;
	mutable idx_t next_partition = 0;
};

//! Scans a partitioned Hive Metastore table: takes the files from the partition locations the metastore records and
//! fills the partition columns with the values it records for them. It is grafted onto the regular Parquet scan, so
//! reading, the metadata cache and filter pushdown stay the stock implementations.
class HMSMultiFileReader : public MultiFileReader {
public:
	explicit HMSMultiFileReader(shared_ptr<const HMSPartitionPlan> plan);

	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table_function);

	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;
	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<string> &names, MultiFileReaderBindData &bind_data) override;
	void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
	                  const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
	                  const vector<ColumnIndex> &global_column_ids, ClientContext &context,
	                  optional_ptr<MultiFileReaderGlobalState> global_state) override;
	unique_ptr<MultiFileReader> Copy() const override;

private:
	//! The partition values for one file, in plan->names order
	vector<Value> ValuesForFile(ClientContext &context, const BaseFileReader &reader) const;

	shared_ptr<const HMSPartitionPlan> plan;
};

} // namespace duckdb
