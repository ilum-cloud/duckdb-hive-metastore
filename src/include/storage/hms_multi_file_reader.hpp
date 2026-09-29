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

//! Carries what the multi-file reader DuckDB creates while binding the scan needs from the table entry: the partition
//! plan, and the entry's columns, which the scan must produce in exactly that order
struct HMSScanFunctionInfo : public TableFunctionInfo {
	HMSScanFunctionInfo(shared_ptr<const HMSPartitionPlan> plan_p, vector<string> column_names_p,
	                    vector<LogicalType> column_types_p)
	    : plan(std::move(plan_p)), column_names(std::move(column_names_p)), column_types(std::move(column_types_p)) {
	}

	shared_ptr<const HMSPartitionPlan> plan;
	vector<string> column_names;
	vector<LogicalType> column_types;
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
	//! Estimates the rows left to scan from the metastore's statistics, or from the sizes of the data files
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context) const override;

protected:
	bool ExpandNextPath() const override;

private:
	//! Lists the partitions under the table location together, with one listing of that location, when that takes
	//! fewer requests than listing them one by one
	void ListTogetherIfCheaper() const;
	//! The data files of one partition, from its own listing
	vector<OpenFileInfo> ListPartition(idx_t partition_index) const;
	//! Whether a file belongs to a partition: it does to the partitions at the deepest location containing it
	bool BelongsTo(const string &path, idx_t partition_index) const;
	//! The partitions at the deepest location containing a file, or nullptr if no partition contains it
	optional_ptr<const vector<idx_t>> PartitionsOwning(const string &path) const;
	//! Keeps the files that match the partition's pattern, else the permissive one, as its two listings would
	vector<OpenFileInfo> SelectDataFiles(const HMSScanPartition &partition, vector<OpenFileInfo> files) const;

	ClientContext &context;
	shared_ptr<const HMSPartitionPlan> plan;
	//! The partitions still to scan, as indexes into plan->partitions
	vector<idx_t> partition_indexes;
	mutable idx_t next_partition = 0;
	mutable bool listing_decided = false;
	//! When the partitions under the table location were listed together: the files of each, by position in
	//! partition_indexes, and which positions were listed that way
	mutable vector<vector<OpenFileInfo>> listed_together;
	mutable vector<bool> is_listed_together;
};

//! Scans a partitioned Hive Metastore table: takes the files from the partition locations the metastore records and
//! fills the partition columns with the values it records for them. The scan produces the columns of the table entry,
//! and every file is matched to them by name. It is grafted onto the regular Parquet scan, so reading, the metadata
//! cache and filter pushdown stay the stock implementations.
class HMSMultiFileReader : public MultiFileReader {
public:
	HMSMultiFileReader(shared_ptr<const HMSPartitionPlan> plan, vector<string> column_names,
	                   vector<LogicalType> column_types);

	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table_function);

	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types, vector<string> &names,
	          MultiFileReaderBindData &bind_data) override;
	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<string> &names, MultiFileReaderBindData &bind_data) override;
	void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
	                  const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
	                  const vector<ColumnIndex> &global_column_ids, ClientContext &context,
	                  optional_ptr<MultiFileReaderGlobalState> global_state) override;
	unique_ptr<MultiFileReader> Copy() const override;

private:
	//! The partition values for one file, in plan->names order. A value the file's path does not carry is not
	//! present: the column is then read from the file itself.
	vector<Value> ValuesForFile(ClientContext &context, const BaseFileReader &reader, vector<bool> &present) const;

	shared_ptr<const HMSPartitionPlan> plan;
	//! The columns of the table entry, partition columns included, in the entry's order
	vector<string> column_names;
	vector<LogicalType> column_types;
};

} // namespace duckdb
