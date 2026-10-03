//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/hms_partition_resolver.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! Key under which a file carries the values of its partition: a STRUCT with one field per partition key
static constexpr const char *HMS_PARTITION_VALUES_KEY = "hms_partition_values";

//! How the partitions of one scan were selected, for EXPLAIN. Set during optimization, read when rendering.
struct HMSScanDiagnostics {
	void Record(const HMSPartitionSelection &selection, idx_t selected);

	mutex lock;
	bool recorded = false;
	HMSPartitionMethod method = HMSPartitionMethod::FULL;
	idx_t selected = 0;
	string hms_filter;
	string reason;
};

//! Carries what the multi-file reader DuckDB creates while binding the scan needs from the table entry: the table's
//! partition metadata cache, and the entry's columns, which the scan must produce in exactly that order
struct HMSScanFunctionInfo : public TableFunctionInfo {
	HMSScanFunctionInfo(shared_ptr<HMSPartitionCache> cache_p, vector<string> column_names_p,
	                    vector<LogicalType> column_types_p, bool bind_to_table_columns_p)
	    : cache(std::move(cache_p)), diagnostics(make_shared_ptr<HMSScanDiagnostics>()),
	      column_names(std::move(column_names_p)), column_types(std::move(column_types_p)),
	      bind_to_table_columns(bind_to_table_columns_p) {
	}

	shared_ptr<HMSPartitionCache> cache;
	shared_ptr<HMSScanDiagnostics> diagnostics;
	vector<string> column_names;
	vector<LogicalType> column_types;
	//! Whether the scan binds to these columns and matches files to them by name (files that name their columns),
	//! rather than binding the format's own way and getting the partition columns appended (CSV)
	bool bind_to_table_columns;
};

//! The files of one partitioned table, one partition at a time. Each partition is globbed at its own location, so
//! partitions stored outside the table location are read as well, and every file carries its partition's values.
//!
//! The partitions are selected lazily: when the optimizer pushes filters on partition columns down, so the metastore
//! can select them, or else on first use. A table with no partition registered reads the files under its location.
class HMSPartitionFileList : public LazyMultiFileList {
public:
	enum class ListingMode : uint8_t {
		//! One listing of the table location when that takes fewer requests (every partition is read)
		TOGETHER_IF_CHEAPER,
		//! Each partition on its own, several at once (a filter selected the partitions)
		PER_PARTITION
	};

	//! Not resolved yet
	HMSPartitionFileList(ClientContext &context, shared_ptr<HMSPartitionCache> cache,
	                     shared_ptr<HMSScanDiagnostics> diagnostics, FileGlobInput glob_input);
	//! Resolved to the given partitions of `plan`
	HMSPartitionFileList(ClientContext &context, shared_ptr<HMSPartitionCache> cache,
	                     shared_ptr<HMSScanDiagnostics> diagnostics, FileGlobInput glob_input,
	                     shared_ptr<const HMSPartitionPlan> plan, vector<idx_t> partition_indexes, ListingMode mode);

	//! Selects the partitions (if not done yet) and drops those whose values cannot satisfy the filters, before their
	//! locations are listed
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) const override;
	//! Shows the partition locations rather than every expanded file; never selects the partitions
	vector<OpenFileInfo> GetDisplayFileList(optional_idx max_files = optional_idx()) const override;
	//! Estimates the rows left to scan from the metastore's statistics, or from the sizes of the data files
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context) const override;
	//! While the scan binds, answers without selecting the partitions: binding happens before the filters are known
	FileExpandResult GetExpandResult() const override;

	//! The scan finished binding: from now on, a question about the files selects the partitions
	void FinishBinding() const;
	//! Whether the table turned out to have no partition registered, so the files under its location are read
	bool ReadsTableLocation() const;

protected:
	bool ExpandNextPath() const override;

private:
	enum class State : uint8_t { UNRESOLVED, PARTITIONS, TABLE_LOCATION };

	//! Selects every partition; the list lock is held
	void ResolveLocked() const;
	void UseSelectionLocked(const HMSPartitionSelection &selection) const;
	//! Lists the partitions under the table location together, with one listing of that location, when that takes
	//! fewer requests than listing them one by one
	void ListTogetherIfCheaper() const;
	//! Lists the partitions from `position` on, several at once, and keeps the results for ExpandNextPath
	void PrefetchFrom(idx_t position) const;
	//! The data files of one partition, from its own listing
	vector<OpenFileInfo> ListPartition(idx_t partition_index) const;
	//! Drops files that belong to a partition nested inside this one's location. A partial plan does not know every
	//! location, so this fetches the complete one when a file lies deeper than the partition's own directory.
	void DropNestedPartitionFiles(idx_t partition_index, vector<OpenFileInfo> &files) const;
	//! Whether a file belongs to a partition: it does to the partitions at the deepest location containing it
	bool BelongsTo(const string &path, idx_t partition_index) const;
	//! The partitions of `plan` at the deepest location containing a file, or nullptr if no partition contains it
	static optional_ptr<const vector<idx_t>> PartitionsOwning(const HMSPartitionPlan &plan, const string &path);
	//! Keeps the files that match the partition's pattern, else the permissive one, as its two listings would
	vector<OpenFileInfo> SelectDataFiles(const HMSScanPartition &partition, vector<OpenFileInfo> files) const;
	//! Records the partition's values on each file and appends them to the expanded files
	void AddPartitionFiles(idx_t partition_index, vector<OpenFileInfo> files) const;

	ClientContext &context;
	shared_ptr<HMSPartitionCache> cache;
	shared_ptr<HMSScanDiagnostics> diagnostics;
	FileGlobInput glob_input;

	mutable State state;
	mutable bool binding;
	mutable shared_ptr<const HMSPartitionPlan> plan;
	//! The partitions still to scan, as indexes into plan->partitions
	mutable vector<idx_t> partition_indexes;
	mutable ListingMode listing_mode = ListingMode::TOGETHER_IF_CHEAPER;
	mutable idx_t next_partition = 0;
	mutable bool listing_decided = false;
	//! When the partitions under the table location were listed together: the files of each, by position in
	//! partition_indexes, and which positions were listed that way
	mutable vector<vector<OpenFileInfo>> listed_together;
	mutable vector<bool> is_listed_together;
	//! Listings done ahead, for positions [prefetched_from, prefetched_from + prefetched.size())
	mutable vector<vector<OpenFileInfo>> prefetched;
	mutable idx_t prefetched_from = 0;
	//! The complete plan, fetched when a partial plan meets a file deeper than its partition's directory
	mutable shared_ptr<const HMSPartitionPlan> ownership;
	//! The files under the table location, when the table has no partition registered
	mutable shared_ptr<MultiFileList> table_location_files;
	mutable MultiFileListScanData table_location_scan;
};

//! Scans a partitioned Hive Metastore table: takes the files from the partition locations the metastore records and
//! fills the partition columns with the values it records for them. The scan produces the columns of the table entry,
//! and every file is matched to them by name. It is grafted onto the regular Parquet, CSV or Avro scan, so reading,
//! the metadata cache and filter pushdown stay the stock implementations.
class HMSMultiFileReader : public MultiFileReader {
public:
	HMSMultiFileReader(shared_ptr<HMSPartitionCache> cache, shared_ptr<HMSScanDiagnostics> diagnostics,
	                   vector<string> column_names, vector<LogicalType> column_types, bool bind_to_table_columns);

	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table_function);

	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types, vector<string> &names,
	          MultiFileReaderBindData &bind_data) override;
	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<string> &names, MultiFileReaderBindData &bind_data) override;
	//! Selects the partitions, or for files under the table location drops those whose key=value path values cannot
	//! satisfy the filters
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, MultiFileList &files,
	                                                const MultiFileOptions &options, MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) override;
	void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
	                  const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
	                  const vector<ColumnIndex> &global_column_ids, ClientContext &context,
	                  optional_ptr<MultiFileReaderGlobalState> global_state) override;
	unique_ptr<MultiFileReader> Copy() const override;

private:
	//! The partition values for one file, in partition key order. A file listed from a partition carries them; for a
	//! file under the table location they come from its key=value path segments, and a value the path does not carry
	//! is not present: the column is then read from the file itself.
	vector<Value> ValuesForFile(ClientContext &context, const BaseFileReader &reader, vector<bool> &present) const;
	//! Drops the files whose key=value path values cannot satisfy the filters (files under the table location)
	unique_ptr<MultiFileList> PrunePathFiles(ClientContext &context, MultiFileList &files, MultiFilePushdownInfo &info,
	                                         vector<unique_ptr<Expression>> &filters) const;

	shared_ptr<HMSPartitionCache> cache;
	shared_ptr<HMSScanDiagnostics> diagnostics;
	//! The columns of the table entry, partition columns included, in the entry's order
	vector<string> column_names;
	vector<LogicalType> column_types;
	bool bind_to_table_columns;
};

} // namespace duckdb
