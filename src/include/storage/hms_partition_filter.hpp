//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_partition_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/hms_partition_options.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class ClientContext;
struct MultiFilePushdownInfo;

//! Which of the columns a scan produces are partition columns: scan column index -> index into `partition_names`
unordered_map<column_t, idx_t> PartitionColumnIds(const MultiFilePushdownInfo &info,
                                                  const vector<string> &partition_names);

enum class PartitionFilterResult : uint8_t {
	//! The filter needs more than the partition values given, so it can only run over the rows
	NEEDS_ROWS,
	PASSES,
	FAILS
};

//! Evaluates a filter with the partition columns replaced by the values given (scan column index -> value)
PartitionFilterResult EvaluatePartitionFilter(ClientContext &context, const Expression &filter,
                                              const unordered_map<column_t, Value> &values, idx_t table_index);

//! Whether the filter references columns of this scan and all of them are partition columns
bool ReferencesOnlyPartitionColumns(const Expression &filter, const unordered_map<column_t, idx_t> &partition_columns,
                                    idx_t table_index);

//! A metastore filter (the argument of get_partitions_by_filter) for some of a scan's filters
struct HMSFilterTranslation {
	//! Empty when none of the filters can be sent
	string filter;
	//! Why filters were left out: when none could be sent, or when filters on excluded keys were
	string reason;
	//! The keys of types other than string the filter compares. A Hive 3.1 metastore evaluates filters on those only
	//! in its direct SQL, which some databases fail: Postgres compares a date key with the literal bound as text.
	vector<idx_t> typed_keys;
};

//! Translates the filters a scan received into a Hive Metastore 3.1 partition filter that selects at least every
//! partition the filters accept: no partition that could hold a matching row is ever left out. Only forms the metastore
//! evaluates exactly as DuckDB does, or as a superset, are sent; the rest are left out, which only widens the
//! selection. `partition_columns` maps scan column indexes to indexes into `key_names` / `key_types` (the metastore's
//! partition key names and type strings). Filters on the keys set in `excluded_keys`, which the metastore refused,
//! are left out.
HMSFilterTranslation TranslatePartitionFilters(const vector<unique_ptr<Expression>> &filters,
                                               const unordered_map<column_t, idx_t> &partition_columns,
                                               idx_t table_index, const vector<string> &key_names,
                                               const vector<string> &key_types, HMSPartitionFilterPushdown mode,
                                               const vector<bool> &excluded_keys);

} // namespace duckdb
