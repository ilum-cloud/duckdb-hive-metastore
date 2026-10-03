//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_partition_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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

} // namespace duckdb
