#include "storage/hms_partition_filter.hpp"

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

unordered_map<column_t, idx_t> PartitionColumnIds(const MultiFilePushdownInfo &info,
                                                  const vector<string> &partition_names) {
	unordered_map<column_t, idx_t> result;
	for (idx_t i = 0; i < info.column_ids.size(); i++) {
		auto column_id = info.column_ids[i];
		if (IsVirtualColumn(column_id) || column_id >= info.column_names.size()) {
			continue;
		}
		for (idx_t k = 0; k < partition_names.size(); k++) {
			if (StringUtil::CIEquals(info.column_names[column_id], partition_names[k])) {
				result[i] = k;
				break;
			}
		}
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

PartitionFilterResult EvaluatePartitionFilter(ClientContext &context, const Expression &filter,
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

bool ReferencesOnlyPartitionColumns(const Expression &filter, const unordered_map<column_t, idx_t> &partition_columns,
                                    idx_t table_index) {
	bool references_any = false;
	bool only_partition_columns = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(filter, [&](const BoundColumnRefExpression &colref) {
		references_any = true;
		if (colref.binding.table_index != table_index ||
		    partition_columns.find(colref.binding.column_index) == partition_columns.end()) {
			only_partition_columns = false;
		}
	});
	return references_any && only_partition_columns;
}

} // namespace duckdb
