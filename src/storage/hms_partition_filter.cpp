#include "storage/hms_partition_filter.hpp"

#include <cstring>

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
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

// --- Translation into a Hive Metastore 3.1 partition filter ---------------------------------------------------------
//
// What the metastore does with a filter (Filter.g and MetaStoreDirectSql in Hive 3.1):
// - it evaluates keys typed exactly "string", "date" and the integral types in its database; any other key type
//   (varchar(n), decimal, ...) makes it refuse the filter;
// - integral and date keys are cast and compared by value, with __HIVE_DEFAULT_PARTITION__ as NULL. That is exactly
//   what DuckDB does, comparisons with NULL included;
// - string keys are compared by the database, under its collation. Equality there accepts every value DuckDB's
//   byte-wise equality accepts (a case- or accent-insensitive collation only adds matches, which DuckDB's own filter
//   then removes), but ranges and inequality can disagree: under en_US, 'a' < 'B', bytewise 'B' < 'a'.
// So string ranges, <> and NOT IN are sent only when asked to (PARTITION_FILTER_PUSHDOWN 'all').
//
// Each translated filter selects a superset of the partitions on which DuckDB finds the original TRUE. AND keeps that
// when untranslatable conjuncts are dropped (each only narrows the selection); OR keeps it only when every disjunct is
// translated. NOT is never sent: a superset negated is a subset.

//! Literals at most, over all filters sent, and the size of the filter (metastore databases cap query parameters)
static constexpr idx_t MAX_FILTER_LITERALS = 1000;
static constexpr idx_t MAX_FILTER_LENGTH = 32768;

enum class HMSKeyKind : uint8_t { STRING, INTEGRAL, DATE, UNSUPPORTED };

static HMSKeyKind ClassifyKey(const string &type) {
	if (type == "string") {
		return HMSKeyKind::STRING;
	}
	if (type == "tinyint" || type == "smallint" || type == "int" || type == "bigint") {
		return HMSKeyKind::INTEGRAL;
	}
	if (type == "date") {
		return HMSKeyKind::DATE;
	}
	return HMSKeyKind::UNSUPPORTED;
}

//! Whether the filter grammar reads the key name as an identifier: [A-Za-z0-9][A-Za-z0-9_]*, not a number, not one
//! of its keywords (which it matches case-sensitively)
static bool IsUsableKeyName(const string &name) {
	if (name.empty() || !StringUtil::CharacterIsAlphaNumeric(name[0])) {
		return false;
	}
	bool all_digits = true;
	for (auto c : name) {
		if (!StringUtil::CharacterIsAlphaNumeric(c) && c != '_') {
			return false;
		}
		if (!StringUtil::CharacterIsDigit(c)) {
			all_digits = false;
		}
	}
	if (all_digits) {
		return false;
	}
	static const char *const KEYWORDS[] = {"AND", "OR", "NOT", "LIKE", "IN", "BETWEEN", "CONST", "STRUCT", "date"};
	for (auto keyword : KEYWORDS) {
		if (name == keyword) {
			return false;
		}
	}
	return true;
}

struct HMSFilterTranslator {
	HMSFilterTranslator(const unordered_map<column_t, idx_t> &partition_columns_p, idx_t table_index_p,
	                    const vector<string> &key_names_p, const vector<string> &key_types,
	                    HMSPartitionFilterPushdown mode_p, const vector<bool> &excluded_p)
	    : partition_columns(partition_columns_p), table_index(table_index_p), key_names(key_names_p), mode(mode_p),
	      excluded(excluded_p), used(key_types.size(), false), skipped(key_types.size(), false) {
		for (auto &type : key_types) {
			key_kinds.push_back(ClassifyKey(type));
		}
	}

	const unordered_map<column_t, idx_t> &partition_columns;
	idx_t table_index;
	const vector<string> &key_names;
	vector<HMSKeyKind> key_kinds;
	HMSPartitionFilterPushdown mode;
	const vector<bool> &excluded;
	idx_t literals = 0;
	//! The keys the translated filters compare, and the excluded keys filters were left out for
	vector<bool> used;
	vector<bool> skipped;

	//! The partition key a column reference stands for, if the expression is a plain reference to a usable one
	optional_idx Key(const Expression &expr) {
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return optional_idx();
		}
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		if (colref.binding.table_index != table_index) {
			return optional_idx();
		}
		auto entry = partition_columns.find(colref.binding.column_index);
		if (entry == partition_columns.end() || key_kinds[entry->second] == HMSKeyKind::UNSUPPORTED ||
		    !IsUsableKeyName(key_names[entry->second])) {
			return optional_idx();
		}
		if (entry->second < excluded.size() && excluded[entry->second]) {
			skipped[entry->second] = true;
			return optional_idx();
		}
		return entry->second;
	}

	//! A constant written as the filter grammar reads it for the key's type; false if it cannot be written exactly
	bool Literal(const Expression &expr, idx_t key, string &result) const {
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return false;
		}
		auto &value = expr.Cast<BoundConstantExpression>().value;
		if (value.IsNull()) {
			return false;
		}
		switch (key_kinds[key]) {
		case HMSKeyKind::STRING: {
			if (value.type().id() != LogicalTypeId::VARCHAR) {
				return false;
			}
			auto &str = StringValue::Get(value);
			// The grammar never unescapes, and an empty or marker value means something else to the metastore. Hive
			// also escapes these characters in partition names, which its fallback evaluation compares against.
			if (str.empty() || str == "__HIVE_DEFAULT_PARTITION__") {
				return false;
			}
			for (auto c : str) {
				auto byte = static_cast<unsigned char>(c);
				if (byte < 0x20 || byte == 0x7F || strchr("\"#%'*/:=?\\{[]^", c)) {
					return false;
				}
			}
			result = "\"" + str + "\"";
			return true;
		}
		case HMSKeyKind::INTEGRAL: {
			switch (value.type().id()) {
			case LogicalTypeId::TINYINT:
			case LogicalTypeId::SMALLINT:
			case LogicalTypeId::INTEGER:
			case LogicalTypeId::BIGINT:
				result = std::to_string(value.GetValue<int64_t>());
				return true;
			default:
				return false;
			}
		}
		case HMSKeyKind::DATE: {
			if (value.type().id() != LogicalTypeId::DATE) {
				return false;
			}
			auto date = value.GetValue<date_t>();
			if (!Date::IsFinite(date)) {
				return false;
			}
			int32_t year, month, day;
			Date::Convert(date, year, month, day);
			if (year < 1 || year > 9999) {
				return false;
			}
			result = StringUtil::Format("%04d-%02d-%02d", year, month, day);
			return true;
		}
		default:
			return false;
		}
	}

	//! Whether the key's type compares by value in the metastore (integral, date), or the user accepted the
	//! database's collation for strings
	bool OrderedComparisons(idx_t key) const {
		return key_kinds[key] != HMSKeyKind::STRING || mode == HMSPartitionFilterPushdown::ALL;
	}

	bool Comparison(const BoundComparisonExpression &comparison, string &result) {
		auto type = comparison.GetExpressionType();
		auto key = Key(*comparison.left);
		const Expression *constant = comparison.right.get();
		if (!key.IsValid()) {
			// Written with the constant first: flip the comparison
			key = Key(*comparison.right);
			constant = comparison.left.get();
			if (!key.IsValid()) {
				return false;
			}
			type = FlipComparisonExpression(type);
		}
		string op;
		switch (type) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			op = "=";
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			op = "<>";
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			op = "<";
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			op = "<=";
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			op = ">";
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			op = ">=";
			break;
		default:
			// IS DISTINCT FROM is TRUE for NULL, which <> is not
			return false;
		}
		if (op != "=" && !OrderedComparisons(key.GetIndex())) {
			return false;
		}
		string literal;
		if (!Literal(*constant, key.GetIndex(), literal)) {
			return false;
		}
		literals++;
		used[key.GetIndex()] = true;
		result = key_names[key.GetIndex()] + " " + op + " " + literal;
		return true;
	}

	bool Between(const BoundBetweenExpression &between, string &result) {
		auto key = Key(*between.input);
		if (!key.IsValid() || !OrderedComparisons(key.GetIndex())) {
			return false;
		}
		string lower, upper;
		if (!Literal(*between.lower, key.GetIndex(), lower) || !Literal(*between.upper, key.GetIndex(), upper)) {
			return false;
		}
		literals += 2;
		used[key.GetIndex()] = true;
		auto &name = key_names[key.GetIndex()];
		if (between.lower_inclusive && between.upper_inclusive) {
			result = name + " BETWEEN " + lower + " AND " + upper;
		} else {
			result = "(" + name + (between.lower_inclusive ? " >= " : " > ") + lower + " AND " + name +
			         (between.upper_inclusive ? " <= " : " < ") + upper + ")";
		}
		return true;
	}

	bool In(const BoundOperatorExpression &op, string &result) {
		auto negated = op.GetExpressionType() == ExpressionType::COMPARE_NOT_IN;
		if (op.children.size() < 2) {
			return false;
		}
		auto key = Key(*op.children[0]);
		if (!key.IsValid() || (negated && !OrderedComparisons(key.GetIndex()))) {
			return false;
		}
		auto &name = key_names[key.GetIndex()];
		auto kind = key_kinds[key.GetIndex()];
		auto count = op.children.size() - 1;
		if (literals + count > MAX_FILTER_LITERALS) {
			// Too many values to send one by one. For keys compared by value, the range they span still selects a
			// superset; a NOT IN or a string list cannot be widened that way.
			if (negated || kind == HMSKeyKind::STRING) {
				return false;
			}
			Value min, max;
			for (idx_t i = 1; i < op.children.size(); i++) {
				if (op.children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					return false;
				}
				auto &value = op.children[i]->Cast<BoundConstantExpression>().value;
				if (value.IsNull()) {
					return false;
				}
				if (min.IsNull() || value < min) {
					min = value;
				}
				if (max.IsNull() || value > max) {
					max = value;
				}
			}
			string lower, upper;
			if (!Literal(BoundConstantExpression(min), key.GetIndex(), lower) ||
			    !Literal(BoundConstantExpression(max), key.GetIndex(), upper)) {
				return false;
			}
			literals += 2;
			used[key.GetIndex()] = true;
			result = name + " BETWEEN " + lower + " AND " + upper;
			return true;
		}
		string list;
		for (idx_t i = 1; i < op.children.size(); i++) {
			string literal;
			if (!Literal(*op.children[i], key.GetIndex(), literal)) {
				return false;
			}
			list += (i > 1 ? ", " : "") + literal;
		}
		literals += count;
		used[key.GetIndex()] = true;
		result = "(" + name + ")" + (negated ? " NOT IN (" : " IN (") + list + ")";
		return true;
	}

	bool Conjunction(const BoundConjunctionExpression &conjunction, string &result) {
		auto is_and = conjunction.GetExpressionType() == ExpressionType::CONJUNCTION_AND;
		vector<string> parts;
		for (auto &child : conjunction.children) {
			string part;
			if (Translate(*child, part)) {
				parts.push_back(std::move(part));
			} else if (!is_and) {
				// A disjunct that cannot be sent would have to select every partition
				return false;
			}
		}
		if (parts.empty()) {
			return false;
		}
		if (parts.size() == 1) {
			result = parts[0];
			return true;
		}
		result = "(" + StringUtil::Join(parts, is_and ? " AND " : " OR ") + ")";
		return true;
	}

	bool Translate(const Expression &expr, string &result) {
		// A part left out must not count: undo what its translated children added
		auto literals_before = literals;
		auto used_before = used;
		if (TranslateExpression(expr, result)) {
			return true;
		}
		literals = literals_before;
		used = std::move(used_before);
		return false;
	}

	bool TranslateExpression(const Expression &expr, string &result) {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::BOUND_COMPARISON:
			return Comparison(expr.Cast<BoundComparisonExpression>(), result);
		case ExpressionClass::BOUND_BETWEEN:
			return Between(expr.Cast<BoundBetweenExpression>(), result);
		case ExpressionClass::BOUND_OPERATOR:
			if (expr.GetExpressionType() == ExpressionType::COMPARE_IN ||
			    expr.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) {
				return In(expr.Cast<BoundOperatorExpression>(), result);
			}
			return false;
		case ExpressionClass::BOUND_CONJUNCTION:
			return Conjunction(expr.Cast<BoundConjunctionExpression>(), result);
		default:
			// NOT, IS NULL, functions (LIKE among them), casts, CASE, parameters: left to DuckDB
			return false;
		}
	}
};

HMSFilterTranslation TranslatePartitionFilters(const vector<unique_ptr<Expression>> &filters,
                                               const unordered_map<column_t, idx_t> &partition_columns,
                                               idx_t table_index, const vector<string> &key_names,
                                               const vector<string> &key_types, HMSPartitionFilterPushdown mode,
                                               const vector<bool> &excluded_keys) {
	HMSFilterTranslation translation;
	if (mode == HMSPartitionFilterPushdown::OFF) {
		translation.reason = "PARTITION_FILTER_PUSHDOWN is 'off'";
		return translation;
	}
	HMSFilterTranslator translator(partition_columns, table_index, key_names, key_types, mode, excluded_keys);
	vector<string> conjuncts;
	idx_t length = 0;
	for (auto &filter : filters) {
		if (!ReferencesOnlyPartitionColumns(*filter, partition_columns, table_index)) {
			continue;
		}
		auto literals_before = translator.literals;
		auto used_before = translator.used;
		string part;
		if (!translator.Translate(*filter, part)) {
			continue;
		}
		if (translator.literals > MAX_FILTER_LITERALS || length + part.size() > MAX_FILTER_LENGTH) {
			// Leaving the rest of the conjuncts out only widens the selection
			translator.literals = literals_before;
			translator.used = std::move(used_before);
			break;
		}
		length += part.size() + 5;
		conjuncts.push_back(std::move(part));
	}
	vector<string> skipped;
	for (idx_t key = 0; key < key_names.size(); key++) {
		if (translator.used[key] && translator.key_kinds[key] != HMSKeyKind::STRING) {
			translation.typed_keys.push_back(key);
		}
		if (translator.skipped[key]) {
			skipped.push_back(key_names[key]);
		}
	}
	if (!skipped.empty()) {
		translation.reason = "the metastore refused filters on " + StringUtil::Join(skipped, ", ") + " before";
	}
	if (conjuncts.empty()) {
		if (translation.reason.empty()) {
			translation.reason = "no filter on the partition columns can be evaluated by the metastore exactly";
		}
		return translation;
	}
	translation.filter = StringUtil::Join(conjuncts, " AND ");
	return translation;
}

} // namespace duckdb
