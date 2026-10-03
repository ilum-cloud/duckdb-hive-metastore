#include "storage/hms_csv_options.hpp"

#include "hms_api.hpp"
#include "hms_constants.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//! A table option looked up the way Spark does, ignoring case: it keeps them as the user spelled them
static bool FindOption(const map<string, string> &parameters, std::initializer_list<const char *> keys, string &value) {
	for (auto key : keys) {
		for (auto &entry : parameters) {
			if (StringUtil::CIEquals(entry.first, key)) {
				value = entry.second;
				return true;
			}
		}
	}
	return false;
}

//! Spark's delimiter option with its escapes decoded (CSVExprUtils.toDelimiterStr); false if Spark would reject it
static bool DecodeSparkDelimiter(const string &value, string &result) {
	result.clear();
	for (idx_t i = 0; i < value.size(); i++) {
		if (value[i] != '\\') {
			result += value[i];
			continue;
		}
		if (value.compare(i, 6, "\\u0000") == 0) {
			result += '\0';
			i += 5;
			continue;
		}
		if (i + 1 == value.size()) {
			return false;
		}
		switch (value[++i]) {
		case 't':
			result += '\t';
			break;
		case 'r':
			result += '\r';
			break;
		case 'b':
			result += '\b';
			break;
		case 'f':
			result += '\f';
			break;
		case '"':
		case '\'':
		case '\\':
			result += value[i];
			break;
		default:
			return false;
		}
	}
	return !result.empty();
}

//! Spark's quote, escape and comment options: one character, where an empty value or \u0000 turns the feature off
static string SparkCharOption(const string &value) {
	if (value.empty() || value == "\\u0000" || value[0] == '\0') {
		return string();
	}
	return value;
}

bool TranslateJavaDateFormat(const string &pattern, string &result) {
	static const pair<const char *, const char *> FIELDS[] = {
	    {"yyyy", "%Y"}, {"uuuu", "%Y"}, {"yy", "%y"},     {"MMMM", "%B"},      {"MMM", "%b"}, {"MM", "%m"},
	    {"M", "%-m"},   {"dd", "%d"},   {"d", "%-d"},     {"EEEE", "%A"},      {"EEE", "%a"}, {"HH", "%H"},
	    {"H", "%-H"},   {"hh", "%I"},   {"h", "%-I"},     {"mm", "%M"},        {"m", "%-M"},  {"ss", "%S"},
	    {"s", "%-S"},   {"SSS", "%g"},  {"SSSSSS", "%f"}, {"SSSSSSSSS", "%n"}, {"a", "%p"},   {"XXX", "%z"},
	    {"XX", "%z"},   {"xxx", "%z"},  {"xx", "%z"},     {"Z", "%z"},         {"ZZ", "%z"}};
	result.clear();
	idx_t i = 0;
	while (i < pattern.size()) {
		auto c = pattern[i];
		if (c == '\'') {
			// Quoted text; two quotes stand for one
			auto end = pattern.find('\'', i + 1);
			if (end == string::npos) {
				return false;
			}
			auto text = end == i + 1 ? string("'") : pattern.substr(i + 1, end - i - 1);
			for (auto ch : text) {
				result += ch == '%' ? string("%%") : string(1, ch);
			}
			i = end + 1;
			continue;
		}
		if (StringUtil::CharacterIsAlpha(c)) {
			idx_t length = 1;
			while (i + length < pattern.size() && pattern[i + length] == c) {
				length++;
			}
			auto field = pattern.substr(i, length);
			const char *format = nullptr;
			for (auto &entry : FIELDS) {
				if (field == entry.first) {
					format = entry.second;
					break;
				}
			}
			if (!format) {
				return false;
			}
			result += format;
			i += length;
			continue;
		}
		if (c == '[' || c == ']' || c == '{' || c == '}' || c == '#') {
			// Optional sections and reserved characters
			return false;
		}
		result += c == '%' ? string("%%") : string(1, c);
		i++;
	}
	return !result.empty();
}

//! The dialect Spark wrote a CSV table's files in, from the options of the table (CSVOptions and their defaults)
static void AddSparkCSVOptions(ClientContext &context, const HMSAPITable &table,
                               case_insensitive_map_t<Value> &options) {
	auto &parameters = table.serde_parameters;
	string value;
	string delimiter = ",";
	if (FindOption(parameters, {"sep", "delimiter"}, value)) {
		if (!DecodeSparkDelimiter(value, delimiter)) {
			delimiter = value;
		}
	}
	options["delim"] = Value(delimiter);
	options["header"] =
	    Value::BOOLEAN(FindOption(parameters, {"header"}, value) && StringUtil::CIEquals(value, "true"));
	options["quote"] = Value(FindOption(parameters, {"quote"}, value) ? SparkCharOption(value) : string("\""));
	options["escape"] = Value(FindOption(parameters, {"escape"}, value) ? SparkCharOption(value) : string("\\"));
	if (FindOption(parameters, {"comment"}, value) && !SparkCharOption(value).empty()) {
		options["comment"] = Value(value);
	}
	// Spark reads a field equal to nullValue (by default the empty field) as NULL, and a quoted empty field as the
	// empty string
	options["nullstr"] = Value(FindOption(parameters, {"nullValue"}, value) ? value : string());
	options["allow_quoted_nulls"] = Value::BOOLEAN(false);
	if (FindOption(parameters, {"emptyValue"}, value) && !value.empty()) {
		DUCKDB_LOG_WARNING(context,
		                   "hive_metastore: table \"%s\" sets the CSV option emptyValue, which is not supported: a "
		                   "quoted empty field reads as the empty string",
		                   table.name);
	}
	if (FindOption(parameters, {"encoding", "charset"}, value) && !StringUtil::CIEquals(value, "utf-8") &&
	    !StringUtil::CIEquals(value, "utf8")) {
		auto encoding = StringUtil::Lower(value);
		if (encoding == "iso-8859-1" || encoding == "latin1") {
			encoding = "latin-1";
		}
		options["encoding"] = Value(encoding);
	}
	if (FindOption(parameters, {"lineSep"}, value) && value != "\n" && value != "\r" && value != "\r\n") {
		DUCKDB_LOG_WARNING(
		    context,
		    "hive_metastore: table \"%s\" sets the CSV option lineSep to a separator DuckDB cannot read; "
		    "lines are split on \\n, \\r and \\r\\n",
		    table.name);
	}
	static const pair<const char *, const char *> DATE_OPTIONS[] = {{"dateFormat", "dateformat"},
	                                                                {"timestampFormat", "timestampformat"}};
	for (auto &date_option : DATE_OPTIONS) {
		if (!FindOption(parameters, {date_option.first}, value)) {
			continue;
		}
		string format;
		if (TranslateJavaDateFormat(value, format)) {
			options[date_option.second] = Value(format);
		} else {
			DUCKDB_LOG_WARNING(context,
			                   "hive_metastore: table \"%s\" sets the CSV option %s to \"%s\", which has no DuckDB "
			                   "equivalent; values are read as ISO 8601",
			                   table.name, date_option.first, value);
		}
	}
	options["auto_detect"] = Value::BOOLEAN(false);
}

void AddCSVReadOptions(ClientContext &context, const HMSAPITable &table, bool has_spark_schema, bool partitioned,
                       case_insensitive_map_t<Value> &options) {
	// Default Hive delimiter is \001 (Ctrl-A)
	string delim = string(1, hms::constants::DEFAULT_HIVE_DELIMITER);
	auto it = table.serde_parameters.find(hms::serde_param::FIELD_DELIM);
	if (it != table.serde_parameters.end()) {
		delim = it->second;
	}
	bool is_default_hive_delim = (delim == string(1, hms::constants::DEFAULT_HIVE_DELIMITER));

	bool is_spark_csv = false;
	auto csv_provider_it = table.parameters.find(hms::spark_param::PROVIDER);
	if (csv_provider_it != table.parameters.end() && StringUtil::CIEquals(csv_provider_it->second, hms::format::CSV)) {
		is_spark_csv = true;
	}

	// Logic to determine CSV parsing mode:
	// 1. A partitioned Spark CSV table (provider=csv) is read in the dialect its options record.
	// 2. An unpartitioned Spark CSV table, or one with a Spark schema AND a non-default delimiter (e.g. comma), is
	//    likely a compatible CSV table: we enable auto_detect. If the delimiter is default (\x01) on a Spark CSV, we
	//    ignore it to let the sniffer find the real one (likely comma).
	// 3. Otherwise (Standard Hive table, usually LazySimpleSerDe, or any partitioned one), we disable auto_detect and
	//    enforce strict parsing.
	if (partitioned && is_spark_csv) {
		AddSparkCSVOptions(context, table, options);
	} else if (!partitioned && (is_spark_csv || (has_spark_schema && !is_default_hive_delim))) {
		// Spark CSV or compatible (e.g. comma separated)
		// Enable auto_detect to allow sniffing of quotes, headers, etc.
		options["auto_detect"] = Value::BOOLEAN(true);

		if (it != table.serde_parameters.end()) {
			// Set sep if explicitly defined, UNLESS it's the default hive delimiter for a Spark CSV
			// (because Spark CSVs often leave SerDe delim as default \x01 while actual file is comma)
			if (!is_spark_csv || !is_default_hive_delim) {
				options["sep"] = Value(delim);
			}
		}
	} else {
		// Strict Hive behavior (LazySimpleSerDe) or Default Hive
		options["header"] = Value::BOOLEAN(false); // Hive tables usually have no header
		options["sep"] = Value(delim);
		options["quote"] = Value("");  // Disable quoting
		options["escape"] = Value(""); // Disable escaping

		// Explicitly disable auto detection for strict Hive tables
		options["auto_detect"] = Value::BOOLEAN(false);

		if (partitioned) {
			// The lines Hive skips at the top of each file, and the text it writes for NULL (\N unless the table
			// says otherwise). An empty field stays NULL as well, as it reads without a null format.
			string value;
			if (FindOption(table.parameters, {"skip.header.line.count"}, value)) {
				int64_t lines;
				if (TryCast::Operation(string_t(value), lines) && lines > 0) {
					options["skip"] = Value::BIGINT(lines);
				}
			}
			string null_format = "\\N";
			if (!FindOption(table.serde_parameters, {"serialization.null.format"}, null_format)) {
				FindOption(table.parameters, {"serialization.null.format"}, null_format);
			}
			vector<Value> null_strings {Value(null_format)};
			if (!null_format.empty()) {
				null_strings.emplace_back("");
			}
			options["nullstr"] = Value::LIST(LogicalType::VARCHAR, std::move(null_strings));
		}
	}

	// If strict mode is failing, we might want to relax it, but for now let's try with correct delimiters
	options["null_padding"] = Value::BOOLEAN(true);  // Hive treats missing columns as null
	options["ignore_errors"] = Value::BOOLEAN(true); // Best effort
}

} // namespace duckdb
